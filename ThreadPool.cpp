#include "ThreadPool.h"
#include <chrono>

ThreadPool::ThreadPool()
{
    printSafe("[ThreadPool] Constructor called.");
}

ThreadPool::~ThreadPool()
{
    shutdownGraceful();
    printSafe("[ThreadPool] Destructor called.");
}

void ThreadPool::initialize(size_t workerCount)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (initialized)
    {
        printSafe("[ThreadPool] Pool is already initialized.");
        return;
    }

    terminated = false;
    paused = false;
    immediateStop = false;

    initialized = true;
    queueTotalSeconds = 0;

    acceptedTasks = 0;
    rejectedTasks = 0;
    completedTasks = 0;
    interruptedTasks = 0;
    droppedQueuedTasks = 0;

    activeTasks = 0;

    createdWorkerThreads = workerCount;

    queueLengthSum = 0;
    queueLengthSamples = 0;

    totalWorkerWaitTime = std::chrono::nanoseconds(0);
    workerWaitEvents = 0;
    totalTaskExecutionTime = std::chrono::nanoseconds(0);

    printSafe("[ThreadPool] Initializing pool with " + std::to_string(workerCount) + " workers...");

    for (size_t i = 0; i < workerCount; ++i)
    {
        workers.emplace_back(&ThreadPool::workerRoutine, this, i + 1);
    }
}

void ThreadPool::shutdownGraceful()
{
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (!initialized)
        {
            return;
        }

        printSafe("[ThreadPool] Graceful shutdown started.");
        terminated = true;
        paused = false;
    }

    cv.notify_all();

    for (std::thread& worker : workers)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }

    workers.clear();
    initialized = false;

    printSafe("[ThreadPool] Graceful shutdown finished.");
}

void ThreadPool::shutdownImmediate()
{
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (!initialized)
        {
            return;
        }

        printSafe("[ThreadPool] Immediate shutdown started.");

        immediateStop = true;
        terminated = true;
        paused = false;

        droppedQueuedTasks += static_cast<int>(taskQueue.size());

        std::priority_queue<Task, std::vector<Task>, TaskComparator> emptyQueue;
        std::swap(taskQueue, emptyQueue);
        queueTotalSeconds = 0;

        sampleQueueLengthUnsafe();
    }

    cv.notify_all();

    for (std::thread& worker : workers)
    {
        if (worker.joinable())
        {
            worker.join();
        }
    }

    workers.clear();
    initialized = false;

    printSafe("[ThreadPool] Immediate shutdown finished.");
}

void ThreadPool::pause()
{
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (!initialized || terminated || immediateStop)
        {
            return;
        }

        if (paused)
        {
            return;
        }

        paused = true;
    }

    printSafe("[ThreadPool] Pool paused.");
}

void ThreadPool::resume()
{
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (!initialized || immediateStop)
        {
            return;
        }

        if (!paused)
        {
            return;
        }

        paused = false;
    }

    printSafe("[ThreadPool] Pool resumed.");
    cv.notify_all();
}

bool ThreadPool::submitTask(const Task& task)
{
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (!initialized || terminated || immediateStop)
        {
            printSafe("[ThreadPool] Cannot add task: pool is not active.");
            return false;
        }

        if (queueTotalSeconds + task.durationSeconds > maxQueueSeconds)
        {
            rejectedTasks++;

            printSafe("[Main] Task rejected: id = " + std::to_string(task.id) +
                ", duration = " + std::to_string(task.durationSeconds) +
                " sec, queue total would become " +
                std::to_string(queueTotalSeconds + task.durationSeconds) + " sec");

            return false;
        }

        taskQueue.push(task);
        queueTotalSeconds += task.durationSeconds;
        acceptedTasks++;

        sampleQueueLengthUnsafe();

        printSafe("[Main] Task added: id = " + std::to_string(task.id) +
            ", duration = " + std::to_string(task.durationSeconds) +
            " sec, queue total = " + std::to_string(queueTotalSeconds) + " sec");
    }

    cv.notify_one();
    return true;
}

void ThreadPool::workerRoutine(size_t workerId)
{
    printSafe("[Worker " + std::to_string(workerId) + "] Started.");

    while (true)
    {
        Task currentTask{};
        bool hasTask = false;

        {
            std::unique_lock<std::mutex> lock(mutex);

            auto waitStart = std::chrono::steady_clock::now();

            cv.wait(lock, [this]()
                {
                    return immediateStop || terminated || (!paused && !taskQueue.empty());
                });

            auto waitEnd = std::chrono::steady_clock::now();
            totalWorkerWaitTime += std::chrono::duration_cast<std::chrono::nanoseconds>(waitEnd - waitStart);
            workerWaitEvents++;

            if (immediateStop)
            {
                printSafe("[Worker " + std::to_string(workerId) + "] Received immediate stop signal.");
                break;
            }

            if (taskQueue.empty())
            {
                if (terminated)
                {
                    printSafe("[Worker " + std::to_string(workerId) + "] Received graceful termination signal.");
                    break;
                }

                continue;
            }

            if (paused && !terminated)
            {
                continue;
            }

            currentTask = taskQueue.top();
            taskQueue.pop();
            queueTotalSeconds -= currentTask.durationSeconds;

            activeTasks++;

            sampleQueueLengthUnsafe();

            hasTask = true;

            printSafe("[Worker " + std::to_string(workerId) + "] Took task " +
                std::to_string(currentTask.id) + " from queue. Queue total now = " +
                std::to_string(queueTotalSeconds) + " sec");
        }

        if (!hasTask)
        {
            continue;
        }

        printSafe("[Worker " + std::to_string(workerId) + "] Started task " +
            std::to_string(currentTask.id) + " (" +
            std::to_string(currentTask.durationSeconds) + " sec)");

        auto taskStart = std::chrono::steady_clock::now();

        bool interrupted = false;
        int remainingMilliseconds = currentTask.durationSeconds * 1000;

        while (remainingMilliseconds > 0)
        {
            int chunk = (remainingMilliseconds >= 100) ? 100 : remainingMilliseconds;
            std::this_thread::sleep_for(std::chrono::milliseconds(chunk));
            remainingMilliseconds -= chunk;

            std::lock_guard<std::mutex> lock(mutex);
            if (immediateStop)
            {
                interrupted = true;
                break;
            }
        }

        auto taskEnd = std::chrono::steady_clock::now();
        auto executionTime = std::chrono::duration_cast<std::chrono::nanoseconds>(taskEnd - taskStart);

        bool shouldExitAfterTask = false;

        {
            std::lock_guard<std::mutex> lock(mutex);

            totalTaskExecutionTime += executionTime;

            if (interrupted)
            {
                interruptedTasks++;
                shouldExitAfterTask = true;
            }
            else
            {
                completedTasks++;
            }

            activeTasks--;
        }

        if (interrupted)
        {
            printSafe("[Worker " + std::to_string(workerId) + "] Task " +
                std::to_string(currentTask.id) + " interrupted by immediate shutdown.");
        }
        else
        {
            printSafe("[Worker " + std::to_string(workerId) + "] Finished task " +
                std::to_string(currentTask.id));
        }

        if (shouldExitAfterTask)
        {
            break;
        }
    }

    printSafe("[Worker " + std::to_string(workerId) + "] Finished.");
}

void ThreadPool::sampleQueueLengthUnsafe()
{
    queueLengthSum += static_cast<long long>(taskQueue.size());
    queueLengthSamples++;
}

bool ThreadPool::isWorkCompleted()
{
    std::lock_guard<std::mutex> lock(mutex);

    return taskQueue.empty() && activeTasks == 0;
}


void ThreadPool::printStatistics(size_t producerThreadCount)
{
    size_t workerCountCopy;
    int acceptedCopy;
    int rejectedCopy;
    int completedCopy;
    int interruptedCopy;
    int droppedQueuedCopy;

    long long queueLengthSumCopy;
    long long queueLengthSamplesCopy;

    std::chrono::nanoseconds totalWorkerWaitTimeCopy;
    long long workerWaitEventsCopy;

    std::chrono::nanoseconds totalTaskExecutionTimeCopy;

    {
        std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex));

        workerCountCopy = createdWorkerThreads;
        acceptedCopy = acceptedTasks;
        rejectedCopy = rejectedTasks;
        completedCopy = completedTasks;
        interruptedCopy = interruptedTasks;
        droppedQueuedCopy = droppedQueuedTasks;

        queueLengthSumCopy = queueLengthSum;
        queueLengthSamplesCopy = queueLengthSamples;

        totalWorkerWaitTimeCopy = totalWorkerWaitTime;
        workerWaitEventsCopy = workerWaitEvents;

        totalTaskExecutionTimeCopy = totalTaskExecutionTime;
    }

    double averageQueueLength = 0.0;
    if (queueLengthSamplesCopy > 0)
    {
        averageQueueLength = static_cast<double>(queueLengthSumCopy) / queueLengthSamplesCopy;
    }

    double averageWorkerWaitMilliseconds = 0.0;
    if (workerWaitEventsCopy > 0)
    {
        averageWorkerWaitMilliseconds =
            std::chrono::duration<double, std::milli>(totalWorkerWaitTimeCopy).count() / workerWaitEventsCopy;
    }

    double averageTaskExecutionMilliseconds = 0.0;
    int countedExecutedTasks = completedCopy + interruptedCopy;
    if (countedExecutedTasks > 0)
    {
        averageTaskExecutionMilliseconds =
            std::chrono::duration<double, std::milli>(totalTaskExecutionTimeCopy).count() / countedExecutedTasks;
    }

    printSafe("========== THREAD POOL STATISTICS ==========");
    printSafe("[Statistics] Worker threads created: " + std::to_string(workerCountCopy));
    printSafe("[Statistics] Producer threads created: " + std::to_string(producerThreadCount));
    printSafe("[Statistics] Total created threads (without main): " +
        std::to_string(workerCountCopy + producerThreadCount));

    printSafe("[Statistics] Accepted tasks: " + std::to_string(acceptedCopy));
    printSafe("[Statistics] Rejected tasks: " + std::to_string(rejectedCopy));
    printSafe("[Statistics] Completed tasks: " + std::to_string(completedCopy));
    printSafe("[Statistics] Interrupted tasks: " + std::to_string(interruptedCopy));
    printSafe("[Statistics] Dropped queued tasks during immediate shutdown: " + std::to_string(droppedQueuedCopy));

    printSafe("[Statistics] Average worker wait time: " +
        std::to_string(averageWorkerWaitMilliseconds) + " ms");

    printSafe("[Statistics] Average task execution time: " +
        std::to_string(averageTaskExecutionMilliseconds) + " ms");

    printSafe("[Statistics] Average queue length: " +
        std::to_string(averageQueueLength));
    printSafe("============================================");
}

void ThreadPool::printSafe(const std::string& message)
{
    std::lock_guard<std::mutex> lock(coutMutex);
    std::cout << message << std::endl;
}