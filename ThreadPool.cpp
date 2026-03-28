#include "ThreadPool.h"
#include <chrono>

ThreadPool::ThreadPool()
{
    printSafe("[ThreadPool] Constructor called.");
}

ThreadPool::~ThreadPool()
{
    terminate();
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
    initialized = true;
    queueTotalSeconds = 0;
    acceptedTasks = 0;
    rejectedTasks = 0;

    printSafe("[ThreadPool] Initializing pool with " + std::to_string(workerCount) + " workers...");

    for (size_t i = 0; i < workerCount; ++i)
    {
        workers.emplace_back(&ThreadPool::workerRoutine, this, i + 1);
    }
}

void ThreadPool::terminate()
{
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (!initialized)
        {
            return;
        }

        printSafe("[ThreadPool] Terminating pool...");
        terminated = true;
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

    printSafe("[ThreadPool] Pool terminated successfully.");
    printSafe("[ThreadPool] Accepted tasks: " + std::to_string(acceptedTasks));
    printSafe("[ThreadPool] Rejected tasks: " + std::to_string(rejectedTasks));
}

bool ThreadPool::submitTask(const Task& task)
{
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (!initialized || terminated)
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
        Task currentTask;

        {
            std::unique_lock<std::mutex> lock(mutex);

            cv.wait(lock, [this]()
                {
                    return terminated || !taskQueue.empty();
                });

            if (terminated && taskQueue.empty())
            {
                printSafe("[Worker " + std::to_string(workerId) + "] Received termination signal.");
                break;
            }

            currentTask = taskQueue.top();
            taskQueue.pop();
            queueTotalSeconds -= currentTask.durationSeconds;

            printSafe("[Worker " + std::to_string(workerId) + "] Took task " +
                std::to_string(currentTask.id) + " from queue. Queue total now = " +
                std::to_string(queueTotalSeconds) + " sec");
        }

        printSafe("[Worker " + std::to_string(workerId) + "] Started task " +
            std::to_string(currentTask.id) + " (" +
            std::to_string(currentTask.durationSeconds) + " sec)");

        std::this_thread::sleep_for(std::chrono::seconds(currentTask.durationSeconds));

        printSafe("[Worker " + std::to_string(workerId) + "] Finished task " +
            std::to_string(currentTask.id));
    }

    printSafe("[Worker " + std::to_string(workerId) + "] Finished.");
}

void ThreadPool::printSafe(const std::string& message)
{
    std::lock_guard<std::mutex> lock(coutMutex);
    std::cout << message << std::endl;
}