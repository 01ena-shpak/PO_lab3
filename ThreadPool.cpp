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
}

void ThreadPool::submitTask(const Task& task)
{
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (!initialized || terminated)
        {
            printSafe("[ThreadPool] Cannot add task: pool is not active.");
            return;
        }

        taskQueue.push(task);
    }

    printSafe("[Main] Task added: id = " + std::to_string(task.id) +
        ", duration = " + std::to_string(task.durationSeconds) + " sec");

    cv.notify_one();
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