#include "ThreadPool.h"
#include <chrono>

ThreadPool::ThreadPool()
{
    std::cout << "[ThreadPool] Constructor called.\n";
}

ThreadPool::~ThreadPool()
{
    terminate();
    std::cout << "[ThreadPool] Destructor called.\n";
}

void ThreadPool::initialize(size_t workerCount)
{
    std::lock_guard<std::mutex> lock(mutex);

    if (initialized)
    {
        std::cout << "[ThreadPool] Pool is already initialized.\n";
        return;
    }

    terminated = false;
    initialized = true;

    std::cout << "[ThreadPool] Initializing pool with " << workerCount << " workers...\n";

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

        std::cout << "[ThreadPool] Terminating pool...\n";
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

    std::cout << "[ThreadPool] Pool terminated successfully.\n";
}

void ThreadPool::workerRoutine(size_t workerId)
{
    std::cout << "[Worker " << workerId << "] Started.\n";

    while (true)
    {
        std::unique_lock<std::mutex> lock(mutex);

        cv.wait_for(lock, std::chrono::milliseconds(500), [this]()
            {
                return terminated;
            });

        if (terminated)
        {
            std::cout << "[Worker " << workerId << "] Received termination signal.\n";
            break;
        }

        std::cout << "[Worker " << workerId << "] Waiting for tasks...\n";
    }

    std::cout << "[Worker " << workerId << "] Finished.\n";
}