#include "ThreadPool.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>
#include <random>

int generateRandomValue(int minValue, int maxValue)
{
    thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<int> distribution(minValue, maxValue);
    return distribution(generator);
}

Task generateRandomTask(int taskId)
{
    Task task;
    task.id = taskId;
    task.durationSeconds = generateRandomValue(6, 12);
    return task;
}

void producerRoutine(ThreadPool& pool, int producerId, int tasksToGenerate, std::atomic<int>& globalTaskId)
{
    for (int i = 0; i < tasksToGenerate; ++i)
    {
        int newTaskId = ++globalTaskId;
        Task task = generateRandomTask(newTaskId);

        pool.printSafe("[Producer " + std::to_string(producerId) +
            "] Generated task " + std::to_string(task.id) +
            " with duration " + std::to_string(task.durationSeconds) + " sec");

        pool.submitTask(task);

        int delayMs = generateRandomValue(300, 800);
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
    }

    pool.printSafe("[Producer " + std::to_string(producerId) + "] Finished generating tasks.");
}

int main()
{
    {
        ThreadPool pool;
        pool.printSafe("========== 1) PAUSE/RESUME + GRACEFUL SHUTDOWN ==========");

        pool.initialize(4);

        const int producerCount = 3;
        const int tasksPerProducer = 6;

        std::atomic<int> globalTaskId = 0;
        std::vector<std::thread> producers;

        for (int i = 0; i < producerCount; ++i)
        {
            producers.emplace_back(
                producerRoutine,
                std::ref(pool),
                i + 1,
                tasksPerProducer,
                std::ref(globalTaskId)
            );
        }

        std::this_thread::sleep_for(std::chrono::seconds(3));
        pool.printSafe("[Main] Pausing thread pool for 5 seconds...");
        pool.pause();

        std::this_thread::sleep_for(std::chrono::seconds(5));
        pool.printSafe("[Main] Resuming thread pool...");
        pool.resume();

        for (std::thread& producer : producers)
        {
            if (producer.joinable())
            {
                producer.join();
            }
        }

        pool.printSafe("[Main] All producers finished. Waiting until workers complete all accepted tasks...");
        while (!pool.isWorkCompleted())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

        pool.printSafe("[Main] All accepted tasks are completed. Starting graceful shutdown...");
        pool.shutdownGraceful();

        pool.printStatistics(producerCount);
        pool.printSafe("[Main] 1) finished.");
    }

    {
        ThreadPool pool;
        pool.printSafe("========== 2) IMMEDIATE SHUTDOWN ==========");

        pool.initialize(4);

        const int producerCount = 2;
        const int tasksPerProducer = 5;

        std::atomic<int> globalTaskId = 0;
        std::vector<std::thread> producers;

        for (int i = 0; i < producerCount; ++i)
        {
            producers.emplace_back(
                producerRoutine,
                std::ref(pool),
                i + 1,
                tasksPerProducer,
                std::ref(globalTaskId)
            );
        }

        std::this_thread::sleep_for(std::chrono::seconds(13));
        pool.printSafe("[Main] Starting immediate shutdown...");
        pool.shutdownImmediate();

        for (std::thread& producer : producers)
        {
            if (producer.joinable())
            {
                producer.join();
            }
        }

        pool.printStatistics(producerCount);
        pool.printSafe("[Main] 2) finished.");
    }

    std::cout << "Program finished." << std::endl;
    return 0;
}