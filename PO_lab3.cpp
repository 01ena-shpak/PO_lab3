#include "ThreadPool.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <vector>
#include <atomic>

Task generateRandomTask(int taskId)
{
    Task task;
    task.id = taskId;
    task.durationSeconds = 6 + std::rand() % 7; // 6 - 12
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

        std::this_thread::sleep_for(std::chrono::milliseconds(300 + std::rand() % 500));
    }

    pool.printSafe("[Producer " + std::to_string(producerId) + "] Finished generating tasks.");
}

int main()
{
    std::srand(std::time(nullptr));

    ThreadPool pool;

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

    for (std::thread& producer : producers)
    {
        if (producer.joinable())
        {
            producer.join();
        }
    }

    pool.printSafe("[Main] All producers finished. Waiting 35 seconds for workers...");
    std::this_thread::sleep_for(std::chrono::seconds(35));

    pool.printSafe("[Main] Now terminating pool...");
    pool.terminate();

    pool.printSafe("[Main] Program finished.");
    return 0;
}