#include "ThreadPool.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <cstdlib>
#include <ctime>

Task generateRandomTask(int taskId)
{
    Task task;
    task.id = taskId;
    task.durationSeconds = 6 + std::rand() % 7; // 6 - 12
    return task;
}

int main()
{
    std::srand(std::time(nullptr));

    ThreadPool pool;
    pool.initialize(4);

    for (int i = 1; i <= 12; ++i)
    {
        Task task = generateRandomTask(i);
        pool.submitTask(task);

        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }

    std::cout << "[Main] Waiting 30 seconds for tasks to be processed...\n";
    std::this_thread::sleep_for(std::chrono::seconds(30));

    std::cout << "[Main] Now terminating pool...\n";
    pool.terminate();

    std::cout << "[Main] Program finished.\n";
    return 0;
}