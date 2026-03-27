#include "ThreadPool.h"
#include <iostream>
#include <thread>
#include <chrono>

int main()
{
    ThreadPool pool;

    pool.initialize(4);

    pool.submitTask({ 1, 5 });
    pool.submitTask({ 2, 2 });
    pool.submitTask({ 3, 4 });
    pool.submitTask({ 4, 1 });
    pool.submitTask({ 5, 3 });

    std::cout << "[Main] Waiting 8 seconds for tasks to be processed...\n";
    std::this_thread::sleep_for(std::chrono::seconds(8));

    std::cout << "[Main] Now terminating pool...\n";
    pool.terminate();

    std::cout << "[Main] Program finished.\n";
    return 0;
}