#include "ThreadPool.h"
#include <iostream>
#include <thread>
#include <chrono>

int main()
{
    ThreadPool pool;
    pool.initialize(4);

    pool.submitTask({ 1, 10 });
    pool.submitTask({ 2, 8 });
    pool.submitTask({ 3, 12 });
    pool.submitTask({ 4, 7 });
    pool.submitTask({ 5, 9 });
    pool.submitTask({ 6, 6 });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    pool.submitTask({ 7, 15 });
    pool.submitTask({ 8, 11 });
    pool.submitTask({ 9, 5 });
    pool.submitTask({ 10, 4 });

    std::cout << "[Main] Waiting 20 seconds for tasks to be processed...\n";
    std::this_thread::sleep_for(std::chrono::seconds(20));

    std::cout << "[Main] Now terminating pool...\n";
    pool.terminate();

    std::cout << "[Main] Program finished.\n";
    return 0;
}