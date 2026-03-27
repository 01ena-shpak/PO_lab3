#include "ThreadPool.h"
#include <iostream>
#include <thread>
#include <chrono>

int main()
{
    ThreadPool pool;

    pool.initialize(4);

    std::cout << "[Main] Pool is running. Waiting 3 seconds...\n";
    std::this_thread::sleep_for(std::chrono::seconds(3));

    std::cout << "[Main] Now terminating pool...\n";
    pool.terminate();

    std::cout << "[Main] Program finished.\n";
    return 0;
}