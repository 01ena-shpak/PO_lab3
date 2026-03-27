#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>

class ThreadPool
{
public:
    ThreadPool();
    ~ThreadPool();

    void initialize(size_t workerCount);
    void terminate();

private:
    void workerRoutine(size_t workerId);

private:
    std::vector<std::thread> workers;

    std::mutex mutex;
    std::condition_variable cv;

    bool initialized = false;
    bool terminated = false;
};