#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <queue>
#include <string>

struct Task
{
    int id;
    int durationSeconds;
};

struct TaskComparator
{
    bool operator()(const Task& a, const Task& b) const
    {
        return a.durationSeconds > b.durationSeconds;
    }
};

class ThreadPool
{
public:
    ThreadPool();
    ~ThreadPool();

    void initialize(size_t workerCount);
    void terminate();

    bool submitTask(const Task& task);

    void printSafe(const std::string& message);

private:
    void workerRoutine(size_t workerId);

private:
    std::vector<std::thread> workers;
    std::priority_queue<Task, std::vector<Task>, TaskComparator> taskQueue;

    std::mutex mutex;
    std::mutex coutMutex;
    std::condition_variable cv;

    bool initialized = false;
    bool terminated = false;

    int queueTotalSeconds = 0;
    const int maxQueueSeconds = 50;

    int acceptedTasks = 0;
    int rejectedTasks = 0;
};