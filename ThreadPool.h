#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <queue>
#include <string>
#include <chrono>

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

    void shutdownGraceful();
    void shutdownImmediate();

    void pause();
    void resume();

    bool submitTask(const Task& task);

    void printSafe(const std::string& message);
    void printStatistics(size_t producerThreadCount);

    bool isWorkCompleted();

private:
    void workerRoutine(size_t workerId);
    void sampleQueueLengthUnsafe();

private:
    std::vector<std::thread> workers;
    std::priority_queue<Task, std::vector<Task>, TaskComparator> taskQueue;

    std::mutex mutex;
    std::mutex coutMutex;
    std::condition_variable cv;

    bool initialized = false;
    bool terminated = false;
    bool paused = false;
    bool immediateStop = false;

    int queueTotalSeconds = 0;
    const int maxQueueSeconds = 50;

    int acceptedTasks = 0;
    int rejectedTasks = 0;
    int completedTasks = 0;
    int interruptedTasks = 0;
    int droppedQueuedTasks = 0;

    int activeTasks = 0;

    size_t createdWorkerThreads = 0;

    long long queueLengthSum = 0;
    long long queueLengthSamples = 0;

    std::chrono::nanoseconds totalWorkerWaitTime{ 0 };
    long long workerWaitEvents = 0;

    std::chrono::nanoseconds totalTaskExecutionTime{ 0 };
};