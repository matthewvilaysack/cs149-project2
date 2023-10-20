#include "tasksys.h"
#include <thread>
#include <mutex>
#include <iostream>
#include <condition_variable>
#include <atomic>
#include <memory>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    this->total_threads = num_threads;
}

// void TaskSystemParallelSpawn::parallelSpawnHelper() {
//     while (1) {
//         int task_id;
//         m_.lock();
//         task_id = total_work - tasks_remain;
//         task_id_num = total_work - tasks_remain;
//         tasks_remain--;
//         m_.unlock();
//         if (task_id < 0 || task_id == total_work) break;
//         std::cout << task_id << std::endl;
//         std::cout << tasks_remain << std::endl;
//         runnable->runTask(task_id, total_work);
//     }
// }

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}
void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // Create thread objects that do not yet represent a running thread
    std::thread workers[total_threads];
    std::mutex m_;
    int tasks_remain = 0;
    for (int i = 0; i < total_threads; i++) {
        workers[i] = std::thread([&runnable, &num_total_tasks, &m_, &tasks_remain]() -> void {
                while (1) {
                    int task_id;
                    m_.lock();
                    task_id = tasks_remain;
                    tasks_remain++;
                    m_.unlock();
                    if (task_id >= num_total_tasks) break;
                    runnable->runTask(task_id, num_total_tasks);
                 }
        });
    }
    for (auto &thread: workers) { thread.join(); }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}
TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    done = false;
    tasks_remain = 0;
    for (int i = 0; i < num_threads; ++i) {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSpinning::busy_waiting, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    done = true;
    for (auto &thread: workers_) { thread.join(); }
}

void TaskSystemParallelThreadPoolSpinning::busy_waiting() {
    int task_id;
    while (!done) {
        task_id = -1;
        m_.lock();
        if (!task_q.empty()) {
            task_id = task_q.front();
            task_q.pop();
        }
        m_.unlock();

        if (task_id != -1) {
            runnable->runTask(task_id, total_work);
            tasks_remain--;
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    this->runnable = runnable;
    this->total_work = num_total_tasks;
    this->tasks_remain = num_total_tasks;
    m_.lock();
    for (int i = 0; i < num_total_tasks; i++) {
        task_q.push(i);
    }
    m_.unlock();
    while (tasks_remain);
}





TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}
TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads) {
    done = false;
    tasks_remain = 0;
    for (int i = 0; i < num_threads; i++) {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::sleeping_thread, this);
    }
}
TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(m_sleep);
        done = true;
    }
    cv_sleep_q.notify_all();
    for (auto &worker : workers_) {
        worker.join();
    }
}
void TaskSystemParallelThreadPoolSleeping::sleeping_thread() {
    int task_id;
    while (1) {
        std::unique_lock<std::mutex> lock(m_sleep);
        cv_sleep_q.wait(lock, [this] { return done || !task_q.empty(); });
        // Exit if no tasks remain 
        if (done) {
            return;
        }

        task_id = task_q.front();
        task_q.pop();
        // Release the lock immediately after dequeuing the task
        lock.unlock();  
        // Several threads can run a task and context switch to 
        // other threads that can run a task here.
        this->runnable->runTask(task_id, total_work);
        // Reacquire the lock only for updating remaining tasks
        tasks_remain--;

        // Signal waiting thread if there are no more tasks
        if (tasks_remain == 0) {
            cv_count_work.notify_one();
        }
    }
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks) {
    this->runnable = runnable;
    this->tasks_remain = num_total_tasks;
    this->total_work = num_total_tasks;
    m_sleep.lock();
    for (int i = 0; i < num_total_tasks; i++) {
        task_q.push(i);
    }
    m_sleep.unlock();
    cv_sleep_q.notify_all();

    // one thread picks up the count lock and waits until no tasks remain
    std::unique_lock<std::mutex> lock(m_count_work);
    cv_count_work.wait(lock, [this] { return tasks_remain == 0; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
