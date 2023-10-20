#include "tasksys.h"


IRunnable::~IRunnable() = default;

ITaskSystem::ITaskSystem(int num_threads) {}

ITaskSystem::~ITaskSystem() = default;

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() = default;

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks) {
	for (int i = 0; i < num_total_tasks; i++) { runnable->runTask(i, num_total_tasks); }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps) {
	for (int i = 0; i < num_total_tasks; i++) {
		runnable->runTask(i, num_total_tasks);
	}

	return 0;
}

void TaskSystemSerial::sync() {}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name() {
	return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads) {
	// NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() = default;

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks) {
	// NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
	for (int i = 0; i < num_total_tasks; i++) {
		runnable->runTask(i, num_total_tasks);
	}
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps) {
	// NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
	for (int i = 0; i < num_total_tasks; i++) {
		runnable->runTask(i, num_total_tasks);
	}

	return 0;
}

void TaskSystemParallelSpawn::sync() {
	// NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name() {
	return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads) {
	// NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() = default;

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks) {
	// NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
	for (int i = 0; i < num_total_tasks; i++) {
		runnable->runTask(i, num_total_tasks);
	}
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps) {
	// NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
	for (int i = 0; i < num_total_tasks; i++) {
		runnable->runTask(i, num_total_tasks);
	}

	return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
	// NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */
const char *TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads) {
    num_group = 0;
    assigned_work = 0;
    total_completed = 0;
    done = false;
    finish = false;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(&TaskSystemParallelThreadPoolSleeping::worker_thread, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    done = true;
    workerCV.notify_all();
    for (auto &thread : threads) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::worker_thread() {
    RunnableTask *task = nullptr;
    TaskGroup *task_belongs_to;
    while (1) {
        std::unique_lock<std::mutex> lock(task_mutex);
        workerCV.wait(lock, [this] { return done || !task_q.empty(); });

        if (done && task_q.empty()) {
            return;  // Exit the thread if done and no more tasks.
        }

        task = task_q.front();
        task_q.pop();
        task_belongs_to = task->belong_to;
        lock.unlock();
        task_belongs_to->runnable->runTask(task->id, task_belongs_to->num_total_tasks);
        lock.lock();
        task_belongs_to->tasks_remain--;


        if (task_belongs_to->tasks_remain == 0) {
            for (auto task_group: task_group_set) { 
                if (task_belongs_to != task_group) {
                    //! remove this task group from groups that depend on it!
                    task_group->depending.erase(task_belongs_to->group_id); 
                }
            }
            std::cout << "Completed all tasks for Group ID #:  " << task_belongs_to->group_id << std::endl;
            total_completed++;
            counterCV.notify_one();
        }
        lock.unlock();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
    auto task_group = new TaskGroup(num_group, runnable, num_total_tasks, deps);
    task_group->num_dependencies = 0; // Initialize to 0.
    task_group_queue.push(task_group);
    task_group_set.insert(task_group);
    // group ID = num_group
    num_group++;
    return num_group;
}





void TaskSystemParallelThreadPoolSleeping::sync() {
    while (!task_group_queue.empty()) {

        TaskGroup *cur_task_group = task_group_queue.top();

        std::cout << "GROUP ID " << cur_task_group->group_id << std::endl;
        std::cout << " TOTAL depending: " << cur_task_group->depending.size() << std::endl;
        std::cout << " total # task groups: " << task_group_queue.size() << std::endl;
        // ! Thread will sleep until a the top of the task group PQ has zero dependencies or all tasks are completed. Then, it will assign work 
        // workerCV.wait(lock, [this, &cur_task_group] { return finish || cur_task_group->depending.empty(); });

        // if (finish || !cur_task_group->depending.empty()) {
        //     return;
        // }
        task_mutex.lock();
        for (int i = 0; i < cur_task_group->num_total_tasks; i++) {
            RunnableTask *cur_runnable = new RunnableTask(cur_task_group, i);
            task_q.push(cur_runnable);
        }
        task_mutex.unlock();
        // std::cout << "Assigned work"  << std::endl;
        assigned_work++;
        workerCV.notify_all();
        task_group_queue.pop();
    }
    // if (task_group_queue.empty()) {


    // }

    // while (1) {
    std::unique_lock<std::mutex> lock(counter_mutex);
		counterCV.wait(lock, [this] { 
            finish = true;

            for (auto taskGroup: task_group_set) {
                if (taskGroup->tasks_remain > 0) {
                    finish = false;
                    break;
			    }   
		    }  
            return finish;
        });
	// }
            // std::cout << "Assigned work: "  << assigned_work << std::endl;
        // std::cout << "totla completed: "  << total_completed << std::endl;
}
