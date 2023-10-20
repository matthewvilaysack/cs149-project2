#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <thread>
#include <atomic>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <set>
#include <utility>
#include <iostream>
/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial : public ITaskSystem {
public:
	explicit TaskSystemSerial(int num_threads);

	~TaskSystemSerial() override;

	const char *name() override;

	void run(IRunnable *runnable, int num_total_tasks) override;

	TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) override;

	void sync() override;
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn : public ITaskSystem {
public:
	explicit TaskSystemParallelSpawn(int num_threads);

	~TaskSystemParallelSpawn() override;

	const char *name() override;

	void run(IRunnable *runnable, int num_total_tasks) override;

	TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) override;

	void sync() override;
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning : public ITaskSystem {
public:
	explicit TaskSystemParallelThreadPoolSpinning(int num_threads);

	~TaskSystemParallelThreadPoolSpinning() override;

	const char *name() override;

	void run(IRunnable *runnable, int num_total_tasks) override;

	TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) override;

	void sync() override;
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
// Task group with dependencies!
struct TaskGroup {
	IRunnable *runnable;
	int num_total_tasks;
	std::atomic<int> tasks_remain;
	int group_id;
	int num_dependencies;
	int dependencies_completed;
	std::set<TaskID> depending;

	TaskGroup(int group_id, IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) {
		this->group_id = group_id;
		this->runnable = runnable;
		this->num_dependencies = 0;  // Initialize to 0.
		this->dependencies_completed = 0;
		this->num_total_tasks = num_total_tasks;
		this->tasks_remain = num_total_tasks;
		this->depending = {};
		for (auto dep: deps) {
			this->depending.insert(dep); 
		}
	}

};

struct TaskGroupComparator {
	// lowest deps come first
    bool operator()(const TaskGroup* a, const TaskGroup* b) const {
        return a->depending.size() < b->depending.size(); // lowest dependencies come first
    }
};
struct RunnableTask {
	TaskGroup *belong_to;
	int id;

	RunnableTask(TaskGroup *belong_to, int id) {
		this->belong_to = belong_to;
		this->id = id;
	}
};

class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
	explicit TaskSystemParallelThreadPoolSleeping(int num_threads);

	~TaskSystemParallelThreadPoolSleeping() override;

	const char *name() override;

	void run(IRunnable *runnable, int num_total_tasks) override;

	TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks, const std::vector<TaskID> &deps) override;
	TaskGroup* findTaskGroupById(TaskID id);
	void sync() override;

private:
	std::vector<std::thread> threads;
	std::mutex counter_mutex;
	std::condition_variable counterCV;
	std::mutex task_mutex;
	std::condition_variable workerCV;
	std::queue<RunnableTask *> task_q;
	std::set<TaskGroup *> task_group_set;
    std::priority_queue<TaskGroup*, std::vector<TaskGroup*>, TaskGroupComparator> task_group_queue;
	bool done;
	int num_group;
	bool finish;
	void worker_thread();
	int assigned_work;
	int total_completed;
};

#endif
