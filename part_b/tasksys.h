#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <set>
#include <map>
#include <algorithm>
#include <iterator>
#include <iostream>
#include <queue>


/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSleeping : public ITaskSystem {
public:
  TaskSystemParallelThreadPoolSleeping(int num_threads);
  ~TaskSystemParallelThreadPoolSleeping();
  const char* name();
  void run(IRunnable *runnable, int num_total_tasks);
  TaskID runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                          const std::vector<TaskID> &deps);
  void sync();

private:
  struct TaskGroup {
    TaskID task_id;
    IRunnable* runnable;
    int num_tasks;
    int num_started;
    int num_completed;
    std::set<TaskID>* prior_deps;
    TaskGroup(TaskID task_id, IRunnable* runnable,int num_tasks,int num_started,int num_completed, std::set<TaskID>* prior_deps) {
      this->task_id = task_id;
      this->runnable = runnable;
      this->num_tasks = num_tasks;
      this->num_started = num_started;
      this->num_completed = num_completed;
      this->prior_deps = prior_deps;
    }
    ~TaskGroup() {
      delete prior_deps;
    }
  };
  // 'global'
  int total_threads;
  // Terminating Condition
  bool done;

  // Main Thread Function
  void main_thread_working();

  // Run
  std::vector<TaskID>* run_init_no_dep;

  // Main Function Variables;
  int task_group_id_tracker_;
  std::condition_variable* mainCV;
  std::thread main_thread;
  std::queue<TaskGroup*>* parallel_tasks_to_run;
  std::map<TaskID, std::vector<TaskGroup*>*>* blocked_tasks_map;
  std::set<TaskID>* completed_task_groups_set;
  std::mutex* main_mutex;
  TaskGroup* curr_task_group;
  bool task_group_finished;
  std::vector<TaskGroup*>* completed_task_groups_vec;

  // Worker Thread Function
  void worker_thread();
  // Worker Thread Variables
  std::thread* workers;
  std::condition_variable* workerCV;
  std::mutex* worker_mutex;

  // Sync Variables
  std::condition_variable* syncCV;
  bool can_sync;


};

#endif