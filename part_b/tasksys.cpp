#include "tasksys.h"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}


/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads) : ITaskSystem{num_threads}  {
  total_threads = num_threads;
  workers = new std::thread[total_threads];
  workerCV = new std::condition_variable();
  worker_mutex = new std::mutex();
  mainCV = new std::condition_variable();
  main_mutex = new std::mutex();
  syncCV = new std::condition_variable();
  can_sync = true;
  done = false;
  task_group_finished = false;
  task_group_id_tracker_ = 0;
  curr_task_group = nullptr;
  blocked_tasks_map = new std::map<TaskID, std::vector<TaskGroup*>*>;
  completed_task_groups_set = new std::set<TaskID>;
  parallel_tasks_to_run = new std::queue<TaskGroup*>;
  completed_task_groups_vec = new std::vector<TaskGroup*>;
  run_init_no_dep = new std::vector<TaskID>();
  auto main_thread_working{[&]() {
    while (1) {
      std::unique_lock<std::mutex> worker_lock(*worker_mutex);
      mainCV->wait(
        worker_lock, [&]() {return (done || !parallel_tasks_to_run->empty() || task_group_finished); }
      );
      if (done) {
        worker_lock.unlock();
        return;
      } else {
        bool assigned_new_task_group = false;
        bool unblocked_new_task_group = false;
        bool local_task_group_finished = task_group_finished;
        task_group_finished = false;
        if (!curr_task_group || curr_task_group->num_started == curr_task_group->num_tasks) {
          main_mutex->lock();
          if (!parallel_tasks_to_run->empty()){
            curr_task_group = parallel_tasks_to_run->front();
            parallel_tasks_to_run->pop();
            can_sync = false;
            main_mutex->unlock();
            worker_lock.unlock();
            workerCV->notify_all();
            assigned_new_task_group = true;
          } else {
            main_mutex->unlock();
            worker_lock.unlock();
          }
        }
        else {
          worker_lock.unlock();
        }
        if (local_task_group_finished) {
          std::unique_lock<std::mutex> lock(*main_mutex);
          for (auto finished_task_group: *completed_task_groups_vec)
          {
            completed_task_groups_set->insert(finished_task_group->task_id);
            //! Unblock all the task groups that this one was previously blocking now that its compelted
            if (blocked_tasks_map->find(finished_task_group->task_id) != blocked_tasks_map->end())
            {
              for (auto activate_task_group: *blocked_tasks_map->at(finished_task_group->task_id))
              {
                activate_task_group->prior_deps->erase(finished_task_group->task_id);
                if (activate_task_group->prior_deps->empty())
                {
                  unblocked_new_task_group = true;
                  parallel_tasks_to_run->push(activate_task_group);
                }
              }
              delete blocked_tasks_map->at(finished_task_group->task_id);
              blocked_tasks_map->erase(finished_task_group->task_id);
            }
            delete finished_task_group;
          }
          // Reset the completed task group 
          completed_task_groups_vec->clear();
        }
        // The local task group finished and now handle case where there 
        // may be no more task groups left to process.
        if (local_task_group_finished && !assigned_new_task_group && !unblocked_new_task_group)
        {
          std::unique_lock<std::mutex> lock(*main_mutex);
          if (parallel_tasks_to_run->empty() && blocked_tasks_map->empty())
          {
            worker_lock.lock();
            if (!curr_task_group || curr_task_group->num_completed == curr_task_group->num_tasks)
            {
              worker_lock.unlock();
              can_sync = true;
              lock.unlock();
              syncCV->notify_one();
            } else {
              can_sync = false;
              worker_lock.unlock();
            }
          } else {
            can_sync = false;
          }
        }
      }
    }
  }};

  auto worker_thread{[&](const int thread_id) {
    while (1) {
      std::unique_lock<std::mutex> worker_lock(*worker_mutex);
      workerCV->wait(worker_lock, [&]()
      {
        return (curr_task_group && curr_task_group->num_started < curr_task_group->num_tasks) 
          || done;
      });
      if (done)
      {
        worker_lock.unlock();
        return;
      }
      while (curr_task_group && curr_task_group->num_started < curr_task_group->num_tasks)
      {
        TaskGroup* local_task_group = curr_task_group;
        int current_task_id = local_task_group->num_started;
        local_task_group->num_started++;
        worker_lock.unlock();
        local_task_group->runnable->runTask(current_task_id, local_task_group->num_tasks);
        worker_lock.lock();
        local_task_group->num_completed++;
        if (local_task_group->num_completed == local_task_group->num_tasks){
          task_group_finished = true;
          main_mutex->lock(); 
          completed_task_groups_vec->push_back(local_task_group);
          main_mutex->unlock();
          worker_lock.unlock();
          mainCV->notify_one();
          worker_lock.lock();
        }
      }
      worker_lock.unlock();
    }
  }};
  main_thread = std::thread(main_thread_working);
  for (int i = 0; i < total_threads; i++) {
    workers[i] = std::thread(worker_thread, i);
  }

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  {
    std::unique_lock<std::mutex> lock(*worker_mutex);
    done = true;
    // !Release lock to reduce contention for the CV signal
  }
  //!End the system of work scheduling and tasks running
  workerCV->notify_all();
  for (int i = 0; i < total_threads; i++) {
    workers[i].join();
  }
  mainCV->notify_one();
  main_thread.join();
  delete completed_task_groups_vec;
  delete workerCV;
  delete mainCV;
  delete syncCV;
  delete run_init_no_dep;
  delete main_mutex;
  delete worker_mutex;
  delete[] workers;
  delete blocked_tasks_map;
  delete completed_task_groups_set;
  delete parallel_tasks_to_run;
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable *runnable,
                                               int num_total_tasks) {
  runAsyncWithDeps(runnable, num_total_tasks, *run_init_no_dep);
  sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    std::set<TaskID>* prior_deps = new std::set<TaskID>();
    std::set<TaskID>* task_deps = new std::set<TaskID>(deps.begin(), deps.end());
    std::unique_lock<std::mutex> lock(*main_mutex);
    int curr_task_group_id = task_group_id_tracker_++;
    //! Compare the difference of our completed tasks and the ones that are required for this group task
    std::set_difference(task_deps->begin(), task_deps->end(), 
      completed_task_groups_set->begin(), completed_task_groups_set->end(), 
      std::inserter(*prior_deps, prior_deps->end()));
    TaskGroup* new_task_group = new TaskGroup(curr_task_group_id, runnable, num_total_tasks, 0 , 0, prior_deps);
    //! If there are dependency groups that the new task group is dependent on, bookkeep them as a dependency
    //! in the blocked_tasks_map
    if (!prior_deps->empty())
    {
      for (auto block_task_id: *prior_deps)
      {
        if (blocked_tasks_map->find(block_task_id) == blocked_tasks_map->end())
        {
          blocked_tasks_map->insert(std::pair<TaskID, std::vector<TaskGroup*>*>(block_task_id, new std::vector<TaskGroup*>({new_task_group})));
        }
        else
        {
          blocked_tasks_map->at(block_task_id)->push_back(new_task_group);
        }
      }
      can_sync = false;
    }
    //! If there are no prior dependencies, schedule this task gorup to be ran in parallel
    else
    {      
      parallel_tasks_to_run->push(new_task_group);
      can_sync = false;
      lock.unlock();
      mainCV->notify_one();
    }
    delete task_deps;
    return curr_task_group_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> sync_lock(*main_mutex);
    syncCV->wait(sync_lock, [this](){return can_sync;});
}