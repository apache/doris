// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <queue>

#include "common/status.h"
#include "pipeline.h"
#include "pipeline_task.h"
#include "util/threadpool.h"

namespace doris::pipeline {

class SubWorkTaskQueue {
    friend class WorkTaskQueue;

public:
    void add_total_time(const uint64_t duration) { _total_consume_time.fetch_add(duration); }

    void push_back(PipelineTask* task) { _queue.emplace(task); }

    PipelineTask* try_take() {
        if (_queue.empty()) {
            return nullptr;
        }
        auto task = _queue.front();
        _queue.pop();
        return task;
    }

    void set_factor_for_normal(double factor_for_normal) { _factor_for_normal = factor_for_normal; }

    // TODO pipeline 1 may overflow here ?
    double total_consume_time() { return _total_consume_time.load() / _factor_for_normal; }

    bool empty() { return _queue.empty(); }

private:
    std::queue<PipelineTask*> _queue;
    // factor for normalization
    double _factor_for_normal = 1;
    // TODO pipeline whether need to set to zero
    // the value cal the queue task time consume, the WorkTaskQueue
    // use it to find the min queue to take task work
    std::atomic<uint64_t> _total_consume_time = 0;
};

// Each thread have private muti level queue
class WorkTaskQueue {
public:
    explicit WorkTaskQueue() : _closed(false) {
        double factor = 1;
        for (int i = SUB_QUEUE_LEVEL - 1; i >= 0; --i) {
            _sub_queues[i].set_factor_for_normal(factor);
            factor *= LEVEL_QUEUE_TIME_FACTOR;
        }
    }

    void close() {
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        _closed = true;
        _wait_task.notify_all();
    }

    PipelineTask* try_take_unprotected() {
        if (_total_task_size == 0 || _closed) {
            return nullptr;
        }
        double min_consume_time = _sub_queues[0].total_consume_time();
        int idx = 0;
        for (int i = 1; i < SUB_QUEUE_LEVEL; ++i) {
            if (!_sub_queues[i].empty()) {
                double consume_time = _sub_queues[i].total_consume_time();
                if (idx == -1 || consume_time < min_consume_time) {
                    idx = i;
                    min_consume_time = consume_time;
                }
            }
        }
        auto task = _sub_queues[idx].try_take();
        if (task) {
            _total_task_size--;
        }
        return task;
    }

    PipelineTask* try_take() {
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        return try_take_unprotected();
    }

    PipelineTask* take() {
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        while (!_closed) {
            auto task = try_take_unprotected();
            if (task) {
                return task;
            } else {
                _wait_task.wait(lock);
            }
        }
        DCHECK(_closed);
        return nullptr;
    }

    void push(PipelineTask* task) {
        size_t level = _compute_level(task);
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        _sub_queues[level].push_back(task);
        _total_task_size++;
        _wait_task.notify_one();
    }

    // Get the each thread task size to do
    size_t size() { return _total_task_size; }

private:
    static constexpr auto LEVEL_QUEUE_TIME_FACTOR = 1.2;
    static constexpr size_t SUB_QUEUE_LEVEL = 5;
    SubWorkTaskQueue _sub_queues[SUB_QUEUE_LEVEL];
    std::mutex _work_size_mutex;
    std::condition_variable _wait_task;
    std::atomic<size_t> _total_task_size = 0;
    bool _closed;

private:
    size_t _compute_level(PipelineTask* task) { return 0; }
};

// Need consider NUMA architecture
class TaskQueue {
public:
    explicit TaskQueue(size_t core_size) : _core_size(core_size) {
        _async_queue.reset(new WorkTaskQueue[core_size]);
    }

    ~TaskQueue() = default;

    void close() {
        for (int i = 0; i < _core_size; ++i) {
            _async_queue[i].close();
        }
    }

    // Get the task by core id. TODO: To think the logic is useful?
    PipelineTask* try_take(size_t core_id) { return _async_queue[core_id].try_take(); }

    // not block， steal task by other core queue
    PipelineTask* steal_take(size_t core_id) {
        DCHECK(core_id < _core_size);
        size_t next_id = core_id;
        for (size_t i = 1; i < _core_size; ++i) {
            ++next_id;
            if (next_id == _core_size) {
                next_id = 0;
            }
            DCHECK(next_id < _core_size);
            auto task = try_take(next_id);
            if (task) {
                return task;
            }
        }
        return nullptr;
    }

    // TODO pipeline 1 add timeout interface, other queue may have new task
    PipelineTask* take(size_t core_id) { return _async_queue[core_id].take(); }

    void push_back(PipelineTask* task) {
        int core_id = task->get_previous_core_id();
        if (core_id < 0) {
            core_id = _next_core.fetch_add(1) % _core_size;
        }
        push_back(task, core_id);
    }

    void push_back(PipelineTask* task, size_t core_id) {
        DCHECK(core_id < _core_size);
        task->start_worker_watcher();
        _async_queue[core_id].push(task);
    }

    int cores() const { return _core_size; }

private:
    std::unique_ptr<WorkTaskQueue[]> _async_queue;
    size_t _core_size;
    std::atomic<size_t> _next_core = 0;
};

// TODO pipeline sr
class BlockedTaskScheduler {
public:
    explicit BlockedTaskScheduler(std::shared_ptr<TaskQueue> task_queue)
            : _task_queue(std::move(task_queue)), _started(false), _shutdown(false) {}

    ~BlockedTaskScheduler() = default;

    Status start();
    void shutdown();
    void add_blocked_task(PipelineTask* task);

private:
    std::shared_ptr<TaskQueue> _task_queue;

    std::mutex _task_mutex;
    std::condition_variable _task_cond;
    std::list<PipelineTask*> _blocked_tasks;

    scoped_refptr<Thread> _thread;
    std::atomic<bool> _started;
    std::atomic<bool> _shutdown;

    static constexpr auto EMPTY_TIMES_TO_YIELD = 64;

private:
    void _schedule();
    void _make_task_run(std::list<PipelineTask*>& local_tasks,
                        std::list<PipelineTask*>::iterator& task_itr,
                        std::vector<PipelineTask*>& ready_tasks,
                        PipelineTaskState state = PipelineTaskState::RUNNABLE);
};

class TaskScheduler {
public:
    TaskScheduler(ExecEnv* exec_env, std::shared_ptr<BlockedTaskScheduler> b_scheduler,
                  std::shared_ptr<TaskQueue> task_queue)
            : _task_queue(std::move(task_queue)),
              _exec_env(exec_env),
              _blocked_task_scheduler(std::move(b_scheduler)),
              _shutdown(false) {}

    ~TaskScheduler();

    Status schedule_task(PipelineTask* task);

    Status start();

    void shutdown();

    ExecEnv* exec_env() { return _exec_env; }

private:
    std::unique_ptr<ThreadPool> _fix_thread_pool;
    std::shared_ptr<TaskQueue> _task_queue;
    std::vector<std::unique_ptr<std::atomic<bool>>> _markers;
    ExecEnv* _exec_env;
    std::shared_ptr<BlockedTaskScheduler> _blocked_task_scheduler;
    std::atomic<bool> _shutdown;

private:
    void _do_work(size_t index);
    void _try_close_task(PipelineTask* task, PipelineTaskState state);
};
} // namespace doris::pipeline