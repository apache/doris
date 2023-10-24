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

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <set>

#include "common/status.h"
#include "pipeline_task.h"
#include "runtime/task_group/task_group.h"

namespace doris {
namespace pipeline {

class TaskQueue {
public:
    TaskQueue(size_t core_size) : _core_size(core_size) {}
    virtual ~TaskQueue();
    virtual void close() = 0;
    // Get the task by core id.
    // TODO: To think the logic is useful?
    virtual PipelineTask* take(size_t core_id) = 0;

    // push from scheduler
    virtual Status push_back(PipelineTask* task) = 0;

    // push from worker
    virtual Status push_back(PipelineTask* task, size_t core_id) = 0;

    virtual void update_statistics(PipelineTask* task, int64_t time_spent) {}

    virtual void update_tg_cpu_share(const taskgroup::TaskGroupInfo& task_group_info,
                                     taskgroup::TGPTEntityPtr entity) = 0;

    int cores() const { return _core_size; }

protected:
    size_t _core_size;
    static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;
};

class SubTaskQueue {
    friend class PriorityTaskQueue;

public:
    void push_back(PipelineTask* task) { _queue.emplace(task); }

    PipelineTask* try_take(bool is_steal);

    void set_level_factor(double level_factor) { _level_factor = level_factor; }

    // note:
    // runtime is the time consumed by the actual execution of the task
    // vruntime(means virtual runtime) = runtime / _level_factor
    double get_vruntime() { return _runtime / _level_factor; }

    void inc_runtime(uint64_t delta_time) { _runtime += delta_time; }

    void adjust_runtime(uint64_t vruntime) { this->_runtime = vruntime * _level_factor; }

    bool empty() { return _queue.empty(); }

private:
    std::queue<PipelineTask*> _queue;
    // depends on LEVEL_QUEUE_TIME_FACTOR
    double _level_factor = 1;

    std::atomic<uint64_t> _runtime = 0;
};

// A Multilevel Feedback Queue
class PriorityTaskQueue {
public:
    PriorityTaskQueue();

    void close();

    PipelineTask* try_take(bool is_steal);

    PipelineTask* take(uint32_t timeout_ms = 0);

    Status push(PipelineTask* task);

    void inc_sub_queue_runtime(int level, uint64_t runtime) {
        _sub_queues[level].inc_runtime(runtime);
    }

    int task_size();

private:
    PipelineTask* _try_take_unprotected(bool is_steal);
    static constexpr auto LEVEL_QUEUE_TIME_FACTOR = 2;
    static constexpr size_t SUB_QUEUE_LEVEL = 6;
    SubTaskQueue _sub_queues[SUB_QUEUE_LEVEL];
    // 1s, 3s, 10s, 60s, 300s
    uint64_t _queue_level_limit[SUB_QUEUE_LEVEL - 1] = {1000000000, 3000000000, 10000000000,
                                                        60000000000, 300000000000};
    std::mutex _work_size_mutex;
    std::condition_variable _wait_task;
    std::atomic<size_t> _total_task_size = 0;
    bool _closed;

    // used to adjust vruntime of a queue when it's not empty
    // protected by lock _work_size_mutex
    uint64_t _queue_level_min_vruntime = 0;

    int _compute_level(uint64_t real_runtime);
};

// Need consider NUMA architecture
class MultiCoreTaskQueue : public TaskQueue {
public:
    explicit MultiCoreTaskQueue(size_t core_size);

    ~MultiCoreTaskQueue() override;

    void close() override;

    // Get the task by core id.
    // TODO: To think the logic is useful?
    PipelineTask* take(size_t core_id) override;

    // TODO combine these methods to `push_back(task, core_id = -1)`
    Status push_back(PipelineTask* task) override;

    Status push_back(PipelineTask* task, size_t core_id) override;

    void update_statistics(PipelineTask* task, int64_t time_spent) override {
        task->inc_runtime_ns(time_spent);
        _prio_task_queue_list[task->get_core_id()].inc_sub_queue_runtime(task->get_queue_level(),
                                                                         time_spent);
    }

    void update_tg_cpu_share(const taskgroup::TaskGroupInfo& task_group_info,
                             taskgroup::TGPTEntityPtr entity) override {
        LOG(FATAL) << "update_tg_cpu_share not implemented";
    }

private:
    PipelineTask* _steal_take(size_t core_id);

    std::unique_ptr<PriorityTaskQueue[]> _prio_task_queue_list;
    std::atomic<size_t> _next_core = 0;
    std::atomic<bool> _closed;
};

class TaskGroupTaskQueue : public TaskQueue {
public:
    explicit TaskGroupTaskQueue(size_t);
    ~TaskGroupTaskQueue() override;

    void close() override;

    PipelineTask* take(size_t core_id) override;

    // from TaskScheduler or BlockedTaskScheduler
    Status push_back(PipelineTask* task) override;

    // from worker
    Status push_back(PipelineTask* task, size_t core_id) override;

    void update_statistics(PipelineTask* task, int64_t time_spent) override;

    void update_tg_cpu_share(const taskgroup::TaskGroupInfo& task_group_info,
                             taskgroup::TGPTEntityPtr entity) override;

    void reset_empty_group_entity();

private:
    template <bool from_executor>
    Status _push_back(PipelineTask* task);
    template <bool from_worker>
    void _enqueue_task_group(taskgroup::TGPTEntityPtr);
    void _dequeue_task_group(taskgroup::TGPTEntityPtr);
    taskgroup::TGPTEntityPtr _next_tg_entity();
    uint64_t _ideal_runtime_ns(taskgroup::TGPTEntityPtr tg_entity) const;
    void _update_min_tg();

    // Like cfs rb tree in sched_entity
    struct TaskGroupSchedEntityComparator {
        bool operator()(const taskgroup::TGPTEntityPtr&, const taskgroup::TGPTEntityPtr&) const;
    };
    using ResouceGroupSet = std::set<taskgroup::TGPTEntityPtr, TaskGroupSchedEntityComparator>;
    ResouceGroupSet _group_entities;
    std::condition_variable _wait_task;
    std::mutex _rs_mutex;
    bool _closed = false;
    int _total_cpu_share = 0;
    std::atomic<taskgroup::TGPTEntityPtr> _min_tg_entity = nullptr;
    uint64_t _min_tg_v_runtime_ns = 0;

    // empty group
    taskgroup::TaskGroupEntity<std::queue<pipeline::PipelineTask*>>* _empty_group_entity =
            new taskgroup::TaskGroupEntity<std::queue<pipeline::PipelineTask*>>();
    PipelineTask* _empty_pip_task = new PipelineTask();
    // todo(wb) support auto-switch cpu mode between soft limit and hard limit
    bool _enable_cpu_hard_limit = config::enable_cpu_hard_limit;
};

} // namespace pipeline
} // namespace doris
