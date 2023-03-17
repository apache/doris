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

#include "pipeline_task.h"

namespace doris {
namespace taskgroup {
class TaskGroup;
}

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

    int cores() const { return _core_size; }

protected:
    size_t _core_size;
    static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;
};

class SubWorkTaskQueue {
    friend class WorkTaskQueue;
    friend class NormalWorkTaskQueue;

public:
    void push_back(PipelineTask* task) { _queue.emplace(task); }

    PipelineTask* try_take(bool is_steal);

    void set_factor_for_normal(double factor_for_normal) { _factor_for_normal = factor_for_normal; }

    double schedule_time_after_normal() { return _schedule_time * _factor_for_normal; }

    bool empty() { return _queue.empty(); }

private:
    std::queue<PipelineTask*> _queue;
    // factor for normalization
    double _factor_for_normal = 1;
    // the value cal the queue task time consume, the WorkTaskQueue
    // use it to find the min queue to take task work
    std::atomic<uint64_t> _schedule_time = 0;
};

// Each thread have private MLFQ
class NormalWorkTaskQueue {
public:
    explicit NormalWorkTaskQueue();

    void close();

    PipelineTask* try_take_unprotected(bool is_steal);

    PipelineTask* try_take(bool is_steal);

    PipelineTask* take(uint32_t timeout_ms = 0);

    Status push(PipelineTask* task);

private:
    static constexpr auto LEVEL_QUEUE_TIME_FACTOR = 1.5;
    static constexpr size_t SUB_QUEUE_LEVEL = 5;
    // 3, 6, 9, 12
    static constexpr uint32_t BASE_LIMIT = 3;
    SubWorkTaskQueue _sub_queues[SUB_QUEUE_LEVEL];
    uint32_t _task_schedule_limit[SUB_QUEUE_LEVEL - 1];
    std::mutex _work_size_mutex;
    std::condition_variable _wait_task;
    std::atomic<size_t> _total_task_size = 0;
    bool _closed;

    int _compute_level(PipelineTask* task);
};

// Need consider NUMA architecture
class NormalTaskQueue : public TaskQueue {
public:
    explicit NormalTaskQueue(size_t core_size);

    ~NormalTaskQueue() override;

    void close() override;

    // Get the task by core id.
    // TODO: To think the logic is useful?
    PipelineTask* take(size_t core_id) override;

    // TODO combine these methods to `push_back(task, core_id = -1)`
    Status push_back(PipelineTask* task) override;

    Status push_back(PipelineTask* task, size_t core_id) override;

    // TODO pipeline update NormalWorkTaskQueue by time_spent.
    // void update_statistics(PipelineTask* task, int64_t time_spent) override;

private:
    PipelineTask* _steal_take(size_t core_id);

    std::unique_ptr<NormalWorkTaskQueue[]> _async_queue;
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

private:
    template <bool from_executor>
    Status _push_back(PipelineTask* task);
    template <bool from_worker>
    void _enqueue_task_group(taskgroup::TGEntityPtr);
    void _dequeue_task_group(taskgroup::TGEntityPtr);
    taskgroup::TGEntityPtr _next_tg_entity();
    int64_t _ideal_runtime_ns(taskgroup::TGEntityPtr tg_entity) const;
    void _update_min_tg();

    // Like cfs rb tree in sched_entity
    struct TaskGroupSchedEntityComparator {
        bool operator()(const taskgroup::TGEntityPtr&, const taskgroup::TGEntityPtr&) const;
    };
    using ResouceGroupSet = std::set<taskgroup::TGEntityPtr, TaskGroupSchedEntityComparator>;
    ResouceGroupSet _group_entities;
    std::condition_variable _wait_task;
    std::mutex _rs_mutex;
    bool _closed = false;
    int _total_cpu_share = 0;
    std::atomic<taskgroup::TGEntityPtr> _min_tg_entity = nullptr;
    uint64_t _min_tg_v_runtime_ns = 0;
};

} // namespace pipeline
} // namespace doris
