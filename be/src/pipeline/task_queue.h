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
namespace pipeline {

class SubWorkTaskQueue {
    friend class WorkTaskQueue;

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
class WorkTaskQueue {
public:
    explicit WorkTaskQueue();

    void close();

    PipelineTask* try_take_unprotected(bool is_steal);

    PipelineTask* try_take(bool is_steal);

    PipelineTask* take(uint32_t timeout_ms = 0);

    Status push(PipelineTask* task);

    // Get the each thread task size to do
    size_t size() { return _total_task_size; }

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
class TaskQueue {
public:
    explicit TaskQueue(size_t core_size) : _core_size(core_size), _closed(false) {
        _async_queue.reset(new WorkTaskQueue[core_size]);
    }

    ~TaskQueue() = default;

    void close();

    // Get the task by core id.
    // TODO: To think the logic is useful?
    PipelineTask* try_take(size_t core_id);

    PipelineTask* steal_take(size_t core_id);

    // TODO combine these methods to `push_back(task, core_id = -1)`
    Status push_back(PipelineTask* task);

    Status push_back(PipelineTask* task, size_t core_id);

    int cores() const { return _core_size; }

private:
    std::unique_ptr<WorkTaskQueue[]> _async_queue;
    size_t _core_size;
    std::atomic<size_t> _next_core = 0;
    std::atomic<bool> _closed;
    static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;
};

} // namespace pipeline
} // namespace doris
