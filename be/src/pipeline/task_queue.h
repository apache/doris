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

namespace doris::pipeline {

class TaskQueue {
public:
    TaskQueue(int core_size) : _core_size(core_size) {}
    virtual ~TaskQueue();
    virtual void close() = 0;
    // Get the task by core id.
    // TODO: To think the logic is useful?
    virtual PipelineTask* take(int core_id) = 0;

    // push from scheduler
    virtual Status push_back(PipelineTask* task) = 0;

    // push from worker
    virtual Status push_back(PipelineTask* task, int core_id) = 0;

    virtual void update_statistics(PipelineTask* task, int64_t time_spent) {}

    int cores() const { return _core_size; }

protected:
    int _core_size;
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

    void adjust_runtime(uint64_t vruntime) { this->_runtime = uint64_t(vruntime * _level_factor); }

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
    explicit MultiCoreTaskQueue(int core_size);

    ~MultiCoreTaskQueue() override;

    void close() override;

    // Get the task by core id.
    PipelineTask* take(int core_id) override;

    // TODO combine these methods to `push_back(task, core_id = -1)`
    Status push_back(PipelineTask* task) override;

    Status push_back(PipelineTask* task, int core_id) override;

    void update_statistics(PipelineTask* task, int64_t time_spent) override;

private:
    PipelineTask* _steal_take(
            int core_id, std::vector<std::unique_ptr<PriorityTaskQueue>>& prio_task_queue_list);

    std::shared_ptr<std::vector<std::unique_ptr<PriorityTaskQueue>>> _prio_task_queue_list;
    std::atomic<int> _next_core = 0;
    std::atomic<bool> _closed;
};

} // namespace doris::pipeline
