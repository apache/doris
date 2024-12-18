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

#include "common/cast_set.h"
#include "common/status.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

class PipelineTask;

enum class TaskState {
    VALID = 0,   // Valid task which is not running
    RUNNING = 1, // Valid task which is executed by a thread
    INVALID = 2, // Invalid task which already de-constructed.
};

struct TaskHolder : public std::enable_shared_from_this<TaskHolder> {
    PipelineTask* task;
    std::atomic<TaskState> state;
    TaskHolder(PipelineTask* task_);
};

using TaskHolderSPtr = std::shared_ptr<TaskHolder>;

class SubTaskQueue {
    friend class PriorityTaskQueue;

public:
    void push_back(TaskHolderSPtr task) { _queue.emplace(task); }

    TaskHolderSPtr try_take(bool is_steal);

    void set_level_factor(double level_factor) { _level_factor = level_factor; }

    // note:
    // runtime is the time consumed by the actual execution of the task
    // vruntime(means virtual runtime) = runtime / _level_factor
    double get_vruntime() { return double(_runtime) / _level_factor; }

    void inc_runtime(uint64_t delta_time) { _runtime += delta_time; }

    void adjust_runtime(uint64_t vruntime) {
        this->_runtime = uint64_t(double(vruntime) * _level_factor);
    }

    bool empty() { return _queue.empty(); }

private:
    std::queue<TaskHolderSPtr> _queue;
    // depends on LEVEL_QUEUE_TIME_FACTOR
    double _level_factor = 1;

    std::atomic<uint64_t> _runtime = 0;
};

// A Multilevel Feedback Queue
class PriorityTaskQueue {
public:
    PriorityTaskQueue();

    void close();

    TaskHolderSPtr try_take(bool is_steal);

    TaskHolderSPtr take(uint32_t timeout_ms = 0);

    Status push(TaskHolderSPtr task);

    void inc_sub_queue_runtime(int level, uint64_t runtime) {
        _sub_queues[level].inc_runtime(runtime);
    }

private:
    TaskHolderSPtr _try_take_unprotected(bool is_steal);
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
class MultiCoreTaskQueue {
public:
    explicit MultiCoreTaskQueue(int core_size);

    ~MultiCoreTaskQueue();

    void close();

    // Get the task by core id.
    TaskHolderSPtr take(int core_id);

    // TODO combine these methods to `push_back(task, core_id = -1)`
    Status push_back(TaskHolderSPtr task);

    Status push_back(TaskHolderSPtr, int core_id);

    void update_statistics(PipelineTask* task, int64_t time_spent);

    int num_queues() const { return cast_set<int>(_prio_task_queues.size()); }

private:
    TaskHolderSPtr _steal_take(int core_id);

    std::vector<PriorityTaskQueue> _prio_task_queues;
    std::atomic<uint32_t> _next_core = 0;
    std::atomic<bool> _closed;

    const int _core_size;
    const int _urgent_queue_idx;
    static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;
};
#include "common/compile_check_end.h"
} // namespace doris::pipeline
