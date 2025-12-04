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

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <vector>

#include "vec/exec/executor/task_handle.h"
#include "vec/exec/executor/task_id.h"
#include "vec/exec/executor/time_sharing/prioritized_split_runner.h"
#include "vec/exec/executor/time_sharing/priority.h"
#include "vec/exec/executor/time_sharing/split_concurrency_controller.h"
#include "vec/exec/executor/time_sharing/split_queue.h"

namespace doris {
namespace vectorized {

class MultilevelSplitQueue;

class TimeSharingTaskHandle : public TaskHandle {
    ENABLE_FACTORY_CREATOR(TimeSharingTaskHandle);

public:
    TimeSharingTaskHandle(const TaskId& task_id, std::shared_ptr<SplitQueue> split_queue,
                          std::function<double()> utilization_supplier,
                          int initial_split_concurrency,
                          std::chrono::nanoseconds split_concurrency_adjust_frequency,
                          std::optional<int> max_concurrency_per_task);

    Status init() override;

    Priority add_scheduled_nanos(int64_t duration_nanos);
    Priority reset_level_priority();
    bool is_closed() const override;
    Priority priority() const;
    TaskId task_id() const override;
    std::optional<int> max_concurrency_per_task() const;
    std::vector<std::shared_ptr<PrioritizedSplitRunner>> close();
    bool enqueue_split(std::shared_ptr<PrioritizedSplitRunner> split);
    bool record_intermediate_split(std::shared_ptr<PrioritizedSplitRunner> split);
    int running_leaf_splits() const;
    int64_t scheduled_nanos() const;
    std::shared_ptr<PrioritizedSplitRunner> poll_next_split();
    void split_finished(std::shared_ptr<PrioritizedSplitRunner> split);
    int next_split_id();
    std::shared_ptr<PrioritizedSplitRunner> get_split(std::shared_ptr<SplitRunner> split,
                                                      bool intermediate) const;

private:
    mutable std::mutex _mutex;
    std::atomic<bool> _closed {false};
    const TaskId _task_id;
    std::shared_ptr<SplitQueue> _split_queue;
    std::function<double()> _utilization_supplier;
    std::optional<int> _max_concurrency_per_task;
    SplitConcurrencyController _concurrency_controller;

    std::queue<std::shared_ptr<PrioritizedSplitRunner>> _queued_leaf_splits;
    //std::vector<std::shared_ptr<PrioritizedSplitRunner>> _running_leaf_splits;
    //std::vector<std::shared_ptr<PrioritizedSplitRunner>> _running_intermediate_splits;
    std::unordered_map<std::shared_ptr<SplitRunner>, std::shared_ptr<PrioritizedSplitRunner>>
            _running_leaf_splits;
    std::unordered_map<std::shared_ptr<SplitRunner>, std::shared_ptr<PrioritizedSplitRunner>>
            _running_intermediate_splits;
    int64_t _scheduled_nanos {0};
    std::atomic<int> _next_split_id {0};
    //    std::atomic<Priority> _priority {Priority(0, 0)};
    Priority _priority {0, 0};
};

} // namespace vectorized
} // namespace doris
