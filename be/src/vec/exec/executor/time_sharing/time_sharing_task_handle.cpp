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

#include "vec/exec/executor/time_sharing/time_sharing_task_handle.h"

namespace doris {
namespace vectorized {
#include "common/compile_check_begin.h"

TimeSharingTaskHandle::TimeSharingTaskHandle(
        const TaskId& task_id, std::shared_ptr<SplitQueue> split_queue,
        std::function<double()> utilization_supplier, int initial_split_concurrency,
        std::chrono::nanoseconds split_concurrency_adjust_frequency,
        std::optional<int> max_concurrency_per_task)
        : _task_id(task_id),
          _split_queue(std::move(split_queue)),
          _utilization_supplier(std::move(utilization_supplier)),
          _max_concurrency_per_task(max_concurrency_per_task),
          _concurrency_controller(initial_split_concurrency, split_concurrency_adjust_frequency) {}

Status TimeSharingTaskHandle::init() {
    return Status::OK();
}

Priority TimeSharingTaskHandle::add_scheduled_nanos(int64_t duration_nanos) {
    std::lock_guard<std::mutex> lock(_mutex);
    _concurrency_controller.update(duration_nanos, _utilization_supplier(),
                                   static_cast<int>(_running_leaf_splits.size()));
    _scheduled_nanos += duration_nanos;

    Priority new_priority =
            _split_queue->update_priority(_priority, duration_nanos, _scheduled_nanos);

    _priority = new_priority;
    return new_priority;
}

Priority TimeSharingTaskHandle::reset_level_priority() {
    std::lock_guard<std::mutex> lock(_mutex);

    Priority current_priority = _priority;
    int64_t level_min_priority =
            _split_queue->get_level_min_priority(current_priority.level(), _scheduled_nanos);

    if (current_priority.level_priority() < level_min_priority) {
        Priority new_priority(current_priority.level(), level_min_priority);
        _priority = new_priority;
        return new_priority;
    }

    return current_priority;
}

bool TimeSharingTaskHandle::is_closed() const {
    return _closed.load();
}

Priority TimeSharingTaskHandle::priority() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _priority;
}

TaskId TimeSharingTaskHandle::task_id() const {
    return _task_id;
}

std::optional<int> TimeSharingTaskHandle::max_concurrency_per_task() const {
    return _max_concurrency_per_task;
}

std::vector<std::shared_ptr<PrioritizedSplitRunner>> TimeSharingTaskHandle::close() {
    std::lock_guard<std::mutex> lock(_mutex);

    if (_closed.exchange(true)) {
        return {};
    }

    std::vector<std::shared_ptr<PrioritizedSplitRunner>> result;
    result.reserve(_running_intermediate_splits.size() + _running_leaf_splits.size() +
                   _queued_leaf_splits.size());

    for (auto& kv : _running_intermediate_splits) {
        result.push_back(kv.second);
    }
    for (auto& kv : _running_leaf_splits) {
        result.push_back(kv.second);
    }

    while (!_queued_leaf_splits.empty()) {
        result.push_back(_queued_leaf_splits.front());
        _queued_leaf_splits.pop();
    }

    _running_intermediate_splits.clear();
    _running_leaf_splits.clear();

    return result;
}

bool TimeSharingTaskHandle::enqueue_split(std::shared_ptr<PrioritizedSplitRunner> split) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_closed) {
        return false;
    }
    _queued_leaf_splits.emplace(std::move(split));
    return true;
}

bool TimeSharingTaskHandle::record_intermediate_split(
        std::shared_ptr<PrioritizedSplitRunner> split) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_closed) {
        return false;
    }
    _running_intermediate_splits[split->split_runner()] = split;
    return true;
}

int TimeSharingTaskHandle::running_leaf_splits() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return static_cast<int>(_running_leaf_splits.size());
}

int64_t TimeSharingTaskHandle::scheduled_nanos() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return _scheduled_nanos;
}

std::shared_ptr<PrioritizedSplitRunner> TimeSharingTaskHandle::poll_next_split() {
    std::lock_guard<std::mutex> lock(_mutex);

    if (_closed) {
        return nullptr;
    }

    if (_running_leaf_splits.size() >= _concurrency_controller.target_concurrency()) {
        return nullptr;
    }

    if (_queued_leaf_splits.empty()) {
        return nullptr;
    }

    auto split = _queued_leaf_splits.front();
    _queued_leaf_splits.pop();
    _running_leaf_splits[split->split_runner()] = split;
    return split;
}

void TimeSharingTaskHandle::split_finished(std::shared_ptr<PrioritizedSplitRunner> split) {
    std::lock_guard<std::mutex> lock(_mutex);
    _concurrency_controller.split_finished(split->scheduled_nanos(), _utilization_supplier(),
                                           static_cast<int>(_running_leaf_splits.size()));

    _running_intermediate_splits.erase(split->split_runner());
    _running_leaf_splits.erase(split->split_runner());
}

int TimeSharingTaskHandle::next_split_id() {
    return _next_split_id.fetch_add(1);
}

std::shared_ptr<PrioritizedSplitRunner> TimeSharingTaskHandle::get_split(
        std::shared_ptr<SplitRunner> split, bool intermediate) const {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!intermediate) {
        auto it = _running_leaf_splits.find(split);
        if (it != _running_leaf_splits.end()) {
            return it->second;
        }
        return nullptr;
    } else {
        auto it = _running_intermediate_splits.find(split);
        if (it != _running_intermediate_splits.end()) {
            return it->second;
        }
        return nullptr;
    }
}

} // namespace vectorized
} // namespace doris
