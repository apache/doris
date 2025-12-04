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

#include <condition_variable>
#include <mutex>
#include <queue>
#include <unordered_set>

#include "vec/exec/executor/time_sharing/split_queue.h"

namespace doris {
namespace vectorized {

class SimulationFIFOSplitQueue : public SplitQueue {
public:
    SimulationFIFOSplitQueue() = default;
    ~SimulationFIFOSplitQueue() override = default;

    int compute_level(int64_t scheduled_nanos) override { return 0; }

    void offer(std::shared_ptr<PrioritizedSplitRunner> split) override { _queue.push(split); }

    std::shared_ptr<PrioritizedSplitRunner> take() override {
        if (_queue.empty()) return nullptr;
        auto split = _queue.front();
        _queue.pop();
        return split;
    }

    size_t size() const override { return _queue.size(); }

    void remove(std::shared_ptr<PrioritizedSplitRunner> split) override {
        std::queue<std::shared_ptr<PrioritizedSplitRunner>> new_queue;
        while (!_queue.empty()) {
            auto current = _queue.front();
            _queue.pop();
            if (current != split) {
                new_queue.emplace(std::move(current));
            }
        }
        _queue.swap(new_queue);
    }

    void remove_all(const std::vector<std::shared_ptr<PrioritizedSplitRunner>>& splits) override {
        std::unordered_set<std::shared_ptr<PrioritizedSplitRunner>> to_remove(splits.begin(),
                                                                              splits.end());
        std::queue<std::shared_ptr<PrioritizedSplitRunner>> new_queue;
        while (!_queue.empty()) {
            auto current = _queue.front();
            _queue.pop();
            if (!to_remove.count(current)) {
                new_queue.emplace(std::move(current));
            }
        }
        _queue.swap(new_queue);
    }

    void clear() override {
        while (!_queue.empty()) {
            _queue.pop();
        }
    }

    Priority update_priority(const Priority& old_priority, int64_t quanta_nanos,
                             int64_t scheduled_nanos) override {
        return old_priority;
    }

    int64_t get_level_min_priority(int level, int64_t task_thread_usage_nanos) override {
        return std::numeric_limits<int64_t>::max();
    }

private:
    std::queue<std::shared_ptr<PrioritizedSplitRunner>> _queue;
};

} // namespace vectorized
} // namespace doris
