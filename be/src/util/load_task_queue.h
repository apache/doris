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

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <unordered_map>
#include <utility>
#include <vector>

namespace doris {

// One-task round robin between ready loads, strict priority within each load.
// The caller serializes push/pop/erase/remove with the same lock.
// Only empty-to-nonempty transitions put a load in _ready_loads. There is no
// per-load concurrency cap. Push/pop are
// amortized O(1), as is erasing a task using its handle.
template <typename T>
class LoadTaskQueue {
public:
    static constexpr size_t NUM_PRIORITIES = 4;

    LoadTaskQueue() = default;
    // Ready positions refer to this queue's list and must not be copied.
    LoadTaskQueue(const LoadTaskQueue&) = delete;
    LoadTaskQueue& operator=(const LoadTaskQueue&) = delete;

    // Valid until this entry is popped or erased. Other entries retain their handles.
    struct Handle {
        int64_t load_id;
        size_t priority;
        typename std::list<T>::iterator position;
    };

    Handle push(int64_t load_id, size_t priority, T task) {
        assert(priority < NUM_PRIORITIES);
        auto [it, inserted] = _loads.try_emplace(load_id);
        if (inserted) {
            it->second.ready_position = _ready_loads.insert(_ready_loads.end(), load_id);
        }
        auto& queue = it->second.queues[priority];
        auto position = queue.insert(queue.end(), std::move(task));
        ++_size;
        return {load_id, priority, position};
    }

    T pop() {
        assert(!empty());
        auto load_id = _ready_loads.front();
        auto it = _loads.find(load_id);
        auto& queues = it->second.queues;
        size_t p = 0;
        while (queues[p].empty()) {
            ++p;
        }
        T task = std::move(queues[p].front());
        queues[p].pop_front();
        --_size;
        if (queues_empty(queues)) {
            _ready_loads.pop_front();
            _loads.erase(it);
        } else {
            // Rotate before the caller executes the task, without reallocating
            // the ready entry or waiting for a running task of this load.
            _ready_loads.splice(_ready_loads.end(), _ready_loads, _ready_loads.begin());
        }
        return task;
    }

    // Remove an already selected task without consuming another load's turn.
    void erase(const Handle& handle) {
        auto it = _loads.find(handle.load_id);
        assert(it != _loads.end());
        it->second.queues[handle.priority].erase(handle.position);
        --_size;
        if (queues_empty(it->second.queues)) {
            _ready_loads.erase(it->second.ready_position);
            _loads.erase(it);
        }
    }

    // Return removed tasks so owners can destroy callbacks outside their lock.
    template <typename Predicate>
    std::vector<T> remove_if(int64_t load_id, Predicate predicate) {
        std::vector<T> removed;
        auto it = _loads.find(load_id);
        if (it == _loads.end()) {
            return removed; // The token may have only running tasks.
        }
        for (auto& queue : it->second.queues) {
            for (auto entry = queue.begin(); entry != queue.end();) {
                if (predicate(*entry)) {
                    removed.push_back(std::move(*entry));
                    entry = queue.erase(entry);
                    --_size;
                } else {
                    ++entry;
                }
            }
        }
        if (queues_empty(it->second.queues)) {
            _ready_loads.erase(it->second.ready_position);
            _loads.erase(it);
        }
        return removed;
    }

    bool empty() const { return _size == 0; }
    size_t size() const { return _size; }

private:
    using Queues = std::array<std::list<T>, NUM_PRIORITIES>;
    static bool queues_empty(const Queues& queues) {
        return std::all_of(queues.begin(), queues.end(), [](const auto& q) { return q.empty(); });
    }

    struct Load {
        Queues queues;
        std::list<int64_t>::iterator ready_position;
    };

    std::unordered_map<int64_t, Load> _loads;
    std::list<int64_t> _ready_loads;
    size_t _size = 0;
};

} // namespace doris
