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

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <list>
#include <utility>
#include <vector>

namespace doris {

// Strict priority across all loads, FIFO within each priority. The caller serializes
// push/pop/erase/remove with the same lock. Running tasks are not preempted.
// Tokens retain their own completion/cancellation boundaries; load IDs do not affect dispatch.
template <typename T>
class LoadTaskQueue {
private:
    struct Entry {
        int64_t load_id;
        T task;
    };

public:
    static constexpr size_t NUM_PRIORITIES = 4;

    LoadTaskQueue() = default;
    // Handles refer to this queue's lists and must not be copied.
    LoadTaskQueue(const LoadTaskQueue&) = delete;
    LoadTaskQueue& operator=(const LoadTaskQueue&) = delete;

    // Valid until this entry is popped or erased. Other entries retain their handles.
    struct Handle {
        size_t priority;
        typename std::list<Entry>::iterator position;
    };

    Handle push(int64_t load_id, size_t priority, T task) {
        assert(priority < NUM_PRIORITIES);
        auto& queue = _queues[priority];
        auto position = queue.insert(queue.end(), Entry {load_id, std::move(task)});
        ++_size;
        return {priority, position};
    }

    T pop() {
        assert(!empty());
        size_t priority = 0;
        while (_queues[priority].empty()) {
            ++priority;
        }
        auto& queue = _queues[priority];
        T task = std::move(queue.front().task);
        queue.pop_front();
        --_size;
        return task;
    }

    // Workers, helpers and token cancellation remove the selected task in O(1).
    void erase(const Handle& handle) {
        _queues[handle.priority].erase(handle.position);
        --_size;
    }

    // Return removed tasks so owners can destroy callbacks outside the pool lock.
    // This bulk operation scans the queue; token cancellation uses erase() handles instead.
    template <typename Predicate>
    std::vector<T> remove_if(int64_t load_id, Predicate predicate) {
        std::vector<T> removed;
        for (auto& queue : _queues) {
            for (auto entry = queue.begin(); entry != queue.end();) {
                if (entry->load_id == load_id && predicate(entry->task)) {
                    removed.push_back(std::move(entry->task));
                    entry = queue.erase(entry);
                    --_size;
                } else {
                    ++entry;
                }
            }
        }
        return removed;
    }

    bool empty() const { return _size == 0; }
    size_t size() const { return _size; }

private:
    std::array<std::list<Entry>, NUM_PRIORITIES> _queues;
    size_t _size = 0;
};

} // namespace doris
