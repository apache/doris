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
#include <deque>
#include <utility>
#include <vector>

namespace doris {

// Strict priority across all foreground load tasks in a resource domain, with
// FIFO dispatch within each priority. The caller serializes push/pop/remove.
template <typename T>
class LoadTaskQueue {
public:
    static constexpr size_t NUM_PRIORITIES = 4;

    void push(size_t priority, T task) {
        assert(priority < NUM_PRIORITIES);
        _queues[priority].push_back(std::move(task));
        ++_size;
    }

    T pop() {
        assert(!empty());
        size_t p = 0;
        while (_queues[p].empty()) {
            ++p;
        }
        T task = std::move(_queues[p].front());
        _queues[p].pop_front();
        --_size;
        return task;
    }

    // Return removed tasks so owners can destroy callbacks outside their lock.
    template <typename Predicate>
    std::vector<T> remove_if(Predicate predicate) {
        std::vector<T> removed;
        for (auto& queue : _queues) {
            auto end = std::remove_if(queue.begin(), queue.end(), [&](T& task) {
                if (!predicate(task)) {
                    return false;
                }
                removed.push_back(std::move(task));
                --_size;
                return true;
            });
            queue.erase(end, queue.end());
        }
        return removed;
    }

    bool empty() const { return _size == 0; }
    size_t size() const { return _size; }

private:
    std::array<std::deque<T>, NUM_PRIORITIES> _queues;
    size_t _size = 0;
};

} // namespace doris
