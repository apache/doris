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
#include <cassert>
#include <queue>
#include <utility>

#include "common/compiler_util.h"

namespace doris {

template <typename T, typename Sequence, typename Compare>
class SortingHeap {
public:
    SortingHeap(const Compare& comp) : _comp(comp) {}

    bool is_valid() const { return !_queue.empty(); }

    T top() { return _queue.front(); }

    size_t size() { return _queue.size(); }

    bool empty() { return _queue.empty(); }

    T& next_child() { return _queue[_next_child_index()]; }

    void replace_top(T new_top) {
        *_queue.begin() = new_top;
        update_top();
    }

    void remove_top() {
        std::pop_heap(_queue.begin(), _queue.end(), _comp);
        _queue.pop_back();
        _next_idx = 0;
    }

    void push(T cursor) {
        _queue.emplace_back(cursor);
        std::push_heap(_queue.begin(), _queue.end(), _comp);
        _next_idx = 0;
    }

    Sequence&& sorted_seq() {
        std::sort_heap(_queue.begin(), _queue.end(), _comp);
        return std::move(_queue);
    }

private:
    Sequence _queue;
    Compare _comp;

    /// Cache comparison between first and second child if the order in queue has not been changed.
    size_t _next_idx = 0;

    size_t _next_child_index() {
        if (_next_idx == 0) {
            _next_idx = 1;
            if (_queue.size() > 2 && _comp(_queue[1], _queue[2])) ++_next_idx;
        }

        return _next_idx;
    }

    void update_top() {
        size_t size = _queue.size();
        if (size < 2) return;

        auto begin = _queue.begin();

        size_t child_idx = _next_child_index();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        if (_comp(*child_it, *begin)) return;

        _next_idx = 0;

        auto curr_it = begin;
        auto top = *begin;
        do {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = *child_it;
            curr_it = child_it;

            // recompute the child based off of the updated parent
            child_idx = 2 * child_idx + 1;

            if (child_idx >= size) break;

            child_it = begin + child_idx;

            if ((child_idx + 1) < size && _comp(*child_it, *(child_it + 1))) {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

        /// Check if we are in order.
        } while (!(_comp(*child_it, top)));
        *curr_it = top;
    }
};
} // namespace doris
