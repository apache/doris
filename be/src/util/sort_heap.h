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

template <typename T, typename _Sequence, typename _Compare>
class SortingHeap {
public:
    SortingHeap(const _Compare& comp) : _comp(comp) {}

    bool isValid() const { return !_queue.empty(); }

    T& current() { return _queue.front(); }

    size_t size() { return _queue.size(); }

    bool empty() { return _queue.empty(); }

    T& nextChild() { return _queue[nextChildIndex()]; }

    void replaceTop(T new_top) {
        current() = new_top;
        updateTop();
    }

    void removeTop() {
        std::pop_heap(_queue.begin(), _queue.end(), _comp);
        _queue.pop_back();
        next_idx = 0;
    }

    void push(T cursor) {
        _queue.emplace_back(cursor);
        std::push_heap(_queue.begin(), _queue.end(), _comp);
        next_idx = 0;
    }

    _Sequence&& sorted_seq() {
        std::sort_heap(_queue.begin(), _queue.end(), _comp);
        return std::move(_queue);
    }

private:
    _Sequence _queue;
    _Compare _comp;

    /// Cache comparison between first and second child if the order in queue has not been changed.
    size_t next_idx = 0;

    size_t nextChildIndex() {
        if (next_idx == 0) {
            next_idx = 1;
            if (_queue.size() > 2 && _comp(_queue[1], _queue[2])) ++next_idx;
        }

        return next_idx;
    }

    void updateTop() {
        size_t size = _queue.size();
        if (size < 2) return;

        auto begin = _queue.begin();

        size_t child_idx = nextChildIndex();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        //if (*child_it < *begin) return;
        if (_comp(*child_it, *begin)) return;

        next_idx = 0;

        auto curr_it = begin;
        auto top(std::move(*begin));
        do {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = std::move(*child_it);
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
            //} while (!(*child_it < top));
        } while (!(_comp(*child_it, top)));
        *curr_it = std::move(top);
    }
};
} // namespace doris
