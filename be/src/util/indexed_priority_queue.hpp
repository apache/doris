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
// This file is porting from
// https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/execution/resourcegroups/IndexedPriorityQueue.java
// to cpp and modified by Doris

#pragma once

#pragma once

#include <functional>
#include <iostream>
#include <map>
#include <optional>
#include <set>

/**
 * A priority queue with constant time contains(E) and log time remove(E)
 * Ties are broken by insertion order.
 * LOW_TO_HIGH is the priority order from low to high,
 * HIGH_TO_LOW is the priority order from high to low.
 * Those with the same priority are arranged in order of insertion.
 */

namespace doris {

template <typename T>
struct IndexedPriorityQueueEntry {
    T value;
    long priority;
    long generation;

    IndexedPriorityQueueEntry(T val, long prio, long gen)
            : value(std::move(val)), priority(prio), generation(gen) {}
};

enum class IndexedPriorityQueuePriorityOrdering { LOW_TO_HIGH, HIGH_TO_LOW };

template <typename T, IndexedPriorityQueuePriorityOrdering priority_ordering>
struct IndexedPriorityQueueComparator {
    bool operator()(const IndexedPriorityQueueEntry<T>& lhs,
                    const IndexedPriorityQueueEntry<T>& rhs) const {
        if constexpr (priority_ordering == IndexedPriorityQueuePriorityOrdering::LOW_TO_HIGH) {
            if (lhs.priority != rhs.priority) {
                return lhs.priority < rhs.priority;
            }
            return lhs.generation < rhs.generation;
        } else {
            if (lhs.priority != rhs.priority) {
                return lhs.priority > rhs.priority;
            }
            return lhs.generation < rhs.generation;
        }
    }
};

template <typename T, IndexedPriorityQueuePriorityOrdering priority_ordering =
                              IndexedPriorityQueuePriorityOrdering::HIGH_TO_LOW>
class IndexedPriorityQueue {
public:
    struct Prioritized {
        T value;
        long priority;
    };

    IndexedPriorityQueue() = default;

    bool add_or_update(T element, long priority) {
        auto it = _index.find(element);
        if (it != _index.end()) {
            if (it->second.priority == priority) {
                return false;
            }
            _queue.erase(it->second);
        }
        IndexedPriorityQueueEntry<T> entry {std::move(element), priority, generation++};
        _queue.insert(std::move(entry));
        _index.insert({entry.value, std::move(entry)});
        return true;
    }

    bool contains(const T& element) const { return _index.find(element) != _index.end(); }

    bool remove(const T& element) {
        auto it = _index.find(element);
        if (it != _index.end()) {
            _queue.erase(it->second);
            _index.erase(it);
            return true;
        }
        return false;
    }

    std::optional<T> poll() {
        if (_queue.empty()) {
            return std::nullopt;
        }
        T value = _queue.begin()->value;
        _index.erase(value);
        _queue.erase(_queue.begin());
        return value;
    }

    std::optional<Prioritized> peek() const {
        if (_queue.empty()) {
            return std::nullopt;
        }
        const IndexedPriorityQueueEntry<T>& entry = *_queue.begin();
        return Prioritized {entry.value, entry.priority};
    }

    int size() const { return _queue.size(); }

    bool is_empty() const { return _queue.empty(); }

    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = T;
        using difference_type = std::ptrdiff_t;
        using pointer = T*;
        using reference = T&;

        Iterator() : _iter() {}
        explicit Iterator(
                typename std::set<
                        IndexedPriorityQueueEntry<T>,
                        IndexedPriorityQueueComparator<T, priority_ordering>>::const_iterator iter)
                : _iter(iter) {}

        const T& operator*() const { return _iter->value; }

        const T* operator->() const { return &(_iter->value); }

        Iterator& operator++() {
            ++_iter;
            return *this;
        }

        Iterator operator++(int) {
            Iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator==(const Iterator& other) const { return _iter == other._iter; }

        bool operator!=(const Iterator& other) const { return !(*this == other); }

    private:
        typename std::set<IndexedPriorityQueueEntry<T>,
                          IndexedPriorityQueueComparator<T, priority_ordering>>::const_iterator
                _iter;
    };

    Iterator begin() const { return Iterator(_queue.begin()); }

    Iterator end() const { return Iterator(_queue.end()); }

private:
    std::map<T, IndexedPriorityQueueEntry<T>> _index;
    std::set<IndexedPriorityQueueEntry<T>, IndexedPriorityQueueComparator<T, priority_ordering>>
            _queue;

    long generation = 0;
};

} // namespace doris
