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

#include "common/cast_set.h"
#include "olap/rowset/segment_v2/inverted_index/util/mock_iterator.h"
#include "olap/rowset/segment_v2/inverted_index/util/term_position_iterator.h"
#include "priority_queue.h"

namespace doris::segment_v2 {

using namespace inverted_index;
#include "common/compile_check_begin.h"

template <typename T>
class DocsQueue : public PriorityQueue<T*> {
public:
    DocsQueue(size_t size) : PriorityQueue<T*>(size) {}
    ~DocsQueue() override = default;

    bool less_than(T* pp1, T* pp2) const override { return pp1->doc_id() < pp2->doc_id(); }
};

template <typename T>
using DocsQueuePtr = std::unique_ptr<DocsQueue<T>>;

class PositionsQueue {
public:
    PositionsQueue() : _array(_array_size) {}
    ~PositionsQueue() = default;

    void add(int32_t i) {
        if (_size == _array_size) {
            grow_array();
        }
        _array[_size++] = i;
    }

    int32_t next() { return _array[_index++]; }

    void sort() { std::sort(_array.begin() + _index, _array.begin() + _index + _size); }

    void clear() {
        _index = 0;
        _size = 0;
    }

    int32_t size() const { return _size; }

private:
    void grow_array() {
        _array_size *= 2;
        _array.resize(_array_size);
    }

    int32_t _array_size = 16;
    int32_t _index = 0;
    int32_t _size = 0;
    std::vector<int32_t> _array;
};
using PositionsQueuePtr = std::unique_ptr<PositionsQueue>;

template <typename T>
class UnionTermIterator {
public:
    using IterPtr = std::shared_ptr<T>;

    static_assert(std::is_same_v<T, TermPositionsIterator> || std::is_same_v<T, MockIterator>,
                  "T must be one of: TermPositionsIterPtr or MockIterPtr");

    UnionTermIterator() = default;
    ~UnionTermIterator() = default;

    UnionTermIterator(std::vector<IterPtr> subs) : _subs(std::move(subs)) {
        _pos_queue = std::make_unique<PositionsQueue>();
        _docs_queue = std::make_unique<DocsQueue<T>>(_subs.size());
        int32_t cost = 0;
        for (auto& sub : _subs) {
            _docs_queue->add(sub.get());
            cost += sub->doc_freq();
        }
        _cost = cost;
    }

    bool is_empty() const { return _subs.empty(); }

    int32_t freq() {
        int32_t doc = doc_id();
        if (doc != pos_queue_doc) {
            _pos_queue->clear();
            for (auto& sub : _subs) {
                if (sub->doc_id() == doc) {
                    int32_t freq = sub->freq();
                    for (int32_t i = 0; i < freq; i++) {
                        _pos_queue->add(sub->next_position());
                    }
                }
            }
            _pos_queue->sort();
            pos_queue_doc = doc;
        }
        return cast_set<int32_t>(_pos_queue->size());
    }

    int32_t next_position() const { return _pos_queue->next(); }

    int32_t doc_id() const { return _docs_queue->top()->doc_id(); }

    int32_t next_doc() const {
        auto* top = _docs_queue->top();
        int32_t doc = top->doc_id();
        do {
            top->next_doc();
            top = _docs_queue->update_top();
        } while (top->doc_id() == doc);
        return top->doc_id();
        return 0;
    }

    int32_t advance(int32_t target) const {
        auto* top = _docs_queue->top();
        do {
            top->advance(target);
            top = _docs_queue->update_top();
        } while (top->doc_id() < target);
        return top->doc_id();
    }

    int32_t doc_freq() const { return _cost; }

private:
    int32_t _cost = 0;
    int32_t pos_queue_doc = -2;
    DocsQueuePtr<T> _docs_queue;
    PositionsQueuePtr _pos_queue;
    std::vector<IterPtr> _subs;
};
using UnionTermIterPtr = std::shared_ptr<UnionTermIterator<TermPositionsIterator>>;

} // namespace doris::segment_v2
#include "common/compile_check_end.h"