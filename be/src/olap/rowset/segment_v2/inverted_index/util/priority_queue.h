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

#include <cassert>
#include <cstdint>
#include <functional>
#include <limits>

#include "common/cast_set.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

template <typename T>
class PriorityQueue {
public:
    PriorityQueue(size_t max_size, std::function<T()> sentinel_object_supplier = nullptr) {
        assert(max_size < std::numeric_limits<int32_t>::max());
        size_t heap_size = (max_size == 0) ? 2 : max_size + 1;
        _heap.resize(heap_size);
        _max_size = max_size;

        if (sentinel_object_supplier) {
            for (size_t i = 1; i < _heap.size(); i++) {
                _heap[i] = sentinel_object_supplier();
            }
            _size = max_size;
        }
    }

    virtual ~PriorityQueue() = default;

    T add(const T& element) {
        size_t index = _size + 1;
        _heap[index] = element;
        _size = index;
        up_heap(index);
        return _heap[1];
    }

    T insert_with_overflow(const T& element) {
        if (_size < _max_size) {
            add(element);
            return T();
        } else if (_size > 0 && less_than(_heap[1], element)) {
            T ret = _heap[1];
            _heap[1] = element;
            update_top();
            return ret;
        } else {
            return element;
        }
    }

    T top() const { return _heap[1]; }

    T pop() {
        if (_size > 0) {
            T result = _heap[1];
            _heap[1] = _heap[_size];
            _heap[_size] = T();
            _size--;
            down_heap(1);
            return result;
        }
        return T();
    }

    T update_top() {
        down_heap(1);
        return _heap[1];
    }

    T update_top(const T& new_top) {
        _heap[1] = new_top;
        return update_top();
    }

    int32_t size() { return cast_set<int32_t>(_size); }

    void clear() {
        for (size_t i = 1; i <= _size; i++) {
            _heap[i] = T();
        }
        _size = 0;
    }

    bool remove(const T& element) {
        for (size_t i = 1; i <= _size; i++) {
            if (_heap[i] == element) {
                _heap[i] = _heap[_size];
                _heap[_size] = T();
                _size--;
                if (i <= _size) {
                    if (!up_heap(i)) {
                        down_heap(i);
                    }
                }
                return true;
            }
        }
        return false;
    }

private:
    virtual bool less_than(T a, T b) const = 0;

    bool up_heap(size_t orig_pos) {
        size_t i = orig_pos;
        T node = _heap[i];
        size_t j = i >> 1;
        while (j > 0 && less_than(node, _heap[j])) {
            _heap[i] = _heap[j];
            i = j;
            j = j >> 1;
        }
        _heap[i] = node;
        return i != orig_pos;
    }

    void down_heap(size_t i) {
        T node = _heap[i];
        size_t j = i << 1;
        size_t k = j + 1;
        if (k <= _size && less_than(_heap[k], _heap[j])) {
            j = k;
        }
        while (j <= _size && less_than(_heap[j], node)) {
            _heap[i] = _heap[j];
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= _size && less_than(_heap[k], _heap[j])) {
                j = k;
            }
        }
        _heap[i] = node;
    }

    size_t _size = 0;
    size_t _max_size = 0;
    std::vector<T> _heap;
};

} // namespace doris::segment_v2::inverted_index
#include "common/compile_check_end.h"