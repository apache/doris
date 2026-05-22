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
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <vector>

#include "common/compiler_util.h"

namespace doris {

template <typename T>
struct BitSetContainerTraits {
    static constexpr size_t RANGE = 0;
};

template <>
struct BitSetContainerTraits<int8_t> {
    static constexpr size_t RANGE = 256;
};

template <>
struct BitSetContainerTraits<int16_t> {
    static constexpr size_t RANGE = 65536;
};

template <typename T>
class BitSetContainer {
public:
    using Self = BitSetContainer;
    using ElementType = T;

    static_assert(std::is_same_v<T, int8_t> || std::is_same_v<T, int16_t>,
                  "BitSetContainer only supports int8_t or int16_t");

    static constexpr size_t RANGE = BitSetContainerTraits<T>::RANGE;

    class Iterator {
    public:
        explicit Iterator(const std::array<bool, RANGE>* data, size_t index)
                : _data(data), _index(index) {
            _find_next();
        }

        Iterator& operator++() {
            ++_index;
            _find_next();
            return *this;
        }

        Iterator operator++(int) {
            Iterator ret_val = *this;
            ++(*this);
            return ret_val;
        }

        bool operator==(Iterator other) const { return _index == other._index; }
        bool operator!=(Iterator other) const { return !(*this == other); }

        T operator*() const { return static_cast<T>(_index); }

        T* operator->() const {
            _cached_value = static_cast<T>(_index);
            return &_cached_value;
        }

        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = T;
        using pointer = const T*;
        using reference = const T&;

    private:
        void _find_next() {
            while (_index < RANGE && !(*_data)[_index]) {
                ++_index;
            }
        }

        const std::array<bool, RANGE>* _data;
        size_t _index;
        mutable T _cached_value = T();
    };

    BitSetContainer() { _data.fill(false); }

    ~BitSetContainer() = default;

    ALWAYS_INLINE void insert(const T& value) {
        auto idx = _to_index(value);
        if (!_data[idx]) {
            _data[idx] = true;
            _size++;
        }
    }

    ALWAYS_INLINE bool find(const T& value) const { return _data[_to_index(value)]; }

    void clear() {
        _data.fill(false);
        _size = 0;
    }

    size_t size() const { return _size; }

    Iterator begin() const { return Iterator(&_data, 0); }
    Iterator end() const { return Iterator(&_data, RANGE); }

    template <typename ColumnData>
    void find_batch(const ColumnData& data, size_t rows, std::vector<bool>& results) const {
        results.resize(rows);
        for (size_t i = 0; i < rows; ++i) {
            results[i] = find(data[i]);
        }
    }

    template <typename ColumnData>
    void find_batch(const ColumnData& data, size_t rows, std::vector<uint8_t>& results) const {
        results.resize(rows);
        for (size_t i = 0; i < rows; ++i) {
            results[i] = find(data[i]) ? 1 : 0;
        }
    }

private:
    ALWAYS_INLINE constexpr size_t _to_index(T value) const {
        if constexpr (std::is_same_v<T, int8_t>) {
            return static_cast<uint8_t>(value);
        } else {
            return static_cast<uint16_t>(value);
        }
    }

    std::array<bool, RANGE> _data;
    size_t _size = 0;
};

} // namespace doris