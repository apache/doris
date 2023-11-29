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

#include <stdint.h>

#include <functional>
#include <utility>

namespace doris {

using MemFootprint = std::pair<int64_t, uint8_t*>;
using GenMemFootprintFunc = std::function<MemFootprint(int64_t size)>;

/**
 * The format of array-typed slot.
 * A new array needs to be initialized before using it.
 */
class CollectionValue {
public:
    CollectionValue() = default;

    explicit CollectionValue(uint64_t length)
            : _data(nullptr), _length(length), _has_null(false), _null_signs(nullptr) {}

    CollectionValue(void* data, uint64_t length)
            : _data(data), _length(length), _has_null(false), _null_signs(nullptr) {}

    CollectionValue(void* data, uint64_t length, bool* null_signs)
            : _data(data), _length(length), _has_null(true), _null_signs(null_signs) {}

    CollectionValue(void* data, uint64_t length, bool has_null, bool* null_signs)
            : _data(data), _length(length), _has_null(has_null), _null_signs(null_signs) {}

    bool is_null_at(uint64_t index) const { return this->_has_null && this->_null_signs[index]; }

    uint64_t size() const { return _length; }

    uint64_t length() const { return _length; }

    void shallow_copy(const CollectionValue* other);

    void copy_null_signs(const CollectionValue* other);

    const void* data() const { return _data; }
    bool has_null() const { return _has_null; }
    const bool* null_signs() const { return _null_signs; }
    void* mutable_data() { return _data; }
    bool* mutable_null_signs() { return _null_signs; }
    void set_length(uint64_t length) { _length = length; }
    void set_has_null(bool has_null) { _has_null = has_null; }
    void set_data(void* data) { _data = data; }
    void set_null_signs(bool* null_signs) { _null_signs = null_signs; }

private:
    // child column data
    void* _data = nullptr;
    uint64_t _length;
    // item has no null value if has_null is false.
    // item ```may``` has null value if has_null is true.
    bool _has_null;
    // null bitmap
    bool* _null_signs = nullptr;
};
} // namespace doris
