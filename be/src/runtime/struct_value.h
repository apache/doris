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

namespace doris {

class StructValue {
public:
    StructValue() = default;

    explicit StructValue(uint32_t size) : _values(nullptr), _size(size), _has_null(false) {}
    StructValue(void** values, uint32_t size) : _values(values), _size(size), _has_null(false) {}
    StructValue(void** values, uint32_t size, bool has_null)
            : _values(values), _size(size), _has_null(has_null) {}

    //void to_struct_val(StructVal* val) const;
    //static StructValue from_struct_val(const StructVal& val);

    uint32_t size() const { return _size; }
    void set_size(uint32_t size) { _size = size; }
    bool has_null() const { return _has_null; }
    void set_has_null(bool has_null) { _has_null = has_null; }
    bool is_null_at(uint32_t index) const {
        return this->_has_null && this->_values[index] == nullptr;
    }

    void shallow_copy(const StructValue* other);

    const void** values() const { return const_cast<const void**>(_values); }
    void** mutable_values() { return _values; }
    void set_values(void** values) { _values = values; }
    const void* child_value(uint32_t index) const { return _values[index]; }
    void* mutable_child_value(uint32_t index) { return _values[index]; }
    void set_child_value(void* value, uint32_t index) { _values[index] = value; }

private:
    // pointer to the start of the vector of children pointers. These pointers are
    // point to children values where a null pointer means that this child is NULL.
    void** _values = nullptr;
    // the number of values in this struct value.
    uint32_t _size;
    // child has no null value if has_null is false.
    // child may has null value if has_null is true.
    bool _has_null;
};
} // namespace doris