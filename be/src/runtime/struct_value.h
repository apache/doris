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

#include <type_traits>

namespace doris_udf {
    class FunctionContext;
    struct AnyVal;
} // namespace doris_udf

namespace doris {

using doris_udf::FunctionContext;
using doris_udf::AnyVal;

class StructValue {
public:
    StructValue() = default;

    explicit StructValue(uint32_t size) : _values(nullptr), _size(size), _has_null(false) {}
    StructValue(void** values, uint32_t size) : _values(values), _size(size), _has_null(false) {}
    StructValue(void** values, uint32_t size, bool has_null) : _values(values), _size(size), _has_null(has_null) {}

    void to_struct_val(StructVal* val) const;
    static StructValue from_struct_val(const StructVal& val);

    uint32_t size() const { return _size; }
    void set_size(uint32_t size) { _size = size; }

    bool is_null_at(uint32_t index) const { return this->_has_null && this->_values[index] == nullptr; }

    void shallow_copy(const StructValue* other);

    // size_t get_byte_size(const TypeDescriptor& type) const;

    // Deep copy collection.
    // NOTICE: The CollectionValue* shallow_copied_cv must be initialized by calling memcpy function first (
    // copy data from origin collection value).
//    static void deep_copy_collection(StructValue* shallow_copied_sv,
//                                     const TypeDescriptor& item_type,
//                                     const GenMemFootprintFunc& gen_mem_footprint,
//                                     bool convert_ptrs);

//    static void deserialize_collection(CollectionValue* cv, const char* tuple_data,
//                                       const TypeDescriptor& item_type);

    const void** values() const { return _values; }
    void** mutable_values() { return _values; }
    void set_values(void** values) { _values = values; }
    const void* child_value(uint32_t index) const { return _values[i]; }
    void* mutable_child_value(uint32_t index) { return _values[i]; }
    void set_child_value(void* value, uint32_t index) { _values[i] = value; }

private:
    // pointer to the start of the vector of children pointers. These pointers are
    // point to children values where a null pointer means that this child is NULL.
    void** _values;
    // the number of values.
    uint32_t _size;
    // child has no null value if has_null is false.
    // child ```may``` has null value if has_null is true.
    bool _has_null;
};

} // namespace doris


