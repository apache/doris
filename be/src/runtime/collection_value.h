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

#include "runtime/primitive_type.h"

namespace doris_udf {
class FunctionContext;
struct AnyVal;
} // namespace doris_udf

namespace doris {

using doris_udf::FunctionContext;
using doris_udf::AnyVal;

using MemFootprint = std::pair<int64_t, uint8_t*>;
using GenMemFootprintFunc = std::function<MemFootprint(int64_t size)>;

struct ArrayIteratorFunctionsBase;
class ArrayIterator;
class Status;
class ObjectPool;
class MemPool;
struct TypeDescriptor;

template <PrimitiveType type>
struct ArrayIteratorFunctions;
template <typename T>
inline constexpr std::enable_if_t<std::is_base_of_v<ArrayIteratorFunctionsBase, T>, bool>
        IsTypeFixedWidth = true;

template <>
inline constexpr bool IsTypeFixedWidth<ArrayIteratorFunctions<TYPE_CHAR>> = false;
template <>
inline constexpr bool IsTypeFixedWidth<ArrayIteratorFunctions<TYPE_VARCHAR>> = false;
template <>
inline constexpr bool IsTypeFixedWidth<ArrayIteratorFunctions<TYPE_STRING>> = false;
template <>
inline constexpr bool IsTypeFixedWidth<ArrayIteratorFunctions<TYPE_ARRAY>> = false;

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

    void to_collection_val(CollectionVal* val) const;

    uint64_t size() const { return _length; }

    uint64_t length() const { return _length; }

    void shallow_copy(const CollectionValue* other);

    void copy_null_signs(const CollectionValue* other);

    size_t get_byte_size(const TypeDescriptor& item_type) const;

    ArrayIterator iterator(PrimitiveType child_type);
    const ArrayIterator iterator(PrimitiveType child_type) const;

    /**
     * init collection, will alloc (children Type's size + 1) * (children Nums) memory  
     */
    static Status init_collection(ObjectPool* pool, uint64_t size, PrimitiveType child_type,
                                  CollectionValue* value);

    static Status init_collection(MemPool* pool, uint64_t size, PrimitiveType child_type,
                                  CollectionValue* value);

    static Status init_collection(FunctionContext* context, uint64_t size, PrimitiveType child_type,
                                  CollectionValue* value);

    static CollectionValue from_collection_val(const CollectionVal& val);

    // Deep copy collection.
    // NOTICE: The CollectionValue* shallow_copied_cv must be initialized by calling memcpy function first (
    // copy data from origin collection value).
    static void deep_copy_collection(CollectionValue* shallow_copied_cv,
                                     const TypeDescriptor& item_type,
                                     const GenMemFootprintFunc& gen_mem_footprint,
                                     bool convert_ptrs);

    static void deserialize_collection(CollectionValue* cv, const char* tuple_data,
                                       const TypeDescriptor& item_type);

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
    using AllocateMemFunc = std::function<uint8_t*(size_t size)>;
    static Status init_collection(CollectionValue* value, const AllocateMemFunc& allocate,
                                  uint64_t size, PrimitiveType child_type);
    ArrayIterator internal_iterator(PrimitiveType child_type) const;

private:
    // child column data
    void* _data;
    uint64_t _length;
    // item has no null value if has_null is false.
    // item ```may``` has null value if has_null is true.
    bool _has_null;
    // null bitmap
    bool* _null_signs;

    friend ArrayIterator;
};

class ArrayIterator {
public:
    int type_size() const { return _type_size; }
    bool is_type_fixed_width() const { return _is_type_fixed_width; }

    bool has_next() const { return _offset < _collection_value->size(); }
    bool next() const {
        if (has_next()) {
            ++_offset;
            return true;
        }
        return false;
    }
    bool seek(uint64_t n) const {
        if (n >= _collection_value->size()) {
            return false;
        }
        _offset = n;
        return true;
    }
    bool is_null() const { return _collection_value->is_null_at(_offset); }
    const void* get() const {
        if (is_null()) {
            return nullptr;
        }
        return reinterpret_cast<const uint8_t*>(_collection_value->data()) + _offset * _type_size;
    }
    void* get() {
        if (is_null()) {
            return nullptr;
        }
        return reinterpret_cast<uint8_t*>(_collection_value->mutable_data()) + _offset * _type_size;
    }
    void get(AnyVal* value) const {
        if (is_null()) {
            value->is_null = true;
            return;
        }
        value->is_null = false;
        _shallow_get(value, get());
    }
    void set(const AnyVal* value) {
        if (_collection_value->mutable_null_signs()) {
            _collection_value->mutable_null_signs()[_offset] = value->is_null;
        }
        if (value->is_null) {
            _collection_value->set_has_null(true);
        } else {
            _shallow_set(get(), value);
        }
    }
    void self_deep_copy(const TypeDescriptor& type_desc,
                        const GenMemFootprintFunc& gen_mem_footprint, bool convert_ptrs) {
        if (is_null()) {
            return;
        }
        _self_deep_copy(get(), type_desc, gen_mem_footprint, convert_ptrs);
    }
    void deserialize(const char* tuple_data, const TypeDescriptor& type_desc) {
        if (is_null()) {
            return;
        }
        _deserialize(get(), tuple_data, type_desc);
    }
    size_t get_byte_size(const TypeDescriptor& type) const {
        if (is_null()) {
            return 0;
        }
        return _get_byte_size(get(), type);
    }
    void raw_value_write(const void* value, const TypeDescriptor& type_desc, MemPool* pool) {
        if (is_null()) {
            return;
        }
        return _raw_value_write(get(), value, type_desc, pool);
    }

private:
    template <typename T,
              typename = std::enable_if_t<std::is_base_of_v<ArrayIteratorFunctionsBase, T>>>
    ArrayIterator(CollectionValue* data, const T*)
            : _shallow_get(T::shallow_get),
              _shallow_set(T::shallow_set),
              _self_deep_copy(T::self_deep_copy),
              _deserialize(T::deserialize),
              _get_byte_size(T::get_byte_size),
              _raw_value_write(T::raw_value_write),
              _collection_value(data),
              _offset(0),
              _type_size(T::get_type_size()),
              _is_type_fixed_width(IsTypeFixedWidth<T>) {}
    void (*_shallow_get)(AnyVal*, const void*);
    void (*_shallow_set)(void*, const AnyVal*);
    void (*_self_deep_copy)(void*, const TypeDescriptor&, const GenMemFootprintFunc&, bool);
    void (*_deserialize)(void*, const char*, const TypeDescriptor&);
    size_t (*_get_byte_size)(const void* item, const TypeDescriptor&);
    void (*_raw_value_write)(void* item, const void* value, const TypeDescriptor& type_desc,
                             MemPool* pool);

private:
    CollectionValue* _collection_value;
    mutable uint64_t _offset;
    const int _type_size;
    const bool _is_type_fixed_width;

    friend CollectionValue;
};
} // namespace doris
