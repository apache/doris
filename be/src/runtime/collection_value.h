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

#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/mem_pool.h"
#include "runtime/primitive_type.h"
#include "udf/udf.h"

namespace doris {

using doris_udf::AnyVal;

class ArrayIterator;

/**
 * The format of array-typed slot.
 * The array's sub-element type just support: 
 * - INT32
 * - CHAR
 * - VARCHAR
 * - NULL
 * 
 * A new array need initialization memory before used
 */
struct CollectionValue {
public:
    CollectionValue() = default;

    explicit CollectionValue(uint32_t length)
            : _data(nullptr), _length(length), _has_null(false), _null_signs(nullptr) {}

    CollectionValue(void* data, uint32_t length)
            : _data(data), _length(length), _has_null(false), _null_signs(nullptr) {}

    CollectionValue(void* data, uint32_t length, bool* null_signs)
            : _data(data), _length(length), _has_null(true), _null_signs(null_signs) {}

    CollectionValue(void* data, uint32_t length, bool has_null, bool* null_signs)
            : _data(data), _length(length), _has_null(has_null), _null_signs(null_signs) {}

    inline bool is_null_at(uint32_t index) const {
        return this->_has_null && this->_null_signs[index];
    }

    void to_collection_val(CollectionVal* val) const;

    inline uint32_t size() const { return _length; }

    inline uint32_t length() const { return _length; }

    void shallow_copy(const CollectionValue* other);

    void copy_null_signs(const CollectionValue* other);

    ArrayIterator iterator(PrimitiveType children_type) const;

    /**
     * just shallow copy sub-elment value
     * For special type, will shared actual value's memory, like StringValue.
     */
    Status set(uint32_t i, PrimitiveType type, const AnyVal* value);

    /**
     * init collection, will alloc (children Type's size + 1) * (children Nums) memory  
     */
    static Status init_collection(ObjectPool* pool, uint32_t size, PrimitiveType child_type,
                                  CollectionValue* val);

    static Status init_collection(MemPool* pool, uint32_t size, PrimitiveType child_type,
                                  CollectionValue* val);

    static Status init_collection(FunctionContext* context, uint32_t size, PrimitiveType child_type,
                                  CollectionValue* val);

    static CollectionValue from_collection_val(const CollectionVal& val);
    const void* data() const { return _data; }

    inline bool has_null() const { return _has_null; }
    inline const bool* null_signs() const { return _null_signs; }
    inline void* mutable_data() { return _data; }
    inline bool* mutable_null_signs() { return _null_signs; }
    inline void set_length(uint32_t length) { _length = length; }
    inline void set_has_null(bool has_null) { _has_null = has_null; }
    inline void set_data(void* data) { _data = data; }
    inline void set_null_signs(bool* null_signs) { _null_signs = null_signs; }

public:
    // child column data
    void* _data;
    uint32_t _length;
    // item has no null value if has_null is false.
    // item ```may``` has null value if has_null is true.
    bool _has_null;
    // null bitmap
    bool* _null_signs;

    friend ArrayIterator;
};

/**
 * Array's Iterator, support read array by special type
 */
class ArrayIterator {
private:
    ArrayIterator(PrimitiveType children_type, const CollectionValue* data);

public:
    bool seek(uint32_t n) {
        if (n >= _data->size()) {
            return false;
        }

        _offset = n;
        return true;
    }

    bool has_next() { return _offset < _data->size(); }

    bool next() {
        if (_offset < _data->size()) {
            _offset++;
            return true;
        }

        return false;
    }

    bool is_null();

    void* value();

    void value(AnyVal* dest);

private:
    size_t _offset;
    int _type_size;
    const PrimitiveType _type;
    const CollectionValue* _data;

    friend CollectionValue;
};
} // namespace doris
