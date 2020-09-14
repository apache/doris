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

#ifndef DORIS_BE_RUNTIME_ARRAY_VALUE_H
#define DORIS_BE_RUNTIME_ARRAY_VALUE_H

#include "udf/udf.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/mem_pool.h"
#include "runtime/primitive_type.h"

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
struct ArrayValue {

public:
    ArrayValue() : _length(0), _null_signs(NULL), _data(NULL) {};

    ArrayValue(int len, bool* null_signs, void* data) : _length(len), _null_signs(null_signs),
                                                             _data(data) {};

    void to_array_val(ArrayVal* arrayVal) const;

    int size() const { return _length; }

    void shallow_copy(const ArrayValue* other);

    void copy_null_signs(const ArrayValue* other);

    ArrayIterator iterator(const PrimitiveType& children_type) const;

    /**
     * just shallow copy sub-elment value
     * For special type, will shared actual value's memory, like StringValue.
     */
    Status set(const int& i, const PrimitiveType& type, const AnyVal* value);

    /**
     * init array, will alloc (children Type's size + 1) * (children Nums) memory  
     */
    static Status
    init_array(ObjectPool* pool, const int& size, const PrimitiveType& child_type, ArrayValue* val);

    static Status
    init_array(MemPool* pool, const int& size, const PrimitiveType& child_type, ArrayValue* val);

    static Status
    init_array(FunctionContext* context, const int& size, const PrimitiveType& child_type, ArrayValue* val);

    static ArrayValue from_array_val(const ArrayVal& val);

public:
    int32_t _length;
    // null signs
    bool* _null_signs;
    // data(include null)
    // TYPE_INT: bool
    // TYPE_TINYINT: int8_t
    // TYPE_SMALLINT: int16_t
    // TYPE_INT: int32_t
    // TYPE_BIGINT: int64_t
    // TYPE_CHAR/VARCHAR/OBJECT: StringValue 
    void* _data;

    friend ArrayIterator;
};

/**
 * Array's Iterator, support read array by special type
 */
class ArrayIterator {
private:
    ArrayIterator(const PrimitiveType& children_type, const ArrayValue* data);

public:

    bool seek(int n) {
        if (n >= _data->size()) {
            return false;
        }

        _offset = n;
        return true;
    }

    bool has_next() {
        return _offset < _data->size();
    }

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
    const ArrayValue* _data;

    friend ArrayValue;
};

}

#endif // DORIS_BE_RUNTIME_ARRAY_VALUE_H