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

#ifndef DORIS_BE_RUNTIME_COLLECTION_VALUE_H
#define DORIS_BE_RUNTIME_COLLECTION_VALUE_H

#include "udf/udf.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/mem_pool.h"
#include "runtime/types.h"

namespace doris {

using doris_udf::AnyVal;

class CollectionIterator;

class CollectionValue {

public:
    CollectionValue() : _length(0), _null_signs(NULL), _data(NULL) {};

    CollectionValue(int len, bool* null_signs, void* data) : _length(len), _null_signs(null_signs),
                                                                 _data(data) {};

    void to_collection_val(CollectionVal* collectionVal) const;
    
    int size() const { return _length; }
    
    void shallow_copy(const CollectionValue* other); 
    
    void deep_copy_bitmap(const CollectionValue* other);

    CollectionIterator iterator(const PrimitiveType& children_type) const;
    
    Status set(const int& i, const PrimitiveType& type, const AnyVal* value);
    
    static Status init_collection(ObjectPool* pool, const int& size, const PrimitiveType& child_type, CollectionValue* val);

    static Status init_collection(MemPool* pool, const int& size, const PrimitiveType& child_type, CollectionValue* val);

    static Status init_collection(FunctionContext* context, const int& size, const PrimitiveType& child_type, CollectionValue* val);
    
    static CollectionValue from_collection_val(const CollectionVal& val);

private:
    int _length;
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
    
    friend CollectionIterator;
};


class CollectionIterator {
private:
    CollectionIterator(const PrimitiveType& children_type, const CollectionValue* data);

public:

    bool seek(int n) {
        if (n >= _data->size()){
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
            _offset ++;
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

}

#endif // DORIS_BE_RUNTIME_COLLECTION_VALUE_H
