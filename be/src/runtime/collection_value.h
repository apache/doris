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
    CollectionValue() : length(0), null_bitmap_data(NULL), data(NULL) {};

    CollectionValue(int len, uint8_t* null_bitmap, void* data) : length(len), null_bitmap_data(null_bitmap),
                                                                 data(data) {};

    void to_collection_val(CollectionVal* collectionVal) const;
    
    int size() const { return length; }
    
    void shallow_copy(const CollectionValue* other); 
    
    void deep_copy_bitmap(const CollectionValue* other);

    CollectionIterator iterator(const TypeDescriptor* children_type) const;
    
    Status set(const int& i, const TypeDescriptor& type, const AnyVal* value);
    
    static Status init_collection(ObjectPool* pool, const int& size, const PrimitiveType& child_type, CollectionValue* val);

    static Status init_collection(MemPool* pool, const int& size, const TypeDescriptor& child_type, CollectionValue* val);

    static Status init_collection(FunctionContext* context, const int& size, const PrimitiveType& child_type, CollectionValue* val);
    
    static CollectionValue from_collection_val(const CollectionVal& val);

public:
    int length;
    // null bitmap
    uint8_t* null_bitmap_data;
    // data(include null)
    // TYPE_INT: bool
    // TYPE_TINYINT: int8_t
    // TYPE_SMALLINT: int16_t
    // TYPE_INT: int32_t
    // TYPE_BIGINT: int64_t
    // TYPE_CHAR/VARCHAR/OBJECT: StringValue 
    void* data;
    
    friend CollectionIterator;
};


class CollectionIterator {
public:
    CollectionIterator(const TypeDescriptor* children_type, const CollectionValue* data);

    bool seek(int n) {
        if (n >= _data->length){
            return false;
        }

        _offset = n;
        return true;
    }

    bool has_next() {
        return _offset < _data->length;
    }

    bool next() {
        if (_offset < _data->length) {
            _offset ++;
            return true;
        }

        return false;
    }

    void* value();

    void value(AnyVal* dest);

private:
    size_t _offset;
    int _type_size;
    const TypeDescriptor* _type;
    const CollectionValue* _data;
};

}

#endif // DORIS_BE_RUNTIME_COLLECTION_VALUE_H
