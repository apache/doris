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

#include "runtime/collection_value.h"

#include "util/bitmap.h"

#include "common/logging.h"
#include "exprs/anyval_util.h"


namespace doris {

int sizeof_type(const PrimitiveType &type) {
    switch (type) {
        case TYPE_INT:
            return sizeof(int32_t);
        case TYPE_CHAR:
        case TYPE_VARCHAR:
            return sizeof(StringValue);
        case TYPE_NULL:
            return 0;
        default:
            DCHECK(false) << "Type not implemented: " << type;
            break;
    }

    return 0;
}

Status type_check(const PrimitiveType& type) {
    switch (type) {
        case TYPE_INT:
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_NULL:
            break;
        default:
            return Status::InvalidArgument("Type not implemented: " + type);
    }
    
    return Status::OK();
}


void CollectionValue::to_collection_val(CollectionVal* collectionVal) const {
    collectionVal->length = _length;
    collectionVal->data = _data;
    collectionVal->null_signs = _null_signs;
}

void CollectionValue::shallow_copy(const CollectionValue *value) {
    _length = value->_length;
    _null_signs = value->_null_signs;
    _data = value->_data;
}

void CollectionValue::deep_copy_bitmap(const CollectionValue *other) {
    memcpy(_null_signs, other->_null_signs, other->size());
}

CollectionIterator CollectionValue::iterator(const PrimitiveType& children_type) const {
    return CollectionIterator(children_type, this);
}


Status CollectionValue::init_collection(ObjectPool* pool, const int& size,
                                            const PrimitiveType& child_type, CollectionValue* val) {
    if (val == NULL) {
        return Status::InvalidArgument("collection value is null");
    }
    
    RETURN_IF_ERROR(type_check(child_type));

    if (size == 0) {
        return Status::OK();
    }

    val->_length = size;
    val->_null_signs = pool->add_array(new bool[size] {0});
    val->_data = pool->add_array(new uint8_t[size * sizeof_type(child_type)]);
    return Status::OK();
}

Status
CollectionValue::init_collection(MemPool* pool, const int& size, const PrimitiveType& child_type, CollectionValue* val) {
    if (val == NULL) {
        return Status::InvalidArgument("collection value is null");
    }

    RETURN_IF_ERROR(type_check(child_type));

    if (size == 0) {
        return Status::OK();
    }

    val->_length = size;
    val->_null_signs = (bool*) pool->allocate(size * sizeof(bool));
    memset(val->_null_signs, 0, size);
    
    val->_data = pool->allocate(sizeof_type(child_type) * size);
    return Status::OK();
}

Status CollectionValue::init_collection(FunctionContext* context, const int& size, const PrimitiveType& child_type,
                                        CollectionValue* val) {
    if (val == NULL) {
        return Status::InvalidArgument("collection value is null");
    }

    RETURN_IF_ERROR(type_check(child_type));

    if (size == 0) {
        return Status::OK();
    }
    
    val->_length = size;
    val->_null_signs = (bool*) context->allocate(size * sizeof(bool));
    memset(val->_null_signs, 0, size);

    val->_data = context->allocate(sizeof_type(child_type) * size);
    return Status::OK();
}


CollectionValue CollectionValue::from_collection_val(const CollectionVal &val) {
    return CollectionValue(val.length, val.null_signs, val.data);
}

Status CollectionValue::set(const int& i, const PrimitiveType& type, const AnyVal* value) {
    RETURN_IF_ERROR(type_check(type));

    CollectionIterator iter(type, this);
    if (!iter.seek(i)) {
        return Status::InvalidArgument("over of collection size");
    }

    if (value->is_null) {
        *(_null_signs + i) = true;
        return Status::OK();
    }

    switch (type) {
        case TYPE_INT:
            *reinterpret_cast<int32_t*>(iter.value()) = reinterpret_cast<const IntVal*>(value)->val;
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            const StringVal* src = reinterpret_cast<const StringVal*>(value);
            StringValue* dest = reinterpret_cast<StringValue*>(iter.value());
            dest->len = src->len;
            dest->ptr = (char*) src->ptr;
            break;
        }
        default:
            DCHECK(false) << "Type not implemented: " << type;
            return Status::InvalidArgument("Type not implemented");       
    }

    return Status::OK();
}
~
/**
 * ----------- Collection Iterator -------- 
 */
CollectionIterator::CollectionIterator(const PrimitiveType& children_type,
                                             const CollectionValue* data) : _offset(0),
                                                                          _type(children_type),
                                                                          _data(data) {
    _type_size = sizeof_type(children_type);
}


void* CollectionIterator::value() {
    if (is_null()) {
        return nullptr;
    }
    return ((char *)_data->_data) + _offset * _type_size;
}

bool CollectionIterator::is_null() {
    return *(_data->_null_signs + _offset);
}

void CollectionIterator::value(AnyVal* dest) {
    TypeDescriptor td(_type);
    AnyValUtil::set_any_val(value(), td, dest);
}

}