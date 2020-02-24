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
        default:
            DCHECK(false) << "Type not implemented: " << type;
            break;
    }

    return 0;
}


void CollectionValue::to_collection_val(CollectionVal* collectionVal) const {
    collectionVal->length = length;
    collectionVal->data = data;
    collectionVal->null_bitmap_data = null_bitmap_data;
}

void CollectionValue::shallow_copy(const CollectionValue *value) {
    length = value->length;
    null_bitmap_data = value->null_bitmap_data;
    data = value->data;
}

void CollectionValue::deep_copy_bitmap(const CollectionValue *other) {
    memcpy(null_bitmap_data, other->null_bitmap_data, BitmapSize(other->length));
}

CollectionIterator CollectionValue::iterator(const TypeDescriptor* children_type) const {
    return CollectionIterator(children_type, this);
}


Status CollectionValue::init_collection(ObjectPool* pool, const int& size,
                                            const PrimitiveType& child_type, CollectionValue* val) {
    if (val == NULL) {
        return Status::InvalidArgument("collection value is null");
    }

    if (size == 0) {
        return Status::OK();
    }

    val->length = size;
    val->null_bitmap_data = pool->add_array(new uint8_t[BitmapSize(size)]);
    val->data = pool->add_array(new uint8_t[size * sizeof_type(child_type)]);
    return Status::OK();
}

Status
CollectionValue::init_collection(MemPool* pool, const int& size, const TypeDescriptor& child_type, CollectionValue* val) {
    if (val == NULL) {
        return Status::InvalidArgument("collection value is null");
    }
    
    if (size == 0) {
        return Status::OK();
    }

    val->length = size;
    val->null_bitmap_data = pool->allocate(BitmapSize(size) * sizeof(uint8_t));
    val->data = pool->allocate(sizeof_type(child_type.type) * size);
    return Status::OK();
}

Status CollectionValue::init_collection(FunctionContext* context, const int& size, const PrimitiveType& child_type,
                                        CollectionValue* val) {
    if (val == NULL) {
        return Status::InvalidArgument("collection value is null");
    }
    
    if (size == 0) {
        return Status::OK();
    }
    
    val->length = size;
    val->null_bitmap_data = context->allocate(BitmapSize(size) * sizeof(uint8_t));
    val->data = context->allocate(sizeof_type(child_type) * size);
    return Status::OK();
}


CollectionValue CollectionValue::from_collection_val(const CollectionVal &val) {
    return CollectionValue(val.length, val.null_bitmap_data, val.data);
}

Status CollectionValue::set(const int& i, const TypeDescriptor& type, const AnyVal* value) {
    CollectionIterator iter(&type, this);
    if (!iter.seek(i)) {
        return Status::InvalidArgument("over of array size");
    }

    if (value->is_null) {
        BitmapChange(null_bitmap_data, i, true);
        return Status::OK();
    }

    switch (type.type) {
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
            DCHECK(false) << "Type not implemented: " << type.type;
            break;
    }

    return Status::OK();
}

/**
 * ----------- Collection Iterator -------- 
 */
CollectionIterator::CollectionIterator(const TypeDescriptor* children_type,
                                             const CollectionValue* data) : _offset(0),
                                                                          _type(children_type),
                                                                          _data(data) {
    _type_size = sizeof_type(children_type->type);
}


void* CollectionIterator::value() {
    if(BitmapTest(_data->null_bitmap_data, _offset)) {
        return nullptr;
    }
    return ((char *)_data->data) + _offset * _type_size;
}


void CollectionIterator::value(AnyVal* dest) {
    AnyValUtil::set_any_val(value(), *_type, dest);
}

}