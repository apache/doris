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

#include "common/logging.h"
#include "exprs/anyval_util.h"

namespace doris {
int sizeof_type(PrimitiveType type) {
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

Status type_check(PrimitiveType type) {
    switch (type) {
    case TYPE_INT:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_NULL:
        break;
    default:
        return Status::InvalidArgument(fmt::format("Type not implemented: {}", type));
    }

    return Status::OK();
}

void CollectionValue::to_collection_val(CollectionVal* val) const {
    val->length = _length;
    val->data = _data;
    val->null_signs = _null_signs;
    val->has_null = _has_null;
}

void CollectionValue::shallow_copy(const CollectionValue* value) {
    _length = value->_length;
    _null_signs = value->_null_signs;
    _data = value->_data;
    _has_null = value->_has_null;
}

void CollectionValue::copy_null_signs(const CollectionValue* other) {
    if (other->_has_null) {
        memcpy(_null_signs, other->_null_signs, other->size());
    } else {
        _null_signs = nullptr;
    }
}

ArrayIterator CollectionValue::iterator(PrimitiveType children_type) const {
    return ArrayIterator(children_type, this);
}

Status CollectionValue::init_collection(ObjectPool* pool, uint32_t size, PrimitiveType child_type,
                                        CollectionValue* val) {
    if (val == nullptr) {
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

Status CollectionValue::init_collection(MemPool* pool, uint32_t size, PrimitiveType child_type,
                                        CollectionValue* val) {
    if (val == nullptr) {
        return Status::InvalidArgument("collection value is null");
    }

    RETURN_IF_ERROR(type_check(child_type));

    if (size == 0) {
        return Status::OK();
    }

    val->_length = size;
    val->_null_signs = (bool*)pool->allocate(size * sizeof(bool));
    memset(val->_null_signs, 0, size);

    val->_data = pool->allocate(sizeof_type(child_type) * size);

    return Status::OK();
}

Status CollectionValue::init_collection(FunctionContext* context, uint32_t size,
                                        PrimitiveType child_type, CollectionValue* val) {
    if (val == nullptr) {
        return Status::InvalidArgument("collection value is null");
    }

    RETURN_IF_ERROR(type_check(child_type));

    if (size == 0) {
        return Status::OK();
    }

    val->_length = size;
    val->_null_signs = (bool*)context->allocate(size * sizeof(bool));
    memset(val->_null_signs, 0, size);

    val->_data = context->allocate(sizeof_type(child_type) * size);

    return Status::OK();
}

CollectionValue CollectionValue::from_collection_val(const CollectionVal& val) {
    return CollectionValue(val.data, val.length, val.null_signs);
}

Status CollectionValue::set(uint32_t i, PrimitiveType type, const AnyVal* value) {
    RETURN_IF_ERROR(type_check(type));

    ArrayIterator iter(type, this);
    if (!iter.seek(i)) {
        return Status::InvalidArgument("over of collection size");
    }

    if (value->is_null) {
        *(_null_signs + i) = true;
        _has_null = true;
        return Status::OK();
    } else {
        *(_null_signs + i) = false;
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
        dest->ptr = (char*)src->ptr;
        break;
    }
    default:
        DCHECK(false) << "Type not implemented: " << type;
        return Status::InvalidArgument("Type not implemented");
    }

    return Status::OK();
}

/**
 * ----------- Array Iterator -------- 
 */
ArrayIterator::ArrayIterator(PrimitiveType children_type, const CollectionValue* data)
        : _offset(0), _type(children_type), _data(data) {
    _type_size = sizeof_type(children_type);
}

void* ArrayIterator::value() {
    if (is_null()) {
        return nullptr;
    }
    return ((char*)_data->_data) + _offset * _type_size;
}

bool ArrayIterator::is_null() {
    return _data->is_null_at(_offset);
}

void ArrayIterator::value(AnyVal* dest) {
    if (is_null()) {
        dest->is_null = true;
        return;
    }
    dest->is_null = false;
    switch (_type) {
    case TYPE_BOOLEAN:
        reinterpret_cast<BooleanVal*>(dest)->val = *reinterpret_cast<const bool*>(value());
        break;

    case TYPE_TINYINT:
        reinterpret_cast<TinyIntVal*>(dest)->val = *reinterpret_cast<const int8_t*>(value());
        break;

    case TYPE_SMALLINT:
        reinterpret_cast<TinyIntVal*>(dest)->val = *reinterpret_cast<const int16_t*>(value());
        break;

    case TYPE_INT:
        reinterpret_cast<IntVal*>(dest)->val = *reinterpret_cast<const int32_t*>(value());
        break;

    case TYPE_BIGINT:
        reinterpret_cast<BigIntVal*>(dest)->val = *reinterpret_cast<const int64_t*>(value());
        break;

    case TYPE_FLOAT:
        reinterpret_cast<FloatVal*>(dest)->val = *reinterpret_cast<const float*>(value());
        break;

    case TYPE_DOUBLE:
        reinterpret_cast<DoubleVal*>(dest)->val = *reinterpret_cast<const double*>(value());
        break;
    case TYPE_HLL:
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        const StringValue* str_value = reinterpret_cast<const StringValue*>(value());
        reinterpret_cast<StringVal*>(dest)->len = str_value->len;
        reinterpret_cast<StringVal*>(dest)->ptr = (uint8_t*)(str_value->ptr);
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        const DateTimeValue* date_time_value = reinterpret_cast<const DateTimeValue*>(value());
        reinterpret_cast<DateTimeVal*>(dest)->packed_time = date_time_value->to_int64();
        reinterpret_cast<DateTimeVal*>(dest)->type = date_time_value->type();
        break;
    }

    case TYPE_DECIMALV2:
        reinterpret_cast<DecimalV2Val*>(dest)->val =
                reinterpret_cast<const PackedInt128*>(value())->value;
        break;

    case TYPE_LARGEINT:
        reinterpret_cast<LargeIntVal*>(dest)->val =
                reinterpret_cast<const PackedInt128*>(value())->value;
        break;

    case TYPE_ARRAY:
        reinterpret_cast<const CollectionValue*>(value())->to_collection_val(
                reinterpret_cast<CollectionVal*>(dest));
        break;

    default:
        DCHECK(false) << "bad type: " << _type;
    }
}
} // namespace doris
