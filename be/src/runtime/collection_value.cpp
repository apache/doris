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

#include <functional>

#include "common/logging.h"
#include "common/utils.h"
#include "runtime/descriptors.h"
#include "util//mem_util.hpp"

namespace doris {

using AllocateMemFunc = std::function<uint8_t*(size_t size)>;
static Status init_collection(CollectionValue* value, const AllocateMemFunc& allocate,
                              uint32_t size, PrimitiveType child_type);

int sizeof_type(PrimitiveType type) {
    switch (type) {
    case TYPE_TINYINT:
        return sizeof(int8_t);
    case TYPE_SMALLINT:
        return sizeof(int16_t);
    case TYPE_INT:
        return sizeof(int32_t);
    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return sizeof(StringValue);
    case TYPE_ARRAY:
        return sizeof(CollectionValue);
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
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_NULL:
    case TYPE_ARRAY:
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

size_t CollectionValue::get_byte_size(const TypeDescriptor& type) const {
    size_t result = 0;
    if (_length == 0) {
        return result;
    }
    if (_has_null) {
        result += _length * sizeof(bool);
    }
    const auto& item_type = type.children[0];
    result += _length * item_type.get_slot_size();
    if (item_type.is_string_type()) {
        for (int i = 0; i < _length; ++i) {
            if (is_null_at(i)) {
                continue;
            }
            int item_offset = i * item_type.get_slot_size();
            StringValue* item = reinterpret_cast<StringValue*>(((uint8_t*)_data) + item_offset);
            result += item->len;
        }
    } else if (item_type.type == TYPE_ARRAY) {
        for (int i = 0; i < _length; ++i) {
            if (is_null_at(i)) {
                continue;
            }
            int item_offset = i * item_type.get_slot_size();
            CollectionValue* item =
                    reinterpret_cast<CollectionValue*>(((uint8_t*)_data) + item_offset);
            result += item->get_byte_size(item_type);
        }
    }
    return result;
}

ArrayIterator CollectionValue::iterator(PrimitiveType children_type) const {
    return ArrayIterator(children_type, this);
}

Status CollectionValue::init_collection(ObjectPool* pool, uint32_t size, PrimitiveType child_type,
                                        CollectionValue* value) {
    return doris::init_collection(
            value, [pool](size_t size) -> uint8_t* { return pool->add_array(new uint8_t[size]); },
            size, child_type);
}

static Status init_collection(CollectionValue* value, const AllocateMemFunc& allocate,
                              uint32_t size, PrimitiveType child_type) {
    if (value == nullptr) {
        return Status::InvalidArgument("collection value is null");
    }

    RETURN_IF_ERROR(type_check(child_type));

    if (size == 0) {
        new (value) CollectionValue(size);
        return Status::OK();
    }

    value->_data = allocate(size * sizeof_type(child_type));
    value->_length = size;
    value->_has_null = false;
    value->_null_signs = reinterpret_cast<bool*>(allocate(size));
    memset(value->_null_signs, 0, size * sizeof(bool));

    return Status::OK();
}

Status CollectionValue::init_collection(MemPool* pool, uint32_t size, PrimitiveType child_type,
                                        CollectionValue* value) {
    return doris::init_collection(
            value, [pool](size_t size) { return pool->allocate(size); }, size, child_type);
}

Status CollectionValue::init_collection(FunctionContext* context, uint32_t size,
                                        PrimitiveType child_type, CollectionValue* value) {
    return doris::init_collection(
            value, [context](size_t size) { return context->allocate(size); }, size, child_type);
}

CollectionValue CollectionValue::from_collection_val(const CollectionVal& val) {
    return CollectionValue(val.data, val.length, val.has_null, val.null_signs);
}

// Deep copy collection.
// NOTICE: The CollectionValue* shallow_copied_cv must be initialized by calling memcpy function first (
// copy data from origin collection value).
void CollectionValue::deep_copy_collection(CollectionValue* shallow_copied_cv,
                                           const TypeDescriptor& item_type,
                                           const GenMemFootprintFunc& gen_mem_footprint,
                                           bool convert_ptrs) {
    CollectionValue* cv = shallow_copied_cv;
    if (cv->length() == 0) {
        return;
    }

    int coll_byte_size = cv->length() * item_type.get_slot_size();
    int nulls_size = cv->has_null() ? cv->length() * sizeof(bool) : 0;

    MemFootprint footprint = gen_mem_footprint(coll_byte_size + nulls_size);
    int64_t offset = footprint.first;
    char* coll_data = reinterpret_cast<char*>(footprint.second);

    // copy and assign null_signs
    if (cv->has_null()) {
        memory_copy(convert_to<bool*>(coll_data), cv->null_signs(), nulls_size);
        cv->set_null_signs(convert_to<bool*>(coll_data));
    } else {
        cv->set_null_signs(nullptr);
    }
    // copy and assgin data
    memory_copy(coll_data + nulls_size, cv->data(), coll_byte_size);
    cv->set_data(coll_data + nulls_size);

    deep_copy_items_in_collection(cv, coll_data, item_type, gen_mem_footprint, convert_ptrs);

    if (convert_ptrs) {
        cv->set_data(convert_to<char*>(offset + nulls_size));
        if (cv->has_null()) {
            cv->set_null_signs(convert_to<bool*>(offset));
        }
    }
}

// Deep copy items in collection.
// NOTICE: The CollectionValue* shallow_copied_cv must be initialized by calling memcpy function first (
// copy data from origin collection value).
void CollectionValue::deep_copy_items_in_collection(CollectionValue* shallow_copied_cv, char* base,
                                                    const TypeDescriptor& item_type,
                                                    const GenMemFootprintFunc& gen_mem_footprint,
                                                    bool convert_ptrs) {
    int nulls_size = shallow_copied_cv->has_null() ? shallow_copied_cv->length() : 0;
    char* item_base = base + nulls_size;
    if (item_type.is_string_type()) {
        // when itemtype is string, copy every string item
        for (int i = 0; i < shallow_copied_cv->length(); ++i) {
            if (shallow_copied_cv->is_null_at(i)) {
                continue;
            }
            char* item_offset = item_base + i * item_type.get_slot_size();
            StringValue* dst_item_v = convert_to<StringValue*>(item_offset);
            if (dst_item_v->len != 0) {
                MemFootprint footprint = gen_mem_footprint(dst_item_v->len);
                int64_t offset = footprint.first;
                char* string_copy = reinterpret_cast<char*>(footprint.second);
                memory_copy(string_copy, dst_item_v->ptr, dst_item_v->len);
                dst_item_v->ptr = (convert_ptrs ? convert_to<char*>(offset) : string_copy);
            }
        }
    } else if (item_type.type == TYPE_ARRAY) {
        for (int i = 0; i < shallow_copied_cv->length(); ++i) {
            if (shallow_copied_cv->is_null_at(i)) {
                continue;
            }
            char* item_offset = item_base + i * item_type.get_slot_size();
            CollectionValue* item_cv = convert_to<CollectionValue*>(item_offset);
            deep_copy_collection(item_cv, item_type.children[0], gen_mem_footprint, convert_ptrs);
        }
    }
}

void CollectionValue::deserialize_collection(CollectionValue* cv, const char* tuple_data,
                                             const TypeDescriptor& type) {
    if (cv->length() == 0) {
        new (cv) CollectionValue(cv->length());
        return;
    }
    // assgin data and null_sign pointer position in tuple_data
    int data_offset = convert_to<int>(cv->data());
    cv->set_data(convert_to<char*>(tuple_data + data_offset));
    if (cv->has_null()) {
        int null_offset = convert_to<int>(cv->null_signs());
        cv->set_null_signs(convert_to<bool*>(tuple_data + null_offset));
    }

    const TypeDescriptor& item_type = type.children[0];
    if (item_type.is_string_type()) {
        // copy every string item
        for (size_t i = 0; i < cv->length(); ++i) {
            if (cv->is_null_at(i)) {
                continue;
            }

            StringValue* dst_item_v =
                    convert_to<StringValue*>((uint8_t*)cv->data() + i * item_type.get_slot_size());

            if (dst_item_v->len != 0) {
                int offset = convert_to<int>(dst_item_v->ptr);
                dst_item_v->ptr = convert_to<char*>(tuple_data + offset);
            }
        }
    } else if (item_type.type == TYPE_ARRAY) {
        for (size_t i = 0; i < cv->length(); ++i) {
            if (cv->is_null_at(i)) {
                continue;
            }

            CollectionValue* item_cv = convert_to<CollectionValue*>((uint8_t*)cv->data() +
                                                                    i * item_type.get_slot_size());
            deserialize_collection(item_cv, tuple_data, item_type);
        }
    }
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
    case TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(iter.value()) = reinterpret_cast<const TinyIntVal*>(value)->val;
        break;
    case TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(iter.value()) =
                reinterpret_cast<const SmallIntVal*>(value)->val;
        break;
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
    case TYPE_ARRAY: {
        const CollectionVal* src = reinterpret_cast<const CollectionVal*>(value);
        CollectionValue* dest = reinterpret_cast<CollectionValue*>(iter.value());
        *dest = CollectionValue::from_collection_val(*src);
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
        reinterpret_cast<SmallIntVal*>(dest)->val = *reinterpret_cast<const int16_t*>(value());
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
