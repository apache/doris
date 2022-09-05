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

#include "common/object_pool.h"
#include "common/utils.h"
#include "runtime/mem_pool.h"
#include "runtime/raw_value.h"
#include "runtime/string_value.h"
#include "runtime/types.h"
#include "util/mem_util.hpp"

namespace doris {

template <PrimitiveType>
struct CollectionValueSubTypeTrait;

template <>
struct CollectionValueSubTypeTrait<TYPE_NULL> {
    using CppType = int8_t; // slot size : 1
};

template <>
struct CollectionValueSubTypeTrait<TYPE_BOOLEAN> {
    using CppType = bool;
    using AnyValType = BooleanVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_TINYINT> {
    using CppType = int8_t;
    using AnyValType = TinyIntVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_SMALLINT> {
    using CppType = int16_t;
    using AnyValType = SmallIntVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_INT> {
    using CppType = int32_t;
    using AnyValType = IntVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_BIGINT> {
    using CppType = int64_t;
    using AnyValType = BigIntVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_LARGEINT> {
    using CppType = __int128_t;
    using AnyValType = LargeIntVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_FLOAT> {
    using CppType = float;
    using AnyValType = FloatVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_DOUBLE> {
    using CppType = double;
    using AnyValType = DoubleVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_CHAR> {
    using CppType = StringValue;
    using AnyValType = StringVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_VARCHAR> {
    using CppType = StringValue;
    using AnyValType = StringVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_STRING> {
    using CppType = StringValue;
    using AnyValType = StringVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_DATE> {
    using CppType = uint24_t;
    using AnyValType = DateTimeVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_DATETIME> {
    using CppType = uint64_t;
    using AnyValType = DateTimeVal;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_DECIMALV2> {
    using CppType = decimal12_t;
    using AnyValType = DecimalV2Val;
};

template <>
struct CollectionValueSubTypeTrait<TYPE_ARRAY> {
    using CppType = CollectionValue;
    using AnyValType = CollectionVal;
};

struct ArrayIteratorFunctionsBase {};

template <PrimitiveType type>
struct GenericArrayIteratorFunctions : public ArrayIteratorFunctionsBase {
    using CppType = typename CollectionValueSubTypeTrait<type>::CppType;
    using AnyValType = typename CollectionValueSubTypeTrait<type>::AnyValType;

    constexpr static int get_type_size() { return sizeof(CppType); }
    static void shallow_set(void* item, const AnyVal* value) {
        *static_cast<CppType*>(item) = static_cast<const AnyValType*>(value)->val;
    }
    static void shallow_get(AnyVal* value, const void* item) {
        static_cast<AnyValType*>(value)->val = *static_cast<const CppType*>(item);
    }
    static void self_deep_copy(void* item, const TypeDescriptor& type_desc,
                               const GenMemFootprintFunc& gen_mem_footprint, bool convert_ptrs) {}
    static void deserialize(void* item, const char* tuple_data, const TypeDescriptor& type_desc) {}
    static size_t get_byte_size(const void* item, const TypeDescriptor& type_desc) { return 0; }
    static void raw_value_write(void* item, const void* value, const TypeDescriptor& type_desc,
                                MemPool* pool) {
        RawValue::write(value, item, type_desc, pool);
    }
};

template <PrimitiveType type>
struct ArrayIteratorFunctions : public GenericArrayIteratorFunctions<type> {};

template <PrimitiveType type>
struct ArrayIteratorFunctionsForString : public GenericArrayIteratorFunctions<type> {
    using CppType = StringValue;
    using AnyValType = StringVal;

    static void shallow_set(void* item, const AnyVal* value) {
        const auto* src = static_cast<const AnyValType*>(value);
        auto* dst = static_cast<CppType*>(item);
        dst->ptr = convert_to<char*>(src->ptr);
        dst->len = src->len;
    }
    static void shallow_get(AnyVal* value, const void* item) {
        const auto* src = static_cast<const CppType*>(item);
        auto* dst = static_cast<AnyValType*>(value);
        dst->ptr = convert_to<uint8_t*>(src->ptr);
        dst->len = src->len;
    }
    static void self_deep_copy(void* item, const TypeDescriptor&,
                               const GenMemFootprintFunc& gen_mem_footprint, bool convert_ptrs) {
        auto* string = static_cast<CppType*>(item);
        if (!string->len) {
            return;
        }
        MemFootprint footprint = gen_mem_footprint(string->len);
        int64_t offset = footprint.first;
        auto* copied_string = reinterpret_cast<char*>(footprint.second);
        memory_copy(copied_string, string->ptr, string->len);
        string->ptr = (convert_ptrs ? convert_to<char*>(offset) : copied_string);
    }
    static void deserialize(void* item, const char* tuple_data, const TypeDescriptor& type_desc) {
        DCHECK((item != nullptr) && (tuple_data != nullptr)) << "item or tuple_data is nullptr";
        auto* string_value = static_cast<CppType*>(item);
        if (string_value->len) {
            int64_t offset = convert_to<int64_t>(string_value->ptr);
            string_value->ptr = convert_to<char*>(tuple_data + offset);
        }
    }
    static size_t get_byte_size(const void* item, const TypeDescriptor&) {
        return static_cast<const CppType*>(item)->len;
    }
};

template <>
struct ArrayIteratorFunctions<TYPE_CHAR> : public ArrayIteratorFunctionsForString<TYPE_CHAR> {};
template <>
struct ArrayIteratorFunctions<TYPE_VARCHAR> : public ArrayIteratorFunctionsForString<TYPE_VARCHAR> {
};
template <>
struct ArrayIteratorFunctions<TYPE_STRING> : public ArrayIteratorFunctionsForString<TYPE_STRING> {};

template <>
struct ArrayIteratorFunctions<TYPE_DATE> : public GenericArrayIteratorFunctions<TYPE_DATE> {
    using GenericArrayIteratorFunctions<TYPE_DATE>::CppType;
    using GenericArrayIteratorFunctions<TYPE_DATE>::AnyValType;

    static void shallow_set(void* item, const AnyVal* value) {
        const auto* src = static_cast<const AnyValType*>(value);
        auto* dst = static_cast<CppType*>(item);
        *dst = DateTimeValue::from_datetime_val(*src).to_olap_date();
    }
    static void shallow_get(AnyVal* value, const void* item) {
        const auto* src = static_cast<const CppType*>(item);
        auto* dst = static_cast<AnyValType*>(value);
        DateTimeValue data;
        data.from_olap_date(uint32_t(*src));
        data.to_datetime_val(dst);
    }
    static void raw_value_write(void* item, const void* value, const TypeDescriptor& type_desc,
                                MemPool* pool) {
        DateTimeVal date_time_val;
        shallow_get(&date_time_val, value);
        shallow_set(item, &date_time_val);
    }
};
template <>
struct ArrayIteratorFunctions<TYPE_DATETIME> : public GenericArrayIteratorFunctions<TYPE_DATETIME> {
    using GenericArrayIteratorFunctions<TYPE_DATETIME>::CppType;
    using GenericArrayIteratorFunctions<TYPE_DATETIME>::AnyValType;

    static void shallow_set(void* item, const AnyVal* value) {
        const auto* src = static_cast<const AnyValType*>(value);
        auto* dst = static_cast<CppType*>(item);
        *dst = DateTimeValue::from_datetime_val(*src).to_olap_datetime();
    }
    static void shallow_get(AnyVal* value, const void* item) {
        const auto* src = static_cast<const CppType*>(item);
        auto* dst = static_cast<AnyValType*>(value);
        DateTimeValue data;
        data.from_olap_datetime(*src);
        data.to_datetime_val(dst);
    }
    static void raw_value_write(void* item, const void* value, const TypeDescriptor& type_desc,
                                MemPool* pool) {
        DateTimeVal date_time_val;
        shallow_get(&date_time_val, value);
        shallow_set(item, &date_time_val);
    }
};

template <>
struct ArrayIteratorFunctions<TYPE_DECIMALV2>
        : public GenericArrayIteratorFunctions<TYPE_DECIMALV2> {
    using GenericArrayIteratorFunctions<TYPE_DECIMALV2>::CppType;
    using GenericArrayIteratorFunctions<TYPE_DECIMALV2>::AnyValType;

    static void shallow_set(void* item, const AnyVal* value) {
        const auto* src = static_cast<const AnyValType*>(value);
        auto* dst = static_cast<CppType*>(item);
        auto decimal_value = DecimalV2Value::from_decimal_val(*src);
        dst->integer = decimal_value.int_value();
        dst->fraction = decimal_value.frac_value();
    }
    static void shallow_get(AnyVal* value, const void* item) {
        const auto* src = static_cast<const CppType*>(item);
        auto* dst = static_cast<AnyValType*>(value);
        DecimalV2Value(src->integer, src->fraction).to_decimal_val(dst);
    }
    static void raw_value_write(void* item, const void* value, const TypeDescriptor& type_desc,
                                MemPool* pool) {
        DecimalV2Val decimal_val;
        shallow_get(&decimal_val, value);
        shallow_set(item, &decimal_val);
    }
};

template <>
struct ArrayIteratorFunctions<TYPE_ARRAY> : public GenericArrayIteratorFunctions<TYPE_ARRAY> {
    using GenericArrayIteratorFunctions<TYPE_ARRAY>::CppType;
    using GenericArrayIteratorFunctions<TYPE_ARRAY>::AnyValType;

    static void shallow_set(void* item, const AnyVal* value) {
        *static_cast<CppType*>(item) =
                CppType::from_collection_val(*static_cast<const AnyValType*>(value));
    }
    static void shallow_get(AnyVal* value, const void* item) {
        static_cast<const CppType*>(item)->to_collection_val(static_cast<AnyValType*>(value));
    }
    static void self_deep_copy(void* item, const TypeDescriptor& type_desc,
                               const GenMemFootprintFunc& gen_mem_footprint, bool convert_ptrs) {
        auto* collection_value = static_cast<CppType*>(item);
        CollectionValue::deep_copy_collection(collection_value, type_desc.children[0],
                                              gen_mem_footprint, convert_ptrs);
    }
    static void deserialize(void* item, const char* tuple_data, const TypeDescriptor& type_desc) {
        CollectionValue::deserialize_collection(static_cast<CppType*>(item), tuple_data,
                                                type_desc.children[0]);
    }
    static size_t get_byte_size(const void* item, const TypeDescriptor& type_desc) {
        const auto* collection_value = static_cast<const CppType*>(item);
        return collection_value->get_byte_size(type_desc.children[0]);
    }
};

ArrayIterator CollectionValue::iterator(PrimitiveType child_type) {
    return internal_iterator(child_type);
}

ArrayIterator CollectionValue::internal_iterator(PrimitiveType child_type) const {
    switch (child_type) {
    case TYPE_BOOLEAN:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_BOOLEAN>*>(nullptr));
    case TYPE_TINYINT:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_TINYINT>*>(nullptr));
    case TYPE_SMALLINT:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_SMALLINT>*>(nullptr));
    case TYPE_INT:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_INT>*>(nullptr));
    case TYPE_BIGINT:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_BIGINT>*>(nullptr));
    case TYPE_LARGEINT:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_LARGEINT>*>(nullptr));
    case TYPE_FLOAT:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_FLOAT>*>(nullptr));
    case TYPE_DOUBLE:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_DOUBLE>*>(nullptr));
    case TYPE_CHAR:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_CHAR>*>(nullptr));
    case TYPE_VARCHAR:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_VARCHAR>*>(nullptr));
    case TYPE_STRING:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_STRING>*>(nullptr));
    case TYPE_DATE:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_DATE>*>(nullptr));
    case TYPE_DATETIME:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_DATETIME>*>(nullptr));
    case TYPE_ARRAY:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_ARRAY>*>(nullptr));
    case TYPE_DECIMALV2:
        return ArrayIterator(const_cast<CollectionValue*>(this),
                             static_cast<ArrayIteratorFunctions<TYPE_DECIMALV2>*>(nullptr));
    default:
        DCHECK(false) << "Invalid child type: " << child_type;
        __builtin_unreachable();
    }
}

const ArrayIterator CollectionValue::iterator(PrimitiveType child_type) const {
    return internal_iterator(child_type);
}

Status type_check(PrimitiveType type) {
    switch (type) {
    case TYPE_NULL:

    case TYPE_BOOLEAN:

    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:

    case TYPE_FLOAT:
    case TYPE_DOUBLE:

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:

    case TYPE_DATE:
    case TYPE_DATETIME:

    case TYPE_DECIMALV2:

    case TYPE_ARRAY:
        break;
    default:
        return Status::InvalidArgument("Type not implemented: {}", type);
    }
    return Status::OK();
}

int sizeof_type(PrimitiveType type) {
    if (type_check(type).ok()) {
        return CollectionValue().iterator(type).type_size();
    } else {
        DCHECK(false) << "Type not implemented: " << type;
        return 0;
    }
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

size_t CollectionValue::get_byte_size(const TypeDescriptor& item_type) const {
    size_t result = 0;
    if (_length == 0) {
        return result;
    }
    if (_has_null) {
        result += _length * sizeof(bool);
    }
    auto iterator = CollectionValue::iterator(item_type.type);
    result += _length * iterator.type_size();

    while (!iterator.is_type_fixed_width() && iterator.has_next()) {
        result += iterator.get_byte_size(item_type);
        iterator.next();
    }
    return result;
}

Status CollectionValue::init_collection(ObjectPool* pool, uint64_t size, PrimitiveType child_type,
                                        CollectionValue* value) {
    return init_collection(
            value, [pool](size_t size) -> uint8_t* { return pool->add_array(new uint8_t[size]); },
            size, child_type);
}

Status CollectionValue::init_collection(CollectionValue* value, const AllocateMemFunc& allocate,
                                        uint64_t size, PrimitiveType child_type) {
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

Status CollectionValue::init_collection(MemPool* pool, uint64_t size, PrimitiveType child_type,
                                        CollectionValue* value) {
    return init_collection(
            value, [pool](size_t size) { return pool->allocate_aligned(size, 16); }, size,
            child_type);
}

Status CollectionValue::init_collection(FunctionContext* context, uint64_t size,
                                        PrimitiveType child_type, CollectionValue* value) {
    return init_collection(
            value, [context](size_t size) { return context->aligned_allocate(16, size); }, size,
            child_type);
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

    auto iterator = cv->iterator(item_type.type);
    uint64_t coll_byte_size = cv->length() * iterator.type_size();
    uint64_t nulls_size = cv->has_null() ? cv->length() * sizeof(bool) : 0;

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

    while (!iterator.is_type_fixed_width() && iterator.has_next()) {
        iterator.self_deep_copy(item_type, gen_mem_footprint, convert_ptrs);
        iterator.next();
    }

    if (convert_ptrs) {
        cv->set_data(convert_to<char*>(offset + nulls_size));
        if (cv->has_null()) {
            cv->set_null_signs(convert_to<bool*>(offset));
        }
    }
}

void CollectionValue::deserialize_collection(CollectionValue* cv, const char* tuple_data,
                                             const TypeDescriptor& item_type) {
    if (cv->length() == 0) {
        new (cv) CollectionValue(cv->length());
        return;
    }
    // assgin data and null_sign pointer position in tuple_data
    int64_t data_offset = convert_to<int64_t>(cv->data());
    cv->set_data(convert_to<char*>(tuple_data + data_offset));
    if (cv->has_null()) {
        int64_t null_offset = convert_to<int64_t>(cv->null_signs());
        cv->set_null_signs(convert_to<bool*>(tuple_data + null_offset));
    }
    auto iterator = cv->iterator(item_type.type);
    while (!iterator.is_type_fixed_width() && iterator.has_next()) {
        iterator.deserialize(tuple_data, item_type);
        iterator.next();
    }
}
} // namespace doris
