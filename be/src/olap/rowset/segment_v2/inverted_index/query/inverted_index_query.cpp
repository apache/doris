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

#include "inverted_index_query.h"

#include <filesystem>
#include <set>

#include "io/fs/file_system.h"
#include "olap/column_predicate.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/types.h"
#include "util/time.h"
#include "vec/common/string_ref.h"

namespace doris::segment_v2 {

template <PrimitiveType Type, PredicateType PT>
Status Helper<Type, PT>::create_and_add_value(const TypeInfo* type_info, char* value,
                                              InvertedIndexQueryType t,
                                              std::unique_ptr<InvertedIndexQueryBase>& result) {
    using CppType = typename PredicatePrimitiveTypeTraits<Type>::PredicateFieldType;

    if (is_range_query(t)) {
        auto range_query_ptr = std::make_unique<InvertedIndexRangeQuery<Type, PT>>(type_info);
        RETURN_IF_ERROR(range_query_ptr->add_value(*reinterpret_cast<CppType*>(value), t));
        result = std::move(range_query_ptr);
    } else {
        auto point_query_ptr = std::make_unique<InvertedIndexPointQuery<Type, PT>>(type_info);
        RETURN_IF_ERROR(point_query_ptr->add_value(*reinterpret_cast<CppType*>(value), t));
        result = std::move(point_query_ptr);
    }

    return Status::OK();
}

template <PredicateType PT>
Status InvertedIndexQueryBase::create_and_add_value_from_field_type(
        const TypeInfo* type_info, char* value, InvertedIndexQueryType t,
        std::unique_ptr<InvertedIndexQueryBase>& result) {
    Status st;
    switch (type_info->type()) {
    case FieldType::OLAP_FIELD_TYPE_DATETIME: {
        st = Helper<PrimitiveType::TYPE_DATETIME, PT>::create_and_add_value(type_info, value, t,
                                                                            result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATE: {
        st = Helper<PrimitiveType::TYPE_DATE, PT>::create_and_add_value(type_info, value, t,
                                                                        result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
        st = Helper<PrimitiveType::TYPE_DATETIMEV2, PT>::create_and_add_value(type_info, value, t,
                                                                              result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        st = Helper<PrimitiveType::TYPE_DATEV2, PT>::create_and_add_value(type_info, value, t,
                                                                          result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        st = Helper<PrimitiveType::TYPE_TINYINT, PT>::create_and_add_value(type_info, value, t,
                                                                           result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_SMALLINT: {
        st = Helper<PrimitiveType::TYPE_SMALLINT, PT>::create_and_add_value(type_info, value, t,
                                                                            result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_INT: {
        st = Helper<PrimitiveType::TYPE_INT, PT>::create_and_add_value(type_info, value, t, result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_LARGEINT: {
        st = Helper<PrimitiveType::TYPE_LARGEINT, PT>::create_and_add_value(type_info, value, t,
                                                                            result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
        st = Helper<PrimitiveType::TYPE_DECIMAL32, PT>::create_and_add_value(type_info, value, t,
                                                                             result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
        st = Helper<PrimitiveType::TYPE_DECIMAL64, PT>::create_and_add_value(type_info, value, t,
                                                                             result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
        st = Helper<PrimitiveType::TYPE_DECIMAL128I, PT>::create_and_add_value(type_info, value, t,
                                                                               result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
        st = Helper<PrimitiveType::TYPE_DOUBLE, PT>::create_and_add_value(type_info, value, t,
                                                                          result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_FLOAT: {
        st = Helper<PrimitiveType::TYPE_FLOAT, PT>::create_and_add_value(type_info, value, t,
                                                                         result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT: {
        st = Helper<PrimitiveType::TYPE_BIGINT, PT>::create_and_add_value(type_info, value, t,
                                                                          result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        st = Helper<PrimitiveType::TYPE_BOOLEAN, PT>::create_and_add_value(type_info, value, t,
                                                                           result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_CHAR: {
        st = Helper<PrimitiveType::TYPE_CHAR, PT>::create_and_add_value(type_info, value, t,
                                                                        result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_VARCHAR: {
        st = Helper<PrimitiveType::TYPE_VARCHAR, PT>::create_and_add_value(type_info, value, t,
                                                                           result);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        st = Helper<PrimitiveType::TYPE_STRING, PT>::create_and_add_value(type_info, value, t,
                                                                          result);
        break;
    }
    default:
        return Status::NotSupported("Unsupported column type for inverted index {}",
                                    type_info->type());
    }
    if (!st.ok()) {
        return st;
    }
    return Status::OK();
}

template Status InvertedIndexQueryBase::create_and_add_value_from_field_type<PredicateType::MATCH>(
        const TypeInfo*, char*, InvertedIndexQueryType, std::unique_ptr<InvertedIndexQueryBase>&);

template <PrimitiveType Type, PredicateType PT>
InvertedIndexPointQuery<Type, PT>::InvertedIndexPointQuery(const TypeInfo* type_info)
        : _type_info(type_info) {
    _value_key_coder = get_key_coder(type_info->type());
}

template <PrimitiveType Type, PredicateType PT>
std::string InvertedIndexPointQuery<Type, PT>::to_string() {
    std::string result;
    if constexpr (std::is_same_v<T, StringRef>) {
        for (const T* v : _values) {
            result += v->to_string();
        }
    } else {
        for (auto& v : _values) {
            result += _type_info->to_string(v);
        }
    }
    return result;
}

template <PrimitiveType Type, PredicateType PT>
Status InvertedIndexPointQuery<Type, PT>::add_value(const T& value, InvertedIndexQueryType t) {
    if constexpr (std::is_same_v<T, StringRef>) {
        auto act_len = strnlen(value.data, value.size);
        std::string value_str(value.data, act_len);
        _values_encoded.push_back(value_str);
    } else {
        std::string tmp;
        _value_key_coder->full_encode_ascending(&value, &tmp);
        _values_encoded.push_back(tmp);
    }
    _values.push_back(&value);
    _type = t;
    return Status::OK();
}

template <PrimitiveType Type, PredicateType PT>
InvertedIndexRangeQuery<Type, PT>::InvertedIndexRangeQuery(const TypeInfo* type_info)
        : _type_info(type_info) {
    _value_key_coder = get_key_coder(type_info->type());
    auto max_v = type_limit<T>::max();
    auto min_v = type_limit<T>::min();
    _value_key_coder->full_encode_ascending(&max_v, &_high_value_encoded);
    _value_key_coder->full_encode_ascending(&min_v, &_low_value_encoded);
}

template <PrimitiveType Type, PredicateType PT>
std::string InvertedIndexRangeQuery<Type, PT>::to_string() {
    std::string low_op = _inclusive_low ? ">=" : ">";
    std::string high_op = _inclusive_high ? "<=" : "<";
    std::string buffer;
    if (_low_value != nullptr) {
        buffer.append(_type_info->to_string(_low_value) + low_op + " ");
    }
    if (_high_value != nullptr) {
        buffer.append(_type_info->to_string(_high_value) + high_op);
    }
    return buffer;
};

template <PrimitiveType Type, PredicateType PT>
Status InvertedIndexRangeQuery<Type, PT>::add_value(const T& value, InvertedIndexQueryType t) {
    switch (t) {
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        _low_value = &value;
        _low_value_encoded.clear();
        _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
        break;
    }

    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        _low_value = &value;
        _inclusive_low = true;
        _low_value_encoded.clear();
        _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
        break;
    }

    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        _high_value = &value;
        _high_value_encoded.clear();
        _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
        break;
    }

    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        _high_value = &value;
        _inclusive_high = true;
        _high_value_encoded.clear();
        _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
        break;
    }
    case InvertedIndexQueryType::EQUAL_QUERY: {
        _high_value = _low_value = &value;
        _high_value_encoded.clear();
        _value_key_coder->full_encode_ascending(&value, &_high_value_encoded);
        _low_value_encoded.clear();
        _value_key_coder->full_encode_ascending(&value, &_low_value_encoded);
        break;
    }
    default: {
        return Status::InternalError("Add value failed! Unsupported PredicateType {}", PT);
    }
    }
    return Status::OK();
}

#define INSTANTIATE_FOR_TYPE_AND_PREDICATE(P, C)          \
    template class C<TYPE_BOOLEAN, PredicateType::P>;     \
    template class C<TYPE_INT, PredicateType::P>;         \
    template class C<TYPE_TINYINT, PredicateType::P>;     \
    template class C<TYPE_SMALLINT, PredicateType::P>;    \
    template class C<TYPE_BIGINT, PredicateType::P>;      \
    template class C<TYPE_LARGEINT, PredicateType::P>;    \
    template class C<TYPE_FLOAT, PredicateType::P>;       \
    template class C<TYPE_DOUBLE, PredicateType::P>;      \
    template class C<TYPE_CHAR, PredicateType::P>;        \
    template class C<TYPE_STRING, PredicateType::P>;      \
    template class C<TYPE_VARCHAR, PredicateType::P>;     \
    template class C<TYPE_TIME, PredicateType::P>;        \
    template class C<TYPE_TIMEV2, PredicateType::P>;      \
    template class C<TYPE_DATE, PredicateType::P>;        \
    template class C<TYPE_DATEV2, PredicateType::P>;      \
    template class C<TYPE_DATETIME, PredicateType::P>;    \
    template class C<TYPE_DATETIMEV2, PredicateType::P>;  \
    template class C<TYPE_DECIMALV2, PredicateType::P>;   \
    template class C<TYPE_DECIMAL32, PredicateType::P>;   \
    template class C<TYPE_DECIMAL64, PredicateType::P>;   \
    template class C<TYPE_DECIMAL128I, PredicateType::P>; 

#define INSTANTIATE_FOR_TYPE(C)                          \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(EQ, C)            \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(NE, C)            \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(LT, C)            \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(LE, C)            \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(GT, C)            \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(GE, C)            \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(IN_LIST, C)       \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(NOT_IN_LIST, C)   \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(IS_NULL, C)       \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(IS_NOT_NULL, C)   \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(BF, C)            \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(BITMAP_FILTER, C) \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(MATCH, C)         \
    INSTANTIATE_FOR_TYPE_AND_PREDICATE(RANGE, C)

INSTANTIATE_FOR_TYPE(InvertedIndexPointQuery)
INSTANTIATE_FOR_TYPE(InvertedIndexRangeQuery)

} // namespace doris::segment_v2
