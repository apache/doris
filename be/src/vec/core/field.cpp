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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Field.cpp
// and modified by Doris

#include "vec/core/field.h"

#include "runtime/jsonb_value.h"
#include "runtime/primitive_type.h"
#include "util/bitmap_value.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/decimal_comparison.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"

namespace doris::vectorized {
class BufferReadable;
class BufferWritable;

template <PrimitiveType T>
bool dec_equal(typename PrimitiveTypeTraits<T>::ColumnItemType x,
               typename PrimitiveTypeTraits<T>::ColumnItemType y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <PrimitiveType T>
bool dec_less(typename PrimitiveTypeTraits<T>::ColumnItemType x,
              typename PrimitiveTypeTraits<T>::ColumnItemType y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <PrimitiveType T>
bool dec_less_or_equal(typename PrimitiveTypeTraits<T>::ColumnItemType x,
                       typename PrimitiveTypeTraits<T>::ColumnItemType y, UInt32 x_scale,
                       UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

#define DECLARE_DECIMAL_COMPARISON(TYPE, PTYPE)                        \
    template <>                                                        \
    bool decimal_equal(TYPE x, TYPE y, UInt32 xs, UInt32 ys) {         \
        return dec_equal<PTYPE>(x, y, xs, ys);                         \
    }                                                                  \
    template <>                                                        \
    bool decimal_less(TYPE x, TYPE y, UInt32 xs, UInt32 ys) {          \
        return dec_less<PTYPE>(x, y, xs, ys);                          \
    }                                                                  \
    template <>                                                        \
    bool decimal_less_or_equal(TYPE x, TYPE y, UInt32 xs, UInt32 ys) { \
        return dec_less_or_equal<PTYPE>(x, y, xs, ys);                 \
    }

DECLARE_DECIMAL_COMPARISON(Decimal32, TYPE_DECIMAL32)
DECLARE_DECIMAL_COMPARISON(Decimal64, TYPE_DECIMAL64)
DECLARE_DECIMAL_COMPARISON(Decimal128V2, TYPE_DECIMALV2)
DECLARE_DECIMAL_COMPARISON(Decimal256, TYPE_DECIMAL256)

template <>
bool decimal_equal(Decimal128V3 x, Decimal128V3 y, UInt32 xs, UInt32 ys) {
    return dec_equal<TYPE_DECIMAL128I>(x, y, xs, ys);
}
template <>
bool decimal_less(Decimal128V3 x, Decimal128V3 y, UInt32 xs, UInt32 ys) {
    return dec_less<TYPE_DECIMAL128I>(x, y, xs, ys);
}
template <>
bool decimal_less_or_equal(Decimal128V3 x, Decimal128V3 y, UInt32 xs, UInt32 ys) {
    return dec_less_or_equal<TYPE_DECIMAL128I>(x, y, xs, ys);
}

template <PrimitiveType Type>
void Field::create_concrete(typename PrimitiveTypeTraits<Type>::NearestFieldType&& x) {
    // In both Field and PODArray, small types may be stored as wider types,
    // e.g. char is stored as UInt64. Field can return this extended value
    // with get<StorageType>(). To avoid uninitialized results from get(),
    // we must initialize the entire wide stored type, and not just the
    // nominal type.
    using StorageType = typename PrimitiveTypeTraits<Type>::NearestFieldType;
    new (&storage) StorageType(x);
    type = Type;
    DCHECK_NE(type, PrimitiveType::INVALID_TYPE);
}

template <PrimitiveType Type>
void Field::create_concrete(const typename PrimitiveTypeTraits<Type>::NearestFieldType& x) {
    // In both Field and PODArray, small types may be stored as wider types,
    // e.g. char is stored as UInt64. Field can return this extended value
    // with get<StorageType>(). To avoid uninitialized results from get(),
    // we must initialize the entire wide stored type, and not just the
    // nominal type.
    using StorageType = typename PrimitiveTypeTraits<Type>::NearestFieldType;
    new (&storage) StorageType(x);
    type = Type;
    DCHECK_NE(type, PrimitiveType::INVALID_TYPE);
}

void Field::create(const Field& field) {
    switch (field.type) {
    case PrimitiveType::TYPE_NULL:
        create_concrete<TYPE_NULL>(
                field.template get<typename PrimitiveTypeTraits<TYPE_NULL>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DATETIMEV2:
        create_concrete<TYPE_DATETIMEV2>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DATEV2:
        create_concrete<TYPE_DATEV2>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DATEV2>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DATETIME:
    case PrimitiveType::TYPE_DATE:
    case PrimitiveType::TYPE_BOOLEAN:
    case PrimitiveType::TYPE_TINYINT:
    case PrimitiveType::TYPE_SMALLINT:
    case PrimitiveType::TYPE_INT:
    case PrimitiveType::TYPE_BIGINT:
        create_concrete<TYPE_BIGINT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_BIGINT>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_LARGEINT:
        create_concrete<TYPE_LARGEINT>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_LARGEINT>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_IPV4:
        create_concrete<TYPE_IPV4>(
                field.template get<typename PrimitiveTypeTraits<TYPE_IPV4>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_IPV6:
        create_concrete<TYPE_IPV6>(
                field.template get<typename PrimitiveTypeTraits<TYPE_IPV6>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_FLOAT:
    case PrimitiveType::TYPE_TIMEV2:
    case PrimitiveType::TYPE_DOUBLE:
        create_concrete<TYPE_DOUBLE>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DOUBLE>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_STRING:
        create_concrete<TYPE_STRING>(
                field.template get<typename PrimitiveTypeTraits<TYPE_STRING>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_CHAR:
        create_concrete<TYPE_CHAR>(
                field.template get<typename PrimitiveTypeTraits<TYPE_CHAR>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_VARCHAR:
        create_concrete<TYPE_NULL>(
                field.template get<typename PrimitiveTypeTraits<TYPE_NULL>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_JSONB:
        create_concrete<TYPE_JSONB>(
                field.template get<typename PrimitiveTypeTraits<TYPE_JSONB>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_ARRAY:
        create_concrete<TYPE_ARRAY>(
                field.template get<typename PrimitiveTypeTraits<TYPE_ARRAY>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_STRUCT:
        create_concrete<TYPE_STRUCT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_STRUCT>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_MAP:
        create_concrete<TYPE_MAP>(
                field.template get<typename PrimitiveTypeTraits<TYPE_MAP>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DECIMAL32:
        create_concrete<TYPE_DECIMAL32>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DECIMAL32>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DECIMAL64:
        create_concrete<TYPE_DECIMAL64>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DECIMAL64>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DECIMALV2:
        create_concrete<TYPE_DECIMALV2>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DECIMALV2>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DECIMAL128I:
        create_concrete<TYPE_DECIMAL128I>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DECIMAL128I>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DECIMAL256:
        create_concrete<TYPE_DECIMAL256>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DECIMAL256>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_VARIANT:
        create_concrete<TYPE_VARIANT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_VARIANT>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_BITMAP:
        create_concrete<TYPE_BITMAP>(
                field.template get<typename PrimitiveTypeTraits<TYPE_BITMAP>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_HLL:
        create_concrete<TYPE_HLL>(
                field.template get<typename PrimitiveTypeTraits<TYPE_HLL>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        create_concrete<TYPE_QUANTILE_STATE>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_QUANTILE_STATE>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_UINT32:
        create_concrete<TYPE_UINT32>(
                field.template get<typename PrimitiveTypeTraits<TYPE_UINT32>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_UINT64:
        create_concrete<TYPE_UINT64>(
                field.template get<typename PrimitiveTypeTraits<TYPE_UINT64>::NearestFieldType>());
        return;
    default:
        throw Exception(Status::FatalError("type not supported, type={}", field.get_type_name()));
    }
}

void Field::destroy() {
    switch (type) {
    case PrimitiveType::TYPE_STRING:
        destroy<typename PrimitiveTypeTraits<TYPE_STRING>::NearestFieldType>();
        break;
    case PrimitiveType::TYPE_CHAR:
        destroy<typename PrimitiveTypeTraits<TYPE_CHAR>::NearestFieldType>();
        break;
    case PrimitiveType::TYPE_VARCHAR:
        destroy<typename PrimitiveTypeTraits<TYPE_VARCHAR>::NearestFieldType>();
        break;
    case PrimitiveType::TYPE_JSONB:
        destroy<typename PrimitiveTypeTraits<TYPE_JSONB>::NearestFieldType>();
        break;
    case PrimitiveType::TYPE_ARRAY:
        destroy<typename PrimitiveTypeTraits<TYPE_ARRAY>::NearestFieldType>();
        break;
    case PrimitiveType::TYPE_STRUCT:
        destroy<typename PrimitiveTypeTraits<TYPE_STRUCT>::NearestFieldType>();
        break;
    case PrimitiveType::TYPE_MAP:
        destroy<typename PrimitiveTypeTraits<TYPE_MAP>::NearestFieldType>();
        break;
    case PrimitiveType::TYPE_VARIANT:
        destroy<typename PrimitiveTypeTraits<TYPE_VARIANT>::NearestFieldType>();
        break;
    case PrimitiveType::TYPE_BITMAP:
        destroy<typename PrimitiveTypeTraits<TYPE_BITMAP>::NearestFieldType>();
        break;
    case PrimitiveType::TYPE_HLL:
        destroy<typename PrimitiveTypeTraits<TYPE_HLL>::NearestFieldType>();
        break;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        destroy<typename PrimitiveTypeTraits<TYPE_QUANTILE_STATE>::NearestFieldType>();
        break;
    default:
        break;
    }

    type = PrimitiveType::
            TYPE_NULL; /// for exception safety in subsequent calls to destroy and create, when create fails.
}

void Field::assign(const Field& field) {
    switch (field.type) {
    case PrimitiveType::TYPE_NULL:
        assign_concrete<TYPE_NULL>(
                field.template get<typename PrimitiveTypeTraits<TYPE_NULL>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DATETIMEV2:
        assign_concrete<TYPE_DATETIMEV2>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DATETIME:
        assign_concrete<TYPE_DATETIME>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DATETIME>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DATE:
        assign_concrete<TYPE_DATE>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DATE>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DATEV2:
        assign_concrete<TYPE_DATEV2>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DATEV2>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_BOOLEAN:
    case PrimitiveType::TYPE_TINYINT:
    case PrimitiveType::TYPE_SMALLINT:
    case PrimitiveType::TYPE_INT:
    case PrimitiveType::TYPE_BIGINT:
        assign_concrete<TYPE_BIGINT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_BIGINT>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_LARGEINT:
        assign_concrete<TYPE_LARGEINT>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_LARGEINT>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_IPV4:
        assign_concrete<TYPE_IPV4>(
                field.template get<typename PrimitiveTypeTraits<TYPE_IPV4>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_IPV6:
        assign_concrete<TYPE_IPV6>(
                field.template get<typename PrimitiveTypeTraits<TYPE_IPV6>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DOUBLE:
        assign_concrete<TYPE_DOUBLE>(
                field.template get<typename PrimitiveTypeTraits<TYPE_DOUBLE>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_STRING:
        assign_concrete<TYPE_STRING>(
                field.template get<typename PrimitiveTypeTraits<TYPE_STRING>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_CHAR:
        assign_concrete<TYPE_CHAR>(
                field.template get<typename PrimitiveTypeTraits<TYPE_CHAR>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_VARCHAR:
        assign_concrete<TYPE_NULL>(
                field.template get<typename PrimitiveTypeTraits<TYPE_NULL>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_JSONB:
        assign_concrete<TYPE_JSONB>(
                field.template get<typename PrimitiveTypeTraits<TYPE_JSONB>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_ARRAY:
        assign_concrete<TYPE_ARRAY>(
                field.template get<typename PrimitiveTypeTraits<TYPE_ARRAY>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_STRUCT:
        assign_concrete<TYPE_STRUCT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_STRUCT>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_MAP:
        assign_concrete<TYPE_MAP>(
                field.template get<typename PrimitiveTypeTraits<TYPE_MAP>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DECIMAL32:
        assign_concrete<TYPE_DECIMAL32>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DECIMAL32>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DECIMAL64:
        assign_concrete<TYPE_DECIMAL64>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DECIMAL64>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DECIMALV2:
        assign_concrete<TYPE_DECIMALV2>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DECIMALV2>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DECIMAL128I:
        assign_concrete<TYPE_DECIMAL128I>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DECIMAL128I>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_DECIMAL256:
        assign_concrete<TYPE_DECIMAL256>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_DECIMAL256>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_VARIANT:
        assign_concrete<TYPE_VARIANT>(
                field.template get<typename PrimitiveTypeTraits<TYPE_VARIANT>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_BITMAP:
        assign_concrete<TYPE_BITMAP>(
                field.template get<typename PrimitiveTypeTraits<TYPE_BITMAP>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_HLL:
        assign_concrete<TYPE_HLL>(
                field.template get<typename PrimitiveTypeTraits<TYPE_HLL>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        assign_concrete<TYPE_QUANTILE_STATE>(
                field.template get<
                        typename PrimitiveTypeTraits<TYPE_QUANTILE_STATE>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_UINT32:
        assign_concrete<TYPE_UINT32>(
                field.template get<typename PrimitiveTypeTraits<TYPE_UINT32>::NearestFieldType>());
        return;
    case PrimitiveType::TYPE_UINT64:
        assign_concrete<TYPE_UINT64>(
                field.template get<typename PrimitiveTypeTraits<TYPE_UINT64>::NearestFieldType>());
        return;
    default:
        throw Exception(Status::FatalError("type not supported, type={}", field.get_type_name()));
    }
}

/// Assuming same types.
template <PrimitiveType Type>
void Field::assign_concrete(typename PrimitiveTypeTraits<Type>::NearestFieldType&& x) {
    auto* MAY_ALIAS ptr =
            reinterpret_cast<typename PrimitiveTypeTraits<Type>::NearestFieldType*>(&storage);
    *ptr = std::forward<typename PrimitiveTypeTraits<Type>::NearestFieldType>(x);
}

template <PrimitiveType Type>
void Field::assign_concrete(const typename PrimitiveTypeTraits<Type>::NearestFieldType& x) {
    auto* MAY_ALIAS ptr =
            reinterpret_cast<typename PrimitiveTypeTraits<Type>::NearestFieldType*>(&storage);
    *ptr = std::forward<const typename PrimitiveTypeTraits<Type>::NearestFieldType>(x);
}

std::string Field::get_type_name() const {
    return type_to_string(type);
}

#define MATCH_PRIMITIVE_TYPE(primite_type)                                                   \
    if (type == primite_type) {                                                              \
        const auto& v = get<typename PrimitiveTypeTraits<primite_type>::NearestFieldType>(); \
        return std::string_view(reinterpret_cast<const char*>(&v), sizeof(v));               \
    }

std::string_view Field::as_string_view() const {
    if (type == PrimitiveType::TYPE_STRING || type == PrimitiveType::TYPE_VARCHAR ||
        type == PrimitiveType::TYPE_CHAR) {
        const auto& s = get<String>();
        return {s.data(), s.size()};
    }
    // MATCH_PRIMITIVE_TYPE(INVALID_TYPE);
    // MATCH_PRIMITIVE_TYPE(TYPE_NULL);
    MATCH_PRIMITIVE_TYPE(TYPE_BOOLEAN);
    MATCH_PRIMITIVE_TYPE(TYPE_TINYINT);
    MATCH_PRIMITIVE_TYPE(TYPE_SMALLINT);
    MATCH_PRIMITIVE_TYPE(TYPE_INT);
    MATCH_PRIMITIVE_TYPE(TYPE_BIGINT);
    MATCH_PRIMITIVE_TYPE(TYPE_LARGEINT);
    MATCH_PRIMITIVE_TYPE(TYPE_FLOAT)
    MATCH_PRIMITIVE_TYPE(TYPE_DOUBLE);
    // MATCH_PRIMITIVE_TYPE(TYPE_VARCHAR);
    MATCH_PRIMITIVE_TYPE(TYPE_DATE);
    MATCH_PRIMITIVE_TYPE(TYPE_DATETIME);
    // MATCH_PRIMITIVE_TYPE(TYPE_BINARY);
    // MATCH_PRIMITIVE_TYPE(TYPE_DECIMAL);
    // MATCH_PRIMITIVE_TYPE(TYPE_CHAR);
    // MATCH_PRIMITIVE_TYPE(TYPE_STRUCT);
    // MATCH_PRIMITIVE_TYPE(TYPE_ARRAY);
    // MATCH_PRIMITIVE_TYPE(TYPE_MAP);
    // MATCH_PRIMITIVE_TYPE(TYPE_HLL);
    MATCH_PRIMITIVE_TYPE(TYPE_DECIMALV2);
    MATCH_PRIMITIVE_TYPE(TYPE_TIME);
    // MATCH_PRIMITIVE_TYPE(TYPE_BITMAP);
    // MATCH_PRIMITIVE_TYPE(TYPE_STRING);
    // MATCH_PRIMITIVE_TYPE(TYPE_QUANTILE_STATE);
    MATCH_PRIMITIVE_TYPE(TYPE_DATEV2);
    MATCH_PRIMITIVE_TYPE(TYPE_DATETIMEV2);
    MATCH_PRIMITIVE_TYPE(TYPE_TIMEV2);
    MATCH_PRIMITIVE_TYPE(TYPE_DECIMAL32);
    MATCH_PRIMITIVE_TYPE(TYPE_DECIMAL64);
    MATCH_PRIMITIVE_TYPE(TYPE_DECIMAL128I);
    // MATCH_PRIMITIVE_TYPE(TYPE_JSONB);
    // MATCH_PRIMITIVE_TYPE(TYPE_VARIANT);
    // MATCH_PRIMITIVE_TYPE(TYPE_LAMBDA_FUNCTION);
    // MATCH_PRIMITIVE_TYPE(TYPE_AGG_STATE);
    MATCH_PRIMITIVE_TYPE(TYPE_DECIMAL256);
    MATCH_PRIMITIVE_TYPE(TYPE_IPV4);
    MATCH_PRIMITIVE_TYPE(TYPE_IPV6);
    MATCH_PRIMITIVE_TYPE(TYPE_UINT32);
    MATCH_PRIMITIVE_TYPE(TYPE_UINT64);
    // MATCH_PRIMITIVE_TYPE(TYPE_FIXED_LENGTH_OBJECT);
    throw Exception(
            Status::FatalError("type not supported for as_string_view, type={}", get_type_name()));
}

#undef MATCH_PRIMITIVE_TYPE

#define DECLARE_FUNCTION(FUNC_NAME)                                                          \
    template void Field::FUNC_NAME<TYPE_NULL>(                                               \
            typename PrimitiveTypeTraits<TYPE_NULL>::NearestFieldType && rhs);               \
    template void Field::FUNC_NAME<TYPE_TINYINT>(                                            \
            typename PrimitiveTypeTraits<TYPE_TINYINT>::NearestFieldType && rhs);            \
    template void Field::FUNC_NAME<TYPE_SMALLINT>(                                           \
            typename PrimitiveTypeTraits<TYPE_SMALLINT>::NearestFieldType && rhs);           \
    template void Field::FUNC_NAME<TYPE_INT>(                                                \
            typename PrimitiveTypeTraits<TYPE_INT>::NearestFieldType && rhs);                \
    template void Field::FUNC_NAME<TYPE_BIGINT>(                                             \
            typename PrimitiveTypeTraits<TYPE_BIGINT>::NearestFieldType && rhs);             \
    template void Field::FUNC_NAME<TYPE_LARGEINT>(                                           \
            typename PrimitiveTypeTraits<TYPE_LARGEINT>::NearestFieldType && rhs);           \
    template void Field::FUNC_NAME<TYPE_DATE>(                                               \
            typename PrimitiveTypeTraits<TYPE_DATE>::NearestFieldType && rhs);               \
    template void Field::FUNC_NAME<TYPE_DATETIME>(                                           \
            typename PrimitiveTypeTraits<TYPE_DATETIME>::NearestFieldType && rhs);           \
    template void Field::FUNC_NAME<TYPE_DATEV2>(                                             \
            typename PrimitiveTypeTraits<TYPE_DATEV2>::NearestFieldType && rhs);             \
    template void Field::FUNC_NAME<TYPE_DATETIMEV2>(                                         \
            typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::NearestFieldType && rhs);         \
    template void Field::FUNC_NAME<TYPE_DECIMAL32>(                                          \
            typename PrimitiveTypeTraits<TYPE_DECIMAL32>::NearestFieldType && rhs);          \
    template void Field::FUNC_NAME<TYPE_DECIMAL64>(                                          \
            typename PrimitiveTypeTraits<TYPE_DECIMAL64>::NearestFieldType && rhs);          \
    template void Field::FUNC_NAME<TYPE_DECIMALV2>(                                          \
            typename PrimitiveTypeTraits<TYPE_DECIMALV2>::NearestFieldType && rhs);          \
    template void Field::FUNC_NAME<TYPE_DECIMAL128I>(                                        \
            typename PrimitiveTypeTraits<TYPE_DECIMAL128I>::NearestFieldType && rhs);        \
    template void Field::FUNC_NAME<TYPE_DECIMAL256>(                                         \
            typename PrimitiveTypeTraits<TYPE_DECIMAL256>::NearestFieldType && rhs);         \
    template void Field::FUNC_NAME<TYPE_CHAR>(                                               \
            typename PrimitiveTypeTraits<TYPE_CHAR>::NearestFieldType && rhs);               \
    template void Field::FUNC_NAME<TYPE_VARCHAR>(                                            \
            typename PrimitiveTypeTraits<TYPE_VARCHAR>::NearestFieldType && rhs);            \
    template void Field::FUNC_NAME<TYPE_STRING>(                                             \
            typename PrimitiveTypeTraits<TYPE_STRING>::NearestFieldType && rhs);             \
    template void Field::FUNC_NAME<TYPE_HLL>(                                                \
            typename PrimitiveTypeTraits<TYPE_HLL>::NearestFieldType && rhs);                \
    template void Field::FUNC_NAME<TYPE_VARIANT>(                                            \
            typename PrimitiveTypeTraits<TYPE_VARIANT>::NearestFieldType && rhs);            \
    template void Field::FUNC_NAME<TYPE_QUANTILE_STATE>(                                     \
            typename PrimitiveTypeTraits<TYPE_QUANTILE_STATE>::NearestFieldType && rhs);     \
    template void Field::FUNC_NAME<TYPE_ARRAY>(                                              \
            typename PrimitiveTypeTraits<TYPE_ARRAY>::NearestFieldType && rhs);              \
    template void Field::FUNC_NAME<TYPE_NULL>(                                               \
            const typename PrimitiveTypeTraits<TYPE_NULL>::NearestFieldType& rhs);           \
    template void Field::FUNC_NAME<TYPE_TINYINT>(                                            \
            const typename PrimitiveTypeTraits<TYPE_TINYINT>::NearestFieldType& rhs);        \
    template void Field::FUNC_NAME<TYPE_SMALLINT>(                                           \
            const typename PrimitiveTypeTraits<TYPE_SMALLINT>::NearestFieldType& rhs);       \
    template void Field::FUNC_NAME<TYPE_INT>(                                                \
            const typename PrimitiveTypeTraits<TYPE_INT>::NearestFieldType& rhs);            \
    template void Field::FUNC_NAME<TYPE_BIGINT>(                                             \
            const typename PrimitiveTypeTraits<TYPE_BIGINT>::NearestFieldType& rhs);         \
    template void Field::FUNC_NAME<TYPE_LARGEINT>(                                           \
            const typename PrimitiveTypeTraits<TYPE_LARGEINT>::NearestFieldType& rhs);       \
    template void Field::FUNC_NAME<TYPE_DATE>(                                               \
            const typename PrimitiveTypeTraits<TYPE_DATE>::NearestFieldType& rhs);           \
    template void Field::FUNC_NAME<TYPE_DATETIME>(                                           \
            const typename PrimitiveTypeTraits<TYPE_DATETIME>::NearestFieldType& rhs);       \
    template void Field::FUNC_NAME<TYPE_DATEV2>(                                             \
            const typename PrimitiveTypeTraits<TYPE_DATEV2>::NearestFieldType& rhs);         \
    template void Field::FUNC_NAME<TYPE_DATETIMEV2>(                                         \
            const typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::NearestFieldType& rhs);     \
    template void Field::FUNC_NAME<TYPE_DECIMAL32>(                                          \
            const typename PrimitiveTypeTraits<TYPE_DECIMAL32>::NearestFieldType& rhs);      \
    template void Field::FUNC_NAME<TYPE_DECIMAL64>(                                          \
            const typename PrimitiveTypeTraits<TYPE_DECIMAL64>::NearestFieldType& rhs);      \
    template void Field::FUNC_NAME<TYPE_DECIMALV2>(                                          \
            const typename PrimitiveTypeTraits<TYPE_DECIMALV2>::NearestFieldType& rhs);      \
    template void Field::FUNC_NAME<TYPE_DECIMAL128I>(                                        \
            const typename PrimitiveTypeTraits<TYPE_DECIMAL128I>::NearestFieldType& rhs);    \
    template void Field::FUNC_NAME<TYPE_DECIMAL256>(                                         \
            const typename PrimitiveTypeTraits<TYPE_DECIMAL256>::NearestFieldType& rhs);     \
    template void Field::FUNC_NAME<TYPE_CHAR>(                                               \
            const typename PrimitiveTypeTraits<TYPE_CHAR>::NearestFieldType& rhs);           \
    template void Field::FUNC_NAME<TYPE_VARCHAR>(                                            \
            const typename PrimitiveTypeTraits<TYPE_VARCHAR>::NearestFieldType& rhs);        \
    template void Field::FUNC_NAME<TYPE_STRING>(                                             \
            const typename PrimitiveTypeTraits<TYPE_STRING>::NearestFieldType& rhs);         \
    template void Field::FUNC_NAME<TYPE_HLL>(                                                \
            const typename PrimitiveTypeTraits<TYPE_HLL>::NearestFieldType& rhs);            \
    template void Field::FUNC_NAME<TYPE_VARIANT>(                                            \
            const typename PrimitiveTypeTraits<TYPE_VARIANT>::NearestFieldType& rhs);        \
    template void Field::FUNC_NAME<TYPE_QUANTILE_STATE>(                                     \
            const typename PrimitiveTypeTraits<TYPE_QUANTILE_STATE>::NearestFieldType& rhs); \
    template void Field::FUNC_NAME<TYPE_ARRAY>(                                              \
            const typename PrimitiveTypeTraits<TYPE_ARRAY>::NearestFieldType& rhs);          \
    template void Field::FUNC_NAME<TYPE_IPV4>(                                               \
            typename PrimitiveTypeTraits<TYPE_IPV4>::NearestFieldType && rhs);               \
    template void Field::FUNC_NAME<TYPE_IPV4>(                                               \
            const typename PrimitiveTypeTraits<TYPE_IPV4>::NearestFieldType& rhs);           \
    template void Field::FUNC_NAME<TYPE_IPV6>(                                               \
            typename PrimitiveTypeTraits<TYPE_IPV6>::NearestFieldType && rhs);               \
    template void Field::FUNC_NAME<TYPE_IPV6>(                                               \
            const typename PrimitiveTypeTraits<TYPE_IPV6>::NearestFieldType& rhs);           \
    template void Field::FUNC_NAME<TYPE_BOOLEAN>(                                            \
            typename PrimitiveTypeTraits<TYPE_BOOLEAN>::NearestFieldType && rhs);            \
    template void Field::FUNC_NAME<TYPE_BOOLEAN>(                                            \
            const typename PrimitiveTypeTraits<TYPE_BOOLEAN>::NearestFieldType& rhs);        \
    template void Field::FUNC_NAME<TYPE_FLOAT>(                                              \
            typename PrimitiveTypeTraits<TYPE_FLOAT>::NearestFieldType && rhs);              \
    template void Field::FUNC_NAME<TYPE_FLOAT>(                                              \
            const typename PrimitiveTypeTraits<TYPE_FLOAT>::NearestFieldType& rhs);          \
    template void Field::FUNC_NAME<TYPE_DOUBLE>(                                             \
            typename PrimitiveTypeTraits<TYPE_DOUBLE>::NearestFieldType && rhs);             \
    template void Field::FUNC_NAME<TYPE_DOUBLE>(                                             \
            const typename PrimitiveTypeTraits<TYPE_DOUBLE>::NearestFieldType& rhs);         \
    template void Field::FUNC_NAME<TYPE_JSONB>(                                              \
            typename PrimitiveTypeTraits<TYPE_JSONB>::NearestFieldType && rhs);              \
    template void Field::FUNC_NAME<TYPE_JSONB>(                                              \
            const typename PrimitiveTypeTraits<TYPE_JSONB>::NearestFieldType& rhs);          \
    template void Field::FUNC_NAME<TYPE_STRUCT>(                                             \
            typename PrimitiveTypeTraits<TYPE_STRUCT>::NearestFieldType && rhs);             \
    template void Field::FUNC_NAME<TYPE_STRUCT>(                                             \
            const typename PrimitiveTypeTraits<TYPE_STRUCT>::NearestFieldType& rhs);         \
    template void Field::FUNC_NAME<TYPE_MAP>(                                                \
            typename PrimitiveTypeTraits<TYPE_MAP>::NearestFieldType && rhs);                \
    template void Field::FUNC_NAME<TYPE_MAP>(                                                \
            const typename PrimitiveTypeTraits<TYPE_MAP>::NearestFieldType& rhs);            \
    template void Field::FUNC_NAME<TYPE_BITMAP>(                                             \
            typename PrimitiveTypeTraits<TYPE_BITMAP>::NearestFieldType && rhs);             \
    template void Field::FUNC_NAME<TYPE_BITMAP>(                                             \
            const typename PrimitiveTypeTraits<TYPE_BITMAP>::NearestFieldType& rhs);         \
    template void Field::FUNC_NAME<TYPE_TIMEV2>(                                             \
            const typename PrimitiveTypeTraits<TYPE_TIMEV2>::NearestFieldType& rhs);         \
    template void Field::FUNC_NAME<TYPE_TIMEV2>(                                             \
            typename PrimitiveTypeTraits<TYPE_TIMEV2>::NearestFieldType && rhs);             \
    template void Field::FUNC_NAME<TYPE_UINT32>(                                             \
            const typename PrimitiveTypeTraits<TYPE_UINT32>::NearestFieldType& rhs);         \
    template void Field::FUNC_NAME<TYPE_UINT32>(                                             \
            typename PrimitiveTypeTraits<TYPE_UINT32>::NearestFieldType && rhs);             \
    template void Field::FUNC_NAME<TYPE_UINT64>(                                             \
            const typename PrimitiveTypeTraits<TYPE_UINT64>::NearestFieldType& rhs);         \
    template void Field::FUNC_NAME<TYPE_UINT64>(                                             \
            typename PrimitiveTypeTraits<TYPE_UINT64>::NearestFieldType && rhs);
DECLARE_FUNCTION(create_concrete)
DECLARE_FUNCTION(assign_concrete)

#undef DECLARE_FUNCTION
} // namespace doris::vectorized
