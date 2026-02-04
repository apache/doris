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

#include "runtime/define_primitive_type.h"
#include "runtime/jsonb_value.h"
#include "runtime/primitive_type.h"
#include "util/bitmap_value.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/decimal_comparison.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/functions/cast/cast_to_string.h"
#include "vec/io/io_helper.h"
#include "vec/io/var_int.h"
#include "vec/runtime/timestamptz_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
class BufferReadable;
class BufferWritable;

template <PrimitiveType T>
bool dec_equal(typename PrimitiveTypeTraits<T>::CppType x,
               typename PrimitiveTypeTraits<T>::CppType y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <PrimitiveType T>
bool dec_less(typename PrimitiveTypeTraits<T>::CppType x,
              typename PrimitiveTypeTraits<T>::CppType y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <PrimitiveType T>
bool dec_less_or_equal(typename PrimitiveTypeTraits<T>::CppType x,
                       typename PrimitiveTypeTraits<T>::CppType y, UInt32 x_scale, UInt32 y_scale) {
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
DECLARE_DECIMAL_COMPARISON(DecimalV2Value, TYPE_DECIMALV2)
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
void Field::create_concrete(typename PrimitiveTypeTraits<Type>::CppType&& x) {
    // In both Field and PODArray, small types may be stored as wider types,
    // e.g. char is stored as UInt64. Field can return this extended value
    // with get<StorageType>(). To avoid uninitialized results from get(),
    // we must initialize the entire wide stored type, and not just the
    // nominal type.
    using StorageType = typename PrimitiveTypeTraits<Type>::CppType;
    new (&storage) StorageType(std::move(x));
    type = Type;
    DCHECK_NE(type, PrimitiveType::INVALID_TYPE);
}

template <PrimitiveType Type>
void Field::create_concrete(const typename PrimitiveTypeTraits<Type>::CppType& x) {
    // In both Field and PODArray, small types may be stored as wider types,
    // e.g. char is stored as UInt64. Field can return this extended value
    // with get<StorageType>(). To avoid uninitialized results from get(),
    // we must initialize the entire wide stored type, and not just the
    // nominal type.
    using StorageType = typename PrimitiveTypeTraits<Type>::CppType;
    new (&storage) StorageType(x);
    type = Type;
    DCHECK_NE(type, PrimitiveType::INVALID_TYPE);
}

void Field::create(Field&& field) {
    switch (field.type) {
    case PrimitiveType::TYPE_NULL:
        create_concrete<TYPE_NULL>(std::move(field.template get<TYPE_NULL>()));
        return;
    case PrimitiveType::TYPE_DATETIMEV2:
        create_concrete<TYPE_DATETIMEV2>(std::move(field.template get<TYPE_DATETIMEV2>()));
        return;
    case PrimitiveType::TYPE_DATEV2:
        create_concrete<TYPE_DATEV2>(std::move(field.template get<TYPE_DATEV2>()));
        return;
    case PrimitiveType::TYPE_TIMESTAMPTZ:
        create_concrete<TYPE_TIMESTAMPTZ>(std::move(field.template get<TYPE_TIMESTAMPTZ>()));
        return;
    case PrimitiveType::TYPE_DATETIME:
        create_concrete<TYPE_DATETIME>(std::move(field.template get<TYPE_DATETIME>()));
        return;
    case PrimitiveType::TYPE_DATE:
        create_concrete<TYPE_DATE>(std::move(field.template get<TYPE_DATE>()));
        return;
    case PrimitiveType::TYPE_BOOLEAN:
        create_concrete<TYPE_BOOLEAN>(std::move(field.template get<TYPE_BOOLEAN>()));
        return;
    case PrimitiveType::TYPE_TINYINT:
        create_concrete<TYPE_TINYINT>(std::move(field.template get<TYPE_TINYINT>()));
        return;
    case PrimitiveType::TYPE_SMALLINT:
        create_concrete<TYPE_SMALLINT>(std::move(field.template get<TYPE_SMALLINT>()));
        return;
    case PrimitiveType::TYPE_INT:
        create_concrete<TYPE_INT>(std::move(field.template get<TYPE_INT>()));
        return;
    case PrimitiveType::TYPE_BIGINT:
        create_concrete<TYPE_BIGINT>(std::move(field.template get<TYPE_BIGINT>()));
        return;
    case PrimitiveType::TYPE_LARGEINT:
        create_concrete<TYPE_LARGEINT>(std::move(field.template get<TYPE_LARGEINT>()));
        return;
    case PrimitiveType::TYPE_IPV4:
        create_concrete<TYPE_IPV4>(std::move(field.template get<TYPE_IPV4>()));
        return;
    case PrimitiveType::TYPE_IPV6:
        create_concrete<TYPE_IPV6>(std::move(field.template get<TYPE_IPV6>()));
        return;
    case PrimitiveType::TYPE_FLOAT:
        create_concrete<TYPE_FLOAT>(std::move(field.template get<TYPE_FLOAT>()));
        return;
    case PrimitiveType::TYPE_TIMEV2:
        create_concrete<TYPE_TIMEV2>(std::move(field.template get<TYPE_TIMEV2>()));
        return;
    case PrimitiveType::TYPE_DOUBLE:
        create_concrete<TYPE_DOUBLE>(std::move(field.template get<TYPE_DOUBLE>()));
        return;
    case PrimitiveType::TYPE_STRING:
        create_concrete<TYPE_STRING>(std::move(field.template get<TYPE_STRING>()));
        return;
    case PrimitiveType::TYPE_CHAR:
        create_concrete<TYPE_CHAR>(std::move(field.template get<TYPE_CHAR>()));
        return;
    case PrimitiveType::TYPE_VARCHAR:
        create_concrete<TYPE_VARCHAR>(std::move(field.template get<TYPE_VARCHAR>()));
        return;
    case PrimitiveType::TYPE_JSONB:
        create_concrete<TYPE_JSONB>(std::move(field.template get<TYPE_JSONB>()));
        return;
    case PrimitiveType::TYPE_ARRAY:
        create_concrete<TYPE_ARRAY>(std::move(field.template get<TYPE_ARRAY>()));
        return;
    case PrimitiveType::TYPE_STRUCT:
        create_concrete<TYPE_STRUCT>(std::move(field.template get<TYPE_STRUCT>()));
        return;
    case PrimitiveType::TYPE_MAP:
        create_concrete<TYPE_MAP>(std::move(field.template get<TYPE_MAP>()));
        return;
    case PrimitiveType::TYPE_DECIMAL32:
        create_concrete<TYPE_DECIMAL32>(std::move(field.template get<TYPE_DECIMAL32>()));
        return;
    case PrimitiveType::TYPE_DECIMAL64:
        create_concrete<TYPE_DECIMAL64>(std::move(field.template get<TYPE_DECIMAL64>()));
        return;
    case PrimitiveType::TYPE_DECIMALV2:
        create_concrete<TYPE_DECIMALV2>(std::move(field.template get<TYPE_DECIMALV2>()));
        return;
    case PrimitiveType::TYPE_DECIMAL128I:
        create_concrete<TYPE_DECIMAL128I>(std::move(field.template get<TYPE_DECIMAL128I>()));
        return;
    case PrimitiveType::TYPE_DECIMAL256:
        create_concrete<TYPE_DECIMAL256>(std::move(field.template get<TYPE_DECIMAL256>()));
        return;
    case PrimitiveType::TYPE_VARIANT:
        create_concrete<TYPE_VARIANT>(std::move(field.template get<TYPE_VARIANT>()));
        return;
    case PrimitiveType::TYPE_BITMAP:
        create_concrete<TYPE_BITMAP>(std::move(field.template get<TYPE_BITMAP>()));
        return;
    case PrimitiveType::TYPE_HLL:
        create_concrete<TYPE_HLL>(std::move(field.template get<TYPE_HLL>()));
        return;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        create_concrete<TYPE_QUANTILE_STATE>(std::move(field.template get<TYPE_QUANTILE_STATE>()));
        return;
    case PrimitiveType::TYPE_VARBINARY:
        create_concrete<TYPE_VARBINARY>(std::move(field.template get<TYPE_VARBINARY>()));
        return;
    default:
        throw Exception(Status::FatalError("type not supported, type={}", field.get_type_name()));
    }
}

Field::Field(const Field& rhs) {
    create(rhs);
}

Field::Field(Field&& rhs) {
    create(std::move(rhs));
}

Field& Field::operator=(const Field& rhs) {
    if (this != &rhs) {
        if (type != rhs.type) {
            destroy();
            create(rhs);
        } else {
            assign(rhs); /// This assigns string or vector without deallocation of existing buffer.
        }
    }
    return *this;
}

void Field::create(const Field& field) {
    switch (field.type) {
    case PrimitiveType::TYPE_NULL:
        create_concrete<TYPE_NULL>(field.template get<TYPE_NULL>());
        return;
    case PrimitiveType::TYPE_DATETIMEV2:
        create_concrete<TYPE_DATETIMEV2>(field.template get<TYPE_DATETIMEV2>());
        return;
    case PrimitiveType::TYPE_DATEV2:
        create_concrete<TYPE_DATEV2>(field.template get<TYPE_DATEV2>());
        return;
    case PrimitiveType::TYPE_TIMESTAMPTZ:
        create_concrete<TYPE_TIMESTAMPTZ>(field.template get<TYPE_TIMESTAMPTZ>());
        return;
    case PrimitiveType::TYPE_DATETIME:
        create_concrete<TYPE_DATETIME>(field.template get<TYPE_DATETIME>());
        return;
    case PrimitiveType::TYPE_DATE:
        create_concrete<TYPE_DATE>(field.template get<TYPE_DATE>());
        return;
    case PrimitiveType::TYPE_BOOLEAN:
        create_concrete<TYPE_BOOLEAN>(field.template get<TYPE_BOOLEAN>());
        return;
    case PrimitiveType::TYPE_TINYINT:
        create_concrete<TYPE_TINYINT>(field.template get<TYPE_TINYINT>());
        return;
    case PrimitiveType::TYPE_SMALLINT:
        create_concrete<TYPE_SMALLINT>(field.template get<TYPE_SMALLINT>());
        return;
    case PrimitiveType::TYPE_INT:
        create_concrete<TYPE_INT>(field.template get<TYPE_INT>());
        return;
    case PrimitiveType::TYPE_BIGINT:
        create_concrete<TYPE_BIGINT>(field.template get<TYPE_BIGINT>());
        return;
    case PrimitiveType::TYPE_LARGEINT:
        create_concrete<TYPE_LARGEINT>(field.template get<TYPE_LARGEINT>());
        return;
    case PrimitiveType::TYPE_IPV4:
        create_concrete<TYPE_IPV4>(field.template get<TYPE_IPV4>());
        return;
    case PrimitiveType::TYPE_IPV6:
        create_concrete<TYPE_IPV6>(field.template get<TYPE_IPV6>());
        return;
    case PrimitiveType::TYPE_FLOAT:
        create_concrete<TYPE_FLOAT>(field.template get<TYPE_FLOAT>());
        return;
    case PrimitiveType::TYPE_TIMEV2:
        create_concrete<TYPE_TIMEV2>(field.template get<TYPE_TIMEV2>());
        return;
    case PrimitiveType::TYPE_DOUBLE:
        create_concrete<TYPE_DOUBLE>(field.template get<TYPE_DOUBLE>());
        return;
    case PrimitiveType::TYPE_STRING:
        create_concrete<TYPE_STRING>(field.template get<TYPE_STRING>());
        return;
    case PrimitiveType::TYPE_CHAR:
        create_concrete<TYPE_CHAR>(field.template get<TYPE_CHAR>());
        return;
    case PrimitiveType::TYPE_VARCHAR:
        create_concrete<TYPE_VARCHAR>(field.template get<TYPE_VARCHAR>());
        return;
    case PrimitiveType::TYPE_JSONB:
        create_concrete<TYPE_JSONB>(field.template get<TYPE_JSONB>());
        return;
    case PrimitiveType::TYPE_ARRAY:
        create_concrete<TYPE_ARRAY>(field.template get<TYPE_ARRAY>());
        return;
    case PrimitiveType::TYPE_STRUCT:
        create_concrete<TYPE_STRUCT>(field.template get<TYPE_STRUCT>());
        return;
    case PrimitiveType::TYPE_MAP:
        create_concrete<TYPE_MAP>(field.template get<TYPE_MAP>());
        return;
    case PrimitiveType::TYPE_DECIMAL32:
        create_concrete<TYPE_DECIMAL32>(field.template get<TYPE_DECIMAL32>());
        return;
    case PrimitiveType::TYPE_DECIMAL64:
        create_concrete<TYPE_DECIMAL64>(field.template get<TYPE_DECIMAL64>());
        return;
    case PrimitiveType::TYPE_DECIMALV2:
        create_concrete<TYPE_DECIMALV2>(field.template get<TYPE_DECIMALV2>());
        return;
    case PrimitiveType::TYPE_DECIMAL128I:
        create_concrete<TYPE_DECIMAL128I>(field.template get<TYPE_DECIMAL128I>());
        return;
    case PrimitiveType::TYPE_DECIMAL256:
        create_concrete<TYPE_DECIMAL256>(field.template get<TYPE_DECIMAL256>());
        return;
    case PrimitiveType::TYPE_VARIANT:
        create_concrete<TYPE_VARIANT>(field.template get<TYPE_VARIANT>());
        return;
    case PrimitiveType::TYPE_BITMAP:
        create_concrete<TYPE_BITMAP>(field.template get<TYPE_BITMAP>());
        return;
    case PrimitiveType::TYPE_HLL:
        create_concrete<TYPE_HLL>(field.template get<TYPE_HLL>());
        return;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        create_concrete<TYPE_QUANTILE_STATE>(field.template get<TYPE_QUANTILE_STATE>());
        return;
    case PrimitiveType::TYPE_UINT32:
        create_concrete<TYPE_UINT32>(field.template get<TYPE_UINT32>());
        return;
    case PrimitiveType::TYPE_UINT64:
        create_concrete<TYPE_UINT64>(field.template get<TYPE_UINT64>());
        return;
    case PrimitiveType::TYPE_VARBINARY:
        create_concrete<TYPE_VARBINARY>(field.template get<TYPE_VARBINARY>());
        return;
    default:
        throw Exception(Status::FatalError("type not supported, type={}", field.get_type_name()));
    }
}

void Field::destroy() {
    switch (type) {
    case PrimitiveType::TYPE_STRING:
        destroy<TYPE_STRING>();
        break;
    case PrimitiveType::TYPE_CHAR:
        destroy<TYPE_CHAR>();
        break;
    case PrimitiveType::TYPE_VARCHAR:
        destroy<TYPE_VARCHAR>();
        break;
    case PrimitiveType::TYPE_JSONB:
        destroy<TYPE_JSONB>();
        break;
    case PrimitiveType::TYPE_ARRAY:
        destroy<TYPE_ARRAY>();
        break;
    case PrimitiveType::TYPE_STRUCT:
        destroy<TYPE_STRUCT>();
        break;
    case PrimitiveType::TYPE_MAP:
        destroy<TYPE_MAP>();
        break;
    case PrimitiveType::TYPE_VARIANT:
        destroy<TYPE_VARIANT>();
        break;
    case PrimitiveType::TYPE_BITMAP:
        destroy<TYPE_BITMAP>();
        break;
    case PrimitiveType::TYPE_HLL:
        destroy<TYPE_HLL>();
        break;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        destroy<TYPE_QUANTILE_STATE>();
        break;
    case PrimitiveType::TYPE_VARBINARY:
        destroy<TYPE_VARBINARY>();
        break;
    default:
        break;
    }

    type = PrimitiveType::
            TYPE_NULL; /// for exception safety in subsequent calls to destroy and create, when create fails.
}

void Field::assign(Field&& field) {
    switch (field.type) {
    case PrimitiveType::TYPE_NULL:
        assign_concrete<TYPE_NULL>(std::move(field.template get<TYPE_NULL>()));
        return;
    case PrimitiveType::TYPE_DATETIMEV2:
        assign_concrete<TYPE_DATETIMEV2>(std::move(field.template get<TYPE_DATETIMEV2>()));
        return;
    case PrimitiveType::TYPE_DATETIME:
        assign_concrete<TYPE_DATETIME>(std::move(field.template get<TYPE_DATETIME>()));
        return;
    case PrimitiveType::TYPE_DATE:
        assign_concrete<TYPE_DATE>(std::move(field.template get<TYPE_DATE>()));
        return;
    case PrimitiveType::TYPE_DATEV2:
        assign_concrete<TYPE_DATEV2>(std::move(field.template get<TYPE_DATEV2>()));
        return;
    case PrimitiveType::TYPE_TIMESTAMPTZ:
        assign_concrete<TYPE_TIMESTAMPTZ>(std::move(field.template get<TYPE_TIMESTAMPTZ>()));
        return;
    case PrimitiveType::TYPE_BOOLEAN:
        assign_concrete<TYPE_BOOLEAN>(std::move(field.template get<TYPE_BOOLEAN>()));
        return;
    case PrimitiveType::TYPE_TINYINT:
        assign_concrete<TYPE_TINYINT>(std::move(field.template get<TYPE_TINYINT>()));
        return;
    case PrimitiveType::TYPE_SMALLINT:
        assign_concrete<TYPE_SMALLINT>(std::move(field.template get<TYPE_SMALLINT>()));
        return;
    case PrimitiveType::TYPE_INT:
        assign_concrete<TYPE_INT>(std::move(field.template get<TYPE_INT>()));
        return;
    case PrimitiveType::TYPE_BIGINT:
        assign_concrete<TYPE_BIGINT>(std::move(field.template get<TYPE_BIGINT>()));
        return;
    case PrimitiveType::TYPE_LARGEINT:
        assign_concrete<TYPE_LARGEINT>(std::move(field.template get<TYPE_LARGEINT>()));
        return;
    case PrimitiveType::TYPE_IPV4:
        assign_concrete<TYPE_IPV4>(std::move(field.template get<TYPE_IPV4>()));
        return;
    case PrimitiveType::TYPE_IPV6:
        assign_concrete<TYPE_IPV6>(std::move(field.template get<TYPE_IPV6>()));
        return;
    case PrimitiveType::TYPE_FLOAT:
        assign_concrete<TYPE_FLOAT>(std::move(field.template get<TYPE_FLOAT>()));
        return;
    case PrimitiveType::TYPE_TIMEV2:
        assign_concrete<TYPE_TIMEV2>(std::move(field.template get<TYPE_TIMEV2>()));
        return;
    case PrimitiveType::TYPE_DOUBLE:
        assign_concrete<TYPE_DOUBLE>(std::move(field.template get<TYPE_DOUBLE>()));
        return;
    case PrimitiveType::TYPE_STRING:
        assign_concrete<TYPE_STRING>(std::move(field.template get<TYPE_STRING>()));
        return;
    case PrimitiveType::TYPE_CHAR:
        assign_concrete<TYPE_CHAR>(std::move(field.template get<TYPE_CHAR>()));
        return;
    case PrimitiveType::TYPE_VARCHAR:
        assign_concrete<TYPE_VARCHAR>(std::move(field.template get<TYPE_VARCHAR>()));
        return;
    case PrimitiveType::TYPE_JSONB:
        assign_concrete<TYPE_JSONB>(std::move(field.template get<TYPE_JSONB>()));
        return;
    case PrimitiveType::TYPE_ARRAY:
        assign_concrete<TYPE_ARRAY>(std::move(field.template get<TYPE_ARRAY>()));
        return;
    case PrimitiveType::TYPE_STRUCT:
        assign_concrete<TYPE_STRUCT>(std::move(field.template get<TYPE_STRUCT>()));
        return;
    case PrimitiveType::TYPE_MAP:
        assign_concrete<TYPE_MAP>(std::move(field.template get<TYPE_MAP>()));
        return;
    case PrimitiveType::TYPE_DECIMAL32:
        assign_concrete<TYPE_DECIMAL32>(std::move(field.template get<TYPE_DECIMAL32>()));
        return;
    case PrimitiveType::TYPE_DECIMAL64:
        assign_concrete<TYPE_DECIMAL64>(std::move(field.template get<TYPE_DECIMAL64>()));
        return;
    case PrimitiveType::TYPE_DECIMALV2:
        assign_concrete<TYPE_DECIMALV2>(std::move(field.template get<TYPE_DECIMALV2>()));
        return;
    case PrimitiveType::TYPE_DECIMAL128I:
        assign_concrete<TYPE_DECIMAL128I>(std::move(field.template get<TYPE_DECIMAL128I>()));
        return;
    case PrimitiveType::TYPE_DECIMAL256:
        assign_concrete<TYPE_DECIMAL256>(std::move(field.template get<TYPE_DECIMAL256>()));
        return;
    case PrimitiveType::TYPE_VARIANT:
        assign_concrete<TYPE_VARIANT>(std::move(field.template get<TYPE_VARIANT>()));
        return;
    case PrimitiveType::TYPE_BITMAP:
        assign_concrete<TYPE_BITMAP>(std::move(field.template get<TYPE_BITMAP>()));
        return;
    case PrimitiveType::TYPE_HLL:
        assign_concrete<TYPE_HLL>(std::move(field.template get<TYPE_HLL>()));
        return;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        assign_concrete<TYPE_QUANTILE_STATE>(std::move(field.template get<TYPE_QUANTILE_STATE>()));
        return;
    case PrimitiveType::TYPE_VARBINARY:
        assign_concrete<TYPE_VARBINARY>(std::move(field.template get<TYPE_VARBINARY>()));
        return;
    default:
        throw Exception(Status::FatalError("type not supported, type={}", field.get_type_name()));
    }
}

void Field::assign(const Field& field) {
    switch (field.type) {
    case PrimitiveType::TYPE_NULL:
        assign_concrete<TYPE_NULL>(field.template get<TYPE_NULL>());
        return;
    case PrimitiveType::TYPE_DATETIMEV2:
        assign_concrete<TYPE_DATETIMEV2>(field.template get<TYPE_DATETIMEV2>());
        return;
    case PrimitiveType::TYPE_DATETIME:
        assign_concrete<TYPE_DATETIME>(field.template get<TYPE_DATETIME>());
        return;
    case PrimitiveType::TYPE_DATE:
        assign_concrete<TYPE_DATE>(field.template get<TYPE_DATE>());
        return;
    case PrimitiveType::TYPE_DATEV2:
        assign_concrete<TYPE_DATEV2>(field.template get<TYPE_DATEV2>());
        return;
    case PrimitiveType::TYPE_TIMESTAMPTZ:
        assign_concrete<TYPE_TIMESTAMPTZ>(field.template get<TYPE_TIMESTAMPTZ>());
        return;
    case PrimitiveType::TYPE_BOOLEAN:
        assign_concrete<TYPE_BOOLEAN>(field.template get<TYPE_BOOLEAN>());
        return;
    case PrimitiveType::TYPE_TINYINT:
        assign_concrete<TYPE_TINYINT>(field.template get<TYPE_TINYINT>());
        return;
    case PrimitiveType::TYPE_SMALLINT:
        assign_concrete<TYPE_SMALLINT>(field.template get<TYPE_SMALLINT>());
        return;
    case PrimitiveType::TYPE_INT:
        assign_concrete<TYPE_INT>(field.template get<TYPE_INT>());
        return;
    case PrimitiveType::TYPE_BIGINT:
        assign_concrete<TYPE_BIGINT>(field.template get<TYPE_BIGINT>());
        return;
    case PrimitiveType::TYPE_LARGEINT:
        assign_concrete<TYPE_LARGEINT>(field.template get<TYPE_LARGEINT>());
        return;
    case PrimitiveType::TYPE_IPV4:
        assign_concrete<TYPE_IPV4>(field.template get<TYPE_IPV4>());
        return;
    case PrimitiveType::TYPE_IPV6:
        assign_concrete<TYPE_IPV6>(field.template get<TYPE_IPV6>());
        return;
    case PrimitiveType::TYPE_FLOAT:
        assign_concrete<TYPE_FLOAT>(field.template get<TYPE_FLOAT>());
        return;
    case PrimitiveType::TYPE_TIMEV2:
        assign_concrete<TYPE_TIMEV2>(field.template get<TYPE_TIMEV2>());
        return;
    case PrimitiveType::TYPE_DOUBLE:
        assign_concrete<TYPE_DOUBLE>(field.template get<TYPE_DOUBLE>());
        return;
    case PrimitiveType::TYPE_STRING:
        assign_concrete<TYPE_STRING>(field.template get<TYPE_STRING>());
        return;
    case PrimitiveType::TYPE_CHAR:
        assign_concrete<TYPE_CHAR>(field.template get<TYPE_CHAR>());
        return;
    case PrimitiveType::TYPE_VARCHAR:
        assign_concrete<TYPE_VARCHAR>(field.template get<TYPE_VARCHAR>());
        return;
    case PrimitiveType::TYPE_JSONB:
        assign_concrete<TYPE_JSONB>(field.template get<TYPE_JSONB>());
        return;
    case PrimitiveType::TYPE_ARRAY:
        assign_concrete<TYPE_ARRAY>(field.template get<TYPE_ARRAY>());
        return;
    case PrimitiveType::TYPE_STRUCT:
        assign_concrete<TYPE_STRUCT>(field.template get<TYPE_STRUCT>());
        return;
    case PrimitiveType::TYPE_MAP:
        assign_concrete<TYPE_MAP>(field.template get<TYPE_MAP>());
        return;
    case PrimitiveType::TYPE_DECIMAL32:
        assign_concrete<TYPE_DECIMAL32>(field.template get<TYPE_DECIMAL32>());
        return;
    case PrimitiveType::TYPE_DECIMAL64:
        assign_concrete<TYPE_DECIMAL64>(field.template get<TYPE_DECIMAL64>());
        return;
    case PrimitiveType::TYPE_DECIMALV2:
        assign_concrete<TYPE_DECIMALV2>(field.template get<TYPE_DECIMALV2>());
        return;
    case PrimitiveType::TYPE_DECIMAL128I:
        assign_concrete<TYPE_DECIMAL128I>(field.template get<TYPE_DECIMAL128I>());
        return;
    case PrimitiveType::TYPE_DECIMAL256:
        assign_concrete<TYPE_DECIMAL256>(field.template get<TYPE_DECIMAL256>());
        return;
    case PrimitiveType::TYPE_VARIANT:
        assign_concrete<TYPE_VARIANT>(field.template get<TYPE_VARIANT>());
        return;
    case PrimitiveType::TYPE_BITMAP:
        assign_concrete<TYPE_BITMAP>(field.template get<TYPE_BITMAP>());
        return;
    case PrimitiveType::TYPE_HLL:
        assign_concrete<TYPE_HLL>(field.template get<TYPE_HLL>());
        return;
    case PrimitiveType::TYPE_QUANTILE_STATE:
        assign_concrete<TYPE_QUANTILE_STATE>(field.template get<TYPE_QUANTILE_STATE>());
        return;
    case PrimitiveType::TYPE_UINT32:
        assign_concrete<TYPE_UINT32>(field.template get<TYPE_UINT32>());
        return;
    case PrimitiveType::TYPE_UINT64:
        assign_concrete<TYPE_UINT64>(field.template get<TYPE_UINT64>());
        return;
    case PrimitiveType::TYPE_VARBINARY:
        assign_concrete<TYPE_VARBINARY>(field.template get<TYPE_VARBINARY>());
        return;
    default:
        throw Exception(Status::FatalError("type not supported, type={}", field.get_type_name()));
    }
}

/// Assuming same types.
template <PrimitiveType Type>
void Field::assign_concrete(typename PrimitiveTypeTraits<Type>::CppType&& x) {
    auto* MAY_ALIAS ptr = reinterpret_cast<typename PrimitiveTypeTraits<Type>::CppType*>(&storage);
    *ptr = std::forward<typename PrimitiveTypeTraits<Type>::CppType>(x);
}

template <PrimitiveType Type>
void Field::assign_concrete(const typename PrimitiveTypeTraits<Type>::CppType& x) {
    auto* MAY_ALIAS ptr = reinterpret_cast<typename PrimitiveTypeTraits<Type>::CppType*>(&storage);
    *ptr = std::forward<const typename PrimitiveTypeTraits<Type>::CppType>(x);
}

std::string Field::get_type_name() const {
    return type_to_string(type);
}

template <PrimitiveType T>
typename PrimitiveTypeTraits<T>::CppType& Field::get() {
    DCHECK(T == type || (is_string_type(type) && is_string_type(T)) || type == TYPE_NULL)
            << "Type mismatch: requested " << type_to_string(T) << ", actual " << get_type_name();
    auto* MAY_ALIAS ptr = reinterpret_cast<typename PrimitiveTypeTraits<T>::CppType*>(&storage);
    return *ptr;
}

template <PrimitiveType T>
const typename PrimitiveTypeTraits<T>::CppType& Field::get() const {
    DCHECK(T == type || (is_string_type(type) && is_string_type(T)) || type == TYPE_NULL)
            << "Type mismatch: requested " << type_to_string(T) << ", actual " << get_type_name();
    const auto* MAY_ALIAS ptr =
            reinterpret_cast<const typename PrimitiveTypeTraits<T>::CppType*>(&storage);
    return *ptr;
}

template <PrimitiveType T>
void Field::destroy() {
    using TargetType = typename PrimitiveTypeTraits<T>::CppType;
    DCHECK(T == type || ((is_string_type(type) && is_string_type(T))))
            << "Type mismatch: requested " << type_to_string(T) << ", actual " << get_type_name();
    auto* MAY_ALIAS ptr = reinterpret_cast<TargetType*>(&storage);
    ptr->~TargetType();
}

std::strong_ordering Field::operator<=>(const Field& rhs) const {
    if (type == PrimitiveType::TYPE_NULL || rhs == PrimitiveType::TYPE_NULL) {
        return type <=> rhs.type;
    }
    if (type != rhs.type) {
        throw Exception(Status::FatalError("lhs type not equal with rhs, lhs={}, rhs={}",
                                           get_type_name(), rhs.get_type_name()));
    }

    switch (type) {
    case PrimitiveType::TYPE_BITMAP:
    case PrimitiveType::TYPE_HLL:
    case PrimitiveType::TYPE_QUANTILE_STATE:
    case PrimitiveType::INVALID_TYPE:
    case PrimitiveType::TYPE_JSONB:
    case PrimitiveType::TYPE_NULL:
    case PrimitiveType::TYPE_ARRAY:
    case PrimitiveType::TYPE_MAP:
    case PrimitiveType::TYPE_STRUCT:
    case PrimitiveType::TYPE_VARIANT:
        return std::strong_ordering::equal; //TODO: throw Exception?
    case PrimitiveType::TYPE_DATETIMEV2:
        return get<PrimitiveType::TYPE_DATETIMEV2>().to_date_int_val() <=>
               rhs.get<PrimitiveType::TYPE_DATETIMEV2>().to_date_int_val();
    case PrimitiveType::TYPE_DATEV2:
        return get<PrimitiveType::TYPE_DATEV2>().to_date_int_val() <=>
               rhs.get<PrimitiveType::TYPE_DATEV2>().to_date_int_val();
    case PrimitiveType::TYPE_TIMESTAMPTZ:
        return get<PrimitiveType::TYPE_TIMESTAMPTZ>().to_date_int_val() <=>
               rhs.get<PrimitiveType::TYPE_TIMESTAMPTZ>().to_date_int_val();
    case PrimitiveType::TYPE_DATE:
        return get<PrimitiveType::TYPE_DATE>() <=> rhs.get<PrimitiveType::TYPE_DATE>();
    case PrimitiveType::TYPE_DATETIME:
        return get<PrimitiveType::TYPE_DATETIME>() <=> rhs.get<PrimitiveType::TYPE_DATETIME>();
    case PrimitiveType::TYPE_BIGINT:
        return get<PrimitiveType::TYPE_BIGINT>() <=> rhs.get<PrimitiveType::TYPE_BIGINT>();
    case PrimitiveType::TYPE_BOOLEAN:
        return get<PrimitiveType::TYPE_BOOLEAN>() <=> rhs.get<PrimitiveType::TYPE_BOOLEAN>();
    case PrimitiveType::TYPE_TINYINT:
        return get<TYPE_TINYINT>() <=> rhs.get<TYPE_TINYINT>();
    case PrimitiveType::TYPE_SMALLINT:
        return get<TYPE_SMALLINT>() <=> rhs.get<TYPE_SMALLINT>();
    case PrimitiveType::TYPE_INT:
        return get<TYPE_INT>() <=> rhs.get<TYPE_INT>();
    case PrimitiveType::TYPE_LARGEINT:
        return get<TYPE_LARGEINT>() <=> rhs.get<TYPE_LARGEINT>();
    case PrimitiveType::TYPE_IPV6:
        return get<TYPE_IPV6>() <=> rhs.get<TYPE_IPV6>();
    case PrimitiveType::TYPE_IPV4:
        return get<TYPE_IPV4>() <=> rhs.get<TYPE_IPV4>();
    case PrimitiveType::TYPE_FLOAT:
        return get<TYPE_FLOAT>() < rhs.get<TYPE_FLOAT>()    ? std::strong_ordering::less
               : get<TYPE_FLOAT>() == rhs.get<TYPE_FLOAT>() ? std::strong_ordering::equal
                                                            : std::strong_ordering::greater;
    case PrimitiveType::TYPE_TIMEV2:
        return get<TYPE_TIMEV2>() < rhs.get<TYPE_TIMEV2>()    ? std::strong_ordering::less
               : get<TYPE_TIMEV2>() == rhs.get<TYPE_TIMEV2>() ? std::strong_ordering::equal
                                                              : std::strong_ordering::greater;
    case PrimitiveType::TYPE_DOUBLE:
        return get<TYPE_DOUBLE>() < rhs.get<TYPE_DOUBLE>()    ? std::strong_ordering::less
               : get<TYPE_DOUBLE>() == rhs.get<TYPE_DOUBLE>() ? std::strong_ordering::equal
                                                              : std::strong_ordering::greater;
    case PrimitiveType::TYPE_STRING:
        return get<TYPE_STRING>() <=> rhs.get<TYPE_STRING>();
    case PrimitiveType::TYPE_CHAR:
        return get<TYPE_CHAR>() <=> rhs.get<TYPE_CHAR>();
    case PrimitiveType::TYPE_VARCHAR:
        return get<TYPE_VARCHAR>() <=> rhs.get<TYPE_VARCHAR>();
    case PrimitiveType::TYPE_VARBINARY:
        return get<TYPE_VARBINARY>() <=> rhs.get<TYPE_VARBINARY>();
    case PrimitiveType::TYPE_DECIMAL32:
        return get<TYPE_DECIMAL32>() <=> rhs.get<TYPE_DECIMAL32>();
    case PrimitiveType::TYPE_DECIMAL64:
        return get<TYPE_DECIMAL64>() <=> rhs.get<TYPE_DECIMAL64>();
    case PrimitiveType::TYPE_DECIMALV2:
        return get<TYPE_DECIMALV2>() <=> rhs.get<TYPE_DECIMALV2>();
    case PrimitiveType::TYPE_DECIMAL128I:
        return get<TYPE_DECIMAL128I>() <=> rhs.get<TYPE_DECIMAL128I>();
    case PrimitiveType::TYPE_DECIMAL256:
        return get<TYPE_DECIMAL256>() <=> rhs.get<TYPE_DECIMAL256>();
    default:
        throw Exception(Status::FatalError("Unsupported type: {}", get_type_name()));
    }
}

#define MATCH_PRIMITIVE_TYPE(primitive_type)                                   \
    if (type == primitive_type) {                                              \
        const auto& v = get<primitive_type>();                                 \
        return std::string_view(reinterpret_cast<const char*>(&v), sizeof(v)); \
    }

std::string_view Field::as_string_view() const {
    if (type == PrimitiveType::TYPE_STRING || type == PrimitiveType::TYPE_VARCHAR ||
        type == PrimitiveType::TYPE_CHAR) {
        const auto& s = get<TYPE_STRING>();
        return {s.data(), s.size()};
    }
    if (type == PrimitiveType::TYPE_VARBINARY) {
        const auto& svf = get<TYPE_VARBINARY>();
        return {svf.data(), svf.size()};
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
    MATCH_PRIMITIVE_TYPE(TYPE_TIMESTAMPTZ);
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

#define DECLARE_FUNCTION(FUNC_NAME)                                                               \
    template void Field::FUNC_NAME<TYPE_NULL>(typename PrimitiveTypeTraits<TYPE_NULL>::CppType && \
                                              rhs);                                               \
    template void Field::FUNC_NAME<TYPE_TINYINT>(                                                 \
            typename PrimitiveTypeTraits<TYPE_TINYINT>::CppType && rhs);                          \
    template void Field::FUNC_NAME<TYPE_SMALLINT>(                                                \
            typename PrimitiveTypeTraits<TYPE_SMALLINT>::CppType && rhs);                         \
    template void Field::FUNC_NAME<TYPE_INT>(typename PrimitiveTypeTraits<TYPE_INT>::CppType &&   \
                                             rhs);                                                \
    template void Field::FUNC_NAME<TYPE_BIGINT>(                                                  \
            typename PrimitiveTypeTraits<TYPE_BIGINT>::CppType && rhs);                           \
    template void Field::FUNC_NAME<TYPE_LARGEINT>(                                                \
            typename PrimitiveTypeTraits<TYPE_LARGEINT>::CppType && rhs);                         \
    template void Field::FUNC_NAME<TYPE_DATE>(typename PrimitiveTypeTraits<TYPE_DATE>::CppType && \
                                              rhs);                                               \
    template void Field::FUNC_NAME<TYPE_DATETIME>(                                                \
            typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType && rhs);                         \
    template void Field::FUNC_NAME<TYPE_DATEV2>(                                                  \
            typename PrimitiveTypeTraits<TYPE_DATEV2>::CppType && rhs);                           \
    template void Field::FUNC_NAME<TYPE_DATETIMEV2>(                                              \
            typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType && rhs);                       \
    template void Field::FUNC_NAME<TYPE_DECIMAL32>(                                               \
            typename PrimitiveTypeTraits<TYPE_DECIMAL32>::CppType && rhs);                        \
    template void Field::FUNC_NAME<TYPE_DECIMAL64>(                                               \
            typename PrimitiveTypeTraits<TYPE_DECIMAL64>::CppType && rhs);                        \
    template void Field::FUNC_NAME<TYPE_DECIMALV2>(                                               \
            typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType && rhs);                        \
    template void Field::FUNC_NAME<TYPE_DECIMAL128I>(                                             \
            typename PrimitiveTypeTraits<TYPE_DECIMAL128I>::CppType && rhs);                      \
    template void Field::FUNC_NAME<TYPE_DECIMAL256>(                                              \
            typename PrimitiveTypeTraits<TYPE_DECIMAL256>::CppType && rhs);                       \
    template void Field::FUNC_NAME<TYPE_CHAR>(typename PrimitiveTypeTraits<TYPE_CHAR>::CppType && \
                                              rhs);                                               \
    template void Field::FUNC_NAME<TYPE_VARCHAR>(                                                 \
            typename PrimitiveTypeTraits<TYPE_VARCHAR>::CppType && rhs);                          \
    template void Field::FUNC_NAME<TYPE_STRING>(                                                  \
            typename PrimitiveTypeTraits<TYPE_STRING>::CppType && rhs);                           \
    template void Field::FUNC_NAME<TYPE_VARBINARY>(                                               \
            typename PrimitiveTypeTraits<TYPE_VARBINARY>::CppType && rhs);                        \
    template void Field::FUNC_NAME<TYPE_HLL>(typename PrimitiveTypeTraits<TYPE_HLL>::CppType &&   \
                                             rhs);                                                \
    template void Field::FUNC_NAME<TYPE_VARIANT>(                                                 \
            typename PrimitiveTypeTraits<TYPE_VARIANT>::CppType && rhs);                          \
    template void Field::FUNC_NAME<TYPE_QUANTILE_STATE>(                                          \
            typename PrimitiveTypeTraits<TYPE_QUANTILE_STATE>::CppType && rhs);                   \
    template void Field::FUNC_NAME<TYPE_ARRAY>(                                                   \
            typename PrimitiveTypeTraits<TYPE_ARRAY>::CppType && rhs);                            \
    template void Field::FUNC_NAME<TYPE_TIME>(typename PrimitiveTypeTraits<TYPE_TIME>::CppType && \
                                              rhs);                                               \
    template void Field::FUNC_NAME<TYPE_TIME>(                                                    \
            const typename PrimitiveTypeTraits<TYPE_TIME>::CppType& rhs);                         \
    template void Field::FUNC_NAME<TYPE_NULL>(                                                    \
            const typename PrimitiveTypeTraits<TYPE_NULL>::CppType& rhs);                         \
    template void Field::FUNC_NAME<TYPE_TINYINT>(                                                 \
            const typename PrimitiveTypeTraits<TYPE_TINYINT>::CppType& rhs);                      \
    template void Field::FUNC_NAME<TYPE_SMALLINT>(                                                \
            const typename PrimitiveTypeTraits<TYPE_SMALLINT>::CppType& rhs);                     \
    template void Field::FUNC_NAME<TYPE_INT>(                                                     \
            const typename PrimitiveTypeTraits<TYPE_INT>::CppType& rhs);                          \
    template void Field::FUNC_NAME<TYPE_BIGINT>(                                                  \
            const typename PrimitiveTypeTraits<TYPE_BIGINT>::CppType& rhs);                       \
    template void Field::FUNC_NAME<TYPE_LARGEINT>(                                                \
            const typename PrimitiveTypeTraits<TYPE_LARGEINT>::CppType& rhs);                     \
    template void Field::FUNC_NAME<TYPE_DATE>(                                                    \
            const typename PrimitiveTypeTraits<TYPE_DATE>::CppType& rhs);                         \
    template void Field::FUNC_NAME<TYPE_DATETIME>(                                                \
            const typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType& rhs);                     \
    template void Field::FUNC_NAME<TYPE_DATEV2>(                                                  \
            const typename PrimitiveTypeTraits<TYPE_DATEV2>::CppType& rhs);                       \
    template void Field::FUNC_NAME<TYPE_DATETIMEV2>(                                              \
            const typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType& rhs);                   \
    template void Field::FUNC_NAME<TYPE_TIMESTAMPTZ>(                                             \
            const typename PrimitiveTypeTraits<TYPE_TIMESTAMPTZ>::CppType& rhs);                  \
    template void Field::FUNC_NAME<TYPE_TIMESTAMPTZ>(                                             \
            typename PrimitiveTypeTraits<TYPE_TIMESTAMPTZ>::CppType && rhs);                      \
    template void Field::FUNC_NAME<TYPE_DECIMAL32>(                                               \
            const typename PrimitiveTypeTraits<TYPE_DECIMAL32>::CppType& rhs);                    \
    template void Field::FUNC_NAME<TYPE_DECIMAL64>(                                               \
            const typename PrimitiveTypeTraits<TYPE_DECIMAL64>::CppType& rhs);                    \
    template void Field::FUNC_NAME<TYPE_DECIMALV2>(                                               \
            const typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType& rhs);                    \
    template void Field::FUNC_NAME<TYPE_DECIMAL128I>(                                             \
            const typename PrimitiveTypeTraits<TYPE_DECIMAL128I>::CppType& rhs);                  \
    template void Field::FUNC_NAME<TYPE_DECIMAL256>(                                              \
            const typename PrimitiveTypeTraits<TYPE_DECIMAL256>::CppType& rhs);                   \
    template void Field::FUNC_NAME<TYPE_CHAR>(                                                    \
            const typename PrimitiveTypeTraits<TYPE_CHAR>::CppType& rhs);                         \
    template void Field::FUNC_NAME<TYPE_VARCHAR>(                                                 \
            const typename PrimitiveTypeTraits<TYPE_VARCHAR>::CppType& rhs);                      \
    template void Field::FUNC_NAME<TYPE_STRING>(                                                  \
            const typename PrimitiveTypeTraits<TYPE_STRING>::CppType& rhs);                       \
    template void Field::FUNC_NAME<TYPE_VARBINARY>(                                               \
            const typename PrimitiveTypeTraits<TYPE_VARBINARY>::CppType& rhs);                    \
    template void Field::FUNC_NAME<TYPE_HLL>(                                                     \
            const typename PrimitiveTypeTraits<TYPE_HLL>::CppType& rhs);                          \
    template void Field::FUNC_NAME<TYPE_VARIANT>(                                                 \
            const typename PrimitiveTypeTraits<TYPE_VARIANT>::CppType& rhs);                      \
    template void Field::FUNC_NAME<TYPE_QUANTILE_STATE>(                                          \
            const typename PrimitiveTypeTraits<TYPE_QUANTILE_STATE>::CppType& rhs);               \
    template void Field::FUNC_NAME<TYPE_ARRAY>(                                                   \
            const typename PrimitiveTypeTraits<TYPE_ARRAY>::CppType& rhs);                        \
    template void Field::FUNC_NAME<TYPE_IPV4>(typename PrimitiveTypeTraits<TYPE_IPV4>::CppType && \
                                              rhs);                                               \
    template void Field::FUNC_NAME<TYPE_IPV4>(                                                    \
            const typename PrimitiveTypeTraits<TYPE_IPV4>::CppType& rhs);                         \
    template void Field::FUNC_NAME<TYPE_IPV6>(typename PrimitiveTypeTraits<TYPE_IPV6>::CppType && \
                                              rhs);                                               \
    template void Field::FUNC_NAME<TYPE_IPV6>(                                                    \
            const typename PrimitiveTypeTraits<TYPE_IPV6>::CppType& rhs);                         \
    template void Field::FUNC_NAME<TYPE_BOOLEAN>(                                                 \
            typename PrimitiveTypeTraits<TYPE_BOOLEAN>::CppType && rhs);                          \
    template void Field::FUNC_NAME<TYPE_BOOLEAN>(                                                 \
            const typename PrimitiveTypeTraits<TYPE_BOOLEAN>::CppType& rhs);                      \
    template void Field::FUNC_NAME<TYPE_FLOAT>(                                                   \
            typename PrimitiveTypeTraits<TYPE_FLOAT>::CppType && rhs);                            \
    template void Field::FUNC_NAME<TYPE_FLOAT>(                                                   \
            const typename PrimitiveTypeTraits<TYPE_FLOAT>::CppType& rhs);                        \
    template void Field::FUNC_NAME<TYPE_DOUBLE>(                                                  \
            typename PrimitiveTypeTraits<TYPE_DOUBLE>::CppType && rhs);                           \
    template void Field::FUNC_NAME<TYPE_DOUBLE>(                                                  \
            const typename PrimitiveTypeTraits<TYPE_DOUBLE>::CppType& rhs);                       \
    template void Field::FUNC_NAME<TYPE_JSONB>(                                                   \
            typename PrimitiveTypeTraits<TYPE_JSONB>::CppType && rhs);                            \
    template void Field::FUNC_NAME<TYPE_JSONB>(                                                   \
            const typename PrimitiveTypeTraits<TYPE_JSONB>::CppType& rhs);                        \
    template void Field::FUNC_NAME<TYPE_STRUCT>(                                                  \
            typename PrimitiveTypeTraits<TYPE_STRUCT>::CppType && rhs);                           \
    template void Field::FUNC_NAME<TYPE_STRUCT>(                                                  \
            const typename PrimitiveTypeTraits<TYPE_STRUCT>::CppType& rhs);                       \
    template void Field::FUNC_NAME<TYPE_MAP>(typename PrimitiveTypeTraits<TYPE_MAP>::CppType &&   \
                                             rhs);                                                \
    template void Field::FUNC_NAME<TYPE_MAP>(                                                     \
            const typename PrimitiveTypeTraits<TYPE_MAP>::CppType& rhs);                          \
    template void Field::FUNC_NAME<TYPE_BITMAP>(                                                  \
            typename PrimitiveTypeTraits<TYPE_BITMAP>::CppType && rhs);                           \
    template void Field::FUNC_NAME<TYPE_BITMAP>(                                                  \
            const typename PrimitiveTypeTraits<TYPE_BITMAP>::CppType& rhs);                       \
    template void Field::FUNC_NAME<TYPE_TIMEV2>(                                                  \
            const typename PrimitiveTypeTraits<TYPE_TIMEV2>::CppType& rhs);                       \
    template void Field::FUNC_NAME<TYPE_TIMEV2>(                                                  \
            typename PrimitiveTypeTraits<TYPE_TIMEV2>::CppType && rhs);                           \
    template void Field::FUNC_NAME<TYPE_UINT32>(                                                  \
            const typename PrimitiveTypeTraits<TYPE_UINT32>::CppType& rhs);                       \
    template void Field::FUNC_NAME<TYPE_UINT32>(                                                  \
            typename PrimitiveTypeTraits<TYPE_UINT32>::CppType && rhs);                           \
    template void Field::FUNC_NAME<TYPE_UINT64>(                                                  \
            const typename PrimitiveTypeTraits<TYPE_UINT64>::CppType& rhs);                       \
    template void Field::FUNC_NAME<TYPE_UINT64>(                                                  \
            typename PrimitiveTypeTraits<TYPE_UINT64>::CppType && rhs);
DECLARE_FUNCTION(create_concrete)
DECLARE_FUNCTION(assign_concrete)
#undef DECLARE_FUNCTION

#define DECLARE_FUNCTION(TYPE_NAME)                                                          \
    template typename PrimitiveTypeTraits<TYPE_NAME>::CppType& Field::get<TYPE_NAME>();      \
    template const typename PrimitiveTypeTraits<TYPE_NAME>::CppType& Field::get<TYPE_NAME>() \
            const;                                                                           \
    template void Field::destroy<TYPE_NAME>();
DECLARE_FUNCTION(TYPE_NULL)
DECLARE_FUNCTION(TYPE_TINYINT)
DECLARE_FUNCTION(TYPE_SMALLINT)
DECLARE_FUNCTION(TYPE_INT)
DECLARE_FUNCTION(TYPE_BIGINT)
DECLARE_FUNCTION(TYPE_LARGEINT)
DECLARE_FUNCTION(TYPE_DATE)
DECLARE_FUNCTION(TYPE_DATETIME)
DECLARE_FUNCTION(TYPE_DATEV2)
DECLARE_FUNCTION(TYPE_DATETIMEV2)
DECLARE_FUNCTION(TYPE_TIMESTAMPTZ)
DECLARE_FUNCTION(TYPE_DECIMAL32)
DECLARE_FUNCTION(TYPE_DECIMAL64)
DECLARE_FUNCTION(TYPE_DECIMALV2)
DECLARE_FUNCTION(TYPE_DECIMAL128I)
DECLARE_FUNCTION(TYPE_DECIMAL256)
DECLARE_FUNCTION(TYPE_CHAR)
DECLARE_FUNCTION(TYPE_VARCHAR)
DECLARE_FUNCTION(TYPE_STRING)
DECLARE_FUNCTION(TYPE_VARBINARY)
DECLARE_FUNCTION(TYPE_HLL)
DECLARE_FUNCTION(TYPE_VARIANT)
DECLARE_FUNCTION(TYPE_QUANTILE_STATE)
DECLARE_FUNCTION(TYPE_ARRAY)
DECLARE_FUNCTION(TYPE_TIME)
DECLARE_FUNCTION(TYPE_IPV4)
DECLARE_FUNCTION(TYPE_IPV6)
DECLARE_FUNCTION(TYPE_BOOLEAN)
DECLARE_FUNCTION(TYPE_FLOAT)
DECLARE_FUNCTION(TYPE_DOUBLE)
DECLARE_FUNCTION(TYPE_JSONB)
DECLARE_FUNCTION(TYPE_STRUCT)
DECLARE_FUNCTION(TYPE_MAP)
DECLARE_FUNCTION(TYPE_BITMAP)
DECLARE_FUNCTION(TYPE_TIMEV2)
DECLARE_FUNCTION(TYPE_UINT32)
DECLARE_FUNCTION(TYPE_UINT64)
#undef DECLARE_FUNCTION
} // namespace doris::vectorized
