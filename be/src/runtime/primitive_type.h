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

#pragma once

#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <cstdint>
#include <string>

#include "olap/decimal12.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/types.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/template_helpers.hpp"

namespace doris {

namespace vectorized {
template <typename T>
class ColumnStr;
using ColumnString = ColumnStr<UInt32>;
} // namespace vectorized

class DecimalV2Value;
struct StringRef;
struct JsonBinaryValue;

constexpr bool is_enumeration_type(PrimitiveType type) {
    switch (type) {
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_NULL:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2:
    case TYPE_TIMEV2:
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
    case TYPE_BOOLEAN:
    case TYPE_ARRAY:
    case TYPE_STRUCT:
    case TYPE_MAP:
    case TYPE_HLL:
        return false;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_DATE:
    case TYPE_DATEV2:
    case TYPE_IPV4:
    case TYPE_IPV6:
        return true;

    case INVALID_TYPE:
    default:
        DCHECK(false);
    }

    return false;
}

constexpr bool is_date_type(PrimitiveType type) {
    return type == TYPE_DATETIME || type == TYPE_DATE || type == TYPE_DATETIMEV2 ||
           type == TYPE_DATEV2;
}

constexpr bool is_string_type(PrimitiveType type) {
    return type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_STRING;
}

constexpr bool is_variant_string_type(PrimitiveType type) {
    return type == TYPE_VARCHAR || type == TYPE_STRING;
}

constexpr bool is_float_or_double(PrimitiveType type) {
    return type == TYPE_FLOAT || type == TYPE_DOUBLE;
}

constexpr bool is_int_or_bool(PrimitiveType type) {
    return type == TYPE_BOOLEAN || type == TYPE_TINYINT || type == TYPE_SMALLINT ||
           type == TYPE_INT || type == TYPE_BIGINT || type == TYPE_LARGEINT;
}

bool is_type_compatible(PrimitiveType lhs, PrimitiveType rhs);

PrimitiveType thrift_to_type(TPrimitiveType::type ttype);
TPrimitiveType::type to_thrift(PrimitiveType ptype);
std::string type_to_string(PrimitiveType t);
std::string type_to_odbc_string(PrimitiveType t);
TTypeDesc gen_type_desc(const TPrimitiveType::type val);
TTypeDesc gen_type_desc(const TPrimitiveType::type val, const std::string& name);

template <PrimitiveType type>
constexpr PrimitiveType PredicateEvaluateType = is_variant_string_type(type) ? TYPE_STRING : type;

template <PrimitiveType type>
struct PrimitiveTypeTraits;

/// CppType as type on compute layer
/// StorageFieldType as type on storage layer
template <>
struct PrimitiveTypeTraits<TYPE_BOOLEAN> {
    using CppType = bool;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnUInt8;
};
template <>
struct PrimitiveTypeTraits<TYPE_TINYINT> {
    using CppType = int8_t;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnInt8;
};
template <>
struct PrimitiveTypeTraits<TYPE_SMALLINT> {
    using CppType = int16_t;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnInt16;
};
template <>
struct PrimitiveTypeTraits<TYPE_INT> {
    using CppType = int32_t;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnInt32;
};
template <>
struct PrimitiveTypeTraits<TYPE_BIGINT> {
    using CppType = int64_t;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnInt64;
};
template <>
struct PrimitiveTypeTraits<TYPE_FLOAT> {
    using CppType = float;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnFloat32;
};
template <>
struct PrimitiveTypeTraits<TYPE_TIME> {
    using CppType = double;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnFloat64;
};
template <>
struct PrimitiveTypeTraits<TYPE_TIMEV2> {
    using CppType = double;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnFloat64;
};
template <>
struct PrimitiveTypeTraits<TYPE_DOUBLE> {
    using CppType = double;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnFloat64;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATE> {
    using CppType = doris::VecDateTimeValue;
    /// Different with compute layer, the DateV1 was stored as uint24_t(3 bytes).
    using StorageFieldType = uint24_t;
    using ColumnType = vectorized::ColumnVector<vectorized::Int64>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATETIME> {
    using CppType = doris::VecDateTimeValue;
    using StorageFieldType = uint64_t;
    using ColumnType = vectorized::ColumnVector<vectorized::Int64>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATETIMEV2> {
    using CppType = DateV2Value<DateTimeV2ValueType>;
    using StorageFieldType = uint64_t;
    using ColumnType = vectorized::ColumnVector<vectorized::UInt64>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATEV2> {
    using CppType = DateV2Value<DateV2ValueType>;
    using StorageFieldType = uint32_t;
    using ColumnType = vectorized::ColumnVector<vectorized::UInt32>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMALV2> {
    using CppType = DecimalV2Value;
    /// Different with compute layer, the DecimalV1 was stored as decimal12_t(12 bytes).
    using StorageFieldType = decimal12_t;
    using ColumnType = vectorized::ColumnDecimal<vectorized::Decimal128V2>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL32> {
    using CppType = vectorized::Decimal32;
    using StorageFieldType = vectorized::Int32;
    using ColumnType = vectorized::ColumnDecimal<vectorized::Decimal32>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL64> {
    using CppType = vectorized::Decimal64;
    using StorageFieldType = vectorized::Int64;
    using ColumnType = vectorized::ColumnDecimal<vectorized::Decimal64>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL128I> {
    using CppType = vectorized::Decimal128V3;
    using StorageFieldType = vectorized::Int128;
    using ColumnType = vectorized::ColumnDecimal<vectorized::Decimal128V3>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL256> {
    using CppType = vectorized::Decimal256;
    using StorageFieldType = wide::Int256;
    using ColumnType = vectorized::ColumnDecimal<vectorized::Decimal256>;
};
template <>
struct PrimitiveTypeTraits<TYPE_LARGEINT> {
    using CppType = __int128_t;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnInt128;
};
template <>
struct PrimitiveTypeTraits<TYPE_IPV4> {
    using CppType = vectorized::IPv4;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnIPv4;
};
template <>
struct PrimitiveTypeTraits<TYPE_IPV6> {
    using CppType = vectorized::IPv6;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnIPv6;
};
template <>
struct PrimitiveTypeTraits<TYPE_CHAR> {
    using CppType = StringRef;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnString;
};
template <>
struct PrimitiveTypeTraits<TYPE_VARCHAR> {
    using CppType = StringRef;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnString;
};
template <>
struct PrimitiveTypeTraits<TYPE_STRING> {
    using CppType = StringRef;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnString;
};
template <>
struct PrimitiveTypeTraits<TYPE_HLL> {
    using CppType = StringRef;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnString;
};
template <>
struct PrimitiveTypeTraits<TYPE_JSONB> {
    using CppType = JsonBinaryValue;
    using StorageFieldType = CppType;
    using ColumnType = vectorized::ColumnString;
};

template <typename Traits>
concept HaveCppType = requires() { sizeof(typename Traits::CppType); };

template <PrimitiveNative type>
struct PrimitiveTypeSizeReducer {
    template <HaveCppType Traits>
    static size_t get_size() {
        return sizeof(typename Traits::CppType);
    }
    template <typename Traits>
    static size_t get_size() {
        return 0;
    }

    static void run(size_t& size) { size = get_size<PrimitiveTypeTraits<PrimitiveType(type)>>(); }
};

inline size_t get_primitive_type_size(PrimitiveType t) {
    size_t size = 0;
    vectorized::constexpr_loop_match<PrimitiveNative, BEGIN_OF_PRIMITIVE_TYPE,
                                     END_OF_PRIMITIVE_TYPE, PrimitiveTypeSizeReducer>::run(t, size);
    return size;
}

template <PrimitiveType PT>
struct PrimitiveTypeConvertor {
    using CppType = typename PrimitiveTypeTraits<PT>::CppType;
    using StorageFieldType = typename PrimitiveTypeTraits<PT>::StorageFieldType;

    static inline StorageFieldType&& to_storage_field_type(CppType&& value) {
        return static_cast<StorageFieldType&&>(std::forward<CppType>(value));
    }

    static inline const StorageFieldType& to_storage_field_type(const CppType& value) {
        return *reinterpret_cast<const StorageFieldType*>(&value);
    }
};

template <>
struct PrimitiveTypeConvertor<TYPE_DATE> {
    using CppType = typename PrimitiveTypeTraits<TYPE_DATE>::CppType;
    using StorageFieldType = typename PrimitiveTypeTraits<TYPE_DATE>::StorageFieldType;

    static inline StorageFieldType to_storage_field_type(const CppType& value) {
        return value.to_olap_date();
    }
};

template <>
struct PrimitiveTypeConvertor<TYPE_DATETIME> {
    using CppType = typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType;
    using StorageFieldType = typename PrimitiveTypeTraits<TYPE_DATETIME>::StorageFieldType;

    static inline StorageFieldType to_storage_field_type(const CppType& value) {
        return value.to_olap_datetime();
    }
};

template <>
struct PrimitiveTypeConvertor<TYPE_DECIMALV2> {
    using CppType = typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType;
    using StorageFieldType = typename PrimitiveTypeTraits<TYPE_DECIMALV2>::StorageFieldType;

    static inline StorageFieldType to_storage_field_type(const CppType& value) {
        return {value.int_value(), value.frac_value()};
    }
};

} // namespace doris
