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

#include "common/cast_set.h"
#include "olap/decimal12.h"
#include "olap/uint24.h"
#include "runtime/define_primitive_type.h"
#include "vec/common/string_view.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/template_helpers.hpp"

namespace doris {
#include "common/compile_check_begin.h"
namespace vectorized {
template <typename T>
class ColumnStr;
class IColumnDummy;
class ColumnMap;
class ColumnVariant;
class ColumnStruct;
class ColumnVarbinary;
using ColumnString = ColumnStr<UInt32>;
class JsonbField;
template <typename T>
class DecimalField;
template <DecimalNativeTypeConcept T>
struct Decimal;
template <PrimitiveType T>
class ColumnComplexType;
using ColumnBitmap = ColumnComplexType<TYPE_BITMAP>;
using ColumnHLL = ColumnComplexType<TYPE_HLL>;
using ColumnQuantileState = ColumnComplexType<TYPE_QUANTILE_STATE>;
template <PrimitiveType T>
class DataTypeNumber;
using DataTypeInt8 = DataTypeNumber<TYPE_TINYINT>;
using DataTypeInt16 = DataTypeNumber<TYPE_SMALLINT>;
using DataTypeInt32 = DataTypeNumber<TYPE_INT>;
using DataTypeInt64 = DataTypeNumber<TYPE_BIGINT>;
using DataTypeInt128 = DataTypeNumber<TYPE_LARGEINT>;
using DataTypeFloat32 = DataTypeNumber<TYPE_FLOAT>;
using DataTypeFloat64 = DataTypeNumber<TYPE_DOUBLE>;
using DataTypeUInt8 = DataTypeNumber<TYPE_BOOLEAN>;
using DataTypeBool = DataTypeNumber<TYPE_BOOLEAN>;

class DataTypeNothing;
class DataTypeTimeV2;
class DataTypeDateTime;
class DataTypeDate;
class DataTypeDateTimeV2;
class DataTypeDateV2;
template <PrimitiveType T>
class DataTypeDecimal;
using DataTypeDecimal32 = DataTypeDecimal<TYPE_DECIMAL32>;
using DataTypeDecimal64 = DataTypeDecimal<TYPE_DECIMAL64>;
using DataTypeDecimalV2 = DataTypeDecimal<TYPE_DECIMALV2>;
using DataTypeDecimal128 = DataTypeDecimal<TYPE_DECIMAL128I>;
using DataTypeDecimal256 = DataTypeDecimal<TYPE_DECIMAL256>;
class DataTypeIPv4;
class DataTypeIPv6;
class DataTypeString;
class DataTypeVarbinary;
class DataTypeHLL;
class DataTypeJsonb;
class DataTypeArray;
class DataTypeMap;
class DataTypeVariant;
class DataTypeStruct;
class DataTypeBitMap;
class DataTypeQuantileState;
template <PrimitiveType T>
class ColumnVector;
using ColumnUInt8 = ColumnVector<TYPE_BOOLEAN>;
using ColumnInt8 = ColumnVector<TYPE_TINYINT>;
using ColumnInt16 = ColumnVector<TYPE_SMALLINT>;
using ColumnInt32 = ColumnVector<TYPE_INT>;
using ColumnInt64 = ColumnVector<TYPE_BIGINT>;
using ColumnInt128 = ColumnVector<TYPE_LARGEINT>;
using ColumnBool = ColumnUInt8;
using ColumnDate = ColumnVector<TYPE_DATE>;
using ColumnDateTime = ColumnVector<TYPE_DATETIME>;
using ColumnDateV2 = ColumnVector<TYPE_DATEV2>;
using ColumnDateTimeV2 = ColumnVector<TYPE_DATETIMEV2>;
using ColumnFloat32 = ColumnVector<TYPE_FLOAT>;
using ColumnFloat64 = ColumnVector<TYPE_DOUBLE>;
using ColumnIPv4 = ColumnVector<TYPE_IPV4>;
using ColumnIPv6 = ColumnVector<TYPE_IPV6>;
using ColumnTime = ColumnVector<TYPE_TIME>;
using ColumnTimeV2 = ColumnVector<TYPE_TIMEV2>;
using ColumnOffset32 = ColumnVector<TYPE_UINT32>;
using ColumnOffset64 = ColumnVector<TYPE_UINT64>;
template <PrimitiveType T>
class ColumnDecimal;
using ColumnDecimal32 = ColumnDecimal<TYPE_DECIMAL32>;
using ColumnDecimal64 = ColumnDecimal<TYPE_DECIMAL64>;
using ColumnDecimal128V2 = ColumnDecimal<TYPE_DECIMALV2>;
using ColumnDecimal128V3 = ColumnDecimal<TYPE_DECIMAL128I>;
using ColumnDecimal256 = ColumnDecimal<TYPE_DECIMAL256>;
class ColumnArray;
} // namespace vectorized

class DecimalV2Value;
struct StringRef;

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
    case TYPE_VARBINARY:
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

constexpr bool is_time_type(PrimitiveType type) {
    return type == TYPE_TIME || type == TYPE_TIMEV2;
}

constexpr bool is_date_or_datetime(PrimitiveType type) {
    return type == TYPE_DATETIME || type == TYPE_DATE;
}

constexpr bool is_date_v2_or_datetime_v2(PrimitiveType type) {
    return type == TYPE_DATETIMEV2 || type == TYPE_DATEV2;
}

constexpr bool is_ip(PrimitiveType type) {
    return type == TYPE_IPV4 || type == TYPE_IPV6;
}

constexpr bool is_varbinary(PrimitiveType type) {
    return type == TYPE_VARBINARY;
}

constexpr bool is_string_type(PrimitiveType type) {
    return type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_STRING;
}

constexpr bool is_var_len_object(PrimitiveType type) {
    return type == TYPE_HLL || type == TYPE_BITMAP || type == TYPE_QUANTILE_STATE;
}

constexpr bool is_complex_type(PrimitiveType type) {
    return type == TYPE_STRUCT || type == TYPE_ARRAY || type == TYPE_MAP;
}

constexpr bool is_variant_string_type(PrimitiveType type) {
    return type == TYPE_VARCHAR || type == TYPE_STRING;
}

constexpr bool is_float_or_double(PrimitiveType type) {
    return type == TYPE_FLOAT || type == TYPE_DOUBLE;
}

constexpr bool is_int(PrimitiveType type) {
    return type == TYPE_TINYINT || type == TYPE_SMALLINT || type == TYPE_INT ||
           type == TYPE_BIGINT || type == TYPE_LARGEINT;
}

constexpr bool is_int_or_bool(PrimitiveType type) {
    return type == TYPE_BOOLEAN || is_int(type);
}

constexpr bool is_decimal(PrimitiveType type) {
    return type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128I ||
           type == TYPE_DECIMAL256 || type == TYPE_DECIMALV2;
}

constexpr bool is_number(PrimitiveType type) {
    return is_int_or_bool(type) || is_float_or_double(type) || is_decimal(type);
}

PrimitiveType thrift_to_type(TPrimitiveType::type ttype);
TPrimitiveType::type to_thrift(PrimitiveType ptype);
std::string type_to_string(PrimitiveType t);
TTypeDesc gen_type_desc(const TPrimitiveType::type val);
TTypeDesc gen_type_desc(const TPrimitiveType::type val, const std::string& name);

template <PrimitiveType type>
constexpr PrimitiveType PredicateEvaluateType = is_variant_string_type(type) ? TYPE_STRING : type;

template <PrimitiveType type>
struct PrimitiveTypeTraits;

/**
 * CppType: Doris type in execution engine
 * StorageFieldType: Doris type in storage engine
 * CppNativeType: Native type in C++ mapping to `CppType`. (e.g. VecDateTime <-> Int64)
 * ColumnItemType: Data item type in column
 * DataType: DataType which is mapping to this PrimitiveType
 * ColumnType: ColumnType which is mapping to this PrimitiveType
 * NearestFieldType: Nearest Doris type in execution engine
 * AvgNearestFieldType: Nearest Doris type in execution engine for Avg
 * AvgNearestFieldType256: Nearest Doris type in execution engine  for Avg
 * NearestPrimitiveType: Nearest primitive type
 */
template <>
struct PrimitiveTypeTraits<TYPE_BOOLEAN> {
    using CppType = bool;
    using StorageFieldType = CppType;
    using CppNativeType = bool;
    using ColumnItemType = vectorized::UInt8;
    using DataType = vectorized::DataTypeBool;
    using ColumnType = vectorized::ColumnUInt8;
    using NearestFieldType = vectorized::Int64;
    using AvgNearestFieldType = vectorized::Int64;
    using AvgNearestFieldType256 = vectorized::Int64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_BIGINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_BIGINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_BIGINT;
};
template <>
struct PrimitiveTypeTraits<TYPE_TINYINT> {
    using CppType = int8_t;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeInt8;
    using ColumnType = vectorized::ColumnInt8;
    using NearestFieldType = vectorized::Int64;
    using AvgNearestFieldType = vectorized::Int64;
    using AvgNearestFieldType256 = vectorized::Int64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_BIGINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_BIGINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_BIGINT;
};
template <>
struct PrimitiveTypeTraits<TYPE_SMALLINT> {
    using CppType = int16_t;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeInt16;
    using ColumnType = vectorized::ColumnInt16;
    using NearestFieldType = vectorized::Int64;
    using AvgNearestFieldType = vectorized::Int64;
    using AvgNearestFieldType256 = vectorized::Int64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_BIGINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_BIGINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_BIGINT;
};
template <>
struct PrimitiveTypeTraits<TYPE_INT> {
    using CppType = int32_t;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeInt32;
    using ColumnType = vectorized::ColumnInt32;
    using NearestFieldType = vectorized::Int64;
    using AvgNearestFieldType = vectorized::Int64;
    using AvgNearestFieldType256 = vectorized::Int64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_BIGINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_BIGINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_BIGINT;
};
template <>
struct PrimitiveTypeTraits<TYPE_BIGINT> {
    using CppType = int64_t;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeInt64;
    using ColumnType = vectorized::ColumnInt64;
    using NearestFieldType = vectorized::Int64;
    using AvgNearestFieldType = vectorized::Int128;
    using AvgNearestFieldType256 = vectorized::Int128;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_BIGINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_LARGEINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_LARGEINT;
};
template <>
struct PrimitiveTypeTraits<TYPE_LARGEINT> {
    using CppType = __int128_t;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeInt128;
    using ColumnType = vectorized::ColumnInt128;
    using NearestFieldType = vectorized::Int128;
    using AvgNearestFieldType = vectorized::Int128;
    using AvgNearestFieldType256 = vectorized::Int128;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_LARGEINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_LARGEINT;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_LARGEINT;
};
template <>
struct PrimitiveTypeTraits<TYPE_NULL> {
    using CppType = vectorized::Null;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeNothing;
    using ColumnType = vectorized::IColumnDummy;
    using NearestFieldType = vectorized::Null;
    using AvgNearestFieldType = vectorized::Null;
    using AvgNearestFieldType256 = vectorized::Null;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_NULL;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_NULL;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_NULL;
};
template <>
struct PrimitiveTypeTraits<TYPE_FLOAT> {
    using CppType = float;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeFloat32;
    using ColumnType = vectorized::ColumnFloat32;
    using NearestFieldType = vectorized::Float64;
    using AvgNearestFieldType = vectorized::Float64;
    using AvgNearestFieldType256 = vectorized::Float64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DOUBLE;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DOUBLE;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DOUBLE;
};
template <>
struct PrimitiveTypeTraits<TYPE_DOUBLE> {
    using CppType = double;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeFloat64;
    using ColumnType = vectorized::ColumnFloat64;
    using NearestFieldType = vectorized::Float64;
    using AvgNearestFieldType = vectorized::Float64;
    using AvgNearestFieldType256 = vectorized::Float64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DOUBLE;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DOUBLE;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DOUBLE;
};
template <>
struct PrimitiveTypeTraits<TYPE_TIMEV2> {
    using CppType = vectorized::Float64;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeTimeV2;
    using ColumnType = vectorized::ColumnTimeV2;
    using NearestFieldType = vectorized::Float64;
    using AvgNearestFieldType = vectorized::Float64;
    using AvgNearestFieldType256 = vectorized::Float64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DOUBLE;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DOUBLE;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DOUBLE;
};
template <>
struct PrimitiveTypeTraits<TYPE_TIME> {
    using CppType = vectorized::Float64;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeTimeV2;
    using ColumnType = vectorized::ColumnTime;
    using NearestFieldType = vectorized::Float64;
    using AvgNearestFieldType = vectorized::Float64;
    using AvgNearestFieldType256 = vectorized::Float64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DOUBLE;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DOUBLE;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DOUBLE;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATE> {
    using CppType = doris::VecDateTimeValue;
    /// Different with compute layer, the DateV1 was stored as uint24_t(3 bytes).
    using StorageFieldType = uint24_t;
    using CppNativeType = vectorized::Int64;
    using ColumnItemType = vectorized::Int64;
    using DataType = vectorized::DataTypeDate;
    using ColumnType = vectorized::ColumnDate;
    using NearestFieldType = vectorized::Int64;
    using AvgNearestFieldType = vectorized::Int64;
    using AvgNearestFieldType256 = vectorized::Int64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DATE;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DATE;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DATE;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATETIME> {
    using CppType = doris::VecDateTimeValue;
    using StorageFieldType = uint64_t;
    using CppNativeType = vectorized::Int64;
    using ColumnItemType = vectorized::Int64;
    using DataType = vectorized::DataTypeDateTime;
    using ColumnType = vectorized::ColumnDateTime;
    using NearestFieldType = vectorized::Int64;
    using AvgNearestFieldType = vectorized::Int64;
    using AvgNearestFieldType256 = vectorized::Int64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DATETIME;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DATETIME;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DATETIME;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATETIMEV2> {
    using CppType = DateV2Value<DateTimeV2ValueType>;
    using StorageFieldType = uint64_t;
    using CppNativeType = uint64_t;
    using ColumnItemType = vectorized::UInt64;
    using DataType = vectorized::DataTypeDateTimeV2;
    using ColumnType = vectorized::ColumnDateTimeV2;
    using NearestFieldType = vectorized::UInt64;
    using AvgNearestFieldType = vectorized::UInt64;
    using AvgNearestFieldType256 = vectorized::UInt64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DATETIMEV2;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DATETIMEV2;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DATETIMEV2;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATEV2> {
    using CppType = DateV2Value<DateV2ValueType>;
    using StorageFieldType = uint32_t;
    using CppNativeType = uint32_t;
    using ColumnItemType = vectorized::UInt32;
    using DataType = vectorized::DataTypeDateV2;
    using ColumnType = vectorized::ColumnDateV2;
    using NearestFieldType = vectorized::UInt64;
    using AvgNearestFieldType = vectorized::UInt32;
    using AvgNearestFieldType256 = vectorized::UInt32;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DATEV2;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DATEV2;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DATEV2;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMALV2> {
    using CppType = DecimalV2Value;
    /// Different with compute layer, the DecimalV1 was stored as decimal12_t(12 bytes).
    using StorageFieldType = decimal12_t;
    using CppNativeType = vectorized::Int128;
    using ColumnItemType = vectorized::Decimal128V2;
    using DataType = vectorized::DataTypeDecimalV2;
    using ColumnType = vectorized::ColumnDecimal128V2;
    using NearestFieldType = vectorized::DecimalField<vectorized::Decimal128V2>;
    using AvgNearestFieldType = vectorized::Decimal128V2;
    using AvgNearestFieldType256 = vectorized::Decimal256;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DECIMALV2;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DECIMALV2;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DECIMALV2;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL32> {
    using CppType = vectorized::Decimal32;
    using StorageFieldType = vectorized::Int32;
    using CppNativeType = vectorized::Int32;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeDecimal32;
    using ColumnType = vectorized::ColumnDecimal32;
    using NearestFieldType = vectorized::DecimalField<vectorized::Decimal32>;
    using AvgNearestFieldType = vectorized::Decimal128V3;
    using AvgNearestFieldType256 = vectorized::Decimal256;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DECIMAL32;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DECIMAL128I;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DECIMAL256;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL64> {
    using CppType = vectorized::Decimal64;
    using StorageFieldType = vectorized::Int64;
    using CppNativeType = vectorized::Int64;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeDecimal64;
    using ColumnType = vectorized::ColumnDecimal64;
    using NearestFieldType = vectorized::DecimalField<vectorized::Decimal64>;
    using AvgNearestFieldType = vectorized::Decimal128V3;
    using AvgNearestFieldType256 = vectorized::Decimal256;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DECIMAL64;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DECIMAL128I;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DECIMAL256;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL128I> {
    using CppType = vectorized::Decimal128V3;
    using StorageFieldType = vectorized::Int128;
    using CppNativeType = vectorized::Int128;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeDecimal128;
    using ColumnType = vectorized::ColumnDecimal128V3;
    using NearestFieldType = vectorized::DecimalField<vectorized::Decimal128V3>;
    using AvgNearestFieldType = vectorized::Decimal128V3;
    using AvgNearestFieldType256 = vectorized::Decimal256;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DECIMAL128I;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DECIMAL128I;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DECIMAL256;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL256> {
    using CppType = vectorized::Decimal256;
    using StorageFieldType = wide::Int256;
    using CppNativeType = wide::Int256;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeDecimal256;
    using ColumnType = vectorized::ColumnDecimal256;
    using NearestFieldType = vectorized::DecimalField<vectorized::Decimal256>;
    using AvgNearestFieldType = vectorized::Decimal256;
    using AvgNearestFieldType256 = vectorized::Decimal256;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_DECIMAL256;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DECIMAL256;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DECIMAL256;
};
template <>
struct PrimitiveTypeTraits<TYPE_IPV4> {
    using CppType = IPv4;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeIPv4;
    using ColumnType = vectorized::ColumnIPv4;
    using NearestFieldType = IPv4;
    using AvgNearestFieldType = IPv4;
    using AvgNearestFieldType256 = IPv4;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_IPV4;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_IPV4;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_IPV4;
};
template <>
struct PrimitiveTypeTraits<TYPE_IPV6> {
    using CppType = IPv6;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeIPv6;
    using ColumnType = vectorized::ColumnIPv6;
    using NearestFieldType = IPv6;
    using AvgNearestFieldType = IPv6;
    using AvgNearestFieldType256 = IPv6;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_IPV6;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_IPV6;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_IPV6;
};
template <>
struct PrimitiveTypeTraits<TYPE_CHAR> {
    using CppType = StringRef;
    using StorageFieldType = CppType;
    using CppNativeType = vectorized::String;
    using ColumnItemType = vectorized::String;
    using DataType = vectorized::DataTypeString;
    using ColumnType = vectorized::ColumnString;
    using NearestFieldType = vectorized::String;
    using AvgNearestFieldType = vectorized::String;
    using AvgNearestFieldType256 = vectorized::String;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_CHAR;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_CHAR;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_CHAR;
};
template <>
struct PrimitiveTypeTraits<TYPE_VARCHAR> {
    using CppType = StringRef;
    using StorageFieldType = CppType;
    using CppNativeType = vectorized::String;
    using ColumnItemType = vectorized::String;
    using DataType = vectorized::DataTypeString;
    using ColumnType = vectorized::ColumnString;
    using NearestFieldType = vectorized::String;
    using AvgNearestFieldType = vectorized::String;
    using AvgNearestFieldType256 = vectorized::String;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_VARCHAR;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_VARCHAR;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_VARCHAR;
};
template <>
struct PrimitiveTypeTraits<TYPE_STRING> {
    using CppType = StringRef;
    using StorageFieldType = CppType;
    using CppNativeType = vectorized::String;
    using ColumnItemType = vectorized::String;
    using DataType = vectorized::DataTypeString;
    using ColumnType = vectorized::ColumnString;
    using NearestFieldType = vectorized::String;
    using AvgNearestFieldType = vectorized::String;
    using AvgNearestFieldType256 = vectorized::String;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_STRING;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_STRING;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_STRING;
};
template <>
struct PrimitiveTypeTraits<TYPE_VARBINARY> {
    using CppType = doris::StringView;
    using StorageFieldType = CppType;
    using CppNativeType = doris::StringView;
    using ColumnItemType = doris::StringView;
    using DataType = vectorized::DataTypeVarbinary;
    using ColumnType = vectorized::ColumnVarbinary;
    using NearestFieldType = doris::StringView;
    using AvgNearestFieldType = doris::StringView;
    using AvgNearestFieldType256 = doris::StringView;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_VARBINARY;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_VARBINARY;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_VARBINARY;
};
template <>
struct PrimitiveTypeTraits<TYPE_HLL> {
    using CppType = HyperLogLog;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = HyperLogLog;
    using DataType = vectorized::DataTypeHLL;
    using ColumnType = vectorized::ColumnHLL;
    using NearestFieldType = HyperLogLog;
    using AvgNearestFieldType = HyperLogLog;
    using AvgNearestFieldType256 = HyperLogLog;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_HLL;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_HLL;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_HLL;
};
template <>
struct PrimitiveTypeTraits<TYPE_JSONB> {
    using CppType = vectorized::JsonbField;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeJsonb;
    using ColumnType = vectorized::ColumnString;
    using NearestFieldType = vectorized::JsonbField;
    using AvgNearestFieldType = vectorized::JsonbField;
    using AvgNearestFieldType256 = vectorized::JsonbField;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_JSONB;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_JSONB;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_JSONB;
};
template <>
struct PrimitiveTypeTraits<TYPE_ARRAY> {
    using CppType = vectorized::Array;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeArray;
    using ColumnType = vectorized::ColumnArray;
    using NearestFieldType = vectorized::Array;
    using AvgNearestFieldType = vectorized::Array;
    using AvgNearestFieldType256 = vectorized::Array;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_ARRAY;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_ARRAY;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_ARRAY;
};
template <>
struct PrimitiveTypeTraits<TYPE_MAP> {
    using CppType = vectorized::Map;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeMap;
    using ColumnType = vectorized::ColumnMap;
    using NearestFieldType = vectorized::Map;
    using AvgNearestFieldType = vectorized::Map;
    using AvgNearestFieldType256 = vectorized::Map;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_MAP;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_MAP;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_MAP;
};
template <>
struct PrimitiveTypeTraits<TYPE_STRUCT> {
    using CppType = vectorized::Tuple;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeStruct;
    using ColumnType = vectorized::ColumnStruct;
    using NearestFieldType = vectorized::Tuple;
    using AvgNearestFieldType = vectorized::Tuple;
    using AvgNearestFieldType256 = vectorized::Tuple;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_STRUCT;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_STRUCT;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_STRUCT;
};
template <>
struct PrimitiveTypeTraits<TYPE_VARIANT> {
    using CppType = vectorized::VariantMap;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeVariant;
    using ColumnType = vectorized::ColumnVariant;
    using NearestFieldType = vectorized::VariantMap;
    using AvgNearestFieldType = vectorized::VariantMap;
    using AvgNearestFieldType256 = vectorized::VariantMap;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_VARIANT;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_VARIANT;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_VARIANT;
};
template <>
struct PrimitiveTypeTraits<TYPE_BITMAP> {
    using CppType = BitmapValue;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeBitMap;
    using ColumnType = vectorized::ColumnBitmap;
    using NearestFieldType = BitmapValue;
    using AvgNearestFieldType = BitmapValue;
    using AvgNearestFieldType256 = BitmapValue;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_BITMAP;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_BITMAP;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_BITMAP;
};
template <>
struct PrimitiveTypeTraits<TYPE_QUANTILE_STATE> {
    using CppType = QuantileState;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeQuantileState;
    using ColumnType = vectorized::ColumnQuantileState;
    using NearestFieldType = QuantileState;
    using AvgNearestFieldType = QuantileState;
    using AvgNearestFieldType256 = QuantileState;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_QUANTILE_STATE;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_QUANTILE_STATE;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_QUANTILE_STATE;
};
template <>
struct PrimitiveTypeTraits<TYPE_UINT32> {
    using CppType = vectorized::UInt32;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeNothing;
    using ColumnType = vectorized::ColumnOffset32;
    using NearestFieldType = vectorized::UInt64;
    using AvgNearestFieldType = vectorized::UInt64;
    using AvgNearestFieldType256 = vectorized::UInt64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_UINT32;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_UINT32;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_UINT32;
};
template <>
struct PrimitiveTypeTraits<TYPE_UINT64> {
    using CppType = vectorized::UInt64;
    using StorageFieldType = CppType;
    using CppNativeType = CppType;
    using ColumnItemType = CppType;
    using DataType = vectorized::DataTypeNothing;
    using ColumnType = vectorized::ColumnOffset64;
    using NearestFieldType = vectorized::UInt64;
    using AvgNearestFieldType = vectorized::Float64;
    using AvgNearestFieldType256 = vectorized::Float64;
    static constexpr PrimitiveType NearestPrimitiveType = TYPE_UINT64;
    static constexpr PrimitiveType AvgNearestPrimitiveType = TYPE_DOUBLE;
    static constexpr PrimitiveType AvgNearestPrimitiveType256 = TYPE_DOUBLE;
};

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
        return StorageFieldType(cast_set<uint32_t>(value.to_olap_date()));
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

#include "common/compile_check_end.h"
} // namespace doris
