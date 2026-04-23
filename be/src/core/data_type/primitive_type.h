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

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <cstdint>
#include <string>

#include "common/cast_set.h"
#include "core/data_type/define_primitive_type.h"
#include "core/decimal12.h"
#include "core/string_view.h"
#include "core/types.h"
#include "core/uint24.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exec/common/template_helpers.hpp"
#include "util/json/path_in_data.h"

namespace doris {
template <typename T>
class ColumnStr;
class IColumnDummy;
class ColumnMap;
class ColumnVariant;
class ColumnStruct;
class ColumnVarbinary;
using ColumnString = ColumnStr<UInt32>;
class JsonbField;
struct Array;
struct Tuple;
struct Map;
struct FieldWithDataType;
using VariantMap = std::map<PathInData, FieldWithDataType>;
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
class DataTypeTimeStampTz;
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
using ColumnTimeStampTz = ColumnVector<TYPE_TIMESTAMPTZ>;
using ColumnDateTimeV2 = ColumnVector<TYPE_DATETIMEV2>;
using ColumnFloat32 = ColumnVector<TYPE_FLOAT>;
using ColumnFloat64 = ColumnVector<TYPE_DOUBLE>;
using ColumnIPv4 = ColumnVector<TYPE_IPV4>;
using ColumnIPv6 = ColumnVector<TYPE_IPV6>;
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

class DecimalV2Value;

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
    case TYPE_TIMESTAMPTZ:
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
    return type == TYPE_TIMEV2;
}

constexpr bool is_timestamptz_type(PrimitiveType type) {
    return type == TYPE_TIMESTAMPTZ;
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

constexpr bool is_double(PrimitiveType type) {
    return type == TYPE_DOUBLE;
}

constexpr bool is_int(PrimitiveType type) {
    return type == TYPE_TINYINT || type == TYPE_SMALLINT || type == TYPE_INT ||
           type == TYPE_BIGINT || type == TYPE_LARGEINT;
}

constexpr bool is_int_or_bool(PrimitiveType type) {
    return type == TYPE_BOOLEAN || is_int(type);
}

constexpr bool is_decimalv2(PrimitiveType type) {
    return type == TYPE_DECIMALV2;
}

constexpr bool is_decimalv3(PrimitiveType type) {
    return type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128I ||
           type == TYPE_DECIMAL256;
}
constexpr bool is_decimal(PrimitiveType type) {
    return is_decimalv3(type) || is_decimalv2(type);
}

constexpr bool is_same_or_wider_decimalv3(PrimitiveType type1, PrimitiveType type2) {
    return is_decimalv3(type1) && is_decimalv3(type2) && (type2 >= type1);
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
 * DataType: DataType which is mapping to this PrimitiveType
 * ColumnType: ColumnType which is mapping to this PrimitiveType
 */
template <>
struct PrimitiveTypeTraits<TYPE_BOOLEAN> {
    using CppType = UInt8;
    using StorageFieldType = CppType;
    using DataType = DataTypeBool;
    using ColumnType = ColumnUInt8;
};
template <>
struct PrimitiveTypeTraits<TYPE_TINYINT> {
    using CppType = int8_t;
    using StorageFieldType = CppType;
    using DataType = DataTypeInt8;
    using ColumnType = ColumnInt8;
};
template <>
struct PrimitiveTypeTraits<TYPE_SMALLINT> {
    using CppType = int16_t;
    using StorageFieldType = CppType;
    using DataType = DataTypeInt16;
    using ColumnType = ColumnInt16;
};
template <>
struct PrimitiveTypeTraits<TYPE_INT> {
    using CppType = int32_t;
    using StorageFieldType = CppType;
    using DataType = DataTypeInt32;
    using ColumnType = ColumnInt32;
};
template <>
struct PrimitiveTypeTraits<TYPE_BIGINT> {
    using CppType = int64_t;
    using StorageFieldType = CppType;
    using DataType = DataTypeInt64;
    using ColumnType = ColumnInt64;
};
template <>
struct PrimitiveTypeTraits<TYPE_LARGEINT> {
    using CppType = __int128_t;
    using StorageFieldType = CppType;
    using DataType = DataTypeInt128;
    using ColumnType = ColumnInt128;
};
template <>
struct PrimitiveTypeTraits<TYPE_NULL> {
    using CppType = Null;
    using StorageFieldType = CppType;
    using DataType = DataTypeNothing;
    using ColumnType = IColumnDummy;
};
template <>
struct PrimitiveTypeTraits<TYPE_FLOAT> {
    using CppType = float;
    using StorageFieldType = CppType;
    using DataType = DataTypeFloat32;
    using ColumnType = ColumnFloat32;
};
template <>
struct PrimitiveTypeTraits<TYPE_DOUBLE> {
    using CppType = double;
    using StorageFieldType = CppType;
    using DataType = DataTypeFloat64;
    using ColumnType = ColumnFloat64;
};
template <>
struct PrimitiveTypeTraits<TYPE_TIMEV2> {
    using CppType = Float64;
    using StorageFieldType = CppType;
    using DataType = DataTypeTimeV2;
    using ColumnType = ColumnTimeV2;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATE> {
    using CppType = doris::VecDateTimeValue;
    /// Different with compute layer, the DateV1 was stored as uint24_t(3 bytes).
    using StorageFieldType = uint24_t;
    using DataType = DataTypeDate;
    using ColumnType = ColumnDate;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATETIME> {
    using CppType = doris::VecDateTimeValue;
    using StorageFieldType = uint64_t;
    using DataType = DataTypeDateTime;
    using ColumnType = ColumnDateTime;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATETIMEV2> {
    using CppType = DateV2Value<DateTimeV2ValueType>;
    using StorageFieldType = uint64_t;
    using DataType = DataTypeDateTimeV2;
    using ColumnType = ColumnDateTimeV2;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATEV2> {
    using CppType = DateV2Value<DateV2ValueType>;
    using StorageFieldType = uint32_t;
    using DataType = DataTypeDateV2;
    using ColumnType = ColumnDateV2;
};

template <>
struct PrimitiveTypeTraits<TYPE_TIMESTAMPTZ> {
    using CppType = TimestampTzValue;
    using StorageFieldType = uint64_t;
    using DataType = DataTypeTimeStampTz;
    using ColumnType = ColumnTimeStampTz;
};

template <>
struct PrimitiveTypeTraits<TYPE_DECIMALV2> {
    using CppType = DecimalV2Value;
    /// Different with compute layer, the DecimalV1 was stored as decimal12_t(12 bytes).
    using StorageFieldType = decimal12_t;
    using DataType = DataTypeDecimalV2;
    using ColumnType = ColumnDecimal128V2;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL32> {
    using CppType = Decimal32;
    using StorageFieldType = Int32;
    using DataType = DataTypeDecimal32;
    using ColumnType = ColumnDecimal32;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL64> {
    using CppType = Decimal64;
    using StorageFieldType = Int64;
    using DataType = DataTypeDecimal64;
    using ColumnType = ColumnDecimal64;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL128I> {
    using CppType = Decimal128V3;
    using StorageFieldType = Int128;
    using DataType = DataTypeDecimal128;
    using ColumnType = ColumnDecimal128V3;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL256> {
    using CppType = Decimal256;
    using StorageFieldType = wide::Int256;
    using DataType = DataTypeDecimal256;
    using ColumnType = ColumnDecimal256;
};
template <>
struct PrimitiveTypeTraits<TYPE_IPV4> {
    using CppType = IPv4;
    using StorageFieldType = CppType;
    using DataType = DataTypeIPv4;
    using ColumnType = ColumnIPv4;
};
template <>
struct PrimitiveTypeTraits<TYPE_IPV6> {
    using CppType = IPv6;
    using StorageFieldType = CppType;
    using DataType = DataTypeIPv6;
    using ColumnType = ColumnIPv6;
};
template <>
struct PrimitiveTypeTraits<TYPE_CHAR> {
    using CppType = String;
    using StorageFieldType = CppType;
    using DataType = DataTypeString;
    using ColumnType = ColumnString;
};
template <>
struct PrimitiveTypeTraits<TYPE_VARCHAR> {
    using CppType = String;
    using StorageFieldType = CppType;
    using DataType = DataTypeString;
    using ColumnType = ColumnString;
};
template <>
struct PrimitiveTypeTraits<TYPE_STRING> {
    using CppType = String;
    using StorageFieldType = CppType;
    using DataType = DataTypeString;
    using ColumnType = ColumnString;
};
template <>
struct PrimitiveTypeTraits<TYPE_VARBINARY> {
    using CppType = doris::StringView;
    using StorageFieldType = CppType;
    using DataType = DataTypeVarbinary;
    using ColumnType = ColumnVarbinary;
};
template <>
struct PrimitiveTypeTraits<TYPE_HLL> {
    using CppType = HyperLogLog;
    using StorageFieldType = CppType;
    using DataType = DataTypeHLL;
    using ColumnType = ColumnHLL;
};
template <>
struct PrimitiveTypeTraits<TYPE_JSONB> {
    using CppType = JsonbField;
    using StorageFieldType = CppType;
    using DataType = DataTypeJsonb;
    using ColumnType = ColumnString;
};
template <>
struct PrimitiveTypeTraits<TYPE_ARRAY> {
    using CppType = Array;
    using StorageFieldType = CppType;
    using DataType = DataTypeArray;
    using ColumnType = ColumnArray;
};
template <>
struct PrimitiveTypeTraits<TYPE_MAP> {
    using CppType = Map;
    using StorageFieldType = CppType;
    using DataType = DataTypeMap;
    using ColumnType = ColumnMap;
};
template <>
struct PrimitiveTypeTraits<TYPE_STRUCT> {
    using CppType = Tuple;
    using StorageFieldType = CppType;
    using DataType = DataTypeStruct;
    using ColumnType = ColumnStruct;
};
template <>
struct PrimitiveTypeTraits<TYPE_VARIANT> {
    using CppType = VariantMap;
    using StorageFieldType = CppType;
    using DataType = DataTypeVariant;
    using ColumnType = ColumnVariant;
};
template <>
struct PrimitiveTypeTraits<TYPE_BITMAP> {
    using CppType = BitmapValue;
    using StorageFieldType = CppType;
    using DataType = DataTypeBitMap;
    using ColumnType = ColumnBitmap;
};
template <>
struct PrimitiveTypeTraits<TYPE_QUANTILE_STATE> {
    using CppType = QuantileState;
    using StorageFieldType = CppType;
    using DataType = DataTypeQuantileState;
    using ColumnType = ColumnQuantileState;
};
template <>
struct PrimitiveTypeTraits<TYPE_UINT32> {
    using CppType = UInt32;
    using StorageFieldType = CppType;
    using DataType = DataTypeNothing;
    using ColumnType = ColumnOffset32;
};
template <>
struct PrimitiveTypeTraits<TYPE_UINT64> {
    using CppType = UInt64;
    using StorageFieldType = CppType;
    using DataType = DataTypeNothing;
    using ColumnType = ColumnOffset64;
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

inline TTypeDesc create_type_desc(PrimitiveType type, int precision = 0, int scale = 0) {
    TTypeDesc type_desc;
    std::vector<TTypeNode> node_type;
    node_type.emplace_back();
    TScalarType scalarType;
    scalarType.__set_type(to_thrift(type));
    scalarType.__set_len(-1);
    scalarType.__set_precision(precision);
    scalarType.__set_scale(scale);
    node_type.back().__set_scalar_type(scalarType);
    type_desc.__set_types(node_type);
    return type_desc;
}

} // namespace doris
