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

#include <string>

#include "runtime/define_primitive_type.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/columns_number.h"
#include "vec/core/types.h"

namespace doris {

namespace vectorized {
class ColumnString;
class ColumnJsonb;
} // namespace vectorized

class DateTimeValue;
class DecimalV2Value;
struct StringValue;
struct JsonBinaryValue;

PrimitiveType convert_type_to_primitive(FunctionContext::Type type);

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
    case TYPE_DECIMAL128:
    case TYPE_BOOLEAN:
    case TYPE_ARRAY:
    case TYPE_HLL:
        return false;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_DATE:
    case TYPE_DATEV2:
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

constexpr bool is_float_or_double(PrimitiveType type) {
    return type == TYPE_FLOAT || type == TYPE_DOUBLE;
}

constexpr bool is_int_or_bool(PrimitiveType type) {
    return type == TYPE_BOOLEAN || type == TYPE_TINYINT || type == TYPE_SMALLINT ||
           type == TYPE_INT || type == TYPE_BIGINT || type == TYPE_LARGEINT;
}

constexpr bool has_variable_type(PrimitiveType type) {
    return type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_OBJECT ||
           type == TYPE_QUANTILE_STATE || type == TYPE_STRING;
}

// Returns the byte size of 'type'  Returns 0 for variable length types.
int get_byte_size(PrimitiveType type);
// Returns the byte size of type when in a tuple
int get_slot_size(PrimitiveType type);

bool is_type_compatible(PrimitiveType lhs, PrimitiveType rhs);

TExprOpcode::type to_in_opcode(PrimitiveType t);
PrimitiveType thrift_to_type(TPrimitiveType::type ttype);
TPrimitiveType::type to_thrift(PrimitiveType ptype);
TColumnType to_tcolumn_type_thrift(TPrimitiveType::type ttype);
std::string type_to_string(PrimitiveType t);
std::string type_to_odbc_string(PrimitiveType t);
TTypeDesc gen_type_desc(const TPrimitiveType::type val);
TTypeDesc gen_type_desc(const TPrimitiveType::type val, const std::string& name);

template <PrimitiveType type>
struct PrimitiveTypeTraits;

template <>
struct PrimitiveTypeTraits<TYPE_BOOLEAN> {
    using CppType = bool;
    using ColumnType = vectorized::ColumnUInt8;
};
template <>
struct PrimitiveTypeTraits<TYPE_TINYINT> {
    using CppType = int8_t;
    using ColumnType = vectorized::ColumnInt8;
};
template <>
struct PrimitiveTypeTraits<TYPE_SMALLINT> {
    using CppType = int16_t;
    using ColumnType = vectorized::ColumnInt16;
};
template <>
struct PrimitiveTypeTraits<TYPE_INT> {
    using CppType = int32_t;
    using ColumnType = vectorized::ColumnInt32;
};
template <>
struct PrimitiveTypeTraits<TYPE_BIGINT> {
    using CppType = int64_t;
    using ColumnType = vectorized::ColumnInt64;
};
template <>
struct PrimitiveTypeTraits<TYPE_FLOAT> {
    using CppType = float;
    using ColumnType = vectorized::ColumnFloat32;
};
template <>
struct PrimitiveTypeTraits<TYPE_TIME> {
    using CppType = double;
    using ColumnType = vectorized::ColumnFloat64;
};
template <>
struct PrimitiveTypeTraits<TYPE_TIMEV2> {
    using CppType = double;
    using ColumnType = vectorized::ColumnFloat64;
};
template <>
struct PrimitiveTypeTraits<TYPE_DOUBLE> {
    using CppType = double;
    using ColumnType = vectorized::ColumnFloat64;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATE> {
    using CppType = doris::DateTimeValue;
    using ColumnType = vectorized::ColumnVector<vectorized::DateTime>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATETIME> {
    using CppType = doris::DateTimeValue;
    using ColumnType = vectorized::ColumnVector<vectorized::DateTime>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATETIMEV2> {
    using CppType = doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>;
    using ColumnType = vectorized::ColumnVector<vectorized::DateTimeV2>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DATEV2> {
    using CppType = doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>;
    using ColumnType = vectorized::ColumnVector<vectorized::DateV2>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMALV2> {
    using CppType = DecimalV2Value;
    using ColumnType = vectorized::ColumnDecimal<vectorized::Decimal128>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL32> {
    using CppType = int32_t;
    using ColumnType = vectorized::ColumnDecimal<vectorized::Decimal32>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL64> {
    using CppType = int64_t;
    using ColumnType = vectorized::ColumnDecimal<vectorized::Decimal64>;
};
template <>
struct PrimitiveTypeTraits<TYPE_DECIMAL128> {
    using CppType = __int128_t;
    using ColumnType = vectorized::ColumnDecimal<vectorized::Decimal128>;
};
template <>
struct PrimitiveTypeTraits<TYPE_LARGEINT> {
    using CppType = __int128_t;
    using ColumnType = vectorized::ColumnInt128;
};
template <>
struct PrimitiveTypeTraits<TYPE_CHAR> {
    using CppType = StringValue;
    using ColumnType = vectorized::ColumnString;
};
template <>
struct PrimitiveTypeTraits<TYPE_VARCHAR> {
    using CppType = StringValue;
    using ColumnType = vectorized::ColumnString;
};

template <>
struct PrimitiveTypeTraits<TYPE_STRING> {
    using CppType = StringValue;
    using ColumnType = vectorized::ColumnString;
};

template <>
struct PrimitiveTypeTraits<TYPE_HLL> {
    using CppType = StringValue;
    using ColumnType = vectorized::ColumnString;
};

template <>
struct PrimitiveTypeTraits<TYPE_JSONB> {
    using CppType = JsonBinaryValue;
    using ColumnType = vectorized::ColumnJsonb;
};

// only for adapt get_predicate_column_ptr
template <PrimitiveType type>
struct PredicatePrimitiveTypeTraits {
    using PredicateFieldType = typename PrimitiveTypeTraits<type>::CppType;
};

template <>
struct PredicatePrimitiveTypeTraits<TYPE_DECIMALV2> {
    using PredicateFieldType = decimal12_t;
};

template <>
struct PredicatePrimitiveTypeTraits<TYPE_DATE> {
    using PredicateFieldType = uint32_t;
};

template <>
struct PredicatePrimitiveTypeTraits<TYPE_DATETIME> {
    using PredicateFieldType = uint64_t;
};

template <>
struct PredicatePrimitiveTypeTraits<TYPE_DATEV2> {
    using PredicateFieldType = uint32_t;
};

template <>
struct PredicatePrimitiveTypeTraits<TYPE_DATETIMEV2> {
    using PredicateFieldType = uint64_t;
};

// used for VInPredicate. VInPredicate should use vectorized data type
template <PrimitiveType type>
struct VecPrimitiveTypeTraits {
    using CppType = typename PrimitiveTypeTraits<type>::CppType;
};

template <>
struct VecPrimitiveTypeTraits<TYPE_DATE> {
    using CppType = vectorized::VecDateTimeValue;
};

template <>
struct VecPrimitiveTypeTraits<TYPE_DATETIME> {
    using CppType = vectorized::VecDateTimeValue;
};

} // namespace doris
