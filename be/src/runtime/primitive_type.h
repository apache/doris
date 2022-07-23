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

#include "vec/columns/column_decimal.h"
#include "vec/columns/columns_number.h"
#include "vec/core/types.h"

namespace doris {

namespace vectorized {
class ColumnString;
}

class DateTimeValue;
class DecimalV2Value;
struct StringValue;

enum PrimitiveType {
    INVALID_TYPE = 0,
    TYPE_NULL,     /* 1 */
    TYPE_BOOLEAN,  /* 2 */
    TYPE_TINYINT,  /* 3 */
    TYPE_SMALLINT, /* 4 */
    TYPE_INT,      /* 5 */
    TYPE_BIGINT,   /* 6 */
    TYPE_LARGEINT, /* 7 */
    TYPE_FLOAT,    /* 8 */
    TYPE_DOUBLE,   /* 9 */
    TYPE_VARCHAR,  /* 10 */
    TYPE_DATE,     /* 11 */
    TYPE_DATETIME, /* 12 */
    TYPE_BINARY,
    /* 13 */                     // Not implemented
    TYPE_DECIMAL [[deprecated]], /* 14 */
    TYPE_CHAR,                   /* 15 */

    TYPE_STRUCT,    /* 16 */
    TYPE_ARRAY,     /* 17 */
    TYPE_MAP,       /* 18 */
    TYPE_HLL,       /* 19 */
    TYPE_DECIMALV2, /* 20 */

    TYPE_TIME,           /* 21 */
    TYPE_OBJECT,         /* 22 */
    TYPE_STRING,         /* 23 */
    TYPE_QUANTILE_STATE, /* 24 */
    TYPE_DATEV2,         /* 25 */
    TYPE_DATETIMEV2,     /* 26 */
    TYPE_TIMEV2,         /* 27 */
    TYPE_DECIMAL32,      /* 28 */
    TYPE_DECIMAL64,      /* 29 */
    TYPE_DECIMAL128,     /* 30 */
};

PrimitiveType convert_type_to_primitive(FunctionContext::Type type);

bool is_enumeration_type(PrimitiveType type);
bool is_date_type(PrimitiveType type);
bool is_string_type(PrimitiveType type);
bool has_variable_type(PrimitiveType type);

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
    using PredicateFieldType = uint24_t;
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
