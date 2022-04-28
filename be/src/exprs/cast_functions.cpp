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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/cast-functions.cpp
// and modified by Doris

#include "exprs/cast_functions.h"

#include <fmt/format.h>

#include <cmath>

#include "exprs/anyval_util.h"
#include "gutil/strings/numbers.h"
#include "runtime/datetime_value.h"
#include "runtime/string_value.h"
#include "string_functions.h"
#include "util/array_parser.h"
#include "util/mysql_global.h"
#include "util/string_parser.hpp"

namespace doris {

void CastFunctions::init() {}

// The maximum number of characters need to represent a floating-point number (float or
// double) as a string. 24 = 17 (maximum significant digits) + 1 (decimal point) + 1 ('E')
// + 3 (exponent digits) + 2 (negative signs) (see http://stackoverflow.com/a/1701085)
const int MAX_FLOAT_CHARS = 24;

#define CAST_FUNCTION(from_type, to_type, type_name)                                         \
    to_type CastFunctions::cast_to_##type_name(FunctionContext* ctx, const from_type& val) { \
        if (val.is_null) return to_type::null();                                             \
        return to_type(val.val);                                                             \
    }

CAST_FUNCTION(TinyIntVal, BooleanVal, boolean_val)
CAST_FUNCTION(SmallIntVal, BooleanVal, boolean_val)
CAST_FUNCTION(IntVal, BooleanVal, boolean_val)
CAST_FUNCTION(BigIntVal, BooleanVal, boolean_val)
CAST_FUNCTION(LargeIntVal, BooleanVal, boolean_val)
CAST_FUNCTION(FloatVal, BooleanVal, boolean_val)
CAST_FUNCTION(DoubleVal, BooleanVal, boolean_val)

CAST_FUNCTION(BooleanVal, TinyIntVal, tiny_int_val)
CAST_FUNCTION(SmallIntVal, TinyIntVal, tiny_int_val)
CAST_FUNCTION(IntVal, TinyIntVal, tiny_int_val)
CAST_FUNCTION(BigIntVal, TinyIntVal, tiny_int_val)
CAST_FUNCTION(LargeIntVal, TinyIntVal, tiny_int_val)
CAST_FUNCTION(FloatVal, TinyIntVal, tiny_int_val)
CAST_FUNCTION(DoubleVal, TinyIntVal, tiny_int_val)

CAST_FUNCTION(BooleanVal, SmallIntVal, small_int_val)
CAST_FUNCTION(TinyIntVal, SmallIntVal, small_int_val)
CAST_FUNCTION(IntVal, SmallIntVal, small_int_val)
CAST_FUNCTION(BigIntVal, SmallIntVal, small_int_val)
CAST_FUNCTION(LargeIntVal, SmallIntVal, small_int_val)
CAST_FUNCTION(FloatVal, SmallIntVal, small_int_val)
CAST_FUNCTION(DoubleVal, SmallIntVal, small_int_val)

CAST_FUNCTION(BooleanVal, IntVal, int_val)
CAST_FUNCTION(TinyIntVal, IntVal, int_val)
CAST_FUNCTION(SmallIntVal, IntVal, int_val)
CAST_FUNCTION(BigIntVal, IntVal, int_val)
CAST_FUNCTION(LargeIntVal, IntVal, int_val)
CAST_FUNCTION(FloatVal, IntVal, int_val)
CAST_FUNCTION(DoubleVal, IntVal, int_val)

CAST_FUNCTION(BooleanVal, BigIntVal, big_int_val)
CAST_FUNCTION(TinyIntVal, BigIntVal, big_int_val)
CAST_FUNCTION(SmallIntVal, BigIntVal, big_int_val)
CAST_FUNCTION(IntVal, BigIntVal, big_int_val)
CAST_FUNCTION(LargeIntVal, BigIntVal, big_int_val)
CAST_FUNCTION(FloatVal, BigIntVal, big_int_val)
CAST_FUNCTION(DoubleVal, BigIntVal, big_int_val)

CAST_FUNCTION(BooleanVal, LargeIntVal, large_int_val)
CAST_FUNCTION(TinyIntVal, LargeIntVal, large_int_val)
CAST_FUNCTION(SmallIntVal, LargeIntVal, large_int_val)
CAST_FUNCTION(IntVal, LargeIntVal, large_int_val)
CAST_FUNCTION(BigIntVal, LargeIntVal, large_int_val)
CAST_FUNCTION(FloatVal, LargeIntVal, large_int_val)
CAST_FUNCTION(DoubleVal, LargeIntVal, large_int_val)

CAST_FUNCTION(BooleanVal, FloatVal, float_val)
CAST_FUNCTION(TinyIntVal, FloatVal, float_val)
CAST_FUNCTION(SmallIntVal, FloatVal, float_val)
CAST_FUNCTION(IntVal, FloatVal, float_val)
CAST_FUNCTION(BigIntVal, FloatVal, float_val)
CAST_FUNCTION(LargeIntVal, FloatVal, float_val)
CAST_FUNCTION(DoubleVal, FloatVal, float_val)

CAST_FUNCTION(BooleanVal, DoubleVal, double_val)
CAST_FUNCTION(TinyIntVal, DoubleVal, double_val)
CAST_FUNCTION(SmallIntVal, DoubleVal, double_val)
CAST_FUNCTION(IntVal, DoubleVal, double_val)
CAST_FUNCTION(BigIntVal, DoubleVal, double_val)
CAST_FUNCTION(LargeIntVal, DoubleVal, double_val)
CAST_FUNCTION(FloatVal, DoubleVal, double_val)

#define CAST_FROM_STRING(num_type, type_name, native_type, string_parser_fn)                    \
    num_type CastFunctions::cast_to_##type_name(FunctionContext* ctx, const StringVal& val) {   \
        if (val.is_null) return num_type::null();                                               \
        StringParser::ParseResult result;                                                       \
        num_type ret;                                                                           \
        ret.val = StringParser::string_parser_fn<native_type>(reinterpret_cast<char*>(val.ptr), \
                                                              val.len, &result);                \
        if (UNLIKELY(result != StringParser::PARSE_SUCCESS || std::isnan(ret.val) ||            \
                     std::isinf(ret.val))) {                                                    \
            return num_type::null();                                                            \
        }                                                                                       \
        return ret;                                                                             \
    }

#define CAST_FROM_STRINGS()                                                \
    CAST_FROM_STRING(TinyIntVal, tiny_int_val, int8_t, string_to_int);     \
    CAST_FROM_STRING(SmallIntVal, small_int_val, int16_t, string_to_int);  \
    CAST_FROM_STRING(IntVal, int_val, int32_t, string_to_int);             \
    CAST_FROM_STRING(BigIntVal, big_int_val, int64_t, string_to_int);      \
    CAST_FROM_STRING(LargeIntVal, large_int_val, __int128, string_to_int); \
    CAST_FROM_STRING(FloatVal, float_val, float, string_to_float);         \
    CAST_FROM_STRING(DoubleVal, double_val, double, string_to_float);

CAST_FROM_STRINGS();

#define CAST_TO_STRING(num_type)                                                             \
    StringVal CastFunctions::cast_to_string_val(FunctionContext* ctx, const num_type& val) { \
        if (val.is_null) return StringVal::null();                                           \
        auto f = fmt::format_int(val.val);                                                   \
        return AnyValUtil::from_buffer_temp(ctx, f.data(), f.size());                        \
    }

CAST_TO_STRING(BooleanVal);
CAST_TO_STRING(TinyIntVal);
CAST_TO_STRING(SmallIntVal);
CAST_TO_STRING(IntVal);
CAST_TO_STRING(BigIntVal);

StringVal CastFunctions::cast_to_string_val(FunctionContext* ctx, const LargeIntVal& val) {
    if (val.is_null) {
        return StringVal::null();
    }

    auto string_value = LargeIntValue::to_string(val.val);
    return AnyValUtil::from_buffer_temp(ctx, string_value.data(), string_value.size());
}

template <typename T>
int float_to_string(T value, char* buf);

template <>
int float_to_string<float>(float value, char* buf) {
    return FloatToBuffer(value, MAX_FLOAT_STR_LENGTH + 2, buf);
}

template <>
int float_to_string<double>(double value, char* buf) {
    return DoubleToBuffer(value, MAX_DOUBLE_STR_LENGTH + 2, buf);
}

#define CAST_FLOAT_TO_STRING(float_type, format)                                               \
    StringVal CastFunctions::cast_to_string_val(FunctionContext* ctx, const float_type& val) { \
        if (val.is_null) return StringVal::null();                                             \
        /* val.val could be -nan, return "nan" instead */                                      \
        if (std::isnan(val.val)) return StringVal("nan");                                      \
        /* Add 1 to MAX_FLOAT_CHARS since snprintf adds a trailing '\0' */                     \
        StringVal sv(ctx, MAX_DOUBLE_STR_LENGTH + 2);                                          \
        if (UNLIKELY(sv.is_null)) {                                                            \
            return sv;                                                                         \
        }                                                                                      \
        const FunctionContext::TypeDesc& returnType = ctx->get_return_type();                  \
        if (returnType.len > 0) {                                                              \
            sv.len = snprintf(reinterpret_cast<char*>(sv.ptr), sv.len, format, val.val);       \
            DCHECK_GT(sv.len, 0);                                                              \
            DCHECK_LE(sv.len, MAX_FLOAT_CHARS);                                                \
            AnyValUtil::TruncateIfNecessary(returnType, &sv);                                  \
        } else if (returnType.len == -1) {                                                     \
            char buf[MAX_DOUBLE_STR_LENGTH + 2];                                               \
            sv.len = float_to_string(val.val, buf);                                            \
            memcpy(sv.ptr, buf, sv.len);                                                       \
        } else {                                                                               \
            DCHECK(false);                                                                     \
        }                                                                                      \
        return sv;                                                                             \
    }

// Floats have up to 9 significant digits, doubles up to 17
// (see http://en.wikipedia.org/wiki/Single-precision_floating-point_format
// and http://en.wikipedia.org/wiki/Double-precision_floating-point_format)
CAST_FLOAT_TO_STRING(FloatVal, "%.9g");
CAST_FLOAT_TO_STRING(DoubleVal, "%.17g");

StringVal CastFunctions::cast_to_string_val(FunctionContext* ctx, const DateTimeVal& val) {
    if (val.is_null) {
        return StringVal::null();
    }
    DateTimeValue tv = DateTimeValue::from_datetime_val(val);
    StringVal sv = StringVal::create_temp_string_val(ctx, 64);
    sv.len = tv.to_string((char*)sv.ptr) - (char*)sv.ptr - 1;
    return sv;
}

StringVal CastFunctions::cast_to_string_val(FunctionContext* ctx, const StringVal& val) {
    if (val.is_null) return StringVal::null();
    StringVal sv;
    sv.ptr = val.ptr;
    sv.len = val.len;

    const FunctionContext::TypeDesc& result_type = ctx->get_return_type();
    if (result_type.len > 0) {
        AnyValUtil::TruncateIfNecessary(result_type, &sv);
    }
    return sv;
}

BooleanVal CastFunctions::cast_to_boolean_val(FunctionContext* ctx, const StringVal& val) {
    if (val.is_null) {
        return BooleanVal::null();
    }
    StringParser::ParseResult result;
    BooleanVal ret;
    IntVal int_val = cast_to_int_val(ctx, val);
    if (!int_val.is_null && int_val.val == 0) {
        ret.val = false;
    } else if (!int_val.is_null && int_val.val == 1) {
        ret.val = true;
    } else {
        ret.val = StringParser::string_to_bool(reinterpret_cast<char*>(val.ptr), val.len, &result);
        if (UNLIKELY(result != StringParser::PARSE_SUCCESS)) {
            return BooleanVal::null();
        }
    }
    return ret;
}

#define CAST_FROM_DATETIME(to_type, type_name)                                                 \
    to_type CastFunctions::cast_to_##type_name(FunctionContext* ctx, const DateTimeVal& val) { \
        if (val.is_null) return to_type::null();                                               \
        DateTimeValue tv = DateTimeValue::from_datetime_val(val);                              \
        return to_type(tv.to_int64());                                                         \
    }

CAST_FROM_DATETIME(BooleanVal, boolean_val);
CAST_FROM_DATETIME(TinyIntVal, tiny_int_val);
CAST_FROM_DATETIME(SmallIntVal, small_int_val);
CAST_FROM_DATETIME(IntVal, int_val);
CAST_FROM_DATETIME(BigIntVal, big_int_val);
CAST_FROM_DATETIME(LargeIntVal, large_int_val);
CAST_FROM_DATETIME(FloatVal, float_val);
CAST_FROM_DATETIME(DoubleVal, double_val);

#define CAST_TO_DATETIME(from_type)                                                               \
    DateTimeVal CastFunctions::cast_to_datetime_val(FunctionContext* ctx, const from_type& val) { \
        if (val.is_null) return DateTimeVal::null();                                              \
        DateTimeValue date_value;                                                                 \
        if (!date_value.from_date_int64(val.val)) return DateTimeVal::null();                     \
        date_value.to_datetime();                                                                 \
        DateTimeVal result;                                                                       \
        date_value.to_datetime_val(&result);                                                      \
        return result;                                                                            \
    }

#define CAST_TO_DATETIMES()        \
    CAST_TO_DATETIME(TinyIntVal);  \
    CAST_TO_DATETIME(SmallIntVal); \
    CAST_TO_DATETIME(IntVal);      \
    CAST_TO_DATETIME(BigIntVal);   \
    CAST_TO_DATETIME(LargeIntVal); \
    CAST_TO_DATETIME(FloatVal);    \
    CAST_TO_DATETIME(DoubleVal);

CAST_TO_DATETIMES();

#define CAST_TO_DATE(from_type)                                                               \
    DateTimeVal CastFunctions::cast_to_date_val(FunctionContext* ctx, const from_type& val) { \
        if (val.is_null) return DateTimeVal::null();                                          \
        DateTimeValue date_value;                                                             \
        if (!date_value.from_date_int64(val.val)) return DateTimeVal::null();                 \
        date_value.cast_to_date();                                                            \
        DateTimeVal result;                                                                   \
        date_value.to_datetime_val(&result);                                                  \
        return result;                                                                        \
    }

#define CAST_TO_DATES()        \
    CAST_TO_DATE(TinyIntVal);  \
    CAST_TO_DATE(SmallIntVal); \
    CAST_TO_DATE(IntVal);      \
    CAST_TO_DATE(BigIntVal);   \
    CAST_TO_DATE(LargeIntVal); \
    CAST_TO_DATE(FloatVal);    \
    CAST_TO_DATE(DoubleVal);

CAST_TO_DATES();

DateTimeVal CastFunctions::cast_to_datetime_val(FunctionContext* ctx, const DateTimeVal& val) {
    if (val.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue date_value = DateTimeValue::from_datetime_val(val);
    date_value.to_datetime();
    // Return null if 'val' did not parse
    DateTimeVal result;
    date_value.to_datetime_val(&result);
    return result;
}

DateTimeVal CastFunctions::cast_to_datetime_val(FunctionContext* ctx, const StringVal& val) {
    if (val.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue date_value;
    if (!date_value.from_date_str((char*)val.ptr, val.len)) {
        return DateTimeVal::null();
    }
    date_value.to_datetime();
    // Return null if 'val' did not parse
    DateTimeVal result;
    date_value.to_datetime_val(&result);
    return result;
}

DateTimeVal CastFunctions::cast_to_date_val(FunctionContext* ctx, const DateTimeVal& val) {
    if (val.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue date_value = DateTimeValue::from_datetime_val(val);
    date_value.cast_to_date();
    // Return null if 'val' did not parse
    DateTimeVal result;
    date_value.to_datetime_val(&result);
    return result;
}

DateTimeVal CastFunctions::cast_to_date_val(FunctionContext* ctx, const StringVal& val) {
    if (val.is_null) {
        return DateTimeVal::null();
    }
    DateTimeValue date_value;
    if (!date_value.from_date_str((char*)val.ptr, val.len)) {
        return DateTimeVal::null();
    }
    date_value.cast_to_date();
    // Return null if 'val' did not parse
    DateTimeVal result;
    date_value.to_datetime_val(&result);
    return result;
}

CollectionVal CastFunctions::cast_to_array_val(FunctionContext* context, const StringVal& val) {
    CollectionVal array_val;
    Status status = ArrayParser::parse(array_val, context, val);
    return status.ok() ? array_val : CollectionVal::null();
}

} // namespace doris
