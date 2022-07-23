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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/cast-functions.h
// and modified by Doris

#pragma once

#include "udf/udf.h"

namespace doris {

class CastFunctions {
public:
    static void init();

    static BooleanVal cast_to_boolean_val(FunctionContext* context, const TinyIntVal& val);
    static BooleanVal cast_to_boolean_val(FunctionContext* context, const SmallIntVal& val);
    static BooleanVal cast_to_boolean_val(FunctionContext* context, const IntVal& val);
    static BooleanVal cast_to_boolean_val(FunctionContext* context, const BigIntVal& val);
    static BooleanVal cast_to_boolean_val(FunctionContext* context, const LargeIntVal& val);
    static BooleanVal cast_to_boolean_val(FunctionContext* context, const FloatVal& val);
    static BooleanVal cast_to_boolean_val(FunctionContext* context, const DoubleVal& val);
    static BooleanVal cast_to_boolean_val(FunctionContext* context, const StringVal& val);
    static BooleanVal cast_to_boolean_val(FunctionContext* context, const DateTimeVal& val);

    static TinyIntVal cast_to_tiny_int_val(FunctionContext* context, const BooleanVal& val);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext* context, const SmallIntVal& val);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext* context, const IntVal& val);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext* context, const BigIntVal& val);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext* context, const LargeIntVal& val);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext* context, const FloatVal& val);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext* context, const DoubleVal& val);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext* context, const StringVal& val);
    static TinyIntVal cast_to_tiny_int_val(FunctionContext* context, const DateTimeVal& val);

    static SmallIntVal cast_to_small_int_val(FunctionContext* context, const BooleanVal& val);
    static SmallIntVal cast_to_small_int_val(FunctionContext* context, const TinyIntVal& val);
    static SmallIntVal cast_to_small_int_val(FunctionContext* context, const IntVal& val);
    static SmallIntVal cast_to_small_int_val(FunctionContext* context, const BigIntVal& val);
    static SmallIntVal cast_to_small_int_val(FunctionContext* context, const LargeIntVal& val);
    static SmallIntVal cast_to_small_int_val(FunctionContext* context, const FloatVal& val);
    static SmallIntVal cast_to_small_int_val(FunctionContext* context, const DoubleVal& val);
    static SmallIntVal cast_to_small_int_val(FunctionContext* context, const StringVal& val);
    static SmallIntVal cast_to_small_int_val(FunctionContext* context, const DateTimeVal& val);

    static IntVal cast_to_int_val(FunctionContext* context, const BooleanVal& val);
    static IntVal cast_to_int_val(FunctionContext* context, const TinyIntVal& val);
    static IntVal cast_to_int_val(FunctionContext* context, const SmallIntVal& val);
    static IntVal cast_to_int_val(FunctionContext* context, const BigIntVal& val);
    static IntVal cast_to_int_val(FunctionContext* context, const LargeIntVal& val);
    static IntVal cast_to_int_val(FunctionContext* context, const FloatVal& val);
    static IntVal cast_to_int_val(FunctionContext* context, const DoubleVal& val);
    static IntVal cast_to_int_val(FunctionContext* context, const StringVal& val);
    static IntVal cast_to_int_val(FunctionContext* context, const DateTimeVal& val);

    static BigIntVal cast_to_big_int_val(FunctionContext* context, const BooleanVal& val);
    static BigIntVal cast_to_big_int_val(FunctionContext* context, const TinyIntVal& val);
    static BigIntVal cast_to_big_int_val(FunctionContext* context, const SmallIntVal& val);
    static BigIntVal cast_to_big_int_val(FunctionContext* context, const IntVal& val);
    static BigIntVal cast_to_big_int_val(FunctionContext* context, const LargeIntVal& val);
    static BigIntVal cast_to_big_int_val(FunctionContext* context, const FloatVal& val);
    static BigIntVal cast_to_big_int_val(FunctionContext* context, const DoubleVal& val);
    static BigIntVal cast_to_big_int_val(FunctionContext* context, const StringVal& val);
    static BigIntVal cast_to_big_int_val(FunctionContext* context, const DateTimeVal& val);

    static LargeIntVal cast_to_large_int_val(FunctionContext* context, const BooleanVal& val);
    static LargeIntVal cast_to_large_int_val(FunctionContext* context, const TinyIntVal& val);
    static LargeIntVal cast_to_large_int_val(FunctionContext* context, const SmallIntVal& val);
    static LargeIntVal cast_to_large_int_val(FunctionContext* context, const IntVal& val);
    static LargeIntVal cast_to_large_int_val(FunctionContext* context, const BigIntVal& val);
    static LargeIntVal cast_to_large_int_val(FunctionContext* context, const FloatVal& val);
    static LargeIntVal cast_to_large_int_val(FunctionContext* context, const DoubleVal& val);
    static LargeIntVal cast_to_large_int_val(FunctionContext* context, const StringVal& val);
    static LargeIntVal cast_to_large_int_val(FunctionContext* context, const DateTimeVal& val);

    static FloatVal cast_to_float_val(FunctionContext* context, const BooleanVal& val);
    static FloatVal cast_to_float_val(FunctionContext* context, const TinyIntVal& val);
    static FloatVal cast_to_float_val(FunctionContext* context, const SmallIntVal& val);
    static FloatVal cast_to_float_val(FunctionContext* context, const IntVal& val);
    static FloatVal cast_to_float_val(FunctionContext* context, const BigIntVal& val);
    static FloatVal cast_to_float_val(FunctionContext* context, const LargeIntVal& val);
    static FloatVal cast_to_float_val(FunctionContext* context, const DoubleVal& val);
    static FloatVal cast_to_float_val(FunctionContext* context, const StringVal& val);
    static FloatVal cast_to_float_val(FunctionContext* context, const DateTimeVal& val);

    static DoubleVal cast_to_double_val(FunctionContext* context, const BooleanVal& val);
    static DoubleVal cast_to_double_val(FunctionContext* context, const TinyIntVal& val);
    static DoubleVal cast_to_double_val(FunctionContext* context, const SmallIntVal& val);
    static DoubleVal cast_to_double_val(FunctionContext* context, const IntVal& val);
    static DoubleVal cast_to_double_val(FunctionContext* context, const BigIntVal& val);
    static DoubleVal cast_to_double_val(FunctionContext* context, const LargeIntVal& val);
    static DoubleVal cast_to_double_val(FunctionContext* context, const FloatVal& val);
    static DoubleVal cast_to_double_val(FunctionContext* context, const StringVal& val);
    static DoubleVal cast_to_double_val(FunctionContext* context, const DateTimeVal& val);

    static StringVal cast_to_string_val(FunctionContext* context, const BooleanVal& val);
    static StringVal cast_to_string_val(FunctionContext* context, const TinyIntVal& val);
    static StringVal cast_to_string_val(FunctionContext* context, const SmallIntVal& val);
    static StringVal cast_to_string_val(FunctionContext* context, const IntVal& val);
    static StringVal cast_to_string_val(FunctionContext* context, const BigIntVal& val);
    static StringVal cast_to_string_val(FunctionContext* context, const LargeIntVal& val);
    static StringVal cast_to_string_val(FunctionContext* context, const FloatVal& val);
    static StringVal cast_to_string_val(FunctionContext* context, const DoubleVal& val);
    static StringVal cast_to_string_val(FunctionContext* context, const DateTimeVal& val);
    static StringVal cast_to_string_val(FunctionContext* context, const StringVal& val);

    static DateTimeVal cast_to_datetime_val(FunctionContext* context, const TinyIntVal& val);
    static DateTimeVal cast_to_datetime_val(FunctionContext* context, const SmallIntVal& val);
    static DateTimeVal cast_to_datetime_val(FunctionContext* context, const IntVal& val);
    static DateTimeVal cast_to_datetime_val(FunctionContext* context, const BigIntVal& val);
    static DateTimeVal cast_to_datetime_val(FunctionContext* context, const LargeIntVal& val);
    static DateTimeVal cast_to_datetime_val(FunctionContext* context, const FloatVal& val);
    static DateTimeVal cast_to_datetime_val(FunctionContext* context, const DoubleVal& val);
    static DateTimeVal cast_to_datetime_val(FunctionContext* context, const DateTimeVal& val);
    static DateTimeVal cast_to_datetime_val(FunctionContext* context, const StringVal& val);
    static DateTimeVal cast_to_datetime_val(FunctionContext* context,
                                            const doris_udf::DateV2Val& val);
    static DateTimeVal cast_to_datetime_val(FunctionContext* context,
                                            const doris_udf::DateTimeV2Val& val);

    static DateTimeVal cast_to_date_val(FunctionContext* context, const TinyIntVal& val);
    static DateTimeVal cast_to_date_val(FunctionContext* context, const SmallIntVal& val);
    static DateTimeVal cast_to_date_val(FunctionContext* context, const IntVal& val);
    static DateTimeVal cast_to_date_val(FunctionContext* context, const BigIntVal& val);
    static DateTimeVal cast_to_date_val(FunctionContext* context, const LargeIntVal& val);
    static DateTimeVal cast_to_date_val(FunctionContext* context, const FloatVal& val);
    static DateTimeVal cast_to_date_val(FunctionContext* context, const DoubleVal& val);
    static DateTimeVal cast_to_date_val(FunctionContext* context, const DateTimeVal& val);
    static DateTimeVal cast_to_date_val(FunctionContext* context, const StringVal& val);
    static DateTimeVal cast_to_date_val(FunctionContext* context, const doris_udf::DateV2Val& val);
    static DateTimeVal cast_to_date_val(FunctionContext* context,
                                        const doris_udf::DateTimeV2Val& val);

    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context, const TinyIntVal& val);
    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context,
                                                   const SmallIntVal& val);
    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context, const IntVal& val);
    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context, const BigIntVal& val);
    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context,
                                                   const LargeIntVal& val);
    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context, const FloatVal& val);
    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context, const DoubleVal& val);
    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context,
                                                   const doris_udf::DateV2Val& val);
    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context, const StringVal& val);
    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context,
                                                   const DateTimeVal& val);
    static doris_udf::DateV2Val cast_to_datev2_val(FunctionContext* context,
                                                   const doris_udf::DateTimeV2Val& val);

    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const TinyIntVal& val);
    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const SmallIntVal& val);
    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const IntVal& val);
    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const BigIntVal& val);
    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const LargeIntVal& val);
    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const FloatVal& val);
    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const DoubleVal& val);
    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const doris_udf::DateV2Val& val);
    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const StringVal& val);
    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const DateTimeVal& val);
    static doris_udf::DateTimeV2Val cast_to_datetimev2_val(FunctionContext* context,
                                                           const doris_udf::DateTimeV2Val& val);

#define DECLARE_CAST_TO_DECIMAL(width)                                                             \
    static Decimal##width##Val cast_to_decimal##width##_val(FunctionContext*, const TinyIntVal&);  \
    static Decimal##width##Val cast_to_decimal##width##_val(FunctionContext*, const SmallIntVal&); \
    static Decimal##width##Val cast_to_decimal##width##_val(FunctionContext*, const IntVal&);      \
    static Decimal##width##Val cast_to_decimal##width##_val(FunctionContext*, const BigIntVal&);   \
    static Decimal##width##Val cast_to_decimal##width##_val(FunctionContext*, const LargeIntVal&); \
    static Decimal##width##Val cast_to_decimal##width##_val(FunctionContext*, const FloatVal&);    \
    static Decimal##width##Val cast_to_decimal##width##_val(FunctionContext*, const DoubleVal&);   \
    static Decimal##width##Val cast_to_decimal##width##_val(FunctionContext*, const DateTimeVal&); \
    static Decimal##width##Val cast_to_decimal##width##_val(FunctionContext*, const StringVal&);   \
    static Decimal##width##Val cast_to_decimal##width##_val(FunctionContext*,                      \
                                                            const Decimal##width##Val&);

    DECLARE_CAST_TO_DECIMAL(32)
    DECLARE_CAST_TO_DECIMAL(64)
    DECLARE_CAST_TO_DECIMAL(128)

    static Decimal64Val cast_to_decimal64_val(FunctionContext*, const Decimal32Val&);
    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const Decimal32Val&);
    static Decimal128Val cast_to_decimal128_val(FunctionContext*, const Decimal64Val&);

    static CollectionVal cast_to_array_val(FunctionContext* context, const StringVal& val);
};

} // namespace doris
