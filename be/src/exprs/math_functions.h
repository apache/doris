// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_SRC_QUERY_EXPRS_MATH_FUNCTIONS_H
#define BDG_PALO_BE_SRC_QUERY_EXPRS_MATH_FUNCTIONS_H

#include <stdint.h>
#include "util/string_parser.hpp"

namespace palo {

class Expr;
struct ExprValue;
class TupleRow;

class MathFunctions {
public:
    static void init();

    static palo_udf::DoubleVal pi(palo_udf::FunctionContext* ctx);
    static palo_udf::DoubleVal e(palo_udf::FunctionContext* ctx);

    static palo_udf::DoubleVal abs(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::FloatVal sign(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& v);

    static palo_udf::DoubleVal sin(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::DoubleVal asin(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::DoubleVal cos(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::DoubleVal acos(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::DoubleVal tan(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::DoubleVal atan(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);

    static palo_udf::BigIntVal ceil(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::BigIntVal floor(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::BigIntVal round(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& v);
    static palo_udf::DoubleVal round_up_to(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& v,
        const palo_udf::IntVal& scale);
    static palo_udf::DoubleVal truncate(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& v, 
        const palo_udf::IntVal& scale);

    static palo_udf::DoubleVal ln(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::DoubleVal log(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& base, 
        const palo_udf::DoubleVal& v);
    static palo_udf::DoubleVal log2(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& v);
    static palo_udf::DoubleVal log10(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::DoubleVal exp(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);

    static palo_udf::DoubleVal radians(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& v);
    static palo_udf::DoubleVal degrees(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& v);

    static palo_udf::DoubleVal sqrt(palo_udf::FunctionContext*, const palo_udf::DoubleVal&);
    static palo_udf::DoubleVal pow(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& base,
        const palo_udf::DoubleVal& exp);

    /// Used for both Rand() and RandSeed()
    static void rand_prepare(
        palo_udf::FunctionContext*, palo_udf::FunctionContext::FunctionStateScope);
    static palo_udf::DoubleVal rand(palo_udf::FunctionContext*);
    static palo_udf::DoubleVal rand_seed(
        palo_udf::FunctionContext*, const palo_udf::BigIntVal& seed);

    static palo_udf::StringVal bin(
        palo_udf::FunctionContext* ctx, const palo_udf::BigIntVal& v);
    static palo_udf::StringVal hex_int(
        palo_udf::FunctionContext* ctx, const palo_udf::BigIntVal& v);
    static palo_udf::StringVal hex_string(
        palo_udf::FunctionContext* ctx, const palo_udf::StringVal& s);
    static palo_udf::StringVal unhex(
        palo_udf::FunctionContext* ctx, const palo_udf::StringVal& s);

    static palo_udf::StringVal conv_int(
        palo_udf::FunctionContext* ctx, const palo_udf::BigIntVal& num,
        const palo_udf::TinyIntVal& src_base, const palo_udf::TinyIntVal& dest_base);
    static palo_udf::StringVal conv_string(
        palo_udf::FunctionContext* ctx, const palo_udf::StringVal& num_str,
        const palo_udf::TinyIntVal& src_base, const palo_udf::TinyIntVal& dest_base);

    static palo_udf::BigIntVal pmod_bigint(
        palo_udf::FunctionContext* ctx, const palo_udf::BigIntVal& a, 
        const palo_udf::BigIntVal& b);
    static palo_udf::DoubleVal pmod_double(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& a, 
        const palo_udf::DoubleVal& b);
    static palo_udf::FloatVal fmod_float(
        palo_udf::FunctionContext*, const palo_udf::FloatVal&, 
        const palo_udf::FloatVal&);
    static palo_udf::DoubleVal fmod_double(
        palo_udf::FunctionContext*, const palo_udf::DoubleVal&, 
        const palo_udf::DoubleVal&);

    static palo_udf::BigIntVal positive_bigint(
        palo_udf::FunctionContext* ctx, const palo_udf::BigIntVal& val);
    static palo_udf::DoubleVal positive_double(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& val);
    static palo_udf::DecimalVal positive_decimal(
        palo_udf::FunctionContext* ctx, const palo_udf::DecimalVal& val);
    static palo_udf::BigIntVal negative_bigint(
        palo_udf::FunctionContext* ctx, const palo_udf::BigIntVal& val);
    static palo_udf::DoubleVal negative_double(
        palo_udf::FunctionContext* ctx, const palo_udf::DoubleVal& val);
    static palo_udf::DecimalVal negative_decimal(
        palo_udf::FunctionContext* ctx, const palo_udf::DecimalVal& val);

    static palo_udf::TinyIntVal least(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::TinyIntVal* args);
    static palo_udf::TinyIntVal greatest(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::TinyIntVal* args);
    static palo_udf::SmallIntVal least(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::SmallIntVal* val);
    static palo_udf::SmallIntVal greatest(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::SmallIntVal* val);
    static palo_udf::IntVal least(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::IntVal* val);
    static palo_udf::IntVal greatest(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::IntVal* val);
    static palo_udf::BigIntVal least(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::BigIntVal* val);
    static palo_udf::BigIntVal greatest(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::BigIntVal* val);
    static palo_udf::LargeIntVal least(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::LargeIntVal* val);
    static palo_udf::LargeIntVal greatest(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::LargeIntVal* val);
    static palo_udf::FloatVal least(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::FloatVal* val);
    static palo_udf::FloatVal greatest(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::FloatVal* val);
    static palo_udf::DoubleVal least(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::DoubleVal* val);
    static palo_udf::DoubleVal greatest(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::DoubleVal* val);
    static palo_udf::StringVal least(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::StringVal* val);
    static palo_udf::StringVal greatest(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::StringVal* val);
    static palo_udf::DateTimeVal least(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::DateTimeVal* val);
    static palo_udf::DateTimeVal greatest(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::DateTimeVal* val);
    static palo_udf::DecimalVal least(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::DecimalVal* val);
    static palo_udf::DecimalVal greatest(
        palo_udf::FunctionContext* ctx, int num_args, const palo_udf::DecimalVal* val);

private:
    static const int32_t MIN_BASE = 2;
    static const int32_t MAX_BASE = 36;
    static const char* _s_alphanumeric_chars;

    // Converts src_num in decimal to dest_base,
    // and fills expr_val.string_val with the result.
    static palo_udf::StringVal decimal_to_base(
        palo_udf::FunctionContext* ctx, int64_t src_num, int8_t dest_base);

    // Converts src_num representing a number in src_base but encoded in decimal
    // into its actual decimal number.
    // For example, if src_num is 21 and src_base is 5,
    // then this function sets *result to 2*5^1 + 1*5^0 = 11.
    // Returns false if overflow occurred, true upon success.
    static bool decimal_in_base_to_decimal(int64_t src_num, int8_t src_base, int64_t* result);

    // Helper function used in Conv to implement behavior consistent
    // with MySQL and Hive in case of numeric overflow during Conv.
    // Inspects parse_res, and in case of overflow sets num to MAXINT64 if dest_base
    // is positive, otherwise to -1.
    // Returns true if no parse_res == PARSE_SUCCESS || parse_res == PARSE_OVERFLOW.
    // Returns false otherwise, indicating some other error condition.
    static bool handle_parse_result(
            int8_t dest_base,
            int64_t* num,
            StringParser::ParseResult parse_res);

};

}

#endif
