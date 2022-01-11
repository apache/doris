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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_MATH_FUNCTIONS_H
#define DORIS_BE_SRC_QUERY_EXPRS_MATH_FUNCTIONS_H

#include <stdint.h>

#include "util/string_parser.hpp"

namespace doris {

class Expr;
struct ExprValue;
class TupleRow;

class MathFunctions {
public:
    static void init();

    static doris_udf::DoubleVal pi(doris_udf::FunctionContext* ctx);
    static doris_udf::DoubleVal e(doris_udf::FunctionContext* ctx);

    static doris_udf::DoubleVal abs(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::FloatVal abs(doris_udf::FunctionContext*, const doris_udf::FloatVal&);
    static doris_udf::DecimalV2Val abs(doris_udf::FunctionContext*, const doris_udf::DecimalV2Val&);

    // For integer math, we have to promote ABS() to the next highest integer type because
    // in two's complement arithmetic, the largest negative value for any bit width is not
    // representable as a positive value within the same width.  For the largest width, we
    // simply overflow.  In the unlikely event a workaround is needed, one can simply cast
    // to a higher precision decimal type.
    static doris_udf::LargeIntVal abs(doris_udf::FunctionContext*, const doris_udf::LargeIntVal&);
    static doris_udf::LargeIntVal abs(doris_udf::FunctionContext*, const doris_udf::BigIntVal&);
    static doris_udf::BigIntVal abs(doris_udf::FunctionContext*, const doris_udf::IntVal&);
    static doris_udf::IntVal abs(doris_udf::FunctionContext*, const doris_udf::SmallIntVal&);
    static doris_udf::SmallIntVal abs(doris_udf::FunctionContext*, const doris_udf::TinyIntVal&);

    static doris_udf::TinyIntVal sign(doris_udf::FunctionContext* ctx,
                                      const doris_udf::DoubleVal& v);

    static doris_udf::DoubleVal sin(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::DoubleVal asin(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::DoubleVal cos(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::DoubleVal acos(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::DoubleVal tan(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::DoubleVal atan(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);

    static doris_udf::BigIntVal ceil(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::BigIntVal floor(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::BigIntVal round(doris_udf::FunctionContext* ctx,
                                      const doris_udf::DoubleVal& v);
    static doris_udf::DoubleVal round_up_to(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DoubleVal& v,
                                            const doris_udf::IntVal& scale);
    static doris_udf::DoubleVal truncate(doris_udf::FunctionContext* ctx,
                                         const doris_udf::DoubleVal& v,
                                         const doris_udf::IntVal& scale);

    static doris_udf::DoubleVal ln(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::DoubleVal log(doris_udf::FunctionContext* ctx,
                                    const doris_udf::DoubleVal& base,
                                    const doris_udf::DoubleVal& v);
    static doris_udf::DoubleVal log2(doris_udf::FunctionContext* ctx,
                                     const doris_udf::DoubleVal& v);
    static doris_udf::DoubleVal log10(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::DoubleVal exp(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);

    static doris_udf::DoubleVal radians(doris_udf::FunctionContext* ctx,
                                        const doris_udf::DoubleVal& v);
    static doris_udf::DoubleVal degrees(doris_udf::FunctionContext* ctx,
                                        const doris_udf::DoubleVal& v);

    static doris_udf::DoubleVal sqrt(doris_udf::FunctionContext*, const doris_udf::DoubleVal&);
    static doris_udf::DoubleVal pow(doris_udf::FunctionContext* ctx,
                                    const doris_udf::DoubleVal& base,
                                    const doris_udf::DoubleVal& exp);

    /// Used for both rand() and rand_seed()
    static void rand_prepare(doris_udf::FunctionContext*,
                             doris_udf::FunctionContext::FunctionStateScope);
    static doris_udf::DoubleVal rand(doris_udf::FunctionContext*);
    static doris_udf::DoubleVal rand_seed(doris_udf::FunctionContext*,
                                          const doris_udf::BigIntVal& seed);
    static void rand_close(FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

    static doris_udf::StringVal bin(doris_udf::FunctionContext* ctx, const doris_udf::BigIntVal& v);
    static doris_udf::StringVal hex_int(doris_udf::FunctionContext* ctx,
                                        const doris_udf::BigIntVal& v);
    static doris_udf::StringVal hex_string(doris_udf::FunctionContext* ctx,
                                           const doris_udf::StringVal& s);
    static doris_udf::StringVal unhex(doris_udf::FunctionContext* ctx,
                                      const doris_udf::StringVal& s);

    static doris_udf::StringVal conv_int(doris_udf::FunctionContext* ctx,
                                         const doris_udf::BigIntVal& num,
                                         const doris_udf::TinyIntVal& src_base,
                                         const doris_udf::TinyIntVal& dest_base);
    static doris_udf::StringVal conv_string(doris_udf::FunctionContext* ctx,
                                            const doris_udf::StringVal& num_str,
                                            const doris_udf::TinyIntVal& src_base,
                                            const doris_udf::TinyIntVal& dest_base);

    static doris_udf::BigIntVal pmod_bigint(doris_udf::FunctionContext* ctx,
                                            const doris_udf::BigIntVal& a,
                                            const doris_udf::BigIntVal& b);
    static doris_udf::DoubleVal pmod_double(doris_udf::FunctionContext* ctx,
                                            const doris_udf::DoubleVal& a,
                                            const doris_udf::DoubleVal& b);
    static doris_udf::FloatVal fmod_float(doris_udf::FunctionContext*, const doris_udf::FloatVal&,
                                          const doris_udf::FloatVal&);
    static doris_udf::DoubleVal fmod_double(doris_udf::FunctionContext*,
                                            const doris_udf::DoubleVal&,
                                            const doris_udf::DoubleVal&);

    static doris_udf::BigIntVal positive_bigint(doris_udf::FunctionContext* ctx,
                                                const doris_udf::BigIntVal& val);
    static doris_udf::DoubleVal positive_double(doris_udf::FunctionContext* ctx,
                                                const doris_udf::DoubleVal& val);
    static doris_udf::DecimalV2Val positive_decimal(doris_udf::FunctionContext* ctx,
                                                    const doris_udf::DecimalV2Val& val);
    static doris_udf::BigIntVal negative_bigint(doris_udf::FunctionContext* ctx,
                                                const doris_udf::BigIntVal& val);
    static doris_udf::DoubleVal negative_double(doris_udf::FunctionContext* ctx,
                                                const doris_udf::DoubleVal& val);
    static doris_udf::DecimalV2Val negative_decimal(doris_udf::FunctionContext* ctx,
                                                    const doris_udf::DecimalV2Val& val);

    static doris_udf::TinyIntVal least(doris_udf::FunctionContext* ctx, int num_args,
                                       const doris_udf::TinyIntVal* args);
    static doris_udf::TinyIntVal greatest(doris_udf::FunctionContext* ctx, int num_args,
                                          const doris_udf::TinyIntVal* args);
    static doris_udf::SmallIntVal least(doris_udf::FunctionContext* ctx, int num_args,
                                        const doris_udf::SmallIntVal* val);
    static doris_udf::SmallIntVal greatest(doris_udf::FunctionContext* ctx, int num_args,
                                           const doris_udf::SmallIntVal* val);
    static doris_udf::IntVal least(doris_udf::FunctionContext* ctx, int num_args,
                                   const doris_udf::IntVal* val);
    static doris_udf::IntVal greatest(doris_udf::FunctionContext* ctx, int num_args,
                                      const doris_udf::IntVal* val);
    static doris_udf::BigIntVal least(doris_udf::FunctionContext* ctx, int num_args,
                                      const doris_udf::BigIntVal* val);
    static doris_udf::BigIntVal greatest(doris_udf::FunctionContext* ctx, int num_args,
                                         const doris_udf::BigIntVal* val);
    static doris_udf::LargeIntVal least(doris_udf::FunctionContext* ctx, int num_args,
                                        const doris_udf::LargeIntVal* val);
    static doris_udf::LargeIntVal greatest(doris_udf::FunctionContext* ctx, int num_args,
                                           const doris_udf::LargeIntVal* val);
    static doris_udf::FloatVal least(doris_udf::FunctionContext* ctx, int num_args,
                                     const doris_udf::FloatVal* val);
    static doris_udf::FloatVal greatest(doris_udf::FunctionContext* ctx, int num_args,
                                        const doris_udf::FloatVal* val);
    static doris_udf::DoubleVal least(doris_udf::FunctionContext* ctx, int num_args,
                                      const doris_udf::DoubleVal* val);
    static doris_udf::DoubleVal greatest(doris_udf::FunctionContext* ctx, int num_args,
                                         const doris_udf::DoubleVal* val);
    static doris_udf::StringVal least(doris_udf::FunctionContext* ctx, int num_args,
                                      const doris_udf::StringVal* val);
    static doris_udf::StringVal greatest(doris_udf::FunctionContext* ctx, int num_args,
                                         const doris_udf::StringVal* val);
    static doris_udf::DateTimeVal least(doris_udf::FunctionContext* ctx, int num_args,
                                        const doris_udf::DateTimeVal* val);
    static doris_udf::DateTimeVal greatest(doris_udf::FunctionContext* ctx, int num_args,
                                           const doris_udf::DateTimeVal* val);
    static doris_udf::DecimalV2Val least(doris_udf::FunctionContext* ctx, int num_args,
                                         const doris_udf::DecimalV2Val* val);
    static doris_udf::DecimalV2Val greatest(doris_udf::FunctionContext* ctx, int num_args,
                                            const doris_udf::DecimalV2Val* val);

    static double my_double_round(double value, int64_t dec, bool dec_unsigned, bool truncate);

    // Converts src_num in decimal to dest_base,
    // and fills expr_val.string_val with the result.
    static doris_udf::StringVal decimal_to_base(doris_udf::FunctionContext* ctx, int64_t src_num,
                                                int8_t dest_base);

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
    static bool handle_parse_result(int8_t dest_base, int64_t* num,
                                    StringParser::ParseResult parse_res);

    static const int32_t MIN_BASE = 2;
    static const int32_t MAX_BASE = 36;

private:
    static const char* _s_alphanumeric_chars;
};

} // namespace doris

#endif
