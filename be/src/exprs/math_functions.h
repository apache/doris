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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/math-functions.h
// and modified by Doris

#pragma once

#include <stdint.h>

#include "util/slice.h"
#include "util/string_parser.hpp"
#include "vec/common/string_ref.h"

namespace doris {
class FunctionContext;

class MathFunctions {
public:
    static double my_double_round(double value, int64_t dec, bool dec_unsigned, bool truncate);

    // Converts src_num in decimal to dest_base,
    // and fills expr_val.string_val with the result.
    static doris::Slice decimal_to_base(doris::FunctionContext* ctx, int64_t src_num,
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

    static constexpr int32_t MIN_BASE = 2;
    static constexpr int32_t MAX_BASE = 36;
};

} // namespace doris
