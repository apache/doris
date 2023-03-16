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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/string-parser.cc
// and modified by Doris

#include "string_parser.hpp"

#include "runtime/large_int_value.h"
#include "vec/common/int_exp.h"

namespace doris {

template <>
__int128 StringParser::numeric_limits<__int128>(bool negative) {
    return negative ? MIN_INT128 : MAX_INT128;
}

template <>
int32_t StringParser::get_scale_multiplier(int scale) {
    return common::exp10_i32(scale);
}

template <>
int64_t StringParser::get_scale_multiplier(int scale) {
    return common::exp10_i64(scale);
}

template <>
__int128 StringParser::get_scale_multiplier(int scale) {
    return common::exp10_i128(scale);
}

} // namespace doris
