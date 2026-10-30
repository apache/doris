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

#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include <limits>

#include "core/wide_integer_to_string.h"

// Include the formatter header before any other Doris header provides the integer definitions.
TEST(WideIntegerFormatting, FormatsSignedAndUnsignedBounds) {
    EXPECT_EQ("-170141183460469231731687303715884105728",
              fmt::format("{}", std::numeric_limits<::Int128>::min()));
    EXPECT_EQ("340282366920938463463374607431768211455",
              fmt::format("{}", std::numeric_limits<::UInt128>::max()));
    EXPECT_EQ("-57896044618658097711785492504343953926634992332820282019728792003956564819968",
              fmt::format("{}", std::numeric_limits<::Int256>::min()));
    EXPECT_EQ("115792089237316195423570985008687907853269984665640564039457584007913129639935",
              fmt::format("{}", std::numeric_limits<::UInt256>::max()));
}
