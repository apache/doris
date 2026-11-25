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

#include "exec/common/arithmetic_overflow.h"

#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <random>

namespace doris {
namespace {

void check_int128_product(__int128 lhs, __int128 rhs) {
    const wide::Int256 expected = wide::Int256(lhs) * wide::Int256(rhs);
    const bool expected_overflow = expected < wide::Int256(std::numeric_limits<__int128>::min()) ||
                                   expected > wide::Int256(std::numeric_limits<__int128>::max());
    __int128 result = 0;
    EXPECT_EQ(common::mul_overflow(lhs, rhs, result), expected_overflow);
    // The low 128 bits must still be the product when the overflow flag is set.
    EXPECT_EQ(static_cast<__uint128_t>(result), static_cast<__uint128_t>(expected));
}

} // namespace

TEST(ArithmeticOverflowTest, Int128MultiplyBoundaries) {
    constexpr __int128 min = std::numeric_limits<__int128>::min();
    constexpr __int128 max = std::numeric_limits<__int128>::max();
    constexpr __int128 two63 = __int128(1) << 63;
    constexpr __int128 two64 = __int128(1) << 64;
    const std::array<__int128, 25> inputs = {
            min,        min + 1,     min / 2 - 1, min / 2,   min / 2 + 1, -two64 - 1, -two64,
            -two64 + 1, -two63,      -3,          -2,        -1,          0,          1,
            2,          3,           two63,       two64 - 1, two64,       two64 + 1,  max / 2 - 1,
            max / 2,    max / 2 + 1, max - 1,     max};
    for (size_t i = 0; i < inputs.size(); ++i) {
        for (size_t j = 0; j < inputs.size(); ++j) {
            SCOPED_TRACE(testing::Message() << "i=" << i << ", j=" << j);
            check_int128_product(inputs[i], inputs[j]);
        }
    }
}

TEST(ArithmeticOverflowTest, Int128MultiplyPowersOfTwo) {
    for (int lhs_bit = 0; lhs_bit < 127; ++lhs_bit) {
        for (int rhs_bit = 0; rhs_bit < 127; ++rhs_bit) {
            SCOPED_TRACE(testing::Message() << "lhs_bit=" << lhs_bit << ", rhs_bit=" << rhs_bit);
            const __int128 lhs = __int128(1) << lhs_bit;
            const __int128 rhs = __int128(1) << rhs_bit;
            check_int128_product(lhs, rhs);
            check_int128_product(-lhs, rhs);
            check_int128_product(lhs, -rhs);
            check_int128_product(-lhs, -rhs);
        }
    }
}

TEST(ArithmeticOverflowTest, Int128MultiplyRandomized) {
    std::mt19937_64 rng(0x128dec1a1);
    for (size_t i = 0; i < 10000; ++i) {
        SCOPED_TRACE(testing::Message() << "iteration=" << i);
        const auto lhs = static_cast<__int128>((__uint128_t(rng()) << 64) | rng());
        const auto rhs = static_cast<__int128>((__uint128_t(rng()) << 64) | rng());
        check_int128_product(lhs, rhs);
        check_int128_product(static_cast<int64_t>(lhs), static_cast<int64_t>(rhs));
    }
}

TEST(ArithmeticOverflowTest, Int128MultiplyAliasedResult) {
    constexpr __int128 min = std::numeric_limits<__int128>::min();
    constexpr __int128 max = std::numeric_limits<__int128>::max();
    for (const __int128 lhs : {min, min / 2, __int128(-1), __int128(0), __int128(1), max}) {
        for (const __int128 rhs :
             {__int128(-2), __int128(-1), __int128(0), __int128(1), __int128(2)}) {
            __int128 expected;
            const bool overflow = common::mul_overflow(lhs, rhs, expected);
            __int128 left = lhs;
            EXPECT_EQ(common::mul_overflow(left, rhs, left), overflow);
            EXPECT_EQ(left, expected);
            __int128 right = rhs;
            EXPECT_EQ(common::mul_overflow(lhs, right, right), overflow);
            EXPECT_EQ(right, expected);
        }
    }
}

} // namespace doris
