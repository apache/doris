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

#include "exprs/math_functions.h"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
namespace doris {

struct MathFunctionsTest : public ::testing::Test {};

TEST_F(MathFunctionsTest, DecimalInBaseToDecimal) {
    struct TestCase {
        int64_t input;
        int8_t base;
        uint64_t expected;
        TestCase(int64_t input_value, int8_t base_value, uint64_t expected_value)
                : input(input_value), base(base_value), expected(expected_value) {}
    };
    const TestCase cases[] = {
            {0, 16, 0},
            {1111111111, 16, 73300775185ULL},
            {10000000, 16, 268435456},
            {80000000, 16, 2147483648ULL},
            {100000000, 16, 4294967296ULL},
            {8000000000000000, 16, 1ULL << 63},
            {std::numeric_limits<int64_t>::min(), 10, 1ULL << 63},
            {-1111111111, 16, 0ULL - 73300775185ULL},
            {15, 4, 1},
            {12345, 4, 27},
            {5111, 4, 0},
            // Invalid leading digits produce an empty prefix.
            {999999999999999999, 9, 0},
            {std::numeric_limits<int64_t>::max(), 10,
             static_cast<uint64_t>(std::numeric_limits<int64_t>::max())},
    };
    for (const auto& test : cases) {
        SCOPED_TRACE(test.input);
        int64_t result = 0;
        ASSERT_TRUE(MathFunctions::decimal_in_base_to_decimal(test.input, test.base, &result));
        EXPECT_EQ(test.expected, static_cast<uint64_t>(result));
    }
}

TEST_F(MathFunctionsTest, DecimalInBaseToDecimalOverflow) {
    int64_t result = 0;
    EXPECT_FALSE(MathFunctions::decimal_in_base_to_decimal(10000000000000000, 16, &result));
    EXPECT_FALSE(MathFunctions::decimal_in_base_to_decimal(-10000000000000000, 16, &result));
    EXPECT_FALSE(MathFunctions::decimal_in_base_to_decimal(999999999999999999, 36, &result));
}

// Regular rounding test (truncate = false)
TEST_F(MathFunctionsTest, DoubleRoundBasic) {
    // Positive number rounding
    EXPECT_DOUBLE_EQ(123.46, MathFunctions::my_double_round(123.456, 2, false, false));
    EXPECT_DOUBLE_EQ(123.45, MathFunctions::my_double_round(123.454, 2, false, false));

    // Negative number rounding
    EXPECT_DOUBLE_EQ(-123.46, MathFunctions::my_double_round(-123.456, 2, false, false));
    EXPECT_DOUBLE_EQ(-123.45, MathFunctions::my_double_round(-123.454, 2, false, false));

    // Integer place rounding
    EXPECT_DOUBLE_EQ(100.0,
                     MathFunctions::my_double_round(123.456, -2, false, false)); // Hundreds place
    EXPECT_DOUBLE_EQ(120.0,
                     MathFunctions::my_double_round(123.456, -1, false, false)); // Tens place
    EXPECT_DOUBLE_EQ(-100.0, MathFunctions::my_double_round(
                                     -123.456, -2, false, false)); // Negative number hundreds place
}

// Truncation mode test (truncate = true)
TEST_F(MathFunctionsTest, DoubleRoundTruncate) {
    // Positive number truncation
    EXPECT_DOUBLE_EQ(123.45, MathFunctions::my_double_round(123.456, 2, false, true));
    EXPECT_DOUBLE_EQ(123.0, MathFunctions::my_double_round(123.789, 0, false, true));

    // Negative number truncation (towards zero)
    EXPECT_DOUBLE_EQ(-123.45, MathFunctions::my_double_round(-123.456, 2, false, true));
    EXPECT_DOUBLE_EQ(-100.0, MathFunctions::my_double_round(-123.456, -2, false, true));
}

// Special value handling (Infinity, NaN)
TEST_F(MathFunctionsTest, DoubleRoundSpecialValues) {
    const double inf = std::numeric_limits<double>::infinity();
    const double nan = std::numeric_limits<double>::quiet_NaN();

    // Infinity remains unchanged
    EXPECT_DOUBLE_EQ(inf, MathFunctions::my_double_round(inf, 2, false, false));
    EXPECT_DOUBLE_EQ(-inf, MathFunctions::my_double_round(-inf, -3, true, true));

    // NaN returns NaN
    EXPECT_TRUE(std::isnan(MathFunctions::my_double_round(nan, 2, false, false)));

    // Large precision causing overflow
    EXPECT_DOUBLE_EQ(0.0, MathFunctions::my_double_round(123.456, -1000, false, false));
    EXPECT_DOUBLE_EQ(123.456, MathFunctions::my_double_round(123.456, -1000, true,
                                                             false)); // dec_unsigned handling
}

// Zero and boundary precision test
TEST_F(MathFunctionsTest, DoubleRoundEdgeCases) {
    // Zero value handling
    EXPECT_DOUBLE_EQ(0.0, MathFunctions::my_double_round(0.0, 3, false, false));
    EXPECT_DOUBLE_EQ(0.0, MathFunctions::my_double_round(-0.0, 2, true, true));

    // Zero precision
    EXPECT_DOUBLE_EQ(123.0, MathFunctions::my_double_round(123.456, 0, false, false));
    EXPECT_DOUBLE_EQ(124.0, MathFunctions::my_double_round(123.789, 0, false, false)); // Rounding
}

}; // namespace doris