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