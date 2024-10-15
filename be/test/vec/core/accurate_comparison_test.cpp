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

#include "vec/core/accurate_comparison.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest_pred_impl.h"

namespace doris::vectorized {
TEST(VAccurateComparison, TestsOP) {
    ASSERT_TRUE(accurate::equalsOp(static_cast<Float32>(123), static_cast<UInt64>(123)));
    ASSERT_TRUE(accurate::lessOp(static_cast<Float32>(123), static_cast<UInt64>(124)));
    ASSERT_TRUE(accurate::lessOp(static_cast<Float32>(-1), static_cast<UInt64>(1)));
    ASSERT_TRUE(accurate::lessOp(static_cast<Int64>(-1), static_cast<UInt64>(1)));
    ASSERT_TRUE(!accurate::equalsOp(static_cast<Int64>(-1), static_cast<UInt64>(-1)));
    ASSERT_TRUE(accurate::equalsOp(-0., 0));
    ASSERT_TRUE(accurate::lessOp(-0., 1));
    ASSERT_TRUE(accurate::lessOp(-0.5, 1));
    ASSERT_TRUE(accurate::lessOp(0.5, 1));
    ASSERT_TRUE(accurate::equalsOp(1.0, 1));
    ASSERT_TRUE(accurate::greaterOp(1.1, 1));
    ASSERT_TRUE(accurate::greaterOp(11.1, 1));
    ASSERT_TRUE(accurate::greaterOp(11.1, 11));
    ASSERT_TRUE(accurate::lessOp(-11.1, 11));
    ASSERT_TRUE(accurate::lessOp(-11.1, -11));
    ASSERT_TRUE(accurate::lessOp(-1.1, -1));
    ASSERT_TRUE(accurate::greaterOp(-1.1, -2));
    ASSERT_TRUE(accurate::greaterOp(1000., 100));
    ASSERT_TRUE(accurate::greaterOp(-100., -1000));
    ASSERT_TRUE(accurate::lessOp(100., 1000));
    ASSERT_TRUE(accurate::lessOp(-1000., -100));

    ASSERT_TRUE(accurate::lessOp(-std::numeric_limits<Float64>::infinity(), 0));
    ASSERT_TRUE(accurate::lessOp(-std::numeric_limits<Float64>::infinity(), 1000));
    ASSERT_TRUE(accurate::lessOp(-std::numeric_limits<Float64>::infinity(), -1000));
    ASSERT_TRUE(accurate::greaterOp(std::numeric_limits<Float64>::infinity(), 0));
    ASSERT_TRUE(accurate::greaterOp(std::numeric_limits<Float64>::infinity(), 1000));
    ASSERT_TRUE(accurate::greaterOp(std::numeric_limits<Float64>::infinity(), -1000));

    ASSERT_TRUE(accurate::lessOp(1, 1e100));
    ASSERT_TRUE(accurate::lessOp(-1, 1e100));
    ASSERT_TRUE(accurate::lessOp(-1e100, 1));
    ASSERT_TRUE(accurate::lessOp(-1e100, -1));

    ASSERT_TRUE(accurate::equalsOp(static_cast<UInt64>(9223372036854775808ULL),
                                   static_cast<Float64>(9223372036854775808ULL)));
    ASSERT_TRUE(accurate::equalsOp(static_cast<UInt64>(9223372036854775808ULL),
                                   static_cast<Float32>(9223372036854775808ULL)));
    ASSERT_TRUE(accurate::lessOp(static_cast<UInt8>(255), 300));
    ASSERT_TRUE(accurate::lessOp(static_cast<UInt8>(255), static_cast<Int16>(300)));
    ASSERT_TRUE(accurate::notEqualsOp(static_cast<UInt8>(255), 44));
    ASSERT_TRUE(accurate::notEqualsOp(static_cast<UInt8>(255), static_cast<Int16>(44)));
}

} // namespace doris::vectorized
