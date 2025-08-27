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

#include <gtest/gtest.h>

#include "vec/common/arithmetic_overflow.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct CheckOverFlowTest : public testing::Test {
    void SetUp() override {
        // This function is called before each test.
    }

    void TearDown() override {
        // This function is called after each test.
    }

    Int128 to_i128(std::string str) {
        StringRef str_ref(str.data(), str.size());
        Int128 val;
        EXPECT_TRUE(try_read_int_text(val, str_ref));
        return val;
    };

    wide::Int256 to_i256(std::string str) { return wide::Int256::_impl::from_str(str.c_str()); };
};

TEST_F(CheckOverFlowTest, test_overflow_int128) {
    {
        Int128 a = to_i128("-15687000000000000000000");
        Int128 b = to_i128("11000000000000000");
        Int128 c;
        EXPECT_TRUE(common::mul_overflow(a, b, c));
    }

    {
        Int128 a = to_i128("-15687000000000000000000");
        Int128 b = to_i128("-11000000000000000");
        Int128 c;
        EXPECT_TRUE(common::mul_overflow(a, b, c));
    }

    {
        Int128 a = to_i128("1000");
        Int128 b = to_i128("12000");
        Int128 c;
        EXPECT_FALSE(common::mul_overflow(a, b, c));
    }
}

TEST_F(CheckOverFlowTest, test_overflow_int256) {
    {
        wide::Int256 a =
                to_i256("-11579208923731619542357098500868790785326998466564056403945758400791");
        wide::Int256 b = to_i256("1157920892373161954235709850086879078532699846656405640394575");
        wide::Int256 c;
        EXPECT_TRUE(common::mul_overflow(a, b, c));
    }

    {
        wide::Int256 a = to_i256("-1157920892373161954235709850086879078532699846656405640394");
        wide::Int256 b = to_i256("-1157920892373161954235709850086879078532699846656405640394");
        wide::Int256 c;
        EXPECT_TRUE(common::mul_overflow(a, b, c));
    }

    {
        wide::Int256 a = to_i256("1000");
        wide::Int256 b = to_i256("12000");
        wide::Int256 c;
        EXPECT_FALSE(common::mul_overflow(a, b, c));
    }
}

} // namespace doris::vectorized
