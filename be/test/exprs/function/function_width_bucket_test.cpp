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

#include <cmath>
#include <limits>
#include <string>

#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"

namespace doris {
using namespace ut_type;

TEST(FunctionWidthBucketTest, WidthBucketNegativeBounds) {
    // Each row isolates a boundary. Negative values and negative bounds remain valid.
    InputTypeSet types = {PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT,
                          PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT};
    DataSet rows = {{{Int64(-1), Int64(-1), Int64(0), Int64(2)}, Int64(1)},
                    {{Int64(-1), Int64(-2), Int64(0), Int64(2)}, Int64(2)},
                    {{Int64(0), std::numeric_limits<Int64>::min(),
                      std::numeric_limits<Int64>::max(), Int64(2)},
                     Int64(2)}};
    // The bucket count is 2 for all rows because this argument must be constant in SQL.
    ASSERT_TRUE(check_function<DataTypeInt64>("width_bucket", types, rows).ok());
    for (Int64 count : {Int64(-1), Int64(0), std::numeric_limits<Int64>::max()}) {
        DataSet invalid = {{{Int64(0), Int64(-1), Int64(0), count}, Int64(0)}};
        auto status = check_function<DataTypeInt64>("width_bucket", types, invalid, -1, -1, true);
        EXPECT_NE(status.to_string().find("number of buckets must be between"), std::string::npos);
    }
    // NULL rows must not compare their internal zero payloads as equal bounds.
    DataSet nullable_rows = {{{Null(), Null(), Null(), Int64(2)}, Null()},
                             {{Int64(-1), Int64(-1), Int64(0), Int64(2)}, Int64(1)}};
    ASSERT_TRUE((check_function<DataTypeInt64, true>("width_bucket", types, nullable_rows).ok()));
    DataSet null_count = {{{Int64(-1), Int64(-1), Int64(0), Null()}, Null()}};
    ASSERT_TRUE((check_function<DataTypeInt64, true>("width_bucket", types, null_count).ok()));
    DataSet equal_bounds = {{{Int64(-1), Int64(-1), Int64(-1), Int64(1)}, Int64(0)}};
    auto status = check_function<DataTypeInt64>("width_bucket", types, equal_bounds, -1, -1, true);
    EXPECT_NE(status.to_string().find("bounds must be different"), std::string::npos);
}

TEST(FunctionWidthBucketTest, WidthBucketNonfiniteInput) {
    InputTypeSet types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE,
                          PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_BIGINT};
    // A single nonfinite input must fail before conversion of its bucket to BIGINT.
    for (double value :
         {std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
        DataSet rows = {{{value, double(-1), double(0), Int64(2)}, Int64(0)}};
        auto status = check_function<DataTypeInt64>("width_bucket", types, rows, -1, -1, true);
        EXPECT_NE(status.to_string().find("values and bounds must be finite"), std::string::npos);
    }
}

TEST(FunctionWidthBucketTest, WidthBucketIndependentNullArguments) {
    // One NULL argument is enough. Checking all-NULL rows would not detect a missing
    // null map for the second or third argument. Each case uses only one input row.
    InputTypeSet types = {PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT,
                          PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT};
    for (const DataSet& rows : {DataSet {{{Null(), Int64(-1), Int64(0), Int64(2)}, Null()}},
                                DataSet {{{Int64(-1), Null(), Int64(0), Int64(2)}, Null()}},
                                DataSet {{{Int64(-1), Int64(-1), Null(), Int64(2)}, Null()}}}) {
        check_function_all_arg_comb<DataTypeInt64, true>("width_bucket", types, rows);
    }
    // The largest valid count must still leave room for the upper out-of-range bucket.
    DataSet upper = {{{Int64(0), Int64(-1), Int64(0), std::numeric_limits<Int64>::max() - 1},
                      std::numeric_limits<Int64>::max()}};
    ASSERT_TRUE(check_function<DataTypeInt64>("width_bucket", types, upper).ok());
}

TEST(FunctionWidthBucketTest, WidthBucketKeepsSignedPhysicalTypes) {
    // Use physical columns, so FE coercion cannot hide a narrow integer or FLOAT path.
    // A unit-wide negative interval needs two distinct buckets even for integer input.
    const auto check = []<typename T>(PrimitiveType type, T value) {
        InputTypeSet types = {Notnull {type}, Notnull {type}, Notnull {type},
                              ConstedNotnull {PrimitiveType::TYPE_BIGINT}};
        DataSet rows = {{{value, value, T(0), Int64(2)}, Int64(1)}};
        ASSERT_TRUE(check_function<DataTypeInt64>("width_bucket", types, rows).ok());
    };
    check(PrimitiveType::TYPE_TINYINT, Int8(-1));
    check(PrimitiveType::TYPE_SMALLINT, Int16(-1));
    check(PrimitiveType::TYPE_INT, Int32(-1));
    check(PrimitiveType::TYPE_BIGINT, Int64(-1));
    check(PrimitiveType::TYPE_FLOAT, float(-1));
    check(PrimitiveType::TYPE_DOUBLE, double(-1));
}

TEST(FunctionWidthBucketTest, WidthBucketFloatingBounds) {
    InputTypeSet types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE,
                          PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_BIGINT};
    // A finite value with either nonfinite bound must fail before bucket conversion.
    for (double bound :
         {std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
        for (const DataSet& rows :
             {DataSet {{{double(-1), bound, double(0), Int64(2)}, Int64(0)}},
              DataSet {{{double(-1), double(-1), bound, Int64(2)}, Int64(0)}}}) {
            auto status = check_function<DataTypeInt64>("width_bucket", types, rows, -1, -1, true);
            EXPECT_NE(status.to_string().find("values and bounds must be finite"),
                      std::string::npos);
        }
    }
    // The value is below the upper bound, even if division rounds the ratio to 1.
    // It must stay in bucket 2 instead of the upper out-of-range bucket 3.
    DataSet inside = {{{-std::numeric_limits<double>::denorm_min(),
                        -std::numeric_limits<double>::max(), double(0), Int64(2)},
                       Int64(2)}};
    check_function_all_arg_comb<DataTypeInt64>("width_bucket", types, inside);
}

TEST(FunctionWidthBucketTest, WidthBucketFloatingInteriorBoundaries) {
    const auto check = []<typename T>(PrimitiveType type) {
        InputTypeSet types = {type, type, type, PrimitiveType::TYPE_BIGINT};
        // With unit-width buckets, 15 is the first value in bucket 16. Dividing
        // 15 by 21 before multiplying by 21 rounds just below 15 on x86_64.
        DataSet rows = {{{std::nextafter(T(15), T(0)), T(0), T(21), Int64(21)}, Int64(15)},
                        {{T(15), T(0), T(21), Int64(21)}, Int64(16)},
                        {{std::nextafter(T(15), T(21)), T(0), T(21), Int64(21)}, Int64(16)}};
        check_function_all_arg_comb<DataTypeInt64>("width_bucket", types, rows);
    };
    check.template operator()<float>(PrimitiveType::TYPE_FLOAT);
    check.template operator()<double>(PrimitiveType::TYPE_DOUBLE);
}
} // namespace doris
