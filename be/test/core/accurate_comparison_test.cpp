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

#include "core/accurate_comparison.h"

#include <gtest/gtest.h>

namespace doris {
TEST(VAccurateComparison, TestsOP) {
    EXPECT_TRUE((EqualsOp<TYPE_INT>::apply(1, 1)));
    EXPECT_FALSE((EqualsOp<TYPE_INT>::apply(1, 2)));
    EXPECT_TRUE((NotEqualsOp<TYPE_INT>::apply(1, 2)));
    EXPECT_FALSE((NotEqualsOp<TYPE_INT>::apply(1, 1)));
    EXPECT_TRUE((LessOp<TYPE_INT>::apply(1, 2)));
    EXPECT_FALSE((LessOp<TYPE_INT>::apply(2, 1)));
    EXPECT_TRUE((GreaterOp<TYPE_INT>::apply(2, 1)));
    EXPECT_FALSE((GreaterOp<TYPE_INT>::apply(1, 2)));
    EXPECT_TRUE((LessOrEqualsOp<TYPE_INT>::apply(1, 2)));
    EXPECT_TRUE((LessOrEqualsOp<TYPE_INT>::apply(1, 1)));
    EXPECT_FALSE((LessOrEqualsOp<TYPE_INT>::apply(2, 1)));
    EXPECT_TRUE((GreaterOrEqualsOp<TYPE_INT>::apply(2, 1)));
    EXPECT_TRUE((GreaterOrEqualsOp<TYPE_INT>::apply(1, 1)));
}

TEST(VAccurateComparison, StringSpecializations) {
    StringRef left("abc", 3);
    StringRef same("abc", 3);
    StringRef right("abd", 3);

    EXPECT_TRUE((EqualsOp<TYPE_STRING>::apply(left, same)));
    EXPECT_FALSE((EqualsOp<TYPE_STRING>::apply(left, right)));
    EXPECT_TRUE((LessOp<TYPE_STRING>::apply(left, right)));
    EXPECT_TRUE((GreaterOp<TYPE_STRING>::apply(right, left)));
}

TEST(VAccurateComparison, DateTimeSpecializations) {
    VecDateTimeValue date_a;
    VecDateTimeValue date_b;
    date_a.from_date_int64(20240101);
    date_b.from_date_int64(20240102);
    EXPECT_TRUE((EqualsOp<TYPE_DATE>::apply(date_a, date_a)));
    EXPECT_TRUE((NotEqualsOp<TYPE_DATE>::apply(date_a, date_b)));
    EXPECT_TRUE((LessOp<TYPE_DATE>::apply(date_a, date_b)));
    EXPECT_TRUE((GreaterOrEqualsOp<TYPE_DATE>::apply(date_b, date_a)));

    VecDateTimeValue datetime_a;
    VecDateTimeValue datetime_b;
    datetime_a.from_date_int64(20240101010203);
    datetime_b.from_date_int64(20240101010204);
    EXPECT_TRUE((LessOrEqualsOp<TYPE_DATETIME>::apply(datetime_a, datetime_b)));
    EXPECT_TRUE((GreaterOp<TYPE_DATETIME>::apply(datetime_b, datetime_a)));

    DateV2Value<DateV2ValueType> date_v2_a;
    DateV2Value<DateV2ValueType> date_v2_b;
    date_v2_a.unchecked_set_time(2024, 1, 1, 0, 0, 0, 0);
    date_v2_b.unchecked_set_time(2024, 1, 2, 0, 0, 0, 0);
    EXPECT_TRUE((EqualsOp<TYPE_DATEV2>::apply(date_v2_a, date_v2_a)));
    EXPECT_TRUE((LessOp<TYPE_DATEV2>::apply(date_v2_a, date_v2_b)));
    EXPECT_TRUE((GreaterOrEqualsOp<TYPE_DATEV2>::apply(date_v2_b, date_v2_a)));

    DateV2Value<DateTimeV2ValueType> datetime_v2_a;
    DateV2Value<DateTimeV2ValueType> datetime_v2_b;
    datetime_v2_a.unchecked_set_time(2024, 1, 1, 1, 2, 3, 0);
    datetime_v2_b.unchecked_set_time(2024, 1, 1, 1, 2, 4, 0);
    EXPECT_TRUE((NotEqualsOp<TYPE_DATETIMEV2>::apply(datetime_v2_a, datetime_v2_b)));
    EXPECT_TRUE((LessOrEqualsOp<TYPE_DATETIMEV2>::apply(datetime_v2_a, datetime_v2_b)));
    EXPECT_TRUE((GreaterOp<TYPE_DATETIMEV2>::apply(datetime_v2_b, datetime_v2_a)));
}

} // namespace doris
