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

#include "exprs/timestamp_functions.h"

#include <gtest/gtest.h>

namespace doris {

class TimestampFunctionsTest : public testing::Test {
public:
    TimestampFunctionsTest() { }
};

TEST_F(TimestampFunctionsTest, day_of_week_test) {
    doris_udf::FunctionContext *context = new doris_udf::FunctionContext();

    doris_udf::DateTimeVal tv;
    //2001-02-03 12:34:56
    tv.packed_time = 1830650338932162560L;
    tv.type = TIME_DATETIME;

    ASSERT_EQ(7, TimestampFunctions::day_of_week(context, tv).val);
}

TEST_F(TimestampFunctionsTest, time_diff_test) {
    doris_udf::FunctionContext *context = new doris_udf::FunctionContext();
    
    DateTimeValue dt1(20190718120000);
    dt1.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv1;
    dt1.to_datetime_val(&tv1);
    
    DateTimeValue dt2(20190718130102);
    dt2.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv2;
    dt2.to_datetime_val(&tv2);
    
    ASSERT_EQ(-3662, TimestampFunctions::time_diff(context, tv1, tv2).val);
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
