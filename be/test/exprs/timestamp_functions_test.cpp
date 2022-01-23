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

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/test_env.h"
#include "testutil/function_utils.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace doris {
class FunctionContextImpl;

doris_udf::DateTimeVal datetime_val(int64_t value) {
    DateTimeValue dt(value);
    doris_udf::DateTimeVal tv;
    dt.to_datetime_val(&tv);
    return tv;
}

class TimestampFunctionsTest : public testing::Test {
public:
    TimestampFunctionsTest() {}

    void SetUp() {
        TQueryGlobals globals;
        globals.__set_now_string("2019-08-06 01:38:57");
        globals.__set_timestamp_ms(1565080737805);
        globals.__set_time_zone("America/Los_Angeles");
        state = new RuntimeState(globals);
        utils = new FunctionUtils(state);
        ctx = utils->get_fn_ctx();
    }

    void TearDown() {
        delete state;
        delete utils;
    }

private:
    RuntimeState* state = nullptr;
    FunctionUtils* utils = nullptr;
    FunctionContext* ctx = nullptr;
};

TEST_F(TimestampFunctionsTest, day_of_week_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    doris_udf::DateTimeVal tv;
    //2001-02-03 12:34:56
    tv.packed_time = 1830650338932162560L;
    tv.type = TIME_DATETIME;

    ASSERT_EQ(7, TimestampFunctions::day_of_week(context, tv).val);

    // 2020-00-01 00:00:00
    DateTimeValue dtv2(20200001000000);
    dtv2.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv2;
    dtv2.to_datetime_val(&tv2);
    ASSERT_EQ(true, TimestampFunctions::day_of_week(context, tv2).is_null);

    // 2020-01-00 00:00:00
    DateTimeValue dtv3(20200100000000);
    dtv3.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv3;
    dtv3.to_datetime_val(&tv3);
    ASSERT_EQ(true, TimestampFunctions::day_of_week(context, tv3).is_null);

    delete context;
}

TEST_F(TimestampFunctionsTest, week_day_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    doris_udf::DateTimeVal tv;
    // 2001-02-03 12:34:56
    tv.packed_time = 1830650338932162560L;
    tv.type = TIME_DATETIME;

    ASSERT_EQ(5, TimestampFunctions::week_day(context, tv).val);

    // 2020-00-01 00:00:00
    DateTimeValue dtv2(20200001000000);
    dtv2.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv2;
    dtv2.to_datetime_val(&tv2);
    ASSERT_EQ(true, TimestampFunctions::week_day(context, tv2).is_null);

    // 2020-01-00 00:00:00
    DateTimeValue dtv3(20200100000000);
    dtv3.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv3;
    dtv3.to_datetime_val(&tv3);
    ASSERT_EQ(true, TimestampFunctions::week_day(context, tv3).is_null);

    delete context;
}

TEST_F(TimestampFunctionsTest, day_of_month_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    // 2020-00-01 00:00:00
    DateTimeValue dtv1(20200001000000);
    dtv1.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv1;
    dtv1.to_datetime_val(&tv1);
    ASSERT_EQ(false, TimestampFunctions::day_of_month(context, tv1).is_null);
    ASSERT_EQ(1, TimestampFunctions::day_of_month(context, tv1).val);

    // 2020-01-00 00:00:00
    DateTimeValue dtv2(20200100000000);
    dtv2.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv2;
    dtv2.to_datetime_val(&tv2);
    ASSERT_EQ(false, TimestampFunctions::day_of_month(context, tv2).is_null);
    ASSERT_EQ(0, TimestampFunctions::day_of_month(context, tv2).val);

    // 2020-02-29 00:00:00
    DateTimeValue dtv3(20200229000000);
    dtv3.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv3;
    dtv3.to_datetime_val(&tv3);
    ASSERT_EQ(false, TimestampFunctions::day_of_month(context, tv3).is_null);
    ASSERT_EQ(29, TimestampFunctions::day_of_month(context, tv3).val);

    delete context;
}

TEST_F(TimestampFunctionsTest, day_of_year_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    // 2020-00-01 00:00:00
    DateTimeValue dtv1(20200001000000);
    dtv1.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv1;
    dtv1.to_datetime_val(&tv1);
    ASSERT_EQ(true, TimestampFunctions::day_of_year(context, tv1).is_null);

    // 2020-01-00 00:00:00
    DateTimeValue dtv2(20200100000000);
    dtv2.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv2;
    dtv2.to_datetime_val(&tv2);
    ASSERT_EQ(true, TimestampFunctions::day_of_year(context, tv2).is_null);

    // 2020-02-29 00:00:00
    DateTimeValue dtv3(20200229000000);
    dtv3.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv3;
    dtv3.to_datetime_val(&tv3);
    ASSERT_EQ(false, TimestampFunctions::day_of_year(context, tv3).is_null);
    ASSERT_EQ(60, TimestampFunctions::day_of_year(context, tv3).val);

    delete context;
}

TEST_F(TimestampFunctionsTest, week_of_year_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    // 2020-00-01 00:00:00
    DateTimeValue dtv1(20200001000000);
    dtv1.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv1;
    dtv1.to_datetime_val(&tv1);
    ASSERT_EQ(true, TimestampFunctions::week_of_year(context, tv1).is_null);

    // 2020-01-00 00:00:00
    DateTimeValue dtv2(20200100000000);
    dtv2.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv2;
    dtv2.to_datetime_val(&tv2);
    ASSERT_EQ(true, TimestampFunctions::week_of_year(context, tv2).is_null);

    // 2020-02-29 00:00:00
    DateTimeValue dtv3(20200229000000);
    dtv3.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv3;
    dtv3.to_datetime_val(&tv3);
    ASSERT_EQ(false, TimestampFunctions::week_of_year(context, tv3).is_null);
    ASSERT_EQ(9, TimestampFunctions::week_of_year(context, tv3).val);

    delete context;
}

TEST_F(TimestampFunctionsTest, year_week_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    DateTimeValue dtv1(20210101000000);
    dtv1.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv1;
    dtv1.to_datetime_val(&tv1);
    ASSERT_EQ(202052, TimestampFunctions::year_week(context, tv1).val);

    DateTimeValue dtv2(20210103000000);
    dtv2.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv2;
    dtv2.to_datetime_val(&tv2);
    ASSERT_EQ(202101, TimestampFunctions::year_week(context, tv2).val);

    DateTimeValue dtv3(20210501000000);
    dtv3.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv3;
    dtv3.to_datetime_val(&tv3);
    ASSERT_EQ(202117, TimestampFunctions::year_week(context, tv3).val);

    DateTimeValue dtv4(20241230000000);
    dtv4.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv4;
    dtv4.to_datetime_val(&tv4);
    ASSERT_EQ(202501, TimestampFunctions::year_week(context, tv4, 1).val);

    DateTimeValue dtv5(20261229121030);
    dtv5.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv5;
    dtv5.to_datetime_val(&tv5);
    ASSERT_EQ(202653, TimestampFunctions::year_week(context, tv5, 3).val);
    delete context;
}

TEST_F(TimestampFunctionsTest, week_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    DateTimeValue dtv1(20210101000000);
    dtv1.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv1;
    dtv1.to_datetime_val(&tv1);
    ASSERT_EQ(0, TimestampFunctions::week(context, tv1).val);

    DateTimeValue dtv2(20210103000000);
    dtv2.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv2;
    dtv2.to_datetime_val(&tv2);
    ASSERT_EQ(1, TimestampFunctions::week(context, tv2).val);

    DateTimeValue dtv3(20210501000000);
    dtv3.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv3;
    dtv3.to_datetime_val(&tv3);
    ASSERT_EQ(17, TimestampFunctions::week(context, tv3).val);

    DateTimeValue dtv4(20210101000000);
    dtv4.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv4;
    dtv4.to_datetime_val(&tv4);
    ASSERT_EQ(0, TimestampFunctions::week(context, tv4, {1}).val);

    DateTimeValue dtv5(20211201000000);
    dtv5.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv5;
    dtv5.to_datetime_val(&tv5);
    ASSERT_EQ(48, TimestampFunctions::week(context, tv5, 2).val);
    delete context;
}

TEST_F(TimestampFunctionsTest, make_date_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();

    ASSERT_EQ(true, TimestampFunctions::make_date(context, 2021, 0).is_null);

    DateTimeValue dtv1(20210101000000);
    dtv1.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv1;
    dtv1.to_datetime_val(&tv1);
    ASSERT_EQ(tv1.packed_time, TimestampFunctions::make_date(context, 2021, 1).packed_time);

    DateTimeValue dtv2(20211027000000);
    dtv2.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv2;
    dtv2.to_datetime_val(&tv2);
    ASSERT_EQ(tv2.packed_time, TimestampFunctions::make_date(context, 2021, 300).packed_time);

    DateTimeValue dtv3(20220204000000);
    dtv3.set_type(TIME_DATE);
    doris_udf::DateTimeVal tv3;
    dtv3.to_datetime_val(&tv3);
    ASSERT_EQ(tv3.packed_time, TimestampFunctions::make_date(context, 2021, 400).packed_time);
    delete context;
}

TEST_F(TimestampFunctionsTest, time_diff_test) {
    DateTimeValue dt1(20190718120000);
    dt1.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv1;
    dt1.to_datetime_val(&tv1);

    DateTimeValue dt2(20190718130102);
    dt2.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv2;
    dt2.to_datetime_val(&tv2);

    ASSERT_EQ(-3662, TimestampFunctions::time_diff(ctx, tv1, tv2).val);

    // invalid
    DateTimeValue dt3(20190018120000);
    dt3.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv3;
    dt3.to_datetime_val(&tv3);

    DateTimeValue dt4(20190718130102);
    dt4.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv4;
    dt4.to_datetime_val(&tv4);

    ASSERT_EQ(true, TimestampFunctions::time_diff(ctx, tv3, tv4).is_null);

    // invalid
    DateTimeValue dt5(20190718120000);
    dt5.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv5;
    dt5.to_datetime_val(&tv5);

    DateTimeValue dt6(20190700130102);
    dt6.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv6;
    dt6.to_datetime_val(&tv6);

    ASSERT_EQ(true, TimestampFunctions::time_diff(ctx, tv5, tv6).is_null);
}

TEST_F(TimestampFunctionsTest, now) {
    DateTimeVal now = TimestampFunctions::now(ctx);
    DateTimeValue dt = DateTimeValue::from_datetime_val(now);
    ASSERT_EQ(20190806013857, dt.to_int64());
}

TEST_F(TimestampFunctionsTest, from_unix) {
    IntVal unixtimestamp(1565080737);
    StringVal sval = TimestampFunctions::from_unix(ctx, unixtimestamp);
    ASSERT_EQ("2019-08-06 01:38:57", std::string((char*)sval.ptr, sval.len));

    IntVal unixtimestamp2(-123);
    sval = TimestampFunctions::from_unix(ctx, unixtimestamp2);
    ASSERT_TRUE(sval.is_null);
}

TEST_F(TimestampFunctionsTest, to_unix) {
    DateTimeVal dt_val;
    dt_val.packed_time = 1847544683002068992;
    dt_val.type = TIME_DATETIME;
    ASSERT_EQ(1565080737, TimestampFunctions::to_unix(ctx).val);
    ASSERT_EQ(1565080737, TimestampFunctions::to_unix(ctx, dt_val).val);
    ASSERT_EQ(1565080737, TimestampFunctions::to_unix(ctx, StringVal("2019-08-06 01:38:57"),
                                                      "%Y-%m-%d %H:%i:%S")
                                  .val);

    DateTimeValue dt_value;
    dt_value.from_date_int64(99991230);
    dt_value.to_datetime_val(&dt_val);
    ASSERT_EQ(0, TimestampFunctions::to_unix(ctx, dt_val).val);

    dt_value.from_date_int64(10000101);
    dt_value.to_datetime_val(&dt_val);
    ASSERT_EQ(0, TimestampFunctions::to_unix(ctx, dt_val).val);
}

TEST_F(TimestampFunctionsTest, curtime) {
    ASSERT_EQ(3600 + 38 * 60 + 57, TimestampFunctions::curtime(ctx).val);
}

TEST_F(TimestampFunctionsTest, convert_tz_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    DateTimeValue dt1(20190806163857);
    dt1.set_type(TIME_DATETIME);
    doris_udf::DateTimeVal tv1;
    dt1.to_datetime_val(&tv1);
    DateTimeVal t = TimestampFunctions::convert_tz(context, tv1, StringVal("Asia/Shanghai"),
                                                   StringVal("America/Los_Angeles"));
    DateTimeValue dt2 = DateTimeValue::from_datetime_val(t);
    ASSERT_EQ(20190806013857, dt2.to_int64());

    t = TimestampFunctions::convert_tz(context, tv1, StringVal("CST"),
                                       StringVal("America/Los_Angeles"));
    DateTimeValue dt3 = DateTimeValue::from_datetime_val(t);
    ASSERT_EQ(20190806013857, dt3.to_int64());
    delete context;
}

#define ASSERT_DIFF(unit, diff, tv1, tv2)                                                         \
    ASSERT_EQ(                                                                                    \
            diff,                                                                                 \
            TimestampFunctions::unit##s_diff(context, datetime_val(tv1), datetime_val(tv2)).val); \
    ASSERT_EQ(                                                                                    \
            -(diff),                                                                              \
            TimestampFunctions::unit##s_diff(context, datetime_val(tv2), datetime_val(tv1)).val);
TEST_F(TimestampFunctionsTest, timestampdiff_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    int64_t tv1 = 20120824000001;
    int64_t tv2 = 20120830000000;

    //YEAR
    ASSERT_DIFF(year, 0, tv2, tv1);
    ASSERT_DIFF(year, 1, tv1, 20100930000000);
    //MONTH
    ASSERT_DIFF(month, 0, tv2, tv1);
    ASSERT_DIFF(month, 0, tv2, tv1);
    ASSERT_DIFF(month, 0, 20120924000000, tv1);
    ASSERT_DIFF(month, 1, tv1, 20120630000000);
    //WEEK
    ASSERT_DIFF(week, 0, tv2, tv1);
    //DAY
    ASSERT_DIFF(day, 5, tv2, tv1);
    ASSERT_DIFF(day, 6, 20120830000001, tv1);
    ASSERT_DIFF(day, 8, 20120901000001, tv1);
    ASSERT_DIFF(day, 0, 20120823000005, tv1);
    ASSERT_DIFF(day, 0, 20120824000001, tv1);

    //HOUR
    ASSERT_DIFF(hour, 143, tv2, tv1);
    //MINUTE
    ASSERT_DIFF(minute, 8639, tv2, tv1);
    //SECOND
    ASSERT_DIFF(second, 518399, tv2, tv1);

    delete context;
}

#define ASSERT_ROUND(unit, floor, ceil, ...)                                       \
    ASSERT_EQ(datetime_val(floor).packed_time,                                     \
              TimestampFunctions::unit##_floor(context, __VA_ARGS__).packed_time); \
    ASSERT_EQ(datetime_val(ceil).packed_time,                                      \
              TimestampFunctions::unit##_ceil(context, __VA_ARGS__).packed_time);
TEST_F(TimestampFunctionsTest, time_round_test) {
    doris_udf::FunctionContext* context = new doris_udf::FunctionContext();
    doris_udf::DateTimeVal tv = datetime_val(20120824132901);

    doris_udf::IntVal three(3);
    doris_udf::DateTimeVal wednesday = datetime_val(20200916000000);
    //YEAR
    ASSERT_ROUND(year, 20120101000000, 20130101000000, tv);
    ASSERT_ROUND(year, 20110916000000, 20120916000000, tv, wednesday);
    ASSERT_ROUND(year, 20110916000000, 20140916000000, tv, three, wednesday);

    //MONTH
    ASSERT_ROUND(month, 20120801000000, 20120901000000, tv);
    ASSERT_ROUND(month, 20120701000000, 20121001000000, tv, three);

    //WEEK
    ASSERT_ROUND(week, 20120819000000, 20120826000000, tv);
    ASSERT_ROUND(week, 20120822000000, 20120829000000, tv, wednesday);
    ASSERT_ROUND(week, 20120808000000, 20120829000000, tv, three, wednesday);

    doris_udf::DateTimeVal tv1 = datetime_val(20200202130920);
    doris_udf::DateTimeVal monday = datetime_val(20200106000000);
    ASSERT_ROUND(week, 20200202000000, 20200209000000, tv1);
    ASSERT_ROUND(week, 20200127000000, 20200203000000, tv1, monday);

    //DAY
    doris_udf::DateTimeVal noon = datetime_val(19700101120000);
    ASSERT_ROUND(day, 20120824000000, 20120825000000, tv);
    ASSERT_ROUND(day, 20120824120000, 20120825120000, tv, noon);

    //HOUR
    doris_udf::DateTimeVal random = datetime_val(29380329113953);
    ASSERT_ROUND(hour, 20120824120000, 20120824150000, tv, three);
    ASSERT_ROUND(hour, 20120824113953, 20120824143953, tv, three, random);

    //MINUTE
    doris_udf::IntVal val90(90);
    ASSERT_ROUND(minute, 20120824132900, 20120824133000, tv);
    ASSERT_ROUND(minute, 20120824120000, 20120824133000, tv, val90);
    ASSERT_ROUND(minute, 20120824130953, 20120824143953, tv, val90, random);

    //SECOND
    ASSERT_ROUND(second, 20120824132900, 20120824132903, tv, three);
    ASSERT_ROUND(second, 20120824132830, 20120824133000, tv, val90);

    delete context;
}

} // namespace doris
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
