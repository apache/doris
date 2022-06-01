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
#include <time.h>

#include <any>
#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "function_test_util.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(TimestampFunctionsTest, day_of_week_test) {
    std::string func_name = "dayofweek";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2001-02-03 12:34:56")}, 7},
                        {{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-00 00:00:00")}, Null()}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, day_of_month_test) {
    std::string func_name = "dayofmonth";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-01 00:00:00")}, 1},
                        {{std::string("2020-02-29 00:00:00")}, 29}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, day_of_year_test) {
    std::string func_name = "dayofyear";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-00 00:00:00")}, Null()},
                        {{std::string("2020-02-29 00:00:00")}, 60}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, week_of_year_test) {
    std::string func_name = "weekofyear";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-00 00:00:00")}, Null()},
                        {{std::string("2020-02-29 00:00:00")}, 9}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, year_test) {
    std::string func_name = "year";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 2021},
                        {{std::string("2021-01-00 00:00:00")}, Null()},
                        {{std::string("2025-05-01 00:00:00")}, 2025}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, quarter_test) {
    std::string func_name = "quarter";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 1},
                        {{std::string("")}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("2025-10-23 00:00:00")}, 4}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, month_test) {
    std::string func_name = "month";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 1},
                        {{std::string("")}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("2025-05-23 00:00:00")}, 5}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, day_test) {
    std::string func_name = "day";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 1},
                        {{std::string("")}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("2025-05-23 00:00:00")}, 23}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, hour_test) {
    std::string func_name = "hour";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 23:59:59")}, 23},
                        {{std::string("2021-01-13 16:56:00")}, 16},
                        {{std::string("")}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, minute_test) {
    std::string func_name = "minute";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 23:59:50")}, 59},
                        {{std::string("2021-01-13 16:20:00")}, 20},
                        {{std::string("")}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, second_test) {
    std::string func_name = "second";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 23:50:59")}, 59},
                        {{std::string("2021-01-13 16:20:00")}, 0},
                        {{std::string("")}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, from_unix_test) {
    std::string func_name = "from_unixtime";

    InputTypeSet input_types = {TypeIndex::Int32};

    DataSet data_set = {{{1565080737}, std::string("2019-08-06 16:38:57")}, {{-123}, Null()}};

    check_function<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, timediff_test) {
    std::string func_name = "timediff";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::DateTime};

    DataSet data_set = {
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 12:00:00")}, 0.0},
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 13:01:02")}, -3662.0},
            {{std::string("2019-00-18 12:00:00"), std::string("2019-07-18 13:01:02")}, Null()},
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-00 13:01:02")}, Null()}};

    check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, date_format_test) {
    std::string func_name = "date_format";

    InputTypeSet input_types = {TypeIndex::DateTime, Consted {TypeIndex::String}};
    {
        DataSet data_set = {{{std::string("2009-10-04 22:23:00"), std::string("%W %M %Y")},
                             std::string("Sunday October 2009")}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
    {
        DataSet data_set = {{{std::string("2007-10-04 22:23:00"), std::string("%H:%i:%s")},
                             std::string("22:23:00")}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
    {
        DataSet data_set = {
                {{std::string("1900-10-04 22:23:00"), std::string("%D %y %a %d %m %b %j")},
                 std::string("4th 00 Thu 04 10 Oct 277")}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
    {
        DataSet data_set = {
                {{std::string("1997-10-04 22:23:00"), std::string("%H %k %I %r %T %S %w")},
                 std::string("22 22 10 10:23:00 PM 22:23:00 00 6")}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
    {
        DataSet data_set = {{{std::string("1999-01-01 00:00:00"), std::string("%X %V")},
                             std::string("1998 52")}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
    {
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%d")}, std::string("01")}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
    {
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%%%d")}, std::string("%01")}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
}
TEST(TimestampFunctionsTest, years_add_test) {
    std::string func_name = "years_add";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 00:00:00"), 5}, str_to_data_time("2025-05-23 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), -5}, str_to_data_time("2015-05-23 00:00:00")},
            {{std::string(""), 5}, Null()},
            {{std::string("2020-05-23 00:00:00"), 8000}, Null()},
            {{Null(), 5}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, years_sub_test) {
    std::string func_name = "years_sub";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 00:00:00"), 5}, str_to_data_time("2015-05-23 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), -5}, str_to_data_time("2025-05-23 00:00:00")},
            {{std::string(""), 5}, Null()},
            {{std::string("2020-05-23 00:00:00"), 3000}, Null()},
            {{Null(), 5}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, months_add_test) {
    std::string func_name = "months_add";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 00:00:00"), -4}, str_to_data_time("2020-06-23 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), 4}, str_to_data_time("2020-09-23 00:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 00:00:00"), 10}, str_to_data_time("2021-03-23 00:00:00")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, months_sub_test) {
    std::string func_name = "months_sub";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 00:00:00"), 4}, str_to_data_time("2020-01-23 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), -4}, str_to_data_time("2020-09-23 00:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 00:00:00"), 10}, str_to_data_time("2019-07-23 00:00:00")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, days_add_test) {
    std::string func_name = "days_add";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 00:00:00"), -4}, str_to_data_time("2020-10-19 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), 4}, str_to_data_time("2020-05-27 00:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 00:00:00"), 10}, str_to_data_time("2020-06-2 00:00:00")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, days_sub_test) {
    std::string func_name = "days_sub";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 00:00:00"), 4}, str_to_data_time("2020-05-19 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), -4}, str_to_data_time("2020-05-27 00:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 00:00:00"), 31}, str_to_data_time("2020-04-22 00:00:00")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, hours_add_test) {
    std::string func_name = "hours_add";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 10:00:00"), -4}, str_to_data_time("2020-10-23 06:00:00")},
            {{std::string("2020-05-23 10:00:00"), 4}, str_to_data_time("2020-05-23 14:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_data_time("2020-05-27 14:00:00")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, hours_sub_test) {
    std::string func_name = "hours_sub";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 10:00:00"), 4}, str_to_data_time("2020-05-23 06:00:00")},
            {{std::string("2020-05-23 10:00:00"), -4}, str_to_data_time("2020-05-23 14:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 31}, str_to_data_time("2020-05-22 03:00:00")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, minutes_add_test) {
    std::string func_name = "minutes_add";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 10:00:00"), 40}, str_to_data_time("2020-10-23 10:40:00")},
            {{std::string("2020-05-23 10:00:00"), -40}, str_to_data_time("2020-05-23 09:20:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_data_time("2020-05-23 11:40:00")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, minutes_sub_test) {
    std::string func_name = "minutes_sub";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 10:00:00"), 40}, str_to_data_time("2020-05-23 09:20:00")},
            {{std::string("2020-05-23 10:00:00"), -40}, str_to_data_time("2020-05-23 10:40:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_data_time("2020-05-23 08:20:00")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, seconds_add_test) {
    std::string func_name = "seconds_add";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 10:00:00"), 40}, str_to_data_time("2020-10-23 10:00:40")},
            {{std::string("2020-05-23 10:00:00"), -40}, str_to_data_time("2020-05-23 09:59:20")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_data_time("2020-05-23 10:01:40")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, seconds_sub_test) {
    std::string func_name = "seconds_sub";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 10:00:00"), 40}, str_to_data_time("2020-05-23 09:59:20")},
            {{std::string("2020-05-23 10:00:00"), -40}, str_to_data_time("2020-05-23 10:00:40")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_data_time("2020-05-23 09:58:20")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, weeks_add_test) {
    std::string func_name = "weeks_add";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 10:00:00"), 5}, str_to_data_time("2020-11-27 10:00:00")},
            {{std::string("2020-05-23 10:00:00"), -5}, str_to_data_time("2020-04-18 10:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_data_time("2022-04-23 10:00:00")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, weeks_sub_test) {
    std::string func_name = "weeks_sub";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 10:00:00"), 5}, str_to_data_time("2020-04-18 10:00:00")},
            {{std::string("2020-05-23 10:00:00"), -5}, str_to_data_time("2020-6-27 10:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_data_time("2018-06-23 10:00:00")},
            {{Null(), 4}, Null()}};

    check_function<DataTypeDateTime, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, to_days_test) {
    std::string func_name = "to_days";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 738156},
                        {{std::string("")}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("0000-01-01 00:00:00")}, 1}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, date_test) {
    std::string func_name = "date";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {
            {{std::string("2021-01-01 06:00:00")}, str_to_data_time("2021-01-01", false)},
            {{std::string("")}, Null()},
            {{Null()}, Null()},
            {{std::string("0000-01-01 00:00:00")}, str_to_data_time("0000-01-01", false)}};

    check_function<DataTypeDate, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, week_test) {
    std::string func_name = "week";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {
            {{std::string("1989-03-21 06:00:00")}, 12},
            {{std::string("")}, Null()},
            {{std::string("9999-12-12 00:00:00")}, 50}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);

    InputTypeSet new_input_types = {TypeIndex::Date};
    DataSet new_data_set = {
            {{std::string("1989-03-21")}, 12},
            {{std::string("")}, Null()},
            {{std::string("9999-12-12")}, 50}};

    check_function<DataTypeInt32, true>(func_name, new_input_types, new_data_set);
}

TEST(TimestampFunctionsTest, yearweek_test) {
    std::string func_name = "yearweek";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {
            {{std::string("1989-03-21 06:00:00")}, 198912},
            {{std::string("")}, Null()},
            {{std::string("9999-12-12 00:00:00")}, 999950}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);

    InputTypeSet new_input_types = {TypeIndex::Date};
    DataSet new_data_set = {
            {{std::string("1989-03-21")}, 198912},
            {{std::string("")}, Null()},
            {{std::string("9999-12-12")}, 999950}};

    check_function<DataTypeInt32, true>(func_name, new_input_types, new_data_set);
}

TEST(TimestampFunctionsTest, makedate_test) {
    std::string func_name = "makedate";

    InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32};

    DataSet data_set = {{{2021, 3}, str_to_data_time("2021-01-03", false)},
                        {{2021, 95}, str_to_data_time("2021-04-05", false)},
                        {{2021, 400}, str_to_data_time("2022-02-04", false)},
                        {{2021, 0}, Null()},
                        {{2021, -10}, Null()},
                        {{-1, 3}, Null()},
                        {{12345, 3}, Null()}};

    check_function<DataTypeDate, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, convert_tz_test) {
    std::string func_name = "convert_tz";

    InputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::String, TypeIndex::String};

    DataSet data_set = {
            {{DATETIME("2019-08-01 13:21:03"), STRING("Asia/Shanghai"),
              STRING("America/Los_Angeles")},
             str_to_data_time("2019-07-31 22:21:03", true)},
            {{DATETIME("2019-08-01 13:21:03"), STRING("+08:00"), STRING("America/Los_Angeles")},
             str_to_data_time("2019-07-31 22:21:03", true)}};

    check_function<DataTypeDate, true>(func_name, input_types, data_set);
}

TEST(VTimestampFunctionsTest, weekday_test) {
    std::string func_name = "weekday";

    {
        InputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {{{std::string("2001-02-03 12:34:56")}, 5},
                            {{std::string("2019-06-25")}, 1},
                            {{std::string("2020-00-01 00:00:00")}, Null()},
                            {{std::string("2020-01-00 00:00:00")}, Null()}};

        check_function<DataTypeInt32, true>(func_name, input_types, data_set);
    }
    InputTypeSet input_types = {TypeIndex::Date};

    DataSet data_set = {{{std::string("2001-02-03")}, 5},
                        {{std::string("2019-06-25")}, 1},
                        {{std::string("2020-00-01")}, Null()},
                        {{std::string("2020-01-00")}, Null()}};

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}
} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
