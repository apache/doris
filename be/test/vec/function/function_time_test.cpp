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

namespace doris {

using vectorized::Null;
using vectorized::DataSet;
using vectorized::TypeIndex;

TEST(TimestampFunctionsTest, day_of_week_test) {
    std::string func_name = "dayofweek";

    std::vector<std::any> input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2001-02-03 12:34:56")}, 7},
                        {{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-00 00:00:00")}, Null()}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, day_of_month_test) {
    std::string func_name = "dayofmonth";

    std::vector<std::any> input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-01 00:00:00")}, 1},
                        {{std::string("2020-02-29 00:00:00")}, 29}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, day_of_year_test) {
    std::string func_name = "dayofyear";

    std::vector<std::any> input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-00 00:00:00")}, Null()},
                        {{std::string("2020-02-29 00:00:00")}, 60}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, week_of_year_test) {
    std::string func_name = "weekofyear";

    std::vector<std::any> input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-00 00:00:00")}, Null()},
                        {{std::string("2020-02-29 00:00:00")}, 9}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, year_test) {
    std::string func_name = "year";

    std::vector<std::any> input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 2021},
                        {{std::string("2021-01-00 00:00:00")}, Null()},
                        {{std::string("2025-05-01 00:00:00")}, 2025}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, quarter_test) {
    std::string func_name = "quarter";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 1},
                        {{std::string("")}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("2025-10-23 00:00:00")}, 4}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, month_test) {
    std::string func_name = "month";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 1},
                        {{std::string("")}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("2025-05-23 00:00:00")}, 5}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, day_test) {
    std::string func_name = "day";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 1},
                        {{std::string("")}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("2025-05-23 00:00:00")}, 23}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, hour_test) {
    std::string func_name = "hour";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 23:59:59")}, 23},
                        {{std::string("2021-01-13 16:56:00")}, 16},
                        {{std::string("")}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, minute_test) {
    std::string func_name = "minute";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 23:59:50")}, 59},
                        {{std::string("2021-01-13 16:20:00")}, 20},
                        {{std::string("")}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, second_test) {
    std::string func_name = "second";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 23:50:59")}, 59},
                        {{std::string("2021-01-13 16:20:00")}, 0},
                        {{std::string("")}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

//TEST(TimestampFunctionsTest, unix_timestamp_test) {
//    sleep(15);
//    std::string func_name = "unix_timestamp";
//
//    std::vector<std::any> input_types = {TypeIndex::DateTime};
//
//    DataSet data_set = {{{std::string("9999-12-30 00:00:00")}, 0},
//                        {{std::string("1000-01-01 00:00:00")}, 0}};
//
//    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
//}

TEST(TimestampFunctionsTest, from_unix_test) {
    std::string func_name = "from_unixtime";

    std::vector<std::any> input_types = {TypeIndex::Int32};

    DataSet data_set = {{{1565080737}, std::string("2019-08-06 16:38:57")}, {{-123}, Null()}};

    vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, timediff_test) {
    std::string func_name = "timediff";

    std::vector<std::any> input_types = {TypeIndex::DateTime, TypeIndex::DateTime};

    DataSet data_set = {
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 12:00:00")}, 0.0},
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 13:01:02")}, -3662.0},
            {{std::string("2019-00-18 12:00:00"), std::string("2019-07-18 13:01:02")}, Null()},
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-00 13:01:02")}, Null()}};

    vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, date_format_test) {
    std::string func_name = "date_format";

    std::vector<std::any> input_types = {TypeIndex::DateTime,
                                         vectorized::Consted {TypeIndex::String}};
    {
        DataSet data_set = {{{std::string("2009-10-04 22:23:00"), std::string("%W %M %Y")},
                             std::string("Sunday October 2009")}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }
    {
        DataSet data_set = {{{std::string("2007-10-04 22:23:00"), std::string("%H:%i:%s")},
                             std::string("22:23:00")}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }
    {
        DataSet data_set = {
                {{std::string("1900-10-04 22:23:00"), std::string("%D %y %a %d %m %b %j")},
                 std::string("4th 00 Thu 04 10 Oct 277")}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }
    {
        DataSet data_set = {
                {{std::string("1997-10-04 22:23:00"), std::string("%H %k %I %r %T %S %w")},
                 std::string("22 22 10 10:23:00 PM 22:23:00 00 6")}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }
    {
        DataSet data_set = {{{std::string("1999-01-01 00:00:00"), std::string("%X %V")},
                             std::string("1998 52")}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }
    {
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%d")}, std::string("01")}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }
    {
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%%%d")}, std::string("%01")}};

        vectorized::check_function<vectorized::DataTypeString, true>(func_name, input_types,
                                                                     data_set);
    }
}
TEST(TimestampFunctionsTest, years_add_test) {
    std::string func_name = "years_add";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-05-23 00:00:00"), 5},
                         vectorized::str_to_data_time("2025-05-23 00:00:00")},
                        {{std::string("2020-05-23 00:00:00"), -5},
                         vectorized::str_to_data_time("2015-05-23 00:00:00")},
                        {{std::string(""), 5}, Null()},
                        {{std::string("2020-05-23 00:00:00"), 8000}, Null()},
                        {{Null(), 5}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, years_sub_test) {
    std::string func_name = "years_sub";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-05-23 00:00:00"), 5},
                         vectorized::str_to_data_time("2015-05-23 00:00:00")},
                        {{std::string("2020-05-23 00:00:00"), -5},
                         vectorized::str_to_data_time("2025-05-23 00:00:00")},
                        {{std::string(""), 5}, Null()},
                        {{std::string("2020-05-23 00:00:00"), 3000}, Null()},
                        {{Null(), 5}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, months_add_test) {
    std::string func_name = "months_add";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-10-23 00:00:00"), -4},
                         vectorized::str_to_data_time("2020-06-23 00:00:00")},
                        {{std::string("2020-05-23 00:00:00"), 4},
                         vectorized::str_to_data_time("2020-09-23 00:00:00")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 00:00:00"), 10},
                         vectorized::str_to_data_time("2021-03-23 00:00:00")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, months_sub_test) {
    std::string func_name = "months_sub";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-05-23 00:00:00"), 4},
                         vectorized::str_to_data_time("2020-01-23 00:00:00")},
                        {{std::string("2020-05-23 00:00:00"), -4},
                         vectorized::str_to_data_time("2020-09-23 00:00:00")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 00:00:00"), 10},
                         vectorized::str_to_data_time("2019-07-23 00:00:00")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, days_add_test) {
    std::string func_name = "days_add";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-10-23 00:00:00"), -4},
                         vectorized::str_to_data_time("2020-10-19 00:00:00")},
                        {{std::string("2020-05-23 00:00:00"), 4},
                         vectorized::str_to_data_time("2020-05-27 00:00:00")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 00:00:00"), 10},
                         vectorized::str_to_data_time("2020-06-2 00:00:00")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, days_sub_test) {
    std::string func_name = "days_sub";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-05-23 00:00:00"), 4},
                         vectorized::str_to_data_time("2020-05-19 00:00:00")},
                        {{std::string("2020-05-23 00:00:00"), -4},
                         vectorized::str_to_data_time("2020-05-27 00:00:00")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 00:00:00"), 31},
                         vectorized::str_to_data_time("2020-04-22 00:00:00")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, hours_add_test) {
    std::string func_name = "hours_add";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-10-23 10:00:00"), -4},
                         vectorized::str_to_data_time("2020-10-23 06:00:00")},
                        {{std::string("2020-05-23 10:00:00"), 4},
                         vectorized::str_to_data_time("2020-05-23 14:00:00")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 10:00:00"), 100},
                         vectorized::str_to_data_time("2020-05-27 14:00:00")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, hours_sub_test) {
    std::string func_name = "hours_sub";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-05-23 10:00:00"), 4},
                         vectorized::str_to_data_time("2020-05-23 06:00:00")},
                        {{std::string("2020-05-23 10:00:00"), -4},
                         vectorized::str_to_data_time("2020-05-23 14:00:00")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 10:00:00"), 31},
                         vectorized::str_to_data_time("2020-05-22 03:00:00")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, minutes_add_test) {
    std::string func_name = "minutes_add";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-10-23 10:00:00"), 40},
                         vectorized::str_to_data_time("2020-10-23 10:40:00")},
                        {{std::string("2020-05-23 10:00:00"), -40},
                         vectorized::str_to_data_time("2020-05-23 09:20:00")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 10:00:00"), 100},
                         vectorized::str_to_data_time("2020-05-23 11:40:00")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, minutes_sub_test) {
    std::string func_name = "minutes_sub";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-05-23 10:00:00"), 40},
                         vectorized::str_to_data_time("2020-05-23 09:20:00")},
                        {{std::string("2020-05-23 10:00:00"), -40},
                         vectorized::str_to_data_time("2020-05-23 10:40:00")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 10:00:00"), 100},
                         vectorized::str_to_data_time("2020-05-23 08:20:00")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, seconds_add_test) {
    std::string func_name = "seconds_add";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-10-23 10:00:00"), 40},
                         vectorized::str_to_data_time("2020-10-23 10:00:40")},
                        {{std::string("2020-05-23 10:00:00"), -40},
                         vectorized::str_to_data_time("2020-05-23 09:59:20")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 10:00:00"), 100},
                         vectorized::str_to_data_time("2020-05-23 10:01:40")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, seconds_sub_test) {
    std::string func_name = "seconds_sub";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-05-23 10:00:00"), 40},
                         vectorized::str_to_data_time("2020-05-23 09:59:20")},
                        {{std::string("2020-05-23 10:00:00"), -40},
                         vectorized::str_to_data_time("2020-05-23 10:00:40")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 10:00:00"), 100},
                         vectorized::str_to_data_time("2020-05-23 09:58:20")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, weeks_add_test) {
    std::string func_name = "weeks_add";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-10-23 10:00:00"), 5},
                         vectorized::str_to_data_time("2020-11-27 10:00:00")},
                        {{std::string("2020-05-23 10:00:00"), -5},
                         vectorized::str_to_data_time("2020-04-18 10:00:00")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 10:00:00"), 100},
                         vectorized::str_to_data_time("2022-04-23 10:00:00")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, weeks_sub_test) {
    std::string func_name = "weeks_sub";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{std::string("2020-05-23 10:00:00"), 5},
                         vectorized::str_to_data_time("2020-04-18 10:00:00")},
                        {{std::string("2020-05-23 10:00:00"), -5},
                         vectorized::str_to_data_time("2020-6-27 10:00:00")},
                        {{std::string(""), 4}, Null()},
                        {{std::string("2020-05-23 10:00:00"), 100},
                         vectorized::str_to_data_time("2018-06-23 10:00:00")},
                        {{Null(), 4}, Null()}};

    vectorized::check_function<vectorized::DataTypeDateTime, true>(func_name, input_types,
                                                                   data_set);
}

TEST(TimestampFunctionsTest, to_days_test) {
    std::string func_name = "to_days";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 738156},
                        {{std::string("")}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("0000-01-01 00:00:00")}, 1}};

    vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, date_test) {
    std::string func_name = "date";

    std::vector<std::any> input_types = {vectorized::TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 06:00:00")},
                         vectorized::str_to_data_time("2021-01-01", false)},
                        {{std::string("")}, Null()},
                        {{Null()}, Null()},
                        {{std::string("0000-01-01 00:00:00")},
                         vectorized::str_to_data_time("0000-01-01", false)}};

    vectorized::check_function<vectorized::DataTypeDate, true>(func_name, input_types, data_set);
}

TEST(TimestampFunctionsTest, makedate_test) {
    std::string func_name = "makedate";

    std::vector<std::any> input_types = {vectorized::TypeIndex::Int32,
                                         vectorized::TypeIndex::Int32};

    DataSet data_set = {{{2021, 3}, vectorized::str_to_data_time("2021-01-03", false)},
                        {{2021, 95}, vectorized::str_to_data_time("2021-04-05", false)},
                        {{2021, 400}, vectorized::str_to_data_time("2022-02-04", false)},
                        {{2021, 0}, Null()},
                        {{2021, -10}, Null()},
                        {{-1, 3}, Null()},
                        {{12345, 3}, Null()}};

    vectorized::check_function<vectorized::DataTypeDate, true>(func_name, input_types, data_set);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
