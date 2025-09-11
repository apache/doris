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

#include <string>

#include "function_test_util.h"
#include "util/timezone_utils.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time.h"
#include "vec/runtime/time_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(VTimestampFunctionsTest, day_of_week_test) {
    std::string func_name = "dayofweek";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2001-02-03 12:34:56")}, int8_t {7}},
                        {{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-00 00:00:00")}, Null()}};

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, day_of_month_test) {
    std::string func_name = "dayofmonth";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-01 00:00:00")}, int8_t {1}},
                        {{std::string("2020-02-29 00:00:00")}, int8_t {29}}};

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, day_of_year_test) {
    std::string func_name = "dayofyear";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-00 00:00:00")}, Null()},
                        {{std::string("2020-02-29 00:00:00")}, int16_t {60}}};

    static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, week_of_year_test) {
    std::string func_name = "weekofyear";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2020-00-01 00:00:00")}, Null()},
                        {{std::string("2020-01-00 00:00:00")}, Null()},
                        {{std::string("2020-02-29 00:00:00")}, int8_t {9}}};

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, year_test) {
    std::string func_name = "year";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int16_t {2021}},
                        {{std::string("2021-01-00 00:00:00")}, Null()},
                        {{std::string("2025-05-01 00:00:00")}, int16_t {2025}}};

    static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, quarter_test) {
    std::string func_name = "quarter";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                        {{Null()}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("2025-10-23 00:00:00")}, int8_t {4}}};

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, month_test) {
    std::string func_name = "month";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                        {{Null()}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("2025-05-23 00:00:00")}, int8_t {5}}};

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, day_test) {
    std::string func_name = "day";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                        {{Null()}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()},
                        {{std::string("2025-05-23 00:00:00")}, int8_t {23}}};

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, hour_test) {
    std::string func_name = "hour";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2021-01-01 23:59:59")}, int8_t {23}},
                        {{std::string("2021-01-13 16:56:00")}, int8_t {16}},
                        {{Null()}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()}};

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, minute_test) {
    std::string func_name = "minute";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2021-01-01 23:59:50")}, int8_t {59}},
                        {{std::string("2021-01-13 16:20:00")}, int8_t {20}},
                        {{Null()}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()}};

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, second_test) {
    std::string func_name = "second";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2021-01-01 23:50:59")}, int8_t {59}},
                        {{std::string("2021-01-13 16:20:00")}, int8_t {0}},
                        {{Null()}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()}};

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, from_unix_test) {
    std::string func_name = "from_unixtime_new";
    TimezoneUtils::load_timezones_to_cache();

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
        DataSet data_set = {
                {{int64_t(1565080737)}, std::string("2019-08-06 16:38:57")},
                {{int64_t(253402271999)}, std::string("9999-12-31 23:59:59")},
                {{int64_t(-123)}, Null()},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 6, 18}};
        DataSet data_set = {
                {{DECIMAL64(1565080737, 999999, 6)}, std::string("2019-08-06 16:38:57.999999")},
                {{DECIMAL64(253402271999, 999999, 6)}, std::string("9999-12-31 23:59:59.999999")},
                {{DECIMAL64(263402271999, 999999, 6)}, Null()},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, unix_timestamp_test) {
    std::string func_name = "unix_timestamp_new";
    TimezoneUtils::load_timezones_to_cache();

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};
        DataSet data_set = {{{std::string("2022-05-24")}, int64_t {1653321600}},
                            {{std::string("9022-05-24")}, int64_t {222551942400}},
                            {{Null()}, Null()}};
        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 3}};
        DataSet data_set = {
                {{std::string("2022-05-24 12:34:56.789")}, DECIMAL64(1653366896, 789, 3)},
                {{Null()}, Null()}};
        static_cast<void>(
                check_function<DataTypeDecimal64, true>(func_name, input_types, data_set, 3, 15));
    }

    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 6}};
        DataSet data_set = {
                {{std::string("2022-05-24 12:34:56.789123")}, DECIMAL64(1653366896, 789123, 6)},
                {{std::string("9022-05-24 12:34:56.789123")}, DECIMAL64(222551987696, 789123, 6)},
                {{std::string("9999-12-31 23:59:59.999")}, DECIMAL64(253402271999, 999000, 6)},
                {{Null()}, Null()}};
        static_cast<void>(
                check_function<DataTypeDecimal64, true>(func_name, input_types, data_set, 6, 18));
    }

    // test out of range
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 6}};
        DataSet data_set = {
                {{std::string("1022-05-24 12:34:56.789123")}, DECIMAL64(0, 0, 0)},
        };
        static_cast<void>(
                check_function<DataTypeDecimal64, true>(func_name, input_types, data_set, 0, 1));
    }
    // negative case
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 6}};
        DataSet data_set = {
                {{std::string("9999-12-30 23:59:59.999")}, DECIMAL64(0, 999000, 6)},
                {{std::string("9999-12-30 23:59:59.999")}, DECIMAL64(0, 999000, 6)},
        };
        static_cast<void>(check_function<DataTypeDecimal64, true>(func_name, input_types, data_set,
                                                                  6, 18, false, true));
    }
}

TEST(VTimestampFunctionsTest, timediff_test) {
    std::string func_name = "timediff";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME, PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 12:00:00")},
             std::string {"0.0"}},
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 13:01:02")},
             std::string {"-01:01:02"}},
            {{std::string("2019-00-18 12:00:00"), std::string("2019-07-18 13:01:02")}, Null()},
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-00 13:01:02")}, Null()}};

    static_cast<void>(check_function<DataTypeTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, convert_tz_test) {
    std::string func_name = "convert_tz";

    TimezoneUtils::clear_timezone_caches();
    TimezoneUtils::load_timezones_to_cache();

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_VARCHAR,
                                PrimitiveType::TYPE_VARCHAR};

    {
        DataSet data_set = {{{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/Shanghai"},
                              std::string {"UTC"}},
                             std::string("2019-07-31 18:18:27")},
                            {{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/Shanghai"},
                              std::string {"UTC"}},
                             std::string("2019-07-31 18:18:27")},
                            {{std::string {"0000-01-01 00:00:00"}, std::string {"+08:00"},
                              std::string {"-02:00"}},
                             Null()},
                            {{std::string {"0000-01-01 00:00:00"}, std::string {"+08:00"},
                              std::string {"+08:00"}},
                             std::string("0000-01-01 00:00:00")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }

    {
        DataSet data_set = {{{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/Shanghai"},
                              std::string {"UTC"}},
                             std::string("2019-07-31 18:18:27")},
                            {{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/Shanghai"},
                              std::string {"Utc"}},
                             std::string("2019-07-31 18:18:27")},
                            {{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/Shanghai"},
                              std::string {"UTC"}},
                             std::string("2019-07-31 18:18:27")},
                            {{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/SHANGHAI"},
                              std::string {"america/Los_angeles"}},
                             std::string("2019-07-31 11:18:27")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, date_format_test) {
    std::string func_name = "date_format";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME,
                                Consted {PrimitiveType::TYPE_VARCHAR}};
    {
        DataSet data_set = {{{std::string("2009-10-04 22:23:00"), std::string("%W %M %Y")},
                             std::string("Sunday October 2009")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {{{std::string("2007-10-04 22:23:00"), std::string("%H:%i:%s")},
                             std::string("22:23:00")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {
                {{std::string("1900-10-04 22:23:00"), std::string("%D %y %a %d %m %b %j")},
                 std::string("4th 00 Thu 04 10 Oct 277")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {
                {{std::string("1997-10-04 22:23:00"), std::string("%H %k %I %r %T %S %w")},
                 std::string("22 22 10 10:23:00 PM 22:23:00 00 6")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {{{std::string("1999-01-01 00:00:00"), std::string("%X %V")},
                             std::string("1998 52")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%d")}, std::string("01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%%%d")}, std::string("%01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, to_days_test) {
    std::string func_name = "to_days";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, 738156},
                        {{Null()}, Null()},
                        {{std::string("2021-01-32 00:00:00")}, Null()}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, date_test) {
    std::string func_name = "date";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("2021-01-01 06:00:00")}, std::string("2021-01-01")},
                        {{Null()}, Null()},
                        {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeDate, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, week_test) {
    std::string func_name = "week";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("1989-03-21 06:00:00")}, int8_t {12}},
                        {{Null()}, Null()},
                        {{std::string("9999-12-12 00:00:00")}, int8_t {50}}};

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, yearweek_test) {
    std::string func_name = "yearweek";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

    DataSet data_set = {{{std::string("1989-03-21 06:00:00")}, 198912},
                        {{Null()}, Null()},
                        {{std::string("9999-12-12 00:00:00")}, 999950}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, makedate_test) {
    std::string func_name = "makedate";

    InputTypeSet input_types = {PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT};

    DataSet data_set = {{{2021, 3}, std::string("2021-01-03")},
                        {{2021, 95}, std::string("2021-04-05")},
                        {{2021, 400}, std::string("2022-02-04")},
                        {{2021, 0}, Null()},
                        {{2021, -10}, Null()},
                        {{-1, 3}, Null()},
                        {{12345, 3}, Null()}};

    static_cast<void>(check_function<DataTypeDate, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, weekday_test) {
    std::string func_name = "weekday";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

        DataSet data_set = {{{std::string("2001-02-03 12:34:56")}, int8_t {5}},
                            {{std::string("2019-06-25")}, int8_t {1}},
                            {{std::string("2020-00-01 00:00:00")}, Null()},
                            {{std::string("2020-01-00 00:00:00")}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, day_of_week_v2_test) {
    std::string func_name = "dayofweek";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2001-02-03")}, int8_t {7}},
                            {{std::string("2020-00-01")}, Null()},
                            {{std::string("2020-01-00")}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2001-02-03 01:00:00")}, int8_t {7}},
                            {{std::string("2001-02-03 01:00:00.213")}, int8_t {7}},
                            {{std::string("2001-02-03 01:00:00.123213")}, int8_t {7}},
                            {{std::string("2001-02-03 01:00:00.123123213")}, int8_t {7}},
                            {{std::string("2001-02-03 25:00:00.123123213")}, Null()},
                            {{std::string("2001-02-03 01:61:00.123123213")}, Null()},
                            {{std::string("2001-02-03 01:00:61.123123213")}, Null()},
                            {{std::string("2020-00-01 01:00:00")}, Null()},
                            {{std::string("2020-01-00 01:00:00")}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, day_of_month_v2_test) {
    std::string func_name = "dayofmonth";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2020-00-01")}, Null()},
                            {{std::string("2020-01-01")}, int8_t {1}},
                            {{std::string("2020-02-29")}, int8_t {29}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2020-00-01 01:00:00")}, Null()},
                            {{std::string("2020-01-01 01:00:00")}, int8_t {1}},
                            {{std::string("2020-02-29 01:00:00.123123")}, int8_t {29}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, day_of_year_v2_test) {
    std::string func_name = "dayofyear";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2020-00-01")}, Null()},
                            {{std::string("2020-01-00")}, Null()},
                            {{std::string("2020-02-29")}, int16_t {60}}};

        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2020-00-01 01:00:00")}, Null()},
                            {{std::string("2020-01-00 01:00:00")}, Null()},
                            {{std::string("2020-02-29 01:00:00.1232")}, int16_t {60}}};

        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, week_of_year_v2_test) {
    std::string func_name = "weekofyear";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2020-00-01")}, Null()},
                            {{std::string("2020-01-00")}, Null()},
                            {{std::string("2020-02-29")}, int8_t {9}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2020-00-01 01:00:00")}, Null()},
                            {{std::string("2020-01-00 01:00:00")}, Null()},
                            {{std::string("2020-02-29 01:00:00")}, int8_t {9}},
                            {{std::string("2020-02-29 01:00:00.12312")}, int8_t {9}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, year_v2_test) {
    std::string func_name = "year";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int16_t {2021}},
                            {{std::string("2021-01-00")}, Null()},
                            {{std::string("2025-05-01")}, int16_t {2025}}};

        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2021-01-01 01:00:00")}, int16_t {2021}},
                            {{std::string("2021-01-00 01:00:00")}, Null()},
                            {{std::string("2025-05-01 01:00:00.123")}, int16_t {2025}}};

        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, quarter_v2_test) {
    std::string func_name = "quarter";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int8_t {1}},
                            {{Null()}, Null()},
                            {{std::string("2021-01-32")}, Null()},
                            {{std::string("2025-10-23")}, int8_t {4}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{Null()}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-10-23 00:00:00")}, int8_t {4}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, month_v2_test) {
    std::string func_name = "month";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int8_t {1}},
                            {{Null()}, Null()},
                            {{std::string("2021-01-32")}, Null()},
                            {{std::string("2025-05-23")}, int8_t {5}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{Null()}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-05-23 00:00:00")}, int8_t {5}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, day_v2_test) {
    std::string func_name = "day";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int8_t {1}},
                            {{Null()}, Null()},
                            {{std::string("2021-01-32")}, Null()},
                            {{std::string("2025-05-23")}, int8_t {23}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{Null()}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-05-23 00:00:00")}, int8_t {23}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, hour_v2_test) {
    std::string func_name = "hour";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int8_t {0}},
                            {{std::string("2021-01-13")}, int8_t {0}},
                            {{Null()}, Null()},
                            {{std::string("2025-05-23")}, int8_t {0}}};
        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00.123")}, int8_t {0}},
                            {{std::string("2021-01-13 01:00:00.123")}, int8_t {1}},
                            {{Null()}, Null()},
                            {{std::string("2025-05-23 23:00:00.123")}, int8_t {23}},
                            {{std::string("2025-05-23 25:00:00.123")}, Null()}};
        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, minute_v2_test) {
    std::string func_name = "minute";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int8_t {0}},
                            {{std::string("2021-01-13")}, int8_t {0}},
                            {{Null()}, Null()},
                            {{std::string("2025-05-23")}, int8_t {0}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00.123")}, int8_t {0}},
                            {{std::string("2021-01-13 00:11:00.123")}, int8_t {11}},
                            {{Null()}, Null()},
                            {{std::string("2025-05-23 00:22:22.123")}, int8_t {22}},
                            {{std::string("2025-05-23 00:60:22.123")}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, second_v2_test) {
    std::string func_name = "second";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int8_t {0}},
                            {{std::string("2021-01-13")}, int8_t {0}},
                            {{Null()}, Null()},
                            {{std::string("2025-05-23")}, int8_t {0}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:01.123")}, int8_t {1}},
                            {{std::string("2021-01-13 00:00:02.123")}, int8_t {2}},
                            {{Null()}, Null()},
                            {{std::string("2025-05-23 00:00:63.123")}, Null()},
                            {{std::string("2025-05-23 00:00:00.123")}, int8_t {0}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, timediff_v2_test) {
    std::string func_name = "timediff";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {
                {{std::string("2019-07-18"), std::string("2019-07-18")}, std::string {"0.0"}},
                {{std::string("2019-07-18"), std::string("2019-07-18")}, std::string {"-0.0"}},
                {{std::string("2019-00-18"), std::string("2019-07-18")}, Null()},
                {{std::string("2019-07-18"), std::string("2019-07-00")}, Null()}};

        static_cast<void>(check_function<DataTypeTimeV2, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {
                {{std::string("2019-07-18 00:00:00"), std::string("2019-07-18 00:00:00")},
                 std::string {"0.0"}},
                {{std::string("2019-07-18 00:00:10"), std::string("2019-07-18 00:00:00")},
                 std::string {"00:00:10"}},
                {{std::string("2019-00-18 00:00:00"), std::string("2019-07-18 00:00:00")}, Null()},
                {{std::string("2019-07-18 00:00:00"), std::string("2019-07-00 00:00:00")}, Null()}};

        static_cast<void>(check_function<DataTypeTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, datediff_v2_test) {
    std::string func_name = "datediff";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2019-07-18"), std::string("2019-07-19")}, -1},
                            {{std::string("2019-07-18"), std::string("2019-07-17")}, 1},
                            {{std::string("2019-00-18"), std::string("2019-07-18")}, Null()},
                            {{std::string("2019-07-18"), std::string("2019-07-00")}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, date_format_v2_test) {
    std::string func_name = "date_format";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2009-10-04"), std::string("%W %M %Y")},
                             std::string("Sunday October 2009")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {
                {{std::string("2007-10-04"), std::string("%H:%i:%s")}, std::string("00:00:00")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("1900-10-04"), std::string("%D %y %a %d %m %b %j")},
                             std::string("4th 00 Thu 04 10 Oct 277")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("1999-01-01 00:00:00"), std::string("%X %V")},
                             std::string("1998 52")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%d")}, std::string("01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%%%d")}, std::string("%01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2009-10-04 22:23:00"), std::string("%W %M %Y")},
                             std::string("Sunday October 2009")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2007-10-04 22:23:00"), std::string("%H:%i:%s")},
                             std::string("22:23:00")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {
                {{std::string("1900-10-04 22:23:00"), std::string("%D %y %a %d %m %b %j")},
                 std::string("4th 00 Thu 04 10 Oct 277")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {
                {{std::string("1997-10-04 22:23:00"), std::string("%H %k %I %r %T %S %w")},
                 std::string("22 22 10 10:23:00 PM 22:23:00 00 6")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("1999-01-01 00:00:00"), std::string("%X %V")},
                             std::string("1998 52")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%d")}, std::string("01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%%%d")}, std::string("%01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, years_add_v2_test) {
    std::string func_name = "years_add";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23"), 5}, std::string("2025-05-23")},
                            {{std::string("2020-05-23"), -5}, std::string("2015-05-23")},
                            {{Null(), 5}, Null()},
                            {{Null(), 5}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23"), 8000}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set, -1,
                                                               -1, true));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23 00:00:11.123"), 5},
                             std::string("2025-05-23 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), -5},
                             std::string("2015-05-23 00:00:11.123")},
                            {{Null(), 5}, Null()},
                            {{Null(), 5}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23 00:00:11.123"), 8000}, Null()}};

        static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set,
                                                                   -1, -1, true));
    }
}

TEST(VTimestampFunctionsTest, years_sub_v2_test) {
    std::string func_name = "years_sub";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23"), 5}, std::string("2015-05-23")},
                            {{std::string("2020-05-23"), -5}, std::string("2025-05-23")},
                            {{Null(), 5}, Null()},
                            {{Null(), 5}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23"), 3000}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set, -1,
                                                               -1, true));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23 00:00:11.123"), 5},
                             std::string("2015-05-23 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), -5},
                             std::string("2025-05-23 00:00:11.123")},
                            {{Null(), 5}, Null()},
                            {{Null(), 5}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23 00:00:11.123"), 3000}, Null()}};

        static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set,
                                                                   -1, -1, true));
    }
}

TEST(VTimestampFunctionsTest, months_add_v2_test) {
    std::string func_name = "months_add";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-10-23"), -4}, std::string("2020-06-23")},
                            {{std::string("2020-05-23"), 4}, std::string("2020-09-23")},
                            {{std::string("2020-05-23"), 10}, std::string("2021-03-23")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 3}, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-10-23 00:00:11.1234"), -4},
                             std::string("2020-06-23 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), 4},
                             std::string("2020-09-23 00:00:11.1234")},
                            {{std::string("2020-05-23 00:00:11.123"), 10},
                             std::string("2021-03-23 00:00:11.123")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 3));
    }

    // eq
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 2}, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-10-23 00:00:11.1234"), -4},
                             std::string("2020-06-23 00:00:11.1200")},
                            {{std::string("2020-05-23 00:00:11.1234"), 4},
                             std::string("2020-09-23 00:00:11.12")}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, 4));
    }
    // negative case
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 2}, PrimitiveType::TYPE_INT};

        DataSet data_set = {
                // input truncated to 2 decimal so output should be .12
                {{std::string("2020-10-23 00:00:11.1234"), -4},
                 std::string("2020-06-23 00:00:11.1234")},
                // output should be .12
                {{std::string("2020-05-23 00:00:11.12"), 4},
                 std::string("2020-09-23 00:00:11.1234")},
        };

        static_cast<void>(check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set,
                                                                   4, -1, false, true));
    }
}

TEST(VTimestampFunctionsTest, months_sub_v2_test) {
    std::string func_name = "months_sub";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23"), 4}, std::string("2020-01-23")},
                            {{std::string("2020-05-23"), -4}, std::string("2020-09-23")},
                            {{std::string("2020-05-23"), 10}, std::string("2019-07-23")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23 00:00:11.123"), 4},
                             std::string("2020-01-23 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), -4},
                             std::string("2020-09-23 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), 10},
                             std::string("2019-07-23 00:00:11.123")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, days_add_v2_test) {
    std::string func_name = "days_add";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-10-23"), -4}, std::string("2020-10-19")},
                            {{std::string("2020-05-23"), 4}, std::string("2020-05-27")},
                            {{std::string("2020-05-23"), 10}, std::string("2020-06-02")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-10-23 00:00:11.123"), -4},
                             std::string("2020-10-19 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), 4},
                             std::string("2020-05-27 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), 10},
                             std::string("2020-06-02 00:00:11.123")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, days_sub_v2_test) {
    std::string func_name = "days_sub";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23"), 4}, std::string("2020-05-19")},
                            {{std::string("2020-05-23"), -4}, std::string("2020-05-27")},
                            {{std::string("2020-05-23"), 31}, std::string("2020-04-22")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23 00:00:11.123"), 4},
                             std::string("2020-05-19 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), -4},
                             std::string("2020-05-27 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), 31},
                             std::string("2020-04-22 00:00:11.123")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, weeks_add_v2_test) {
    std::string func_name = "weeks_add";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-10-23"), 5}, std::string("2020-11-27")},
                            {{std::string("2020-05-23"), -5}, std::string("2020-04-18")},
                            {{std::string("2020-05-23"), 100}, std::string("2022-04-23")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-10-23 00:00:11.123"), 5},
                             std::string("2020-11-27 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), -5},
                             std::string("2020-04-18 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), 100},
                             std::string("2022-04-23 00:00:11.123")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, weeks_sub_v2_test) {
    std::string func_name = "weeks_sub";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23"), 5}, std::string("2020-04-18")},
                            {{std::string("2020-05-23"), -5}, std::string("2020-06-27")},
                            {{std::string("2020-05-23"), 100}, std::string("2018-06-23")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23 00:00:11.123"), 5},
                             std::string("2020-04-18 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), -5},
                             std::string("2020-06-27 00:00:11.123")},
                            {{std::string("2020-05-23 00:00:11.123"), 100},
                             std::string("2018-06-23 00:00:11.123")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, to_days_v2_test) {
    std::string func_name = "to_days";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2021-01-01")}, 738156},
                            {{Null()}, Null()},
                            {{std::string("2021-01-32")}, Null()},
                            {{std::string("0000-01-01")}, 1}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:11.123")}, 738156},
                            {{Null()}, Null()},
                            {{std::string("2021-01-32 00:00:11.123")}, Null()},
                            {{std::string("0000-01-01 00:00:11.123")}, 1}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, date_v2_test) {
    std::string func_name = "date";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2021-01-01")}, std::string("2021-01-01")},
                            {{Null()}, Null()},
                            {{Null()}, Null()},
                            {{std::string("0000-01-01")}, std::string("0000-01-01")}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:11.123")}, std::string("2021-01-01")},
                            {{Null()}, Null()},
                            {{Null()}, Null()},
                            {{std::string("0000-01-01 00:00:11.123")}, std::string("0000-01-01")}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, week_v2_test) {
    std::string func_name = "week";

    {
        InputTypeSet new_input_types = {PrimitiveType::TYPE_DATEV2};
        DataSet new_data_set = {{{std::string("1989-03-21")}, int8_t {12}},
                                {{Null()}, Null()},
                                {{std::string("9999-12-12")}, int8_t {50}}};

        static_cast<void>(
                check_function<DataTypeInt8, true>(func_name, new_input_types, new_data_set));
    }
    {
        InputTypeSet new_input_types = {PrimitiveType::TYPE_DATETIMEV2};
        DataSet new_data_set = {{{std::string("1989-03-21 00:00:11.123")}, int8_t {12}},
                                {{Null()}, Null()},
                                {{std::string("9999-12-12 00:00:11.123")}, int8_t {50}}};

        static_cast<void>(
                check_function<DataTypeInt8, true>(func_name, new_input_types, new_data_set));
    }
}

TEST(VTimestampFunctionsTest, yearweek_v2_test) {
    std::string func_name = "yearweek";

    {
        InputTypeSet new_input_types = {PrimitiveType::TYPE_DATEV2};
        DataSet new_data_set = {{{std::string("1989-03-21")}, 198912},
                                {{Null()}, Null()},
                                {{std::string("9999-12-12")}, 999950}};

        static_cast<void>(
                check_function<DataTypeInt32, true>(func_name, new_input_types, new_data_set));
    }
    {
        InputTypeSet new_input_types = {PrimitiveType::TYPE_DATETIMEV2};
        DataSet new_data_set = {{{std::string("1989-03-21 00:00:11.123")}, 198912},
                                {{Null()}, Null()},
                                {{std::string("9999-12-12 00:00:11.123")}, 999950}};

        static_cast<void>(
                check_function<DataTypeInt32, true>(func_name, new_input_types, new_data_set));
    }
}

TEST(VTimestampFunctionsTest, from_days_test) {
    std::string func_name = "from_days";

    InputTypeSet input_types = {PrimitiveType::TYPE_INT};

    {
        DataSet data_set = {{{730669}, std::string("2000-07-03")}, {{0}, Null()}};

        static_cast<void>(check_function<DataTypeDate, true>(func_name, input_types, data_set));
    }

    {
        std::cout << "test date 0000-02-28" << std::endl;
        DataSet data_set = {{{59}, std::string("0000-02-28")}, {{0}, Null()}};

        static_cast<void>(check_function<DataTypeDate, true>(func_name, input_types, data_set));
    }

    {
        std::cout << "test date 0000-03-01" << std::endl;
        DataSet data_set = {{{60}, std::string("0000-03-01")}, {{0}, Null()}};

        static_cast<void>(check_function<DataTypeDate, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, weekday_v2_test) {
    std::string func_name = "weekday";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2001-02-03")}, int8_t {5}},
                            {{std::string("2019-06-25")}, int8_t {1}},
                            {{std::string("2020-00-01")}, Null()},
                            {{std::string("2020-01-00")}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2001-02-03 00:00:11.123")}, int8_t {5}},
                            {{std::string("2019-06-25 00:00:11.123")}, int8_t {1}},
                            {{std::string("2020-00-01 00:00:11.123")}, Null()},
                            {{std::string("2020-01-00 00:00:11.123")}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, dayname_test) {
    std::string func_name = "dayname";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};

        DataSet data_set = {{{std::string("2007-02-03")}, std::string("Saturday")},
                            {{std::string("2020-01-00")}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME};

        DataSet data_set = {{{std::string("2007-02-03 00:00:00")}, std::string("Saturday")},
                            {{std::string("2020-01-00 00:00:00")}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

        DataSet data_set = {{{std::string("2007-02-03 00:00:11.123")}, std::string("Saturday")},
                            {{std::string("2020-01-00 00:00:11.123")}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, datetrunc_test) {
    std::string func_name = "date_trunc";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("second")},
                             std::string("2022-10-08 11:44:23")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("minute")},
                             std::string("2022-10-08 11:44:00")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("hour")},
                             std::string("2022-10-08 11:00:00")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("day")},
                             std::string("2022-10-08 00:00:00")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("month")},
                             std::string("2022-10-01 00:00:00")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIME,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("year")},
                             std::string("2022-01-01 00:00:00")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23.123"), std::string("second")},
                             std::string("2022-10-08 11:44:23.000")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("minute")},
                             std::string("2022-10-08 11:44:00")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("hour")},
                             std::string("2022-10-08 11:00:00")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("day")},
                             std::string("2022-10-08 00:00:00")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("month")},
                             std::string("2022-10-01 00:00:00")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("year")},
                             std::string("2022-01-01 00:00:00")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, hours_add_v2_test) {
    std::string func_name = "hours_add";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-10-23 10:00:00.123"), -4},
                             std::string("2020-10-23 06:00:00.123")},
                            {{std::string("2020-05-23 10:00:00.123"), 4},
                             std::string("2020-05-23 14:00:00.123")},
                            {{std::string("2020-05-23 10:00:00.123"), 100},
                             std::string("2020-05-27 14:00:00.123")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, hours_sub_v2_test) {
    std::string func_name = "hours_sub";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_INT};

        DataSet data_set = {{{std::string("2020-05-23 10:00:00.123"), 4},
                             std::string("2020-05-23 06:00:00.123")},
                            {{std::string("2020-05-23 10:00:00.123"), -4},
                             std::string("2020-05-23 14:00:00.123")},
                            {{std::string("2020-05-23 10:00:00.123"), 31},
                             std::string("2020-05-22 03:00:00.123")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, minutes_add_v2_test) {
    std::string func_name = "minutes_add";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_BIGINT};

        DataSet data_set = {{{std::string("2020-10-23 10:00:00.123"), int64_t(40)},
                             std::string("2020-10-23 10:40:00.123")},
                            {{std::string("2020-05-23 10:00:00.123"), int64_t(-40)},
                             std::string("2020-05-23 09:20:00.123")},
                            {{std::string("2020-05-23 10:00:00.123"), int64_t(100)},
                             std::string("2020-05-23 11:40:00.123")},
                            {{Null(), int64_t(4)}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, minutes_sub_v2_test) {
    std::string func_name = "minutes_sub";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_BIGINT};

        DataSet data_set = {{{std::string("2020-05-23 10:00:00.123"), int64_t(40)},
                             std::string("2020-05-23 09:20:00.123")},
                            {{std::string("2020-05-23 10:00:00.123"), int64_t(-40)},
                             std::string("2020-05-23 10:40:00.123")},
                            {{std::string("2020-05-23 10:00:00.123"), int64_t(100)},
                             std::string("2020-05-23 08:20:00.123")},
                            {{Null(), int64_t(4)}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, seconds_add_v2_test) {
    std::string func_name = "seconds_add";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_BIGINT};

        DataSet data_set = {{{std::string("2020-10-23 10:00:00.123"), int64_t(40)},
                             std::string("2020-10-23 10:00:40.123")},
                            {{std::string("2020-05-23 10:00:00.123"), int64_t(-40)},
                             std::string("2020-05-23 09:59:20.123")},
                            {{std::string("2020-05-23 10:00:00.123"), int64_t(100)},
                             std::string("2020-05-23 10:01:40.123")},
                            {{Null(), int64_t(4)}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, seconds_sub_v2_test) {
    std::string func_name = "seconds_sub";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_BIGINT};

        DataSet data_set = {{{std::string("2020-05-23 10:00:00.123"), int64_t(40)},
                             std::string("2020-05-23 09:59:20.123")},
                            {{std::string("2020-05-23 10:00:00.123"), int64_t(-40)},
                             std::string("2020-05-23 10:00:40.123")},
                            {{std::string("2020-05-23 10:00:00.123"), int64_t(100)},
                             std::string("2020-05-23 09:58:20.123")},
                            {{Null(), int64_t(4)}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, year_of_week_test) {
    std::string func_name = "year_of_week";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2};
        DataSet data_set = {{{std::string("2005-01-01")}, int16_t(2004)},
                            {{std::string("2008-12-30")}, int16_t(2009)},
                            {{std::string("12008-12-30")}, Null()},
                            {{Null()}, Null()}};
        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, months_between_test) {
    std::string func_name = "months_between";
    InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_DATEV2,
                                PrimitiveType::TYPE_BOOLEAN};
    DataSet data_set = {
            {{std::string("2020-01-01"), std::string("2020-02-01"), uint8_t(0)}, double(-1.0)},
            {{std::string("2020-01-01"), std::string("2020-03-01"), uint8_t(1)}, double(-2.0)},
            {{std::string("2020-01-01"), std::string("2020-04-01"), uint8_t(0)}, double(-3.0)},
            {{std::string("2020-01-01"), std::string("2020-12-01"), uint8_t(1)}, double(-11.0)},
            {{std::string("2020-01-01"), std::string("2021-01-01"), uint8_t(0)}, double(-12.0)},
            {{std::string("2020-01-01"), std::string("2022-01-01"), uint8_t(1)}, double(-24.0)},
            {{std::string("2020-01-01"), std::string("2020-01-01"), uint8_t(0)}, double(0.0)},
            {{std::string("2020-12-01"), std::string("2020-01-01"), uint8_t(1)}, double(11.0)},
            {{std::string("2021-01-01"), std::string("2020-01-01"), uint8_t(0)}, double(12.0)},
            {{std::string("2022-01-01"), std::string("2020-01-01"), uint8_t(1)}, double(24.0)},
            {{Null(), std::string("2020-01-01"), uint8_t(1)}, Null()},
            {{std::string("2020-01-01"), Null(), uint8_t(1)}, Null()}};
    static_cast<void>(
            check_function_all_arg_comb<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, next_day_test) {
    std::string func_name = "next_day";
    InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2, PrimitiveType::TYPE_VARCHAR};
    {
        DataSet data_set = {
                {{std::string("2020-01-01"), std::string("MO")}, std::string("2020-01-06")},
                {{std::string("2020-01-01"), std::string("MON")}, std::string("2020-01-06")},
                {{std::string("2020-01-01"), std::string("MONDAY")}, std::string("2020-01-06")},
                {{std::string("2020-01-01"), std::string("TU")}, std::string("2020-01-07")},
                {{std::string("2020-01-01"), std::string("TUE")}, std::string("2020-01-07")},
                {{std::string("2020-01-01"), std::string("TUESDAY")}, std::string("2020-01-07")},
                {{std::string("2020-01-01"), std::string("WE")}, std::string("2020-01-08")},
                {{std::string("2020-01-01"), std::string("WED")}, std::string("2020-01-08")},
                {{std::string("2020-01-01"), std::string("WEDNESDAY")}, std::string("2020-01-08")},
                {{std::string("2020-01-01"), std::string("TH")}, std::string("2020-01-02")},
                {{std::string("2020-01-01"), std::string("THU")}, std::string("2020-01-02")},
                {{std::string("2020-01-01"), std::string("THURSDAY")}, std::string("2020-01-02")},
                {{std::string("2020-01-01"), std::string("FR")}, std::string("2020-01-03")},
                {{std::string("2020-01-01"), std::string("FRI")}, std::string("2020-01-03")},
                {{std::string("2020-01-01"), std::string("FRIDAY")}, std::string("2020-01-03")},
                {{std::string("2020-01-01"), std::string("SA")}, std::string("2020-01-04")},
                {{std::string("2020-01-01"), std::string("SAT")}, std::string("2020-01-04")},
                {{std::string("2020-01-01"), std::string("SATURDAY")}, std::string("2020-01-04")},
                {{std::string("2020-01-01"), std::string("SU")}, std::string("2020-01-05")},
                {{std::string("2020-01-01"), std::string("SUN")}, std::string("2020-01-05")},
                {{std::string("2020-01-01"), std::string("SUNDAY")}, std::string("2020-01-05")},
                {{Null(), std::string("MON")}, Null()}};
        static_cast<void>(check_function_all_arg_comb<DataTypeDateV2, true>(func_name, input_types,
                                                                            data_set));
    }
    {
        DataSet data_set = {
                // date over month
                {{std::string("2020-01-28"), std::string("MON")}, std::string("2020-02-03")},
                {{std::string("2020-01-31"), std::string("SAT")}, std::string("2020-02-01")},

                // date over year
                {{std::string("2020-12-28"), std::string("FRI")}, std::string("2021-01-01")},
                {{std::string("2020-12-31"), std::string("THU")}, std::string("2021-01-07")},

                // leap year(29 Feb)
                {{std::string("2020-02-27"), std::string("SAT")}, std::string("2020-02-29")},
                {{std::string("2020-02-29"), std::string("MON")}, std::string("2020-03-02")},

                // non leap year(28 Feb)
                {{std::string("2019-02-26"), std::string("THU")}, std::string("2019-02-28")},
                {{std::string("2019-02-28"), std::string("SUN")}, std::string("2019-03-03")},

                // date over month
                {{std::string("2020-04-29"), std::string("FRI")}, std::string("2020-05-01")},
                {{std::string("2020-05-31"), std::string("MON")}, std::string("2020-06-01")}};
        static_cast<void>(check_function_all_arg_comb<DataTypeDateV2, true>(func_name, input_types,
                                                                            data_set));
    }
}

TEST(VTimestampFunctionsTest, from_iso8601_date) {
    std::string func_name = "from_iso8601_date";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
            {{std::string("2020-01-01")}, std::string("2020-01-01")},
            {{std::string("2020-01-01")}, std::string("2020-01-01")},
            {{std::string("-1")}, Null()},
            {{std::string("2025-07-11")}, std::string("2025-07-11")},
            {{std::string("2024-02-29")}, std::string("2024-02-29")},
            {{std::string("2025-02-29")}, Null()},
            {{std::string("2025-13-01")}, Null()},
            {{std::string("2020-W10")}, std::string("2020-03-02")},
            {{std::string("2025-W28")}, std::string("2025-07-07")},
            {{std::string("2025-W53")}, std::string("2025-12-29")},
            {{std::string("2025-W00")}, Null()},
            {{std::string("2020-123")}, std::string("2020-05-02")},
            {{std::string("2025-192")}, std::string("2025-07-11")},
            {{std::string("2024-366")}, std::string("2024-12-31")},
            {{std::string("2025-366")}, Null()},
            {{std::string("2025-000")}, std::string("2024-12-31")},
            {{std::string("2025/07/11")}, Null()},
            {{std::string("25-07-11")}, Null()},
            {{std::string("2025-7-11")}, Null()},
            {{std::string("invalid-date")}, Null()},
            {{std::string("2025-07-11T12:34:56")}, Null()},
            {{std::string("-1")}, Null()},
            {{std::string("9999-12-31")}, std::string("9999-12-31")},
            {{std::string("10000-01-01")}, Null()},
            {{std::string("0001-01-01")}, std::string("0001-01-01")},
            {{std::string("0000-12-31")}, std::string("0000-12-31")},
            {{std::string("-0001-01-01")}, Null()},
            {{std::string("2025-01-01")}, std::string("2025-01-01")},
            {{std::string("2025-12-31")}, std::string("2025-12-31")},
            {{std::string("2025-00-01")}, Null()},
            {{std::string("2025-13-01")}, Null()},
            {{std::string("2025--01-01")}, Null()},
            {{std::string("2025-01-31")}, std::string("2025-01-31")},
            {{std::string("2025-04-30")}, std::string("2025-04-30")},
            {{std::string("2025-02-28")}, std::string("2025-02-28")},
            {{std::string("2024-02-29")}, std::string("2024-02-29")},
            {{std::string("2025-01-32")}, Null()},
            {{std::string("2025-04-31")}, Null()},
            {{std::string("2025-02-29")}, Null()},
            {{std::string("2025-02-30")}, Null()},
            {{std::string("2025-01-00")}, Null()},
            {{std::string("2025-01--01")}, Null()},
            {{std::string("2000-02-29")}, std::string("2000-02-29")},
            {{std::string("2024-02-29")}, std::string("2024-02-29")},
            {{std::string("1900-02-29")}, Null()},
            {{std::string("2100-02-29")}, Null()},
            {{std::string("2025-02-29")}, Null()},
            {{std::string("-2025-01-01")}, Null()},
            {{std::string("2025--07-01")}, Null()},
            {{std::string("2025-07--01")}, Null()},
            {{std::string("")}, Null()},
            {{std::string("2025")}, std::string("2025-01-01")},
            {{std::string("2025-07")}, std::string("2025-07-01")},
            {{std::string("99999-01-01")}, Null()},
            {{std::string("2025-123-01")}, Null()},
            {{std::string("2025-01-123")}, Null()},
            {{std::string("2025/01/01")}, Null()},
            {{std::string("2025.01.01")}, Null()},
            {{std::string("2025-01-01X")}, Null()},
            {{std::string("2025--01--01")}, Null()},
            {{std::string("abcd-01-01")}, Null()},
            {{std::string("2025-ab-01")}, Null()},
            {{std::string("2025-01-ab")}, Null()},
    };

    static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, time) {
    std::string func_name = "time";

    InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2};

    DataSet data_set = {
            {{std::string("2020-01-01 12:00:00")}, std::string("12:00:00")},
            {{std::string("2020-01-01 05:03:01")}, std::string("05:03:01")},
            {{std::string("2020-01-01 05:03:01.1")}, std::string("05:03:01.1")},
            {{std::string("2020-01-01 05:03:01.12")}, std::string("05:03:01.12")},
            {{std::string("2020-01-01 05:03:01.123")}, std::string("05:03:01.123")},
            {{std::string("2020-01-01 05:03:01.1234")}, std::string("05:03:01.1234")},
            {{std::string("2020-01-01 05:03:01.12345")}, std::string("05:03:01.12345")},
            {{std::string("2020-01-01 05:03:01.123456")}, std::string("05:03:01.123456")},
            {{std::string("2000-01-01 25:00:00")}, Null()},
            {{std::string("2000-01-01 12:60:00")}, Null()},
            {{std::string("2000-01-01 12:00:60")}, Null()},
            {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeTimeV2, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
