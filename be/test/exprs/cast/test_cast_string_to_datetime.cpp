
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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <limits>
#include <type_traits>
// #include <format>

#include "common/exception.h"
#include "common/expected.h"
#include "olap/olap_common.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/test_util.h"
#include "udf/udf.h"
#include "util/string_parser.hpp"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/function_cast.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace vectorized {
using ZoneList = std::unordered_map<std::string, cctz::time_zone>;
}
using namespace vectorized;
const ZoneList* get_time_zone_cache();

class CastStringToDateTimeTest : public ::testing::Test {
    static void SetUpTestSuite() { TimezoneUtils::load_timezones_to_cache(); }

    template <typename T>
    void test_cast(FunctionContext& fn_context, int scale, const string& str, bool expect_null,
                   const T& expected, bool expect_throw = false) {
        using DateV2ValueExactType =
                std::conditional_t<T::is_datetime, DataTypeDateTimeV2, DataTypeDateV2>;
        using ColumnType = std::conditional_t<T::is_datetime, ColumnDateTimeV2, ColumnDateV2>;

        auto field_type = expected.is_datetime ? FieldType::OLAP_FIELD_TYPE_DATETIMEV2
                                               : FieldType::OLAP_FIELD_TYPE_DATEV2;
        auto data_type_str = DataTypeFactory::instance().create_data_type(TypeIndex::String);
        auto data_type_dtv2 = DataTypeFactory::instance().create_data_type(field_type, 0, scale);
        const auto* data_type_dtv2_real_type_ptr =
                assert_cast<const DateV2ValueExactType*>(data_type_dtv2.get());
        // auto data_type_dtv2_nullable = make_nullable(data_type_dtv2);

        Block block;

        auto col_str = ColumnString::create();
        col_str->insert_data(str.data(), str.size());
        block.insert({std::move(col_str), data_type_str, "col_str_cast_to_dtv2"});

        auto col_dtv2 = data_type_dtv2->create_column();
        // auto col_dtv2_nullable = ColumnNullable::create(std::move(col_dtv2), ColumnUInt8::create());
        // block.insert({std::move(col_dtv2_nullable), data_type_dtv2_nullable, "col_dtv2"});
        block.insert({std::move(col_dtv2), data_type_dtv2, "col_dtv2"});

        ColumnNumbers args {0};
        Status status;
        if (expect_throw) {
            EXPECT_THROW(
                    (status =
                             vectorized::StringParsing<DateV2ValueExactType, vectorized::NameCast>::
                                     execute(&fn_context, block, args, 1, block.rows(), true)),
                    Exception);
            return;
        } else {
            status = vectorized::StringParsing<DateV2ValueExactType, vectorized::NameCast>::execute(
                    &fn_context, block, args, 1, block.rows(), false);
        }
        EXPECT_TRUE(status.ok());
        const auto* cast_res_col_nullable =
                assert_cast<const ColumnNullable*>(block.get_by_position(1).column.get());
        EXPECT_EQ(1, cast_res_col_nullable->size());

        const auto* cast_res_col_dt = assert_cast<const ColumnType*>(
                cast_res_col_nullable->get_nested_column_ptr().get());

        if (expect_null) {
            if (!cast_res_col_nullable->is_null_at(0)) {
                auto real_value =
                        data_type_dtv2_real_type_ptr->to_string(cast_res_col_dt->get_element(0));
                std::cerr << "cast result is not null, but expect null, " << real_value
                          << std::endl;
            }
            EXPECT_TRUE(cast_res_col_nullable->is_null_at(0));
            return;
        }
        EXPECT_FALSE(cast_res_col_nullable->is_null_at(0));
        auto actual_value =
                binary_cast<typename T::underlying_value, T>(cast_res_col_dt->get_element(0));
        EXPECT_EQ(actual_value, expected);
    }

    int scale = 6;
    // test ordinary date time with microsecond: '2024-12-31 23:59:59.999999'
    uint16_t year_ = 2024;
    uint8_t month_ = 12;
    uint8_t day_ = 31;
    uint8_t hour_ = 23;
    uint8_t minute_ = 59;
    uint8_t second_ = 59;
    uint32_t microsecond_ = 999999;
    uint32_t microsecond_need_round_ = 9999995;

    // uint16_t year_two_digits_ = 24;
    uint8_t month_one_digit_ = 1;
    uint8_t day_one_digit_ = 2;
    uint8_t hour_one_digit_ = 3;
    uint8_t minute_one_digit_ = 4;
    uint8_t second_one_digit_ = 5;

    std::vector<DateV2Value<DateTimeV2ValueType>> expected_res_datetimes = {
            {year_, month_, day_, hour_, minute_, second_, microsecond_},
            {year_, month_one_digit_, day_one_digit_, hour_one_digit_, minute_one_digit_,
             second_one_digit_, microsecond_},
    };
    std::vector<DateV2Value<DateTimeV2ValueType>> expected_res_datetimes_only_date = {
            {year_, month_, day_, 0, 0, 0, 0},
            {year_, month_one_digit_, day_one_digit_, 0, 0, 0, 0},
    };
    std::vector<DateV2Value<DateV2ValueType>> expected_res_dates = {
            {year_, month_, day_, 0, 0, 0, 0},
            {year_, month_one_digit_, day_one_digit_, 0, 0, 0, 0},
    };

    DateV2Value<DateV2ValueType> datetime_round_need_round_ = {year_,
                                                               month_one_digit_,
                                                               day_one_digit_,
                                                               hour_one_digit_,
                                                               minute_one_digit_,
                                                               second_one_digit_,
                                                               microsecond_need_round_};
    DateV2Value<DateV2ValueType> expected_res_datetime_round_ = {2025, 1, 1, 0, 0, 0, 0};

    // std::ispunct except '+'
    std::string valid_date_separators_ = R"(!"#$%&'()*,-./:;<=>?@[\]^_`{|}~)";
    // std::string valid_date_separators_ = R"(!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~)";

    // invalid seperators between year_, month and day
    std::string alpha_ = R"(abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXY)";
    // R"( abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ)";
};

/*
MySQL:
select cast('2025-3/31 11%34|59.999999+08:00' as datetime(6));
+--------------------------------------------------------+
| cast('2025-3/31 11%34|59.999999+08:00' as datetime(6)) |
+--------------------------------------------------------+
| 2025-03-31 03:34:59.999999                             |
+--------------------------------------------------------+
1 row in set, 1 warning (0.01 sec)

PG:
ERROR:  invalid input syntax for type timestamp: "2025-3/31 11%34|59.999999+08:00"


MySQL special cases
mysql>  select cast('2025-^%$!@12---31 23:59:59.999999' as datetime(6));
+----------------------------------------------------------+
| cast('2025-^%$!@12---31 23:59:59.999999' as datetime(6)) |
+----------------------------------------------------------+
| 2025-12-31 23:59:59.999999                               |
+----------------------------------------------------------+
1 row in set, 1 warning (0.00 sec)

PG:
postgres=#  select cast('2025-^%$!@12---31 23:59:59.999999' as timestamp(6));
ERROR:  invalid input syntax for type timestamp: "2025-^%$!@12---31 23:59:59.999999"
LINE 1: select cast('2025-^%$!@12---31 23:59:59.999999' as timestamp...

*/
TEST_F(CastStringToDateTimeTest, normal_cases) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    state._enable_ansi_mode = false;
    FunctionContext fn_context;
    fn_context._state = &state;

    auto test_datetime_func = [&](const auto& expected_dt_values, bool only_date_part) {
        for (const auto& expected : expected_dt_values) {
            // test all supported date seperators
            for (const auto sep : valid_date_separators_) {
                // test datetime
                std::string str = fmt::format("{}{}{}{}{}", expected.year(), sep, expected.month(),
                                              sep, expected.day());
                if (!only_date_part) {
                    str += fmt::format(" {}{}{}{}{}.{}", expected.hour(), sep, expected.minute(),
                                       sep, expected.second(), expected.microsecond());
                }
                std::cout << "test cast string to datetimev2, test string: " << str << std::endl;
                test_cast(fn_context, scale, str, false, expected);

                if (only_date_part) {
                    // with leading and trailing spaces
                    str = fmt::format("    {}{}{}{}{}   ", expected.year(), sep, expected.month(),
                                      sep, expected.day());
                    std::cout << "test cast string to datetimev2, only date part, with leading and "
                                 "trailing spaces, "
                                 "test "
                                 "string: "
                              << str << " |" << std::endl;
                    test_cast(fn_context, scale, str, false, expected);
                } else {
                    // arbitrary count of spaces between date and time
                    str = fmt::format("{}{}{}{}{}     {}{}{}{}{}.{}", expected.year(), sep,
                                      expected.month(), sep, expected.day(), expected.hour(), sep,
                                      expected.minute(), sep, expected.second(),
                                      expected.microsecond());
                    std::cout << "test cast string to datetimev2, arbitrary count of spaces "
                                 "between date "
                                 "and time, test string: "
                              << str << std::endl;
                    test_cast(fn_context, scale, str, false, expected);
                    // with leading and trailing spaces, and with T separator between date and time
                    str = fmt::format("    {}{}{}{}{}T{}{}{}{}{}.{}   ", expected.year(), sep,
                                      expected.month(), sep, expected.day(), expected.hour(), sep,
                                      expected.minute(), sep, expected.second(),
                                      expected.microsecond());
                    std::cout << "test cast string to datetimev2 with leading and trailing spaces, "
                                 "test "
                                 "string: "
                              << str << " |" << std::endl;
                    test_cast(fn_context, scale, str, false, expected);
                }
            }

            // no separators
            char str[256] = {0};
            auto printed_len = std::sprintf(str, "%04d%02d%02d", expected.year(), expected.month(),
                                            expected.day());
            if (!only_date_part) {
                std::sprintf(str + printed_len, "%02d%02d%02d.%06d", expected.hour(),
                             expected.minute(), expected.second(), expected.microsecond());
            }
            std::cout << "test cast string to datetimev2, no separators, test string: " << str
                      << std::endl;
            test_cast(fn_context, scale, str, false, expected);

            // with T between date and time
            if (!only_date_part) {
                std::sprintf(str, "%04d%02d%02dT%02d%02d%02d.%06d", expected.year(),
                             expected.month(), expected.day(), expected.hour(), expected.minute(),
                             expected.second(), expected.microsecond());
                std::cout << "test cast string to datetimev2, no separators, with T, test string: "
                          << str << std::endl;
                test_cast(fn_context, scale, str, false, expected);
            }

            if (only_date_part) {
                std::sprintf(str, "  %04d%02d%02d  ", expected.year(), expected.month(),
                             expected.day());
                test_cast(fn_context, scale, str, false, expected);
            } else {
                // with leading and trailing spaces
                std::printf(str, "  %04d%02d%02d%02d%02d%02d.%06d  ", expected.year(),
                            expected.month(), expected.day(), expected.hour(), expected.minute(),
                            expected.second(), expected.microsecond());
                test_cast(fn_context, scale, str, false, expected);

                // with leading and trailing spaces, and with T separator between date and time
                std::printf(str, "  %04d%02d%02dT%02d%02d%02d.%06d  ", expected.year(),
                            expected.month(), expected.day(), expected.hour(), expected.minute(),
                            expected.second(), expected.microsecond());
                test_cast(fn_context, scale, str, false, expected);
            }
        }
    };
    test_datetime_func(expected_res_datetimes, false);
    test_datetime_func(expected_res_datetimes_only_date, true);

    // test date
    auto test_date_func = [&](const auto& expected_dt_values) {
        for (const auto& expected : expected_dt_values) {
            // test all supported date seperators
            for (const auto sep : valid_date_separators_) {
                auto str = fmt::format("{}{}{}{}{}", expected.year(), sep, expected.month(), sep,
                                       expected.day());
                std::cout << "test cast string to datev2, test string: " << str << std::endl;
                test_cast(fn_context, scale, str, false, expected);

                // leading and trailing spaces
                str = fmt::format("   {}{}{}{}{}   ", expected.year(), sep, expected.month(), sep,
                                  expected.day());
                std::cout
                        << "test cast string to datev2, leading and trailing spaces, test string: "
                        << str << std::endl;
                test_cast(fn_context, scale, str, false, expected);

                // date, string with time part
                str = fmt::format("{}{}{}{}{} {}{}{}{}{}.{}", expected.year(), sep,
                                  expected.month(), sep, expected.day(), hour_, sep, minute_, sep,
                                  second_, microsecond_);
                test_cast(fn_context, scale, str, false, expected);

                // date, string with time part, leading and trailing spaces
                str = fmt::format("  {}{}{}{}{} {}{}{}{}{}.{} ", expected.year(), sep,
                                  expected.month(), sep, expected.day(), hour_, sep, minute_, sep,
                                  second_, microsecond_);
                test_cast(fn_context, scale, str, false, expected);
            }

            // no separators
            char str[256] = {0};
            std::sprintf(str, "%04d%02d%02d", expected.year(), expected.month(), expected.day());
            std::cout << "test cast string to datev2, no separators, test string: " << str
                      << std::endl;
            test_cast(fn_context, scale, str, false, expected);

            // leading and trailing spaces
            std::sprintf(str, "  %04d%02d%02d  ", expected.year(), expected.month(),
                         expected.day());
            test_cast(fn_context, scale, str, false, expected);

            // date, string with time part
            std::sprintf(str, "%04d%02d%02d%02d%02d%02d.%06d", expected.year(), expected.month(),
                         expected.day(), hour_, minute_, second_, microsecond_);
            std::cout << "test cast string to datetimev2, no separators, with T, test string: "
                      << str << std::endl;
            test_cast(fn_context, scale, str, false, expected);

            // date, string with time part, leading and trailing spaces
            std::sprintf(str, "   %04d%02d%02d%02d%02d%02d.%06d ", expected.year(),
                         expected.month(), expected.day(), hour_, minute_, second_, microsecond_);
            test_cast(fn_context, scale, str, false, expected);

            // with T between date and time
            std::sprintf(str, "%04d%02d%02dT%02d%02d%02d.%06d", expected.year(), expected.month(),
                         expected.day(), hour_, minute_, second_, microsecond_);
            std::cout << "test cast string to datev2, no separators, with T, test string: " << str
                      << std::endl;
            test_cast(fn_context, scale, str, false, expected);

            // with T between date and time, leading and trailing spaces
            std::sprintf(str, "  %04d%02d%02dT%02d%02d%02d.%06d  ", expected.year(),
                         expected.month(), expected.day(), hour_, minute_, second_, microsecond_);
            test_cast(fn_context, scale, str, false, expected);
        }
    };
    test_date_func(expected_res_dates);
}

TEST_F(CastStringToDateTimeTest, normal_cases_two_digits_year) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    state._enable_ansi_mode = false;
    FunctionContext fn_context;
    fn_context._state = &state;

    for (uint16_t year = 0; year < 100; ++year) {
        uint16_t exptected_year = year + (year < YY_PART_YEAR ? 2000 : 1900);
        {
            auto test_datetime_func = [&](const auto& expected, bool only_date_part) {
                char str[256] = {0};
                // with separator
                auto printed_len = std::sprintf(str, "%02d-%02d-%02d", year, month_, day_);
                if (!only_date_part) {
                    std::sprintf(str + printed_len, " %02d:%02d:%02d.%06d", hour_, minute_, second_,
                                 microsecond_);
                }
                std::cout << "test cast string to datetimev2, two digits year, test string: " << str
                          << std::endl;
                test_cast(fn_context, scale, str, false, expected);

                if (only_date_part) {
                    std::sprintf(str, "  %02d-%02d-%02d  ", year, month_, day_);
                    test_cast(fn_context, scale, str, false, expected);
                } else {
                    // with leading and trailing spaces
                    // str = std::format("  {: >2}-{: >}-{: >} {}:{}:{}.{}  ", year, month_, day_,  hour_, minute_, second_, microsecond_);
                    std::sprintf(str, "  %02d-%02d-%02d %02d:%02d:%02d.%06d  ", year, month_, day_,
                                 hour_, minute_, second_, microsecond_);
                    std::cout
                            << "test cast string to datetimev2, two digits year, with leading and "
                               "trailing "
                               "spaces, "
                               "test string: "
                            << str << " |" << std::endl;
                    test_cast(fn_context, scale, str, false, expected);

                    // with leading and trailing spaces, and with T separator between date and time
                    std::sprintf(str, "  %02d-%02d-%02dT%02d:%02d:%02d.%06d  ", year, month_, day_,
                                 hour_, minute_, second_, microsecond_);
                    std::cout
                            << "test cast string to datetimev2, two digits year, with leading and "
                               "trailing "
                               "spaces, "
                               "test string: "
                            << str << " |" << std::endl;
                    test_cast(fn_context, scale, str, false, expected);
                }

                // no separator
                printed_len = std::sprintf(str, "%02d%02d%02d", year, month_, day_);
                if (!only_date_part) {
                    std::sprintf(str + printed_len, "%02d%02d%02d.%06d", hour_, minute_, second_,
                                 microsecond_);
                }
                std::cout << "test cast string to datetimev2, only date part, no separator, test "
                             "string: "
                          << str << std::endl;
                test_cast(fn_context, scale, str, false, expected);

                // with leading and trailing spaces
                if (only_date_part) {
                    std::sprintf(str, "  %02d%02d%02d ", year, month_, day_);
                    test_cast(fn_context, scale, str, false, expected);
                } else {
                    std::sprintf(str, "  %02d%02d%02d%02d%02d%02d.%06d ", year, month_, day_, hour_,
                                 minute_, second_, microsecond_);
                    test_cast(fn_context, scale, str, false, expected);

                    // with leading and trailing spaces, and with T separator between date and time
                    std::sprintf(str, "  %02d%02d%02dT%02d%02d%02d.%06d  ", year, month_, day_,
                                 hour_, minute_, second_, microsecond_);
                    test_cast(fn_context, scale, str, false, expected);
                }
            };
            DateV2Value<DateTimeV2ValueType> expected_res_datetime {
                    exptected_year, month_, day_, hour_, minute_, second_, microsecond_};
            DateV2Value<DateTimeV2ValueType> expected_res_datetime_no_date_part {
                    exptected_year, month_, day_, 0, 0, 0, 0};
            test_datetime_func(expected_res_datetime, false);
            test_datetime_func(expected_res_datetime_no_date_part, true);
        }

        {
            auto test_date_func = [&](const auto& expected) {
                char str[256] = {0};
                // with separator
                std::sprintf(str, "%02d-%02d-%02d", year, month_, day_);
                std::cout << "test cast string to datetimev2, two digits year, test string: " << str
                          << std::endl;
                test_cast(fn_context, scale, str, false, expected);

                // with leading and trailing spaces
                std::sprintf(str, "  %02d-%02d-%02d  ", year, month_, day_);
                test_cast(fn_context, scale, str, false, expected);

                // date, string with time part
                std::sprintf(str, "%02d-%02d-%02d %02d:%02d:%02d.%06d", year, month_, day_, hour_,
                             minute_, second_, microsecond_);
                test_cast(fn_context, scale, str, false, expected);

                // with leading and trailing spaces
                std::sprintf(str, "  %02d-%02d-%02d %02d:%02d:%02d.%06d  ", year, month_, day_,
                             hour_, minute_, second_, microsecond_);
                test_cast(fn_context, scale, str, false, expected);

                // with leading and trailing spaces, and with T separator between date and time
                std::sprintf(str, "  %02d-%02d-%02dT%02d:%02d:%02d.%06d  ", year, month_, day_,
                             hour_, minute_, second_, microsecond_);
                test_cast(fn_context, scale, str, false, expected);

                // no separator
                std::sprintf(str, "%02d%02d%02d", year, month_, day_);
                test_cast(fn_context, scale, str, false, expected);

                // with leading and trailing spaces
                std::sprintf(str, "  %02d%02d%02d ", year, month_, day_);
                test_cast(fn_context, scale, str, false, expected);

                // date, string with time part
                std::sprintf(str, "%02d%02d%02d%02d%02d%02d.%06d", year, month_, day_, hour_,
                             minute_, second_, microsecond_);
                test_cast(fn_context, scale, str, false, expected);

                std::sprintf(str, "  %02d%02d%02d%02d%02d%02d.%06d ", year, month_, day_, hour_,
                             minute_, second_, microsecond_);
                test_cast(fn_context, scale, str, false, expected);

                // with leading and trailing spaces, and with T separator between date and time
                std::sprintf(str, "  %02d%02d%02dT%02d%02d%02d.%06d  ", year, month_, day_, hour_,
                             minute_, second_, microsecond_);
                test_cast(fn_context, scale, str, false, expected);
            };
            DateV2Value<DateV2ValueType> expected_res_date {
                    exptected_year, month_, day_, 0, 0, 0, 0};
            test_date_func(expected_res_date);
        }
    }
}

// test timezone names
TEST_F(CastStringToDateTimeTest, normal_cases_with_time_zone) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    state._enable_ansi_mode = false;
    FunctionContext fn_context;
    fn_context._state = &state;

    DateV2Value<DateTimeV2ValueType> expected_res_datetime {year_,   month_,  day_,        hour_,
                                                            minute_, second_, microsecond_};
    const auto* timezones = get_time_zone_cache();
    for (const auto sep : valid_date_separators_) {
        for (const auto& [tz_name, tz] : *timezones) {
            auto given = cctz::convert(cctz::civil_second {}, tz);
            auto local = cctz::convert(cctz::civil_second {}, state.timezone_obj());
            auto sec_offset =
                    std::chrono::duration_cast<std::chrono::seconds>(given - local).count();
            auto expected_res_datetime_with_tz = expected_res_datetime;
            auto b = expected_res_datetime_with_tz.date_add_interval<TimeUnit::SECOND>(
                    TimeInterval {TimeUnit::SECOND, sec_offset, false});
            EXPECT_TRUE(b);

            auto str = fmt::format("{}{}{}{}{} {}:{}:{}.{}{}", year_, sep, month_, sep, day_, hour_,
                                   minute_, second_, microsecond_, tz_name);
            std::cout << "test cast string to datetimev2 with timezone name, test string: " << str
                      << std::endl;
            test_cast(fn_context, scale, str, false, expected_res_datetime_with_tz);

            // arbitrary count of spaces between time and timezone name
            str = fmt::format("   {}{}{}{}{} {}:{}:{}.{}   {}   ", year_, sep, month_, sep, day_,
                              hour_, minute_, second_, microsecond_, tz_name);
            std::cout << "test cast string to datetimev2 with timezone, with spaces between time "
                         "and timezone name, test string: "
                      << str << std::endl;
            test_cast(fn_context, scale, str, false, expected_res_datetime_with_tz);
        }

        // with timezone offset
        auto str = fmt::format("  {}{}{}{}{} {}:{}:{}.{}+08:00 ", year_, sep, month_, sep, day_,
                               hour_, minute_, second_, microsecond_);
        std::cout << "test cast string to datetimev2 with timezone offset, test string: " << str
                  << std::endl;
        uint8_t hour_minus_8 = hour_ - 8;
        auto expected_res_datetime_with_tz_offset = DateV2Value<DateTimeV2ValueType> {
                year_, month_, day_, hour_minus_8, minute_, second_, microsecond_};
        test_cast(fn_context, scale, str, false, expected_res_datetime_with_tz_offset);

        // arbitrary spaces before timezone offset
        str = fmt::format("  {}{}{}{}{} {}:{}:{}.{}   +08:00 ", year_, sep, month_, sep, day_,
                          hour_, minute_, second_, microsecond_);
        std::cout << "test cast string to datetimev2 with timezone offset, arbitrary spaces before "
                     "timezone offset, test string: "
                  << str << std::endl;
        test_cast(fn_context, scale, str, false, expected_res_datetime_with_tz_offset);
    }
}

TEST_F(CastStringToDateTimeTest, round) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    FunctionContext fn_context;
    fn_context._state = &state;

    {
        std::string dt_str = "2025-04-01 23:59:59.9999999";
        DateV2Value<DateTimeV2ValueType> expected_res_datetime {2025, 4, 2, 0, 0, 0, 0};
        std::cout << "test cast string to datetimev2 round, test string: " << dt_str << std::endl;
        state._enable_ansi_mode = false;
        test_cast(fn_context, scale, dt_str, false, expected_res_datetime);
        state._enable_ansi_mode = true;
        test_cast(fn_context, scale, dt_str, false, expected_res_datetime);

        state._enable_ansi_mode = false;
        test_cast(fn_context, 3, dt_str, false, expected_res_datetime);
        state._enable_ansi_mode = true;
        test_cast(fn_context, 3, dt_str, false, expected_res_datetime);
    }
    {
        std::string dt_str = "2024-12-31 23:59:59.9999999";
        DateV2Value<DateTimeV2ValueType> expected_res_datetime {2025, 1, 1, 0, 0, 0, 0};
        std::cout << "test cast string to datetimev2 round, test string: " << dt_str << std::endl;
        state._enable_ansi_mode = false;
        test_cast(fn_context, scale, dt_str, false, expected_res_datetime);
        state._enable_ansi_mode = true;
        test_cast(fn_context, scale, dt_str, false, expected_res_datetime);

        state._enable_ansi_mode = false;
        test_cast(fn_context, 3, dt_str, false, expected_res_datetime);
        state._enable_ansi_mode = true;
        test_cast(fn_context, 3, dt_str, false, expected_res_datetime);
    }
}

TEST_F(CastStringToDateTimeTest, invalid_date_separators) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    FunctionContext fn_context;
    fn_context._state = &state;

    // non-strict mode
    state._enable_ansi_mode = false;
    for (const auto sep : alpha_) {
        std::string str = fmt::format("{}{}{}{}{} {}:{}:{}.{}", year_, sep, month_, sep, day_,
                                      hour_, minute_, second_, microsecond_);
        std::cout << "test cast string to datetimev2, test string: " << str << std::endl;
        test_cast(fn_context, scale, str, true, DateV2Value<DateTimeV2ValueType> {});
    }
    state._enable_ansi_mode = true;
    for (const auto sep : alpha_) {
        std::string str = fmt::format("{}{}{}{}{} {}:{}:{}.{}", year_, sep, month_, sep, day_,
                                      hour_, minute_, second_, microsecond_);
        std::cout << "test cast string to datetimev2, test string: " << str << std::endl;
        test_cast(fn_context, scale, str, true, DateV2Value<DateTimeV2ValueType> {}, true);
    }
}

TEST_F(CastStringToDateTimeTest, invalid_date_time_separators) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    FunctionContext fn_context;
    fn_context._state = &state;
    std::string alpha_except_t = R"(abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSUVWXY)";

    for (const auto dt_sep : alpha_except_t) {
        // non-strict mode
        state._enable_ansi_mode = false;
        for (const auto sep : valid_date_separators_) {
            std::string str = fmt::format("{}{}{}{}{}{}{}:{}:{}.{}", year_, sep, month_, sep, day_,
                                          dt_sep, hour_, minute_, second_, microsecond_);
            std::cout
                    << "test cast string to datetimev2, invalid date time separator, test string: "
                    << str << std::endl;
            test_cast(fn_context, scale, str, true, DateV2Value<DateTimeV2ValueType> {});
        }
        // strict mode
        state._enable_ansi_mode = true;
        for (const auto sep : valid_date_separators_) {
            std::string str = fmt::format("{}{}{}{}{}{}{}:{}:{}.{}", year_, sep, month_, sep, day_,
                                          dt_sep, hour_, minute_, second_, microsecond_);
            std::cout << "test cast string to datetimev2, invalid date time separator, strict "
                         "mode, test string: "
                      << str << std::endl;
            test_cast(fn_context, scale, str, false, DateV2Value<DateTimeV2ValueType> {}, true);
        }
    }
}

TEST_F(CastStringToDateTimeTest, invalid_timezone_names) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    state._enable_ansi_mode = false;
    FunctionContext fn_context;
    fn_context._state = &state;

    std::string sep("-");
    std::vector<std::string> invalid_timezones {
            "Asia/Shanghaii", " Asia/Shanghaii", "abc", "  abc", "AM", "   AM", "PM", "   PM"};
    {
        std::string timezone("Asia/Shanghaii");
        std::string str = fmt::format("{}{}{}{}{} {}:{}:{}.{}{}", year_, sep, month_, sep, day_,
                                      hour_, minute_, second_, microsecond_, timezone);
        std::cout << "test cast string to datetimev2 with invalid timezone name, non strict mode, "
                     "test string: "
                  << str << std::endl;
        state._enable_ansi_mode = false;
        test_cast(fn_context, scale, str, true, DateV2Value<DateTimeV2ValueType> {});

        // strict mode
        state._enable_ansi_mode = true;
        std::cout << "test cast string to datetimev2 with invalid timezone name, strict mode, test "
                     "string: "
                  << str << std::endl;
        test_cast(fn_context, scale, str, false, DateV2Value<DateTimeV2ValueType> {}, true);
    }
}
TEST_F(CastStringToDateTimeTest, invalid_datetime_formats) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    FunctionContext fn_context;
    fn_context._state = &state;

    // test invalid spaces and invalid separators between date and time
    std::vector<std::string> invalid_datetimes {
            "2025 04-01 16:59:59.999999",   "2025   04-01 16:59:59.999999",
            "2025-04 01 16:59:59.999999",   "2025-04   01 16:59:59.999999",
            "2025-04-01 16 59:59.999999",   "2025-04-01 16   59:59.999999",
            "2025-04-01 16:59 59.999999",   "2025-04-01 16:59  59.999999",
            "2025-04-01 16:59:59 999999",   "2025-04-01 16:59:59  999999",
            "2025-04-01 16:59:59-999999",   "2025-04-01 16:59:59/999999",
            "2025-04-01 16:59:59|999999",   "2025-04-01TT16:59:59.999999",
            "2025-04-01 T 16:59:59.999999",
    };
    for (const auto& dt_str : invalid_datetimes) {
        std::cout << "test cast string to datetimev2 with invalid datetime format, test string: "
                  << dt_str << std::endl;
        state._enable_ansi_mode = false;
        test_cast(fn_context, scale, dt_str, true, DateV2Value<DateTimeV2ValueType> {});
        state._enable_ansi_mode = true;
        test_cast(fn_context, scale, dt_str, false, DateV2Value<DateTimeV2ValueType> {}, true);
    }
}
TEST_F(CastStringToDateTimeTest, invalid_datetime_with_separator) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    FunctionContext fn_context;
    fn_context._state = &state;

    // test invalid spaces and invalid separators between date and time
    std::vector<std::string> invalid_datetimes {
            "",
            " ",
            "abc",
            "T",
            "UTC",
            "Asia/Shanghai",
            "aaaa-12-30 23:59:59.999999",
            "2025-bb-30 23:59:59.999999",
            "2025-12-cc 23:59:59.999999",
            "2025-12-30 aa:59:59.999999",
            "2025-12-30 23:bb:59.999999",
            "2025-12-30 23:59:cc.999999",
            "2025-12-30 23:59:59.aa",
            // invalid year
            "4294967297-11-11 17:15:39.999999",
            "20250-11-11 17:15:39.999999",
            "-2025-11-11 17:15:39.999999",
            // invalid month
            "2025-4294967297-11 17:15:39.999999",
            "2025-13-11 17:15:39.999999",
            "2025-90-11 17:15:39.999999",
            "2025-100-11 17:15:39.999999",
            // invalid day
            "2025-12-4294967297 17:15:39.999999",
            "2025-12-32 17:15:39.999999",
            "2025-12-100 17:15:39.999999",
            // invalid hour
            "2025-12-30 4294967297:15:39.999999",
            "2025-12-30 25:15:39.999999",
            "2025-12-30 100:15:39.999999",
            // "2025-12-30 -20:15:39.999999", // it's OK in MySQL: 2025-12-30 20:15:40
            // invalid minute
            "2025-12-30 20:4294967295:39.999999",
            "2025-12-30 20:60:39.999999",
            // "2025-12-30 20:-20:39.999999", // it's OK in MySQL: 2025-12-30 20:20:40
            "2025-12-30 20:111:39.999999",
            // invalid second
            "2025-12-30 20:59:4294967295.999999",
            "2025-12-30 20:59:60.999999",
            "2025-12-30 20:59:100.999999",
            "2025-03-11 17:15:39abc",
            "2025-03-11 17:15:39 abc",
            "2025-12-31 T 23:59:59.999999",
            "2025-12-31TT23:59:59.999999",
    };
    for (const auto& dt_str : invalid_datetimes) {
        std::cout << "test cast string to datetimev2 with invalid datetime, test string: " << dt_str
                  << std::endl;
        state._enable_ansi_mode = false;
        test_cast(fn_context, scale, dt_str, true, DateV2Value<DateTimeV2ValueType> {});
        state._enable_ansi_mode = true;
        test_cast(fn_context, scale, dt_str, false, DateV2Value<DateTimeV2ValueType> {}, true);
    }
}
TEST_F(CastStringToDateTimeTest, overflow) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    FunctionContext fn_context;
    fn_context._state = &state;

    // test invalid spaces and invalid separators between date and time
    std::vector<std::string> invalid_datetimes {
            "9999-12-31 23:59:59.9999999",
            "10000-01-01 00:00:00.000000",
    };
    for (const auto& dt_str : invalid_datetimes) {
        std::cout << "test cast string to datetimev2 overflow, test string: " << dt_str
                  << std::endl;
        state._enable_ansi_mode = false;
        test_cast(fn_context, scale, dt_str, true, DateV2Value<DateTimeV2ValueType> {});
        state._enable_ansi_mode = true;
        test_cast(fn_context, scale, dt_str, false, DateV2Value<DateTimeV2ValueType> {}, true);
    }
}
TEST_F(CastStringToDateTimeTest, special_formats) {
    MockRuntimeState state;
    state._timezone = "UTC";
    EXPECT_TRUE(TimezoneUtils::find_cctz_time_zone(state._timezone, state._timezone_obj));
    FunctionContext fn_context;
    fn_context._state = &state;

    std::vector<std::string> invalid_datetimes {"20251231    23:59:59.9999995"};
    for (const auto& dt_str : invalid_datetimes) {
        std::cout << "test cast string to datetimev2 special formats, test string: " << dt_str
                  << std::endl;
        state._enable_ansi_mode = false;
        test_cast(fn_context, scale, dt_str, true, DateV2Value<DateTimeV2ValueType> {});
        state._enable_ansi_mode = true;
        test_cast(fn_context, scale, dt_str, false, DateV2Value<DateTimeV2ValueType> {}, true);
    }
}
} // namespace doris