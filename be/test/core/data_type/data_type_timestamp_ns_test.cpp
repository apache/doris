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

#include "core/data_type/data_type_timestamp_ns.h"

#include <arrow/api.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <limits>
#include <memory>
#include <orc/Vector.hh>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/data_type_serde/data_type_timestamp_ns_serde.h"
#include "core/string_buffer.hpp"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_parameters.h"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"
#include "util/mysql_row_buffer.h"
#include "util/slice.h"
#include "util/timezone_utils.h"

namespace doris {

TEST(DataTypeTimeStampNsTest, Int64EpochRangeAndOrdering) {
    const TimeStampNsValue epoch(0);
    const TimeStampNsValue before_epoch(-1);
    const TimeStampNsValue minimum(std::numeric_limits<int64_t>::min());
    const TimeStampNsValue maximum(std::numeric_limits<int64_t>::max());

    EXPECT_EQ(epoch.to_string(9), "1970-01-01 00:00:00.000000000");
    EXPECT_EQ(before_epoch.to_string(9), "1969-12-31 23:59:59.999999999");
    EXPECT_EQ(minimum.to_string(9), "1677-09-21 00:12:43.145224192");
    EXPECT_EQ(maximum.to_string(9), "2262-04-11 23:47:16.854775807");
    EXPECT_LT(minimum, before_epoch);
    EXPECT_LT(before_epoch, epoch);
    EXPECT_LT(epoch, maximum);
}

TEST(DataTypeTimeStampNsTest, NegativeEpochUsesFloorSecondAndNormalizedFraction) {
    struct TestCase {
        int64_t epoch_nanos;
        int64_t epoch_seconds;
        uint32_t nanosecond;
    };
    const std::vector<TestCase> cases = {
            {-1000000001, -2, 999999999},
            {-1000000000, -1, 0},
            {-999999999, -1, 1},
            {-1, -1, 999999999},
            {0, 0, 0},
            {1, 0, 1},
    };

    for (const auto& test_case : cases) {
        const TimeStampNsValue value(test_case.epoch_nanos);
        EXPECT_EQ(value.epoch_seconds(), test_case.epoch_seconds);
        EXPECT_EQ(value.nanosecond(), test_case.nanosecond);
        EXPECT_EQ(
                static_cast<__int128>(value.epoch_seconds()) * TimeStampNsValue::NANOS_PER_SECOND +
                        value.nanosecond(),
                test_case.epoch_nanos);
    }
}

TEST(DataTypeTimeStampNsTest, ParseAtFixedNanosecondPrecision) {
    int64_t value = 0;

    ASSERT_TRUE(parse_timestamp_ns(StringRef("1970-01-01 00:00:00.12345675"), &value).ok());
    EXPECT_EQ(TimeStampNsValue(value).to_string(), "1970-01-01 00:00:00.123456750");

    ASSERT_TRUE(parse_timestamp_ns(StringRef("1969-12-31 23:59:59.999999999"), &value).ok());
    EXPECT_EQ(value, -1);

    ASSERT_TRUE(parse_timestamp_ns(StringRef("1970-01-01 00:00:00.999999995"), &value).ok());
    EXPECT_EQ(TimeStampNsValue(value).to_string(), "1970-01-01 00:00:00.999999995");

    ASSERT_TRUE(parse_timestamp_ns(StringRef("1970-01-01 00:00:00.9999999995"), &value).ok());
    EXPECT_EQ(TimeStampNsValue(value).to_string(), "1970-01-01 00:00:01.000000000");
}

TEST(DataTypeTimeStampNsTest, ParseTimezoneSuffixInSessionTimezone) {
    TimezoneUtils::load_timezones_to_cache();
    cctz::time_zone shanghai;
    ASSERT_TRUE(cctz::load_time_zone("Asia/Shanghai", &shanghai));

    int64_t value = 0;
    auto status =
            parse_timestamp_ns(StringRef("2023-08-17T01:41:18.123456789Z"), &value, &shanghai);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(TimeStampNsValue(value).to_string(9), "2023-08-17 09:41:18.123456789");

    ASSERT_TRUE(parse_timestamp_ns(StringRef("2023-08-17T01:41:18.123456789America/Los_Angeles"),
                                   &value, &shanghai)
                        .ok());
    EXPECT_EQ(TimeStampNsValue(value).to_string(9), "2023-08-17 16:41:18.123456789");

    EXPECT_FALSE(
            parse_timestamp_ns(StringRef("1677-09-21T00:12:43.145224192+14:00"), &value, &shanghai)
                    .ok());
    EXPECT_FALSE(
            parse_timestamp_ns(StringRef("2262-04-11T23:47:16.854775807-01:00"), &value, &shanghai)
                    .ok());
}

TEST(DataTypeTimeStampNsTest, ParseAcceptsFractionalWidthsAndRejectsMalformedValues) {
    struct ValidCase {
        const char* input;
        const char* expected;
    };
    const std::vector<ValidCase> valid_cases = {
            {"2024-02-29 12:34:56.1234567", "2024-02-29 12:34:56.123456700"},
            {"2024-02-29 12:34:56.12345678", "2024-02-29 12:34:56.123456780"},
            {"2024-02-29 12:34:56.123456789", "2024-02-29 12:34:56.123456789"},
            {"2024-02-29 12:34:56.1234567894", "2024-02-29 12:34:56.123456789"},
            {"2024-02-29 12:34:56.1234567895", "2024-02-29 12:34:56.123456790"},
            {"2024-02-29 12:34:56", "2024-02-29 12:34:56.000000000"},
    };

    for (const auto& test_case : valid_cases) {
        int64_t value = 0;
        ASSERT_TRUE(parse_timestamp_ns(StringRef(test_case.input), &value).ok()) << test_case.input;
        EXPECT_EQ(TimeStampNsValue(value).to_string(), test_case.expected);
    }

    const std::vector<const char*> invalid_values = {
            "",
            "not-a-date",
            "2023-02-29 00:00:00.000000000",
            "2024-13-01",
            "2024-01-01 24:00:00",
            "2024-01-01 00:00:00.trailing",
            "2024-01-01 00:00:00.123.456",
            "2024-01-01.123 00:00:00",
            "2024.01-01 00:00:00",
    };
    for (const char* input : invalid_values) {
        int64_t value = 0;
        EXPECT_FALSE(parse_timestamp_ns(StringRef(input), &value).ok()) << input;
    }
}

TEST(DataTypeTimeStampNsTest, RejectValuesOutsideEpochRange) {
    int64_t value = 0;
    EXPECT_FALSE(parse_timestamp_ns(StringRef("0000-01-01 00:00:00.000000000"), &value).ok());
    EXPECT_FALSE(parse_timestamp_ns(StringRef("1677-09-21 00:12:43.145224191"), &value).ok());
    EXPECT_FALSE(parse_timestamp_ns(StringRef("2262-04-11 23:47:16.854775808"), &value).ok());
    EXPECT_FALSE(parse_timestamp_ns(StringRef("9999-12-31 23:59:59.999999999"), &value).ok());
}

TEST(DataTypeTimeStampNsTest, CivilRoundTripPreservesSubMicrosecondDigits) {
    DateV2Value<DateTimeV2ValueType> civil;
    civil.unchecked_set_time(2024, 2, 29, 23, 59, 58, 123456);

    TimeStampNsValue value;
    ASSERT_TRUE(value.from_datetime(civil, 789));
    EXPECT_EQ(value.to_string(9), "2024-02-29 23:59:58.123456789");
    EXPECT_EQ(value.year(), 2024);
    EXPECT_EQ(value.month(), 2);
    EXPECT_EQ(value.day(), 29);
    EXPECT_EQ(value.hour(), 23);
    EXPECT_EQ(value.minute(), 59);
    EXPECT_EQ(value.second(), 58);
    EXPECT_EQ(value.microsecond(), 123456);
    EXPECT_EQ(value.nanosecond_remainder(), 789);
    EXPECT_EQ(value.to_datetime().to_date_int_val(), civil.to_date_int_val());
}

TEST(DataTypeTimeStampNsTest, CalendarArithmeticPreservesSubMicrosecondDigits) {
    int64_t raw_value = 0;
    ASSERT_TRUE(parse_timestamp_ns(StringRef("2024-01-31 23:59:59.123456789"), &raw_value).ok());
    TimeStampNsValue value(raw_value);

    ASSERT_TRUE(value.date_add_interval<TimeUnit::MONTH>(TimeInterval(TimeUnit::MONTH, 1, false)));
    EXPECT_EQ(value.to_string(9), "2024-02-29 23:59:59.123456789");

    ASSERT_TRUE(
            value.date_add_interval<TimeUnit::SECOND>(TimeInterval(TimeUnit::SECOND, 1, false)));
    EXPECT_EQ(value.to_string(9), "2024-03-01 00:00:00.123456789");
}

TEST(DataTypeTimeStampNsTest, DiffTruncatesIncompleteUnitsTowardZero) {
    int64_t lhs_raw = 0;
    int64_t rhs_raw = 0;
    ASSERT_TRUE(parse_timestamp_ns(StringRef("1970-01-01 00:00:00.000000001"), &lhs_raw).ok());
    ASSERT_TRUE(parse_timestamp_ns(StringRef("1970-01-01 00:00:01.999999999"), &rhs_raw).ok());
    const TimeStampNsValue lhs(lhs_raw);
    const TimeStampNsValue rhs(rhs_raw);

    EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(lhs, rhs), 1);
    EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(rhs, lhs), -1);
    EXPECT_EQ(datetime_diff<TimeUnit::MILLISECOND>(lhs, rhs), 1999);
    EXPECT_EQ(datetime_diff<TimeUnit::MILLISECOND>(rhs, lhs), -1999);
    EXPECT_EQ(lhs.datetime_diff_in_microseconds(rhs), -1999999);
    EXPECT_EQ(rhs.datetime_diff_in_microseconds(lhs), 1999999);
}

TEST(DataTypeTimeStampNsTest, FactoryKeepsTimestampNsSeparateFromDateTimeV2) {
    const auto microseconds = create_datetimev2(6);
    const auto timestamp_ns = std::make_shared<DataTypeTimeStampNs>();

    EXPECT_EQ(microseconds->get_primitive_type(), TYPE_DATETIMEV2);
    EXPECT_THROW(create_datetimev2(7), Exception);
    EXPECT_THROW(create_datetimev2(8), Exception);
    EXPECT_THROW(create_datetimev2(9), Exception);
    EXPECT_EQ(timestamp_ns->get_primitive_type(), TYPE_TIMESTAMP_NS);
    EXPECT_EQ(timestamp_ns->get_storage_field_type(), FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS);
    EXPECT_EQ(timestamp_ns->get_scale(), 9);
    EXPECT_EQ(microseconds->get_family_name(), "DateTimeV2");
    EXPECT_EQ(timestamp_ns->get_family_name(), "TimeStampNs");
}

TEST(DataTypeTimeStampNsTest, SerDeRoundTripsTextProtobufAndBinary) {
    const auto type = std::make_shared<DataTypeTimeStampNs>();
    const auto serde = type->get_serde();
    auto source = type->create_column();
    DataTypeSerDe::FormatOptions options;
    const std::vector<std::string> inputs = {
            "1677-09-21 00:12:43.145224192", "1969-12-31 23:59:59.999999999",
            "1970-01-01 00:00:00.000000000", "2024-02-29 12:34:56.123456789",
            "2262-04-11 23:47:16.854775807",
    };
    for (const auto& input : inputs) {
        StringRef ref(input);
        ASSERT_TRUE(serde->from_string(ref, *source, options).ok()) << input;
    }
    const auto& source_data = assert_cast<const ColumnTimeStampNs&>(*source).get_data();

    PValues protobuf_values;
    ASSERT_TRUE(serde->write_column_to_pb(*source, protobuf_values, 0, source->size()).ok());
    auto protobuf_result = type->create_column();
    ASSERT_TRUE(serde->read_column_from_pb(*protobuf_result, protobuf_values).ok());
    const auto& protobuf_data = assert_cast<const ColumnTimeStampNs&>(*protobuf_result).get_data();
    EXPECT_EQ(protobuf_data, source_data);

    ColumnString::Chars binary;
    std::vector<size_t> offsets = {0};
    for (size_t row = 0; row < source->size(); ++row) {
        serde->write_one_cell_to_binary(*source, binary, row);
        offsets.push_back(binary.size());
    }
    constexpr size_t bytes_per_row = sizeof(uint8_t) + sizeof(uint8_t) + sizeof(int64_t);
    ASSERT_EQ(binary.size(), source->size() * bytes_per_row);
    auto binary_result = ColumnNullable::create(type->create_column(), ColumnUInt8::create());
    for (size_t row = 0; row < source->size(); ++row) {
        const uint8_t* begin = binary.data() + offsets[row];
        const uint8_t* end = DataTypeSerDe::deserialize_binary_to_column(begin, *binary_result);
        EXPECT_EQ(end - begin, bytes_per_row);
    }
    const auto& binary_data =
            assert_cast<const ColumnTimeStampNs&>(binary_result->get_nested_column()).get_data();
    EXPECT_EQ(binary_data, source_data);
}

/*
TEST(DataTypeTimeStampNsTest, SerDeRoundTripsArrowNanosecondsBeforeEpoch) {
    const auto type = std::make_shared<DataTypeTimeStampNs>();
    const auto serde = type->get_serde();
    auto source = type->create_column();
    auto& source_data = assert_cast<ColumnTimeStampNs&>(*source).get_data();
    source_data.push_back(TimeStampNsValue(-1));
    source_data.push_back(TimeStampNsValue(0));
    source_data.push_back(TimeStampNsValue(1234567890));

    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::NANO),
                                    arrow::default_memory_pool());
    ASSERT_TRUE(serde->write_column_to_arrow(*source, nullptr, &builder, 0, source->size(),
                                             cctz::utc_time_zone())
                        .ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());
    const auto* timestamps = assert_cast<const arrow::TimestampArray*>(array.get());
    EXPECT_EQ(timestamps->Value(0), -1);
    EXPECT_EQ(timestamps->Value(1), 0);
    EXPECT_EQ(timestamps->Value(2), 1234567890);

    auto result = type->create_column();
    ASSERT_TRUE(serde->read_column_from_arrow(*result, array.get(), 0, array->length(),
                                              cctz::utc_time_zone())
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(*result).get_data(), source_data);
}
*/

TEST(DataTypeTimeStampNsTest, DecodedTimestampUnitsAndValidation) {
    const auto type = std::make_shared<DataTypeTimeStampNs>();
    const auto serde = type->get_serde();
    const int64_t values[] = {-1, 0, 1};

    const auto check_unit = [&](DecodedTimeUnit unit, int64_t multiplier) {
        auto column = type->create_column();
        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT64;
        view.time_unit = unit;
        view.row_count = 3;
        view.values = reinterpret_cast<const uint8_t*>(values);
        ASSERT_TRUE(serde->read_column_from_decoded_values(*column, view).ok());
        const auto& data = assert_cast<const ColumnTimeStampNs&>(*column).get_data();
        ASSERT_EQ(data.size(), 3);
        EXPECT_EQ(data[0].epoch_nanos(), -multiplier);
        EXPECT_EQ(data[1].epoch_nanos(), 0);
        EXPECT_EQ(data[2].epoch_nanos(), multiplier);
    };

    check_unit(DecodedTimeUnit::MILLIS, TimeStampNsValue::NANOS_PER_MILLISECOND);
    check_unit(DecodedTimeUnit::MICROS, TimeStampNsValue::NANOS_PER_MICROSECOND);
    check_unit(DecodedTimeUnit::NANOS, 1);

    {
        auto column = type->create_column();
        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT64;
        view.time_unit = DecodedTimeUnit::UNKNOWN;
        view.row_count = 3;
        view.values = reinterpret_cast<const uint8_t*>(values);
        EXPECT_FALSE(serde->read_column_from_decoded_values(*column, view).ok());
    }

    {
        const int64_t overflow = std::numeric_limits<int64_t>::max();
        auto column = type->create_column();
        DecodedColumnView view;
        view.value_kind = DecodedValueKind::INT64;
        view.time_unit = DecodedTimeUnit::MILLIS;
        view.row_count = 1;
        view.values = reinterpret_cast<const uint8_t*>(&overflow);
        EXPECT_FALSE(serde->read_column_from_decoded_values(*column, view).ok());
    }
}

TEST(DataTypeTimeStampNsTest, DataTypeLiteralFieldAndCivilCasts) {
    const DataTypeTimeStampNs type;
    const DataTypeDateTimeV2 legacy6(6);

    EXPECT_TRUE(type.equals(DataTypeTimeStampNs {}));
    EXPECT_FALSE(type.equals(legacy6));
    EXPECT_FALSE(type.equals_ignore_precision(legacy6));
    EXPECT_FALSE(legacy6.equals_ignore_precision(type));

    TExprNode node;
    node.date_literal.value = "2024-02-29 12:34:56.123456789";
    const Field field = type.get_field(node);
    EXPECT_EQ(field.get<TYPE_TIMESTAMP_NS>().to_string(9), "2024-02-29 12:34:56.123456789");
    node.date_literal.value = "not-a-datetime";
    EXPECT_THROW(type.get_field(node), Exception);

    auto column = type.create_column();
    column->insert(field);
    const auto field_with_type = type.get_field_with_data_type(*column, 0);
    EXPECT_EQ(field_with_type.field, field);
    EXPECT_EQ(field_with_type.base_scalar_type_id, TYPE_TIMESTAMP_NS);
    EXPECT_EQ(field_with_type.precision, -1);
    EXPECT_EQ(field_with_type.scale, 9);

    DateV2Value<DateV2ValueType> date;
    date.unchecked_set_time(2024, 2, 29, 0, 0, 0, 0);
    TimeStampNsValue nano_from_date;
    DataTypeDateV2::cast_to_date_time_v2(date, nano_from_date);
    EXPECT_EQ(nano_from_date.to_string(9), "2024-02-29 00:00:00.000000000");

    VecDateTimeValue date_v1;
    VecDateTimeValue datetime_v1;
    DateV2Value<DateV2ValueType> date_v2;
    DataTypeDateTimeV2::cast_to_date(field.get<TYPE_TIMESTAMP_NS>(), date_v1);
    DataTypeDateTimeV2::cast_to_date_time(field.get<TYPE_TIMESTAMP_NS>(), datetime_v1);
    DataTypeDateTimeV2::cast_to_date_v2(field.get<TYPE_TIMESTAMP_NS>(), date_v2);
    char date_v1_text[64] = {};
    char datetime_v1_text[64] = {};
    date_v1.to_string(date_v1_text);
    datetime_v1.to_string(datetime_v1_text);
    EXPECT_STREQ(date_v1_text, "2024-02-29");
    EXPECT_STREQ(datetime_v1_text, "2024-02-29 12:34:56");
    EXPECT_EQ(date_v2.to_string(), "2024-02-29");
}

TEST(DataTypeTimeStampNsTest, CalendarHelpersArithmeticAndHash) {
    int64_t raw = 0;
    ASSERT_TRUE(parse_timestamp_ns(StringRef("2024-02-29 12:34:56.123456789"), &raw).ok());
    TimeStampNsValue value(raw);

    EXPECT_EQ(value.quarter(), 1);
    EXPECT_GT(value.daynr(), 0);
    EXPECT_GT(value.year_of_week(), 0);
    EXPECT_GT(value.week(0), 0);
    EXPECT_GT(value.year_week(0), 0);
    EXPECT_EQ(value.day_of_year(), 60);
    EXPECT_GT(value.day_of_week(), 0);
    EXPECT_LT(value.weekday(), 7);
    EXPECT_EQ(value.time_part_to_seconds(), 12 * 3600 + 34 * 60 + 56);
    EXPECT_EQ(value.time_part_to_microsecond(), (12 * 3600 + 34 * 60 + 56) * 1000000LL + 123456);
    EXPECT_TRUE(value.is_valid_date());

    static const char* const day_names[] = {"Monday", "Tuesday",  "Wednesday", "Thursday",
                                            "Friday", "Saturday", "Sunday"};
    static const char* const month_names[] = {
            "",     "January", "February",  "March",   "April",    "May",     "June",
            "July", "August",  "September", "October", "November", "December"};
    EXPECT_STREQ(value.day_name_with_locale(day_names), "Thursday");
    EXPECT_STREQ(value.month_name_with_locale(month_names), "February");

    char formatted[64] = {};
    constexpr char format[] = "%Y-%m-%d %H:%i:%s.%f";
    ASSERT_TRUE(value.to_format_string_conservative(format, std::strlen(format), formatted,
                                                    sizeof(formatted)));
    EXPECT_STREQ(formatted, "2024-02-29 12:34:56.123456");

    char text[40] = {};
    const char* end = value.to_string(text, 9);
    EXPECT_STREQ(text, "2024-02-29 12:34:56.123456789");
    EXPECT_EQ(end, text + std::strlen(text) + 1);

    TimeStampNsValue adjusted = value;
    adjusted += 2;
    EXPECT_EQ(adjusted.epoch_nanos(), value.epoch_nanos() + 2000000000LL);
    adjusted -= 3;
    EXPECT_EQ(adjusted.epoch_nanos(), value.epoch_nanos() - 1000000000LL);
    EXPECT_EQ(value.hash(17), value.hash(17));
    EXPECT_EQ(std::hash<TimeStampNsValue> {}(value), std::hash<int64_t> {}(value.epoch_nanos()));

    int64_t earlier_raw = 0;
    int64_t later_raw = 0;
    ASSERT_TRUE(parse_timestamp_ns(StringRef("2020-01-31 12:00:00.900000009"), &earlier_raw).ok());
    ASSERT_TRUE(parse_timestamp_ns(StringRef("2022-03-31 11:00:00.100000001"), &later_raw).ok());
    const TimeStampNsValue earlier(earlier_raw);
    const TimeStampNsValue later(later_raw);

    EXPECT_EQ(datetime_diff<TimeUnit::YEAR>(earlier, later), 2);
    EXPECT_EQ(datetime_diff<TimeUnit::YEAR>(later, earlier), -2);
    EXPECT_EQ(datetime_diff<TimeUnit::MONTH>(earlier, later), 25);
    EXPECT_EQ(datetime_diff<TimeUnit::MONTH>(later, earlier), -25);
    EXPECT_EQ(datetime_diff<TimeUnit::QUARTER>(earlier, later), 8);
    EXPECT_EQ(datetime_diff<TimeUnit::WEEK>(earlier, later), 112);
    EXPECT_EQ(datetime_diff<TimeUnit::DAY>(earlier, later), 789);
    EXPECT_EQ(datetime_diff<TimeUnit::DAY>(later, earlier), -789);
    EXPECT_EQ(earlier.date_diff_in_days(later), -790);
    EXPECT_EQ(later.date_diff_in_days_round_to_zero_by_time(earlier), 789);
    EXPECT_EQ(earlier.date_diff_in_days_round_to_zero_by_time(later), -789);
    EXPECT_EQ(later.datetime_diff_in_seconds_round_to_zero_by_ms(earlier),
              -earlier.datetime_diff_in_seconds_round_to_zero_by_ms(later));

    const auto legacy = later.to_datetime();
    EXPECT_EQ(earlier.time_part_diff_in_ms(legacy), 3600800000LL);
    EXPECT_EQ(earlier.datetime_diff_in_microseconds(legacy),
              earlier.datetime_diff_in_seconds(legacy) * 1000000LL +
                      earlier.time_part_diff_in_ms(legacy) % 1000000LL);

    TimeStampNsValue truncated = value;
    ASSERT_TRUE(truncated.datetime_trunc<TimeUnit::DAY>());
    EXPECT_EQ(truncated.to_string(9), "2024-02-29 00:00:00.000000000");
}

TEST(DataTypeTimeStampNsTest, SerDeStrictBatchJsonJsonbMysqlAndBinaryField) {
    const DataTypeTimeStampNs type;
    const auto serde = type.get_serde();
    DataTypeSerDe::FormatOptions options;
    options.field_delim = ";";

    auto strings = ColumnString::create();
    strings->insert_data("1970-01-01 00:00:00.000000001", 29);
    strings->insert_data("ignored-invalid-value", 21);
    strings->insert_data("2024-02-29 12:34:56.123456789", 29);
    NullMap null_map = {0, 1, 0};
    auto strict_result = type.create_column();
    ASSERT_TRUE(
            serde->from_string_strict_mode_batch(*strings, *strict_result, options, null_map.data())
                    .ok());
    const auto& strict_data = assert_cast<const ColumnTimeStampNs&>(*strict_result).get_data();
    EXPECT_EQ(strict_data[0].epoch_nanos(), 1);
    EXPECT_EQ(strict_data[2].to_string(9), "2024-02-29 12:34:56.123456789");

    auto invalid_strings = ColumnString::create();
    invalid_strings->insert_data("invalid", 7);
    auto invalid_result = type.create_column();
    EXPECT_FALSE(serde->from_string_strict_mode_batch(*invalid_strings, *invalid_result, options,
                                                      nullptr)
                         .ok());

    auto source = type.create_column();
    for (const std::string input :
         {"1970-01-01 00:00:00.000000001", "2024-02-29 12:34:56.123456789"}) {
        StringRef ref(input);
        ASSERT_TRUE(serde->from_string_strict_mode(ref, *source, options).ok());
    }

    auto serialized = ColumnString::create();
    VectorBufferWriter writer(*serialized);
    ASSERT_TRUE(serde->serialize_column_to_json(*source, 0, source->size(), writer, options).ok());
    writer.commit();
    EXPECT_EQ(serialized->get_data_at(0).to_string(),
              "1970-01-01 00:00:00.000000001;2024-02-29 12:34:56.123456789");

    std::vector<std::string> json_values = {"1970-01-01 00:00:00.000000001",
                                            "2024-02-29 12:34:56.123456789"};
    std::vector<Slice> slices;
    for (auto& json_value : json_values) {
        slices.emplace_back(json_value.data(), json_value.size());
    }
    auto json_result = type.create_column();
    uint64_t num_deserialized = 0;
    ASSERT_TRUE(serde->deserialize_column_from_json_vector(*json_result, slices, &num_deserialized,
                                                           options)
                        .ok());
    EXPECT_EQ(num_deserialized, json_values.size());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(*json_result).get_data(),
              assert_cast<const ColumnTimeStampNs&>(*source).get_data());

    const auto nested_serde = type.get_serde(2);
    auto nested_json = ColumnString::create();
    VectorBufferWriter nested_writer(*nested_json);
    ASSERT_TRUE(nested_serde->serialize_one_cell_to_json(*source, 1, nested_writer, options).ok());
    nested_writer.commit();
    EXPECT_EQ(nested_json->get_data_at(0).to_string(), "\"2024-02-29 12:34:56.123456789\"");
    auto nested_result = type.create_column();
    std::string quoted = nested_json->get_data_at(0).to_string();
    Slice quoted_slice(quoted.data(), quoted.size());
    ASSERT_TRUE(nested_serde->deserialize_one_cell_from_json(*nested_result, quoted_slice, options)
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(*nested_result).get_element(0),
              assert_cast<const ColumnTimeStampNs&>(*source).get_element(1));

    auto one_value = source->clone_resized(1);
    auto const_column = ColumnConst::create(std::move(one_value), 2);
    auto const_json = ColumnString::create();
    VectorBufferWriter const_writer(*const_json);
    ASSERT_TRUE(serde->serialize_one_cell_to_json(*const_column, 1, const_writer, options).ok());
    const_writer.commit();
    EXPECT_EQ(const_json->get_data_at(0).to_string(), "1970-01-01 00:00:00.000000001");

    JsonbWriter jsonb_writer;
    ASSERT_TRUE(serde->serialize_column_to_jsonb(*source, 1, jsonb_writer).ok());
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(jsonb_writer.getOutput()->getBuffer(),
                                                jsonb_writer.getOutput()->getSize()),
              "\"2024-02-29 12:34:56.123456789\"");
    CastParameters cast_params {.status = Status::OK(), .is_strict = true};
    auto jsonb_result = type.create_column();
    ASSERT_TRUE(serde->deserialize_column_from_jsonb(*jsonb_result, jsonb_writer.getValue(),
                                                     cast_params)
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(*jsonb_result).get_element(0),
              assert_cast<const ColumnTimeStampNs&>(*source).get_element(1));

    auto jsonb_values = ColumnString::create();
    ASSERT_TRUE(serde->serialize_column_to_jsonb_vector(*source, *jsonb_values).ok());
    auto jsonb_vector_result = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    ASSERT_TRUE(serde->deserialize_column_from_jsonb_vector(*jsonb_vector_result, *jsonb_values,
                                                            cast_params)
                        .ok());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(jsonb_vector_result->get_nested_column())
                      .get_data(),
              assert_cast<const ColumnTimeStampNs&>(*source).get_data());

    JsonbWriter row_store_writer;
    Arena row_store_arena;
    row_store_writer.writeStartObject();
    serde->write_one_cell_to_jsonb(*source, row_store_writer, row_store_arena, 0, 1, options);
    row_store_writer.writeEndObject();
    const JsonbDocument* row_store_document = nullptr;
    ASSERT_TRUE(JsonbDocument::checkAndCreateDocument(row_store_writer.getOutput()->getBuffer(),
                                                      row_store_writer.getOutput()->getSize(),
                                                      &row_store_document)
                        .ok());
    auto row_store_result = type.create_column();
    serde->read_one_cell_from_jsonb(*row_store_result, (*row_store_document)->begin()->value());
    EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(*row_store_result).get_element(0),
              assert_cast<const ColumnTimeStampNs&>(*source).get_element(1));

    MysqlRowBinaryBuffer mysql_buffer;
    ASSERT_TRUE(serde->write_column_to_mysql_binary(*source, mysql_buffer, 1, false, options).ok());
    ASSERT_EQ(static_cast<uint8_t>(mysql_buffer.buf()[0]), 29);
    EXPECT_EQ(std::string(mysql_buffer.buf() + 1, 29), "2024-02-29 12:34:56.123456789");

    ColumnString::Chars binary;
    serde->write_one_cell_to_binary(*source, binary, 1);
    Field binary_field;
    FieldInfo info;
    const uint8_t* end =
            DataTypeSerDe::deserialize_binary_to_field(binary.data(), binary_field, info);
    EXPECT_EQ(end, binary.data() + binary.size());
    EXPECT_EQ(info.scalar_type_id, TYPE_TIMESTAMP_NS);
    EXPECT_EQ(info.scale, 9);
    EXPECT_EQ(binary_field.get<TYPE_TIMESTAMP_NS>().to_string(9), "2024-02-29 12:34:56.123456789");
}

TEST(DataTypeTimeStampNsTest, SerDeBatchRejectsMultipleFractionalSeparators) {
    const DataTypeTimeStampNs type;
    const auto serde = type.get_serde();
    DataTypeSerDe::FormatOptions options;

    auto strings = ColumnString::create();
    strings->insert_data("2024-01-01 00:00:00.123456789", 29);
    strings->insert_data("2024-01-01 00:00:00.123.456", 27);

    auto permissive_result = ColumnNullable::create(type.create_column(), ColumnUInt8::create());
    ASSERT_TRUE(serde->from_string_batch(*strings, *permissive_result, options).ok());
    const auto& permissive_null_map = permissive_result->get_null_map_data();
    ASSERT_EQ(permissive_null_map.size(), 2);
    EXPECT_EQ(permissive_null_map[0], 0);
    EXPECT_EQ(permissive_null_map[1], 1);

    auto invalid_string = ColumnString::create();
    invalid_string->insert_data("2024-01-01 00:00:00.123.456", 27);
    auto strict_result = type.create_column();
    EXPECT_FALSE(
            serde->from_string_strict_mode_batch(*invalid_string, *strict_result, options, nullptr)
                    .ok());
}

/*
TEST(DataTypeTimeStampNsTest, SerDeArrowUnitsNullTimezoneAndErrors) {
    TimezoneUtils::load_timezones_to_cache();
    cctz::time_zone shanghai;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Asia/Shanghai", shanghai));

    const DataTypeTimeStampNs type;
    const auto serde = type.get_serde();
    auto source = type.create_column();
    DataTypeSerDe::FormatOptions options;
    for (const std::string input :
         {"1970-01-01 08:00:01.000000000", "1970-01-01 08:00:02.000000000"}) {
        StringRef ref(input);
        ASSERT_TRUE(serde->from_string(ref, *source, options).ok());
    }
    const NullMap null_map = {0, 1};

    const std::array<std::pair<arrow::TimeUnit::type, int64_t>, 4> units = {
            std::pair {arrow::TimeUnit::SECOND, 1LL},
            std::pair {arrow::TimeUnit::MILLI, 1000LL},
            std::pair {arrow::TimeUnit::MICRO, 1000000LL},
            std::pair {arrow::TimeUnit::NANO, 1000000000LL},
    };
    for (const auto& [unit, multiplier] : units) {
        arrow::TimestampBuilder builder(arrow::timestamp(unit, "Asia/Shanghai"),
                                        arrow::default_memory_pool());
        ASSERT_TRUE(serde->write_column_to_arrow(*source, &null_map, &builder, 0, source->size(),
                                                 shanghai)
                            .ok());
        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        const auto& timestamps = assert_cast<const arrow::TimestampArray&>(*array);
        EXPECT_EQ(timestamps.Value(0), multiplier);
        EXPECT_TRUE(timestamps.IsNull(1));

        auto result = type.create_column();
        ASSERT_TRUE(serde->read_column_from_arrow(*result, array.get(), 0, 1, shanghai).ok());
        EXPECT_EQ(assert_cast<const ColumnTimeStampNs&>(*result).get_element(0),
                  assert_cast<const ColumnTimeStampNs&>(*source).get_element(0));
    }

    arrow::Int64Builder wrong_builder;
    ASSERT_TRUE(wrong_builder.Append(1).ok());
    std::shared_ptr<arrow::Array> wrong_array;
    ASSERT_TRUE(wrong_builder.Finish(&wrong_array).ok());
    auto wrong_result = type.create_column();
    EXPECT_FALSE(
            serde->read_column_from_arrow(*wrong_result, wrong_array.get(), 0, 1, shanghai).ok());

    for (const auto unit :
         {arrow::TimeUnit::SECOND, arrow::TimeUnit::MILLI, arrow::TimeUnit::MICRO}) {
        arrow::TimestampBuilder overflow_builder(arrow::timestamp(unit),
                                                 arrow::default_memory_pool());
        ASSERT_TRUE(overflow_builder.Append(std::numeric_limits<int64_t>::max()).ok());
        std::shared_ptr<arrow::Array> overflow_array;
        ASSERT_TRUE(overflow_builder.Finish(&overflow_array).ok());
        auto overflow_result = type.create_column();
        EXPECT_FALSE(serde->read_column_from_arrow(*overflow_result, overflow_array.get(), 0, 1,
                                                   cctz::utc_time_zone())
                             .ok());
    }

    arrow::TimestampBuilder boundary_builder(
            arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai"), arrow::default_memory_pool());
    ASSERT_TRUE(boundary_builder.Append(std::numeric_limits<int64_t>::max()).ok());
    std::shared_ptr<arrow::Array> boundary_array;
    ASSERT_TRUE(boundary_builder.Finish(&boundary_array).ok());
    auto boundary_result = type.create_column();
    EXPECT_FALSE(
            serde->read_column_from_arrow(*boundary_result, boundary_array.get(), 0, 1, shanghai)
                    .ok());

    auto minimum = type.create_column();
    assert_cast<ColumnTimeStampNs&>(*minimum).insert_value(
            TimeStampNsValue(std::numeric_limits<int64_t>::min()));
    arrow::TimestampBuilder minimum_builder(
            arrow::timestamp(arrow::TimeUnit::NANO, "Asia/Shanghai"), arrow::default_memory_pool());
    EXPECT_FALSE(
            serde->write_column_to_arrow(*minimum, nullptr, &minimum_builder, 0, 1, shanghai).ok());
}

TEST(DataTypeTimeStampNsTest, SerDeDecodedNullTimezoneAndOrc) {
    TimezoneUtils::load_timezones_to_cache();
    cctz::time_zone shanghai;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Asia/Shanghai", shanghai));

    const DataTypeTimeStampNs type;
    const auto serde = type.get_serde();
    const int64_t values[] = {0, 1};
    const NullMap null_map = {0, 1};
    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT64;
    view.time_unit = DecodedTimeUnit::NANOS;
    view.row_count = 2;
    view.values = reinterpret_cast<const uint8_t*>(values);
    view.null_map = null_map.data();
    view.timestamp_is_adjusted_to_utc = true;
    view.timezone = &shanghai;
    auto decoded = type.create_column();
    ASSERT_TRUE(serde->read_column_from_decoded_values(*decoded, view).ok());
    const auto& decoded_data = assert_cast<const ColumnTimeStampNs&>(*decoded).get_data();
    EXPECT_EQ(decoded_data[0].to_string(9), "1970-01-01 08:00:00.000000000");
    EXPECT_EQ(decoded_data[1].epoch_nanos(), 0);

    view.value_kind = DecodedValueKind::INT32;
    auto invalid_kind = type.create_column();
    EXPECT_FALSE(serde->read_column_from_decoded_values(*invalid_kind, view).ok());

    auto source = type.create_column();
    int64_t source_raw = 0;
    ASSERT_TRUE(
            parse_timestamp_ns(StringRef("1970-01-01 08:00:01.123456789"), &source_raw).ok());
    assert_cast<ColumnTimeStampNs&>(*source).insert_value(TimeStampNsValue(source_raw));
    assert_cast<ColumnTimeStampNs&>(*source).insert_value(TimeStampNsValue(0));

    Arena arena;
    DataTypeSerDe::FormatOptions options;
    orc::TimestampVectorBatch batch(2, *orc::getDefaultPool());
    batch.resize(2);
    batch.notNull[0] = 1;
    batch.notNull[1] = 0;
    batch.hasNulls = true;
    ASSERT_TRUE(serde->write_column_to_orc("Asia/Shanghai", *source, nullptr, &batch, 0, 2, arena,
                                           options)
                        .ok());
    EXPECT_EQ(batch.numElements, 2);
    EXPECT_EQ(batch.data[0], 1);
    EXPECT_EQ(batch.nanoseconds[0], 123456789);
    EXPECT_EQ(batch.data[1], 0);
    EXPECT_FALSE(serde->write_column_to_orc("invalid/timezone", *source, nullptr, &batch, 0, 2,
                                            arena, options)
                         .ok());

    auto minimum = type.create_column();
    assert_cast<ColumnTimeStampNs&>(*minimum).insert_value(
            TimeStampNsValue(std::numeric_limits<int64_t>::min()));
    orc::TimestampVectorBatch minimum_batch(1, *orc::getDefaultPool());
    minimum_batch.resize(1);
    minimum_batch.notNull[0] = 1;
    EXPECT_FALSE(serde->write_column_to_orc("Asia/Shanghai", *minimum, nullptr, &minimum_batch, 0,
                                            1, arena, options)
                         .ok());
}
*/

} // namespace doris
