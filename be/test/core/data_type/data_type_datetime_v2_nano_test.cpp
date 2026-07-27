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

#include <arrow/api.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type_serde/data_type_datetimev2_nano_serde.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/value/vdatetime_value.h"
#include "util/timezone_utils.h"

namespace doris {

TEST(DataTypeDateTimeV2NanoTest, Int64EpochRangeAndOrdering) {
    const DateTimeV2NanoValue epoch(0);
    const DateTimeV2NanoValue before_epoch(-1);
    const DateTimeV2NanoValue minimum(std::numeric_limits<int64_t>::min());
    const DateTimeV2NanoValue maximum(std::numeric_limits<int64_t>::max());

    EXPECT_EQ(epoch.to_string(9), "1970-01-01 00:00:00.000000000");
    EXPECT_EQ(before_epoch.to_string(9), "1969-12-31 23:59:59.999999999");
    EXPECT_EQ(minimum.to_string(9), "1677-09-21 00:12:43.145224192");
    EXPECT_EQ(maximum.to_string(9), "2262-04-11 23:47:16.854775807");
    EXPECT_LT(minimum, before_epoch);
    EXPECT_LT(before_epoch, epoch);
    EXPECT_LT(epoch, maximum);
}

TEST(DataTypeDateTimeV2NanoTest, NegativeEpochUsesFloorSecondAndNormalizedFraction) {
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
        const DateTimeV2NanoValue value(test_case.epoch_nanos);
        EXPECT_EQ(value.epoch_seconds(), test_case.epoch_seconds);
        EXPECT_EQ(value.nanosecond(), test_case.nanosecond);
        EXPECT_EQ(static_cast<__int128>(value.epoch_seconds()) *
                                  DateTimeV2NanoValue::NANOS_PER_SECOND +
                          value.nanosecond(),
                  test_case.epoch_nanos);
    }
}

TEST(DataTypeDateTimeV2NanoTest, ParseAndRoundToDeclaredScale) {
    int64_t value = 0;

    ASSERT_TRUE(parse_datetimev2_nano(StringRef("1970-01-01 00:00:00.12345675"), 7, &value).ok());
    EXPECT_EQ(DateTimeV2NanoValue(value).to_string(7), "1970-01-01 00:00:00.1234568");

    ASSERT_TRUE(parse_datetimev2_nano(StringRef("1969-12-31 23:59:59.999999999"), 9, &value).ok());
    EXPECT_EQ(value, -1);

    ASSERT_TRUE(parse_datetimev2_nano(StringRef("1970-01-01 00:00:00.999999995"), 8, &value).ok());
    EXPECT_EQ(DateTimeV2NanoValue(value).to_string(8), "1970-01-01 00:00:01.00000000");
}

TEST(DataTypeDateTimeV2NanoTest, ParseTimezoneSuffixInSessionTimezone) {
    TimezoneUtils::load_timezones_to_cache();
    cctz::time_zone shanghai;
    ASSERT_TRUE(cctz::load_time_zone("Asia/Shanghai", &shanghai));

    int64_t value = 0;
    auto status = parse_datetimev2_nano(StringRef("2023-08-17T01:41:18.123456789Z"), 9, &value,
                                        &shanghai);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(DateTimeV2NanoValue(value).to_string(9), "2023-08-17 09:41:18.123456789");

    ASSERT_TRUE(parse_datetimev2_nano(StringRef("2023-08-17T01:41:18.123456789America/Los_Angeles"),
                                      9, &value, &shanghai)
                        .ok());
    EXPECT_EQ(DateTimeV2NanoValue(value).to_string(9), "2023-08-17 16:41:18.123456789");

    EXPECT_FALSE(parse_datetimev2_nano(StringRef("1677-09-21T00:12:43.145224192+14:00"), 9, &value,
                                       &shanghai)
                         .ok());
    EXPECT_FALSE(parse_datetimev2_nano(StringRef("2262-04-11T23:47:16.854775807-01:00"), 9, &value,
                                       &shanghai)
                         .ok());
}

TEST(DataTypeDateTimeV2NanoTest, ParseAcceptsAllNanoScalesAndRejectsMalformedValues) {
    struct ValidCase {
        int scale;
        const char* input;
        const char* expected;
    };
    const std::vector<ValidCase> valid_cases = {
            {7, "2024-02-29 12:34:56.12345674", "2024-02-29 12:34:56.1234567"},
            {7, "2024-02-29 12:34:56.12345675", "2024-02-29 12:34:56.1234568"},
            {8, "2024-02-29 12:34:56.123456784", "2024-02-29 12:34:56.12345678"},
            {8, "2024-02-29 12:34:56.123456785", "2024-02-29 12:34:56.12345679"},
            {9, "2024-02-29 12:34:56.123456789", "2024-02-29 12:34:56.123456789"},
            {9, "2024-02-29 12:34:56", "2024-02-29 12:34:56.000000000"},
    };

    for (const auto& test_case : valid_cases) {
        int64_t value = 0;
        ASSERT_TRUE(parse_datetimev2_nano(StringRef(test_case.input), test_case.scale, &value).ok())
                << test_case.input;
        EXPECT_EQ(DateTimeV2NanoValue(value).to_string(test_case.scale), test_case.expected);
    }

    const std::vector<const char*> invalid_values = {
            "",           "not-a-date",          "2023-02-29 00:00:00.000000000",
            "2024-13-01", "2024-01-01 24:00:00", "2024-01-01 00:00:00.trailing",
    };
    for (const char* input : invalid_values) {
        int64_t value = 0;
        EXPECT_FALSE(parse_datetimev2_nano(StringRef(input), 9, &value).ok()) << input;
    }
}

TEST(DataTypeDateTimeV2NanoTest, RejectValuesOutsideEpochRange) {
    int64_t value = 0;
    EXPECT_FALSE(parse_datetimev2_nano(StringRef("0000-01-01 00:00:00.000000000"), 9, &value).ok());
    EXPECT_FALSE(parse_datetimev2_nano(StringRef("1677-09-21 00:12:43.145224191"), 9, &value).ok());
    EXPECT_FALSE(parse_datetimev2_nano(StringRef("2262-04-11 23:47:16.854775808"), 9, &value).ok());
    EXPECT_FALSE(parse_datetimev2_nano(StringRef("9999-12-31 23:59:59.999999999"), 9, &value).ok());
}

TEST(DataTypeDateTimeV2NanoTest, CivilRoundTripPreservesSubMicrosecondDigits) {
    DateV2Value<DateTimeV2ValueType> civil;
    civil.unchecked_set_time(2024, 2, 29, 23, 59, 58, 123456);

    DateTimeV2NanoValue value;
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

TEST(DataTypeDateTimeV2NanoTest, CalendarArithmeticPreservesSubMicrosecondDigits) {
    int64_t raw_value = 0;
    ASSERT_TRUE(
            parse_datetimev2_nano(StringRef("2024-01-31 23:59:59.123456789"), 9, &raw_value).ok());
    DateTimeV2NanoValue value(raw_value);

    ASSERT_TRUE(value.date_add_interval<TimeUnit::MONTH>(TimeInterval(TimeUnit::MONTH, 1, false)));
    EXPECT_EQ(value.to_string(9), "2024-02-29 23:59:59.123456789");

    ASSERT_TRUE(
            value.date_add_interval<TimeUnit::SECOND>(TimeInterval(TimeUnit::SECOND, 1, false)));
    EXPECT_EQ(value.to_string(9), "2024-03-01 00:00:00.123456789");
}

TEST(DataTypeDateTimeV2NanoTest, DiffTruncatesIncompleteUnitsTowardZero) {
    int64_t lhs_raw = 0;
    int64_t rhs_raw = 0;
    ASSERT_TRUE(
            parse_datetimev2_nano(StringRef("1970-01-01 00:00:00.000000001"), 9, &lhs_raw).ok());
    ASSERT_TRUE(
            parse_datetimev2_nano(StringRef("1970-01-01 00:00:01.999999999"), 9, &rhs_raw).ok());
    const DateTimeV2NanoValue lhs(lhs_raw);
    const DateTimeV2NanoValue rhs(rhs_raw);

    EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(lhs, rhs), 1);
    EXPECT_EQ(datetime_diff<TimeUnit::SECOND>(rhs, lhs), -1);
    EXPECT_EQ(datetime_diff<TimeUnit::MILLISECOND>(lhs, rhs), 1999);
    EXPECT_EQ(datetime_diff<TimeUnit::MILLISECOND>(rhs, lhs), -1999);
    EXPECT_EQ(lhs.datetime_diff_in_microseconds(rhs), -1999999);
    EXPECT_EQ(rhs.datetime_diff_in_microseconds(lhs), 1999999);
}

TEST(DataTypeDateTimeV2NanoTest, FactoryKeepsLegacyAndNanoPhysicalTypesSeparate) {
    const auto microseconds = create_datetimev2(6);
    const auto nanoseconds = create_datetimev2(9);

    EXPECT_EQ(microseconds->get_primitive_type(), TYPE_DATETIMEV2);
    EXPECT_EQ(nanoseconds->get_primitive_type(), TYPE_DATETIMEV2_NANO);
    EXPECT_EQ(nanoseconds->get_storage_field_type(), FieldType::OLAP_FIELD_TYPE_DATETIMEV2_NANO);
    EXPECT_EQ(nanoseconds->get_scale(), 9);
    EXPECT_EQ(microseconds->get_family_name(), "DateTimeV2");
    EXPECT_EQ(nanoseconds->get_family_name(), "DateTimeV2Nano");
}

TEST(DataTypeDateTimeV2NanoTest, SerDeRoundTripsTextProtobufAndBinary) {
    const auto type = std::make_shared<DataTypeDateTimeV2Nano>(9);
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
    const auto& source_data = assert_cast<const ColumnDateTimeV2Nano&>(*source).get_data();

    PValues protobuf_values;
    ASSERT_TRUE(serde->write_column_to_pb(*source, protobuf_values, 0, source->size()).ok());
    auto protobuf_result = type->create_column();
    ASSERT_TRUE(serde->read_column_from_pb(*protobuf_result, protobuf_values).ok());
    const auto& protobuf_data =
            assert_cast<const ColumnDateTimeV2Nano&>(*protobuf_result).get_data();
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
            assert_cast<const ColumnDateTimeV2Nano&>(binary_result->get_nested_column()).get_data();
    EXPECT_EQ(binary_data, source_data);
}

TEST(DataTypeDateTimeV2NanoTest, SerDeRoundTripsArrowNanosecondsBeforeEpoch) {
    const auto type = std::make_shared<DataTypeDateTimeV2Nano>(9);
    const auto serde = type->get_serde();
    auto source = type->create_column();
    auto& source_data = assert_cast<ColumnDateTimeV2Nano&>(*source).get_data();
    source_data.push_back(DateTimeV2NanoValue(-1));
    source_data.push_back(DateTimeV2NanoValue(0));
    source_data.push_back(DateTimeV2NanoValue(1234567890));

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
    EXPECT_EQ(assert_cast<const ColumnDateTimeV2Nano&>(*result).get_data(), source_data);
}

TEST(DataTypeDateTimeV2NanoTest, DecodedTimestampUnitsAndValidation) {
    const auto type = std::make_shared<DataTypeDateTimeV2Nano>(9);
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
        const auto& data = assert_cast<const ColumnDateTimeV2Nano&>(*column).get_data();
        ASSERT_EQ(data.size(), 3);
        EXPECT_EQ(data[0].epoch_nanos(), -multiplier);
        EXPECT_EQ(data[1].epoch_nanos(), 0);
        EXPECT_EQ(data[2].epoch_nanos(), multiplier);
    };

    check_unit(DecodedTimeUnit::MILLIS, DateTimeV2NanoValue::NANOS_PER_MILLISECOND);
    check_unit(DecodedTimeUnit::MICROS, DateTimeV2NanoValue::NANOS_PER_MICROSECOND);
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

} // namespace doris
