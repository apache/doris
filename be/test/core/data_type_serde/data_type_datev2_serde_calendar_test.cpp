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

// Doris DATE values are numbered in MySQL's calendar, where year 0 is not a leap year. Arrow
// `date32`, Parquet `DATE` and ORC `DATE` are all days since 1970-01-01 in the proleptic
// Gregorian calendar, where year 0 IS a leap year. These tests pin the boundary so the two
// numberings cannot drift apart again: 0000-01-01 must leave Doris as -719528, not -719527.

#include <arrow/api.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/data_type_datev2_serde.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/value/vdatetime_value.h"

namespace doris {

namespace {

struct DateCase {
    int year;
    int month;
    int day;
    int32_t epoch_days;
};

// 0000-01-01 .. 0000-02-28 are the only dates where Doris's daynr and the proleptic Gregorian
// ordinal disagree, so the set brackets that window on both sides.
const std::vector<DateCase>& boundary_cases() {
    static const std::vector<DateCase> cases = {
            {0, 1, 1, -719528}, {0, 2, 28, -719470}, {0, 3, 1, -719468},  {1, 1, 1, -719162},
            {1969, 12, 31, -1}, {1970, 1, 1, 0},     {2024, 1, 1, 19723}, {9999, 12, 31, 2932896}};
    return cases;
}

DateV2Value<DateV2ValueType> make_date(int year, int month, int day) {
    DateV2Value<DateV2ValueType> value;
    value.unchecked_set_time(year, month, day, 0, 0, 0, 0);
    return value;
}

ColumnDateV2::MutablePtr boundary_column() {
    auto column = ColumnDateV2::create();
    auto& data = column->get_data();
    for (const auto& c : boundary_cases()) {
        auto value = make_date(c.year, c.month, c.day);
        data.push_back(*reinterpret_cast<UInt32*>(&value));
    }
    return column;
}

std::shared_ptr<arrow::Array> build_date32(const std::vector<int32_t>& days) {
    arrow::Date32Builder builder;
    for (int32_t d : days) {
        EXPECT_TRUE(builder.Append(d).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

} // namespace

class DataTypeDateV2SerDeCalendarTest : public ::testing::Test {
protected:
    DataTypeDateV2SerDe serde;
    cctz::time_zone tz = cctz::utc_time_zone();
};

TEST_F(DataTypeDateV2SerDeCalendarTest, WriteArrowUsesProlepticGregorian) {
    auto column = boundary_column();
    arrow::Date32Builder builder;
    ASSERT_TRUE(serde.write_column_to_arrow(*column, nullptr, &builder, 0,
                                            static_cast<int64_t>(column->size()), tz)
                        .ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    const auto& date32 = assert_cast<const arrow::Date32Array&>(*array);
    ASSERT_EQ(boundary_cases().size(), static_cast<size_t>(date32.length()));
    for (size_t i = 0; i < boundary_cases().size(); ++i) {
        EXPECT_EQ(boundary_cases()[i].epoch_days, date32.Value(static_cast<int64_t>(i)))
                << "row " << i;
    }
}

TEST_F(DataTypeDateV2SerDeCalendarTest, WriteArrowHonoursNullMap) {
    auto column = boundary_column();
    NullMap null_map;
    null_map.resize_fill(column->size(), 0);
    null_map[0] = 1; // 0000-01-01
    null_map[2] = 1; // 0000-03-01

    arrow::Date32Builder builder;
    ASSERT_TRUE(serde.write_column_to_arrow(*column, &null_map, &builder, 0,
                                            static_cast<int64_t>(column->size()), tz)
                        .ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    const auto& date32 = assert_cast<const arrow::Date32Array&>(*array);
    for (size_t i = 0; i < boundary_cases().size(); ++i) {
        if (null_map[i]) {
            EXPECT_TRUE(date32.IsNull(static_cast<int64_t>(i))) << "row " << i;
        } else {
            EXPECT_EQ(boundary_cases()[i].epoch_days, date32.Value(static_cast<int64_t>(i)))
                    << "row " << i;
        }
    }
}

TEST_F(DataTypeDateV2SerDeCalendarTest, ReadDate32RestoresTheSameCalendarDay) {
    std::vector<int32_t> days;
    for (const auto& c : boundary_cases()) {
        days.push_back(c.epoch_days);
    }
    auto array = build_date32(days);

    auto column = ColumnDateV2::create();
    ASSERT_TRUE(serde.read_column_from_arrow(*column, array.get(), 0, array->length(), tz).ok());

    ASSERT_EQ(boundary_cases().size(), column->size());
    const auto& values = column->get_data();
    for (size_t i = 0; i < boundary_cases().size(); ++i) {
        const auto& c = boundary_cases()[i];
        EXPECT_EQ(c.year, values[i].year()) << "row " << i;
        EXPECT_EQ(c.month, values[i].month()) << "row " << i;
        EXPECT_EQ(c.day, values[i].day()) << "row " << i;
    }
}

TEST_F(DataTypeDateV2SerDeCalendarTest, ArrowRoundTripIsLossless) {
    auto column = boundary_column();
    arrow::Date32Builder builder;
    ASSERT_TRUE(serde.write_column_to_arrow(*column, nullptr, &builder, 0,
                                            static_cast<int64_t>(column->size()), tz)
                        .ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    auto restored = ColumnDateV2::create();
    ASSERT_TRUE(serde.read_column_from_arrow(*restored, array.get(), 0, array->length(), tz).ok());
    ASSERT_EQ(column->size(), restored->size());
    for (size_t i = 0; i < column->size(); ++i) {
        EXPECT_EQ(column->get_data()[i], restored->get_data()[i]) << "row " << i;
    }
}

TEST_F(DataTypeDateV2SerDeCalendarTest, ReadDate32RejectsUnrepresentableDays) {
    const auto expect_rejected = [&](int32_t days, const char* why) {
        auto array = build_date32({days});
        auto column = ColumnDateV2::create();
        const auto status = serde.read_column_from_arrow(*column, array.get(), 0, 1, tz);
        EXPECT_FALSE(status.ok()) << why << " (days=" << days << ")";
        EXPECT_NE(std::string::npos, status.to_string().find("outside the Doris DATE range"))
                << status.to_string();
    };

    expect_rejected(-719529, "one day before 0000-01-01");
    // 0000-02-29 exists in the proleptic Gregorian calendar but Doris has no such date. The old
    // `encoded + 719528` mapping decoded it as 0000-02-28; it must be rejected instead.
    expect_rejected(-719469, "proleptic-only leap day 0000-02-29");
    expect_rejected(2932897, "one day after 9999-12-31");
}

TEST_F(DataTypeDateV2SerDeCalendarTest, ReadDate64UsesTheSameCalendar) {
    constexpr int64_t millis_per_day = 24LL * 60 * 60 * 1000;
    arrow::Date64Builder builder;
    for (const auto& c : boundary_cases()) {
        ASSERT_TRUE(builder.Append(static_cast<int64_t>(c.epoch_days) * millis_per_day).ok());
    }
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(builder.Finish(&array).ok());

    auto column = ColumnDateV2::create();
    ASSERT_TRUE(serde.read_column_from_arrow(*column, array.get(), 0, array->length(), tz).ok());

    ASSERT_EQ(boundary_cases().size(), column->size());
    const auto& values = column->get_data();
    for (size_t i = 0; i < boundary_cases().size(); ++i) {
        const auto& c = boundary_cases()[i];
        EXPECT_EQ(c.year, values[i].year()) << "row " << i;
        EXPECT_EQ(c.month, values[i].month()) << "row " << i;
        EXPECT_EQ(c.day, values[i].day()) << "row " << i;
    }
}

TEST_F(DataTypeDateV2SerDeCalendarTest, NestedArrayOfDateUsesTheSameCalendar) {
    // ARRAY<DATE> has no encoding of its own; it delegates every element to the DATE SerDe. The
    // reported bug showed up through this path too, so keep it covered.
    // DataTypeArray stores its element type as DataTypeNullablePtr, so the nested column has to be
    // a ColumnNullable for the element SerDe to match.
    auto nested = boundary_column();
    const auto element_count = nested->size();
    auto element_null_map = ColumnUInt8::create();
    element_null_map->get_data().resize_fill(element_count, 0);
    auto nullable_nested = ColumnNullable::create(std::move(nested), std::move(element_null_map));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(static_cast<ColumnArray::Offset64>(element_count));
    auto array_column = ColumnArray::create(std::move(nullable_nested), std::move(offsets));

    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateV2>());
    auto array_serde = array_type->get_serde();

    auto value_builder = std::make_shared<arrow::Date32Builder>();
    arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
    ASSERT_TRUE(array_serde->write_column_to_arrow(*array_column, nullptr, &list_builder, 0, 1, tz)
                        .ok());
    std::shared_ptr<arrow::Array> arrow_array;
    ASSERT_TRUE(list_builder.Finish(&arrow_array).ok());

    const auto& list = assert_cast<const arrow::ListArray&>(*arrow_array);
    const auto& values = assert_cast<const arrow::Date32Array&>(*list.values());
    ASSERT_EQ(boundary_cases().size(), static_cast<size_t>(values.length()));
    for (size_t i = 0; i < boundary_cases().size(); ++i) {
        EXPECT_EQ(boundary_cases()[i].epoch_days, values.Value(static_cast<int64_t>(i)))
                << "element " << i;
    }
}

// The Parquet and ORC readers do not go through Arrow: rows, dictionary entries and column
// statistics all land in `read_column_from_decoded_values()`. Cover that entry point with the same
// boundary set, in both the strict and the null-on-failure mode the file scanners use.
TEST_F(DataTypeDateV2SerDeCalendarTest, ReadDecodedValuesUsesTheSameCalendar) {
    std::vector<int32_t> values;
    for (const auto& c : boundary_cases()) {
        values.push_back(c.epoch_days);
    }
    values.push_back(0); // payload of the null row below, never decoded
    std::vector<uint8_t> null_map(values.size(), 0);
    null_map.back() = 1;

    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT32;
    view.row_count = static_cast<int64_t>(values.size());
    view.values = reinterpret_cast<const uint8_t*>(values.data());
    view.null_map = null_map.data();

    auto column = ColumnDateV2::create();
    ASSERT_TRUE(serde.read_column_from_decoded_values(*column, view).ok());
    ASSERT_EQ(values.size(), column->size());
    const auto& data = column->get_data();
    for (size_t i = 0; i < boundary_cases().size(); ++i) {
        const auto& c = boundary_cases()[i];
        EXPECT_EQ(c.year, data[i].year()) << "row " << i;
        EXPECT_EQ(c.month, data[i].month()) << "row " << i;
        EXPECT_EQ(c.day, data[i].day()) << "row " << i;
    }
}

TEST_F(DataTypeDateV2SerDeCalendarTest, ReadDecodedValuesRejectsUnrepresentableDaysWhenStrict) {
    // -719469 is the proleptic-only 0000-02-29: the only value inside the file-format range that
    // Doris cannot represent, and the one the old dictionary fallback decoded as 0000-02-28.
    const std::vector<int32_t> values = {-719528, -719470, -719469, -719468, 19723};
    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT32;
    view.row_count = static_cast<int64_t>(values.size());
    view.values = reinterpret_cast<const uint8_t*>(values.data());
    view.enable_strict_mode = true;

    auto column = ColumnDateV2::create();
    const auto status = serde.read_column_from_decoded_values(*column, view);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("outside the Doris DATE range"))
            << status.to_string();
    EXPECT_NE(std::string::npos, status.to_string().find("-719469")) << status.to_string();
    // A failed batch must not leave a half-written column behind.
    EXPECT_EQ(0, column->size());
}

TEST_F(DataTypeDateV2SerDeCalendarTest, ReadDecodedValuesNullsOnlyTheUnrepresentableRow) {
    const std::vector<int32_t> values = {-719528, -719470, -719469, -719468, 19723};
    NullMap conversion_failures;
    conversion_failures.resize_fill(values.size(), 0);

    DecodedColumnView view;
    view.value_kind = DecodedValueKind::INT32;
    view.row_count = static_cast<int64_t>(values.size());
    view.values = reinterpret_cast<const uint8_t*>(values.data());
    view.conversion_failure_null_map = &conversion_failures;

    auto column = ColumnDateV2::create();
    ASSERT_TRUE(serde.read_column_from_decoded_values(*column, view).ok());
    ASSERT_EQ(values.size(), column->size());
    const std::vector<uint8_t> expected_failures = {0, 0, 1, 0, 0};
    for (size_t i = 0; i < expected_failures.size(); ++i) {
        EXPECT_EQ(expected_failures[i], conversion_failures[i]) << "row " << i;
    }
    const auto& data = column->get_data();
    EXPECT_EQ(0, data[0].year());
    EXPECT_EQ(1, data[0].month());
    EXPECT_EQ(1, data[0].day());
    EXPECT_EQ(0, data[1].year());
    EXPECT_EQ(2, data[1].month());
    EXPECT_EQ(28, data[1].day());
    EXPECT_EQ(0, data[3].year());
    EXPECT_EQ(3, data[3].month());
    EXPECT_EQ(1, data[3].day());
    EXPECT_EQ(2024, data[4].year());
}

} // namespace doris
