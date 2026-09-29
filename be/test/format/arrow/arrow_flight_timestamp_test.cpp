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
#include <gtest/gtest.h>

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format/arrow/arrow_block_convertor.h"
#include "util/timezone_utils.h"

namespace doris {
namespace {
using DateTime = DateV2Value<DateTimeV2ValueType>;

Block timestamp_block(int scale, const std::vector<DateTime>& values) {
    auto column = ColumnDateTimeV2::create();
    for (const auto& value : values) {
        column->insert_value(value);
    }
    return Block {{std::move(column), std::make_shared<DataTypeDateTimeV2>(scale), "event_time"}};
}

class ArrowFlightTimestampTest : public testing::Test {
protected:
    static void SetUpTestSuite() { TimezoneUtils::load_timezones_to_cache(); }
};

TEST_F(ArrowFlightTimestampTest, RejectsOutOfRangeInEveryUnitWithoutPublishingBatch) {
    for (int scale : {0, 3, 6}) {
        for (const auto& value :
             {DateTime {}, DateTime(0, 12, 31, 0, 0, 0, 0), DateTime(10000, 1, 1, 0, 0, 0, 0)}) {
            SCOPED_TRACE(scale);
            auto block = timestamp_block(scale, {value});
            ArrowFlightArrowBlockConvertor flight(block, "UTC", cctz::utc_time_zone(), true);
            ASSERT_TRUE(flight.init().ok());
            const ArrowBlockConvertor& converter = flight;
            std::shared_ptr<arrow::RecordBatch> batch;
            const auto status =
                    converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
            EXPECT_EQ(ErrorCode::INVALID_ARGUMENT, status.code()) << status;
            EXPECT_NE(std::string::npos, status.to_string().find("event_time"));
            EXPECT_NE(std::string::npos, status.to_string().find("row 1"));
            EXPECT_NE(std::string::npos, status.to_string().find("0001-9999"));
            EXPECT_EQ(nullptr, batch);

            // Other Arrow consumers retain their existing date semantics.
            DorisArrowBlockConvertor ordinary(block, "UTC", cctz::utc_time_zone(), true);
            ASSERT_TRUE(ordinary.init().ok());
            ASSERT_TRUE(
                    ordinary.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
            EXPECT_TRUE(batch->ValidateFull().ok());
        }
    }
}

TEST_F(ArrowFlightTimestampTest, PreservesCalendarBoundariesAndPreEpochFractions) {
    for (int scale : {0, 3, 6}) {
        const int64_t factor = scale == 0 ? 1 : scale == 3 ? 1000 : 1000000;
        const uint32_t fraction = scale == 0 ? 0 : scale == 3 ? 999000 : 999999;
        auto block = timestamp_block(
                scale, {DateTime(1, 1, 1, 0, 0, 0, 0), DateTime(9999, 12, 31, 23, 59, 59, fraction),
                        DateTime(1969, 12, 31, 23, 59, 59, fraction)});
        ArrowFlightArrowBlockConvertor converter(block, "UTC", cctz::utc_time_zone(), true);
        ASSERT_TRUE(converter.init().ok());
        std::shared_ptr<arrow::RecordBatch> batch;
        ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
        const auto& values = static_cast<const arrow::TimestampArray&>(*batch->column(0));
        EXPECT_EQ(-62135596800LL * factor, values.Value(0));
        EXPECT_EQ(253402300800LL * factor - 1, values.Value(1));
        EXPECT_EQ(-1, values.Value(2));
    }
}

TEST_F(ArrowFlightTimestampTest, ChecksSlicesAndSubsequentBatches) {
    auto block = timestamp_block(6, {DateTime(2024, 1, 1, 0, 0, 0, 0), DateTime {}});
    ArrowFlightArrowBlockConvertor converter(block, "UTC", cctz::utc_time_zone(), true);
    ASSERT_TRUE(converter.init().ok());
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 0, 1).ok());
    auto previous = batch;
    const auto status =
            converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 1, 2);
    EXPECT_EQ(ErrorCode::INVALID_ARGUMENT, status.code());
    EXPECT_NE(std::string::npos, status.to_string().find("row 2"));
    EXPECT_EQ(previous, batch);
}

TEST_F(ArrowFlightTimestampTest, RejectsNestedTimestampValuesAndMapKeys) {
    auto datetime = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
    const auto invalid = Field::create_field<TYPE_DATETIMEV2>(DateTime {});
    const auto valid = Field::create_field<TYPE_DATETIMEV2>(DateTime(2024, 1, 1, 0, 0, 0, 0));
    DataTypes types {std::make_shared<DataTypeArray>(datetime),
                     std::make_shared<DataTypeStruct>(DataTypes {datetime}, Strings {"child"}),
                     std::make_shared<DataTypeMap>(datetime, datetime),
                     std::make_shared<DataTypeMap>(datetime, datetime),
                     std::make_shared<DataTypeArray>(std::make_shared<DataTypeStruct>(
                             DataTypes {datetime}, Strings {"child"}))};
    FieldVector fields {
            Field::create_field<TYPE_ARRAY>(Array {valid, invalid}),
            Field::create_field<TYPE_STRUCT>(Struct {invalid}),
            Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {valid}),
                                               Field::create_field<TYPE_ARRAY>(Array {invalid})}),
            Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {invalid}),
                                               Field::create_field<TYPE_ARRAY>(Array {valid})}),
            Field::create_field<TYPE_ARRAY>(
                    Array {Field::create_field<TYPE_STRUCT>(Struct {invalid})})};
    for (size_t i = 0; i < types.size(); ++i) {
        SCOPED_TRACE(types[i]->get_name());
        auto column = types[i]->create_column();
        column->insert_default();
        column->insert(fields[i]);
        Block block {{std::move(column), types[i], "nested"}};
        ArrowFlightArrowBlockConvertor converter(block, "UTC", cctz::utc_time_zone(), true);
        ASSERT_TRUE(converter.init().ok());
        std::shared_ptr<arrow::RecordBatch> batch;
        const auto status =
                converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch, 1, 2);
        EXPECT_EQ(ErrorCode::INVALID_ARGUMENT, status.code()) << status;
        EXPECT_NE(std::string::npos, status.to_string().find("nested"));
        EXPECT_NE(std::string::npos, status.to_string().find("row 2"));
        EXPECT_EQ(nullptr, batch);
    }
}

TEST_F(ArrowFlightTimestampTest, ChecksBothUtcAndZonedCalendarBounds) {
    const std::vector<std::pair<std::string, DateTime>> cases {
            {"+08:00", DateTime(1, 1, 1, 0, 0, 0, 0)},
            {"-08:00", DateTime(9999, 12, 31, 23, 0, 0, 0)},
            {"-08:00", DateTime(0, 12, 31, 23, 0, 0, 0)},
            {"+08:00", DateTime(10000, 1, 1, 1, 0, 0, 0)}};
    for (const auto& [zone, value] : cases) {
        SCOPED_TRACE(zone);
        cctz::time_zone timezone;
        ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(zone, timezone));
        auto block = timestamp_block(6, {value});
        ArrowFlightArrowBlockConvertor converter(block, zone, timezone);
        ASSERT_TRUE(converter.init().ok());
        std::shared_ptr<arrow::RecordBatch> batch;
        const auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
        EXPECT_EQ(ErrorCode::INVALID_ARGUMENT, status.code()) << status;
        EXPECT_EQ(nullptr, batch);

        auto valid = timestamp_block(
                6, {DateTime(1, 1, 2, 0, 0, 0, 0), DateTime(9999, 12, 30, 23, 0, 0, 0)});
        ASSERT_TRUE(converter.convert_to_arrow(valid, arrow::default_memory_pool(), &batch).ok());
    }
}

TEST_F(ArrowFlightTimestampTest, NaiveBoundsDoNotDependOnSessionTimezone) {
    cctz::time_zone timezone;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("+08:00", timezone));
    auto block = timestamp_block(
            6, {DateTime(1, 1, 1, 0, 0, 0, 0), DateTime(9999, 12, 31, 23, 59, 59, 999999)});
    ArrowFlightArrowBlockConvertor converter(block, "+08:00", timezone, true);
    ASSERT_TRUE(converter.init().ok());
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch).ok());
    const auto& values = static_cast<const arrow::TimestampArray&>(*batch->column(0));
    EXPECT_EQ(-62135596800000000LL, values.Value(0));
    EXPECT_EQ(253402300799999999LL, values.Value(1));
}

TEST_F(ArrowFlightTimestampTest, IgnoresTimestampsMaskedByNullParents) {
    auto datetime = std::make_shared<DataTypeDateTimeV2>(6);
    const auto invalid = Field::create_field<TYPE_DATETIMEV2>(DateTime {});
    DataTypes types {
            datetime, std::make_shared<DataTypeStruct>(DataTypes {datetime}, Strings {"child"}),
            std::make_shared<DataTypeArray>(datetime),
            std::make_shared<DataTypeMap>(make_nullable(datetime), make_nullable(datetime))};
    FieldVector fields {
            invalid, Field::create_field<TYPE_STRUCT>(Struct {invalid}),
            Field::create_field<TYPE_ARRAY>(Array {invalid}),
            Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {invalid}),
                                               Field::create_field<TYPE_ARRAY>(Array {invalid})})};
    for (size_t i = 0; i < types.size(); ++i) {
        auto data = types[i]->create_column();
        data->insert(fields[i]);
        auto nulls = ColumnUInt8::create();
        nulls->insert_value(1);
        Block block {{ColumnNullable::create(std::move(data), std::move(nulls)),
                      make_nullable(types[i]), "masked"}};
        ArrowFlightArrowBlockConvertor converter(block, "UTC", cctz::utc_time_zone(), true);
        ASSERT_TRUE(converter.init().ok());
        std::shared_ptr<arrow::RecordBatch> batch;
        const auto status = converter.convert_to_arrow(block, arrow::default_memory_pool(), &batch);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(batch->column(0)->IsNull(0));
    }
}

} // namespace
} // namespace doris
