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

#include "format/parquet/parquet_column_convert.h"

#include <cctz/time_zone.h>

#include <chrono>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "util/timezone_utils.h"

namespace doris::parquet {

static FieldSchema make_timestamp_field_schema(bool is_adjusted_to_utc) {
    FieldSchema field_schema;
    field_schema.parquet_schema.__set_name("ts");
    field_schema.parquet_schema.__set_logicalType(tparquet::LogicalType());
    field_schema.parquet_schema.logicalType.__set_TIMESTAMP(tparquet::TimestampType());
    field_schema.parquet_schema.logicalType.TIMESTAMP.__set_isAdjustedToUTC(is_adjusted_to_utc);
    field_schema.parquet_schema.logicalType.TIMESTAMP.__set_unit(tparquet::TimeUnit());
    field_schema.parquet_schema.logicalType.TIMESTAMP.unit.__set_MICROS(tparquet::MicroSeconds());
    return field_schema;
}

TEST(ParquetColumnConvertTest, InitFixedOffsetDetection) {
    TimezoneUtils::load_timezones_to_cache();

    cctz::time_zone utc_tz;
    cctz::time_zone plus8_tz;
    cctz::time_zone shanghai_tz;

    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("UTC", utc_tz));
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("+8:00", plus8_tz));
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Asia/Shanghai", shanghai_tz));

    auto adjusted_field = make_timestamp_field_schema(true);
    ConvertParams utc_params;
    utc_params.init(&adjusted_field, &utc_tz);
    EXPECT_TRUE(utc_params.is_fixed_offset);
    EXPECT_EQ(0, utc_params.fixed_offset_seconds);

    ConvertParams plus8_params;
    plus8_params.init(&adjusted_field, &plus8_tz);
    EXPECT_TRUE(plus8_params.is_fixed_offset);
    EXPECT_EQ(8 * 3600, plus8_params.fixed_offset_seconds);

    ConvertParams shanghai_params;
    shanghai_params.init(&adjusted_field, &shanghai_tz);
    EXPECT_FALSE(shanghai_params.is_fixed_offset);

    auto non_adjusted_field = make_timestamp_field_schema(false);
    ConvertParams non_adjusted_params;
    non_adjusted_params.init(&non_adjusted_field, &shanghai_tz);
    EXPECT_TRUE(non_adjusted_params.is_fixed_offset);
    EXPECT_EQ(0, non_adjusted_params.fixed_offset_seconds);
}

TEST(ParquetColumnConvertTest, FixedOffsetPathMatchesOriginal) {
    struct TestCase {
        int64_t timestamp;
        int32_t offset_seconds;
    };
    const std::vector<TestCase> test_cases {
            {.timestamp = 0, .offset_seconds = 0},
            {.timestamp = -1, .offset_seconds = 0},
            {.timestamp = -1, .offset_seconds = 8 * 3600},
            {.timestamp = 1713249600, .offset_seconds = 8 * 3600},
            {.timestamp = 1713249600, .offset_seconds = -(6 * 3600)},
            {.timestamp = 1713249600, .offset_seconds = 5 * 3600 + 45 * 60},
    };

    for (const auto& test_case : test_cases) {
        DateV2Value<DateTimeV2ValueType> fast_value;
        DateV2Value<DateTimeV2ValueType> original_value;
        const auto timezone = cctz::fixed_time_zone(cctz::seconds(test_case.offset_seconds));

        ASSERT_TRUE(detail::try_convert_timestamp_with_fixed_offset(fast_value, test_case.timestamp,
                                                                    test_case.offset_seconds));
        original_value.from_unixtime(test_case.timestamp, timezone);

        EXPECT_EQ(original_value.to_date_int_val(), fast_value.to_date_int_val());
    }
}

TEST(ParquetColumnConvertTest, LookupPathMatchesOriginal) {
    cctz::time_zone shanghai_tz;
    cctz::time_zone new_york_tz;
    ASSERT_TRUE(cctz::load_time_zone("Asia/Shanghai", &shanghai_tz));
    ASSERT_TRUE(cctz::load_time_zone("America/New_York", &new_york_tz));

    const std::vector<int64_t> timestamps {0, -1, 1710053940, 1710054060, 541526400};

    for (const auto timestamp : timestamps) {
        DateV2Value<DateTimeV2ValueType> lookup_value;
        DateV2Value<DateTimeV2ValueType> original_value;

        ASSERT_TRUE(
                detail::try_convert_timestamp_with_lookup(lookup_value, timestamp, shanghai_tz));
        original_value.from_unixtime(timestamp, shanghai_tz);
        EXPECT_EQ(original_value.to_date_int_val(), lookup_value.to_date_int_val());

        ASSERT_TRUE(
                detail::try_convert_timestamp_with_lookup(lookup_value, timestamp, new_york_tz));
        original_value.from_unixtime(timestamp, new_york_tz);
        EXPECT_EQ(original_value.to_date_int_val(), lookup_value.to_date_int_val());
    }
}

TEST(ParquetColumnConvertTest, AlignNullMapUsesAppendedSourceSlice) {
    auto dst_nested_column = ColumnFloat64::create();
    dst_nested_column->insert_value(1);
    dst_nested_column->insert_value(2);
    auto dst_null_map_column = ColumnUInt8::create();
    dst_null_map_column->insert_value(0);
    dst_null_map_column->insert_value(0);
    ColumnPtr dst_column =
            ColumnNullable::create(std::move(dst_nested_column), std::move(dst_null_map_column));

    auto src_nested_column = ColumnInt64::create();
    for (int i = 0; i < 5; ++i) {
        src_nested_column->insert_value(i);
    }
    auto src_null_map_column = ColumnUInt8::create();
    src_null_map_column->insert_value(0);
    src_null_map_column->insert_value(0);
    src_null_map_column->insert_value(1);
    src_null_map_column->insert_value(0);
    src_null_map_column->insert_value(1);
    ColumnPtr src_column =
            ColumnNullable::create(std::move(src_nested_column), std::move(src_null_map_column));

    align_null_map(src_column, dst_column, 2, 3, 2);

    const auto* nullable_column = assert_cast<const ColumnNullable*>(dst_column.get());
    const auto& null_map = nullable_column->get_null_map_data();
    ASSERT_EQ(null_map.size(), 5);
    EXPECT_EQ(null_map[0], 0);
    EXPECT_EQ(null_map[1], 0);
    EXPECT_EQ(null_map[2], 1);
    EXPECT_EQ(null_map[3], 0);
    EXPECT_EQ(null_map[4], 1);
}

TEST(ParquetColumnConvertTest, AlignNullMapUsesNullablePrefixForCachedReadColumn) {
    auto dst_nested_column = ColumnFloat64::create();
    dst_nested_column->insert_value(1);
    dst_nested_column->insert_value(2);
    auto dst_null_map_column = ColumnUInt8::create();
    dst_null_map_column->insert_value(0);
    dst_null_map_column->insert_value(0);
    ColumnPtr dst_column =
            ColumnNullable::create(std::move(dst_nested_column), std::move(dst_null_map_column));

    auto src_nested_column = ColumnInt64::create();
    src_nested_column->insert_value(8);
    src_nested_column->insert_value(9);
    src_nested_column->insert_value(10);
    src_nested_column->insert_value(11);
    src_nested_column->insert_value(12);
    auto src_null_map_column = ColumnUInt8::create();
    src_null_map_column->insert_value(0);
    src_null_map_column->insert_value(0);
    src_null_map_column->insert_value(1);
    src_null_map_column->insert_value(0);
    src_null_map_column->insert_value(1);
    ColumnPtr src_column =
            ColumnNullable::create(std::move(src_nested_column), std::move(src_null_map_column));

    align_null_map(src_column, dst_column, get_null_map_size_or_inner_column_size(dst_column), 3,
                   get_appended_null_map_start(src_column, 3));

    const auto* nullable_column = assert_cast<const ColumnNullable*>(dst_column.get());
    const auto& null_map = nullable_column->get_null_map_data();
    ASSERT_EQ(null_map.size(), 5);
    EXPECT_EQ(null_map[0], 0);
    EXPECT_EQ(null_map[1], 0);
    EXPECT_EQ(null_map[2], 1);
    EXPECT_EQ(null_map[3], 0);
    EXPECT_EQ(null_map[4], 1);
}

TEST(ParquetColumnConvertTest, ConvertNullableFloatToDoubleUsesCurrentSourceNullMapSlice) {
    FieldSchema field_schema;
    field_schema.name = "float_col";
    field_schema.parquet_schema.__set_name("float_col");
    field_schema.parquet_schema.__set_type(tparquet::Type::FLOAT);
    field_schema.data_type = DataTypeFactory::instance().create_data_type(TYPE_FLOAT, true);

    const auto dst_type = DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, true);
    auto converter = PhysicalToLogicalConverter::get_converter(
            &field_schema, field_schema.data_type, dst_type, nullptr);
    ASSERT_TRUE(converter->support()) << converter->get_error_msg();

    auto src_nested_column = ColumnFloat32::create();
    src_nested_column->insert_value(1.5F);
    src_nested_column->insert_value(2.5F);
    src_nested_column->insert_value(3.5F);
    auto src_null_map_column = ColumnUInt8::create();
    src_null_map_column->insert_value(0);
    src_null_map_column->insert_value(1);
    src_null_map_column->insert_value(0);
    ColumnPtr src_column =
            ColumnNullable::create(std::move(src_nested_column), std::move(src_null_map_column));

    ColumnPtr dst_column = dst_type->create_column();
    ColumnPtr dst_alias = dst_column;

    ASSERT_TRUE(converter->convert(src_column, field_schema.data_type, dst_type, dst_column, false)
                        .ok());

    const auto* nullable_column = assert_cast<const ColumnNullable*>(dst_column.get());
    ASSERT_EQ(nullable_column->size(), 3);
    const auto& null_map = nullable_column->get_null_map_data();
    EXPECT_EQ(null_map[0], 0);
    EXPECT_EQ(null_map[1], 1);
    EXPECT_EQ(null_map[2], 0);

    const auto& nested_column =
            assert_cast<const ColumnFloat64&>(nullable_column->get_nested_column());
    EXPECT_DOUBLE_EQ(nested_column.get_data()[0], 1.5);
    EXPECT_DOUBLE_EQ(nested_column.get_data()[2], 3.5);

    const auto* original_dst = assert_cast<const ColumnNullable*>(dst_alias.get());
    EXPECT_EQ(original_dst->size(), 0);
}

TEST(ParquetColumnConvertTest,
     ConvertNullableFixedLengthStringToVarbinaryPreservesExistingDstPrefix) {
    FieldSchema field_schema;
    field_schema.name = "fixed_binary_col";
    field_schema.parquet_schema.__set_name("fixed_binary_col");
    field_schema.parquet_schema.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    field_schema.parquet_schema.__set_type_length(2);
    field_schema.data_type = DataTypeFactory::instance().create_data_type(TYPE_STRING, true);

    const auto dst_type = DataTypeFactory::instance().create_data_type(TYPE_VARBINARY, true);
    auto converter = PhysicalToLogicalConverter::get_converter(
            &field_schema, field_schema.data_type, dst_type, nullptr);
    ASSERT_TRUE(converter->support()) << converter->get_error_msg();

    auto src_nested_column = ColumnFixedLengthObject::create(2);
    src_nested_column->insert_data("aa", 2);
    src_nested_column->insert_data("bb", 2);
    src_nested_column->insert_data("cc", 2);
    auto src_null_map_column = ColumnUInt8::create();
    src_null_map_column->insert_value(0);
    src_null_map_column->insert_value(1);
    src_null_map_column->insert_value(0);
    ColumnPtr src_column =
            ColumnNullable::create(std::move(src_nested_column), std::move(src_null_map_column));

    auto dst_nested_column = ColumnVarbinary::create();
    dst_nested_column->insert_data("zz", 2);
    auto dst_null_map_column = ColumnUInt8::create();
    dst_null_map_column->insert_value(0);
    ColumnPtr dst_column =
            ColumnNullable::create(std::move(dst_nested_column), std::move(dst_null_map_column));
    ColumnPtr dst_alias = dst_column;

    ASSERT_TRUE(converter->convert(src_column, field_schema.data_type, dst_type, dst_column, false)
                        .ok());

    const auto* nullable_column = assert_cast<const ColumnNullable*>(dst_column.get());
    ASSERT_EQ(nullable_column->size(), 4);
    const auto& null_map = nullable_column->get_null_map_data();
    EXPECT_EQ(null_map[0], 0);
    EXPECT_EQ(null_map[1], 0);
    EXPECT_EQ(null_map[2], 1);
    EXPECT_EQ(null_map[3], 0);

    const auto& nested_column =
            assert_cast<const ColumnVarbinary&>(nullable_column->get_nested_column());
    ASSERT_EQ(nested_column.size(), 4);
    EXPECT_EQ(nested_column.get_data_at(0).to_string(), "zz");
    EXPECT_EQ(nested_column.get_data_at(1).to_string(), "aa");
    EXPECT_EQ(nested_column.get_data_at(3).to_string(), "cc");

    const auto* original_dst = assert_cast<const ColumnNullable*>(dst_alias.get());
    ASSERT_EQ(original_dst->size(), 1);
    EXPECT_EQ(original_dst->get_data_at(0).to_string(), "zz");
}

} // namespace doris::parquet
