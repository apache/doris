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

} // namespace doris::parquet
