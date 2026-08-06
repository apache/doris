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

// This test exercises the serde batch functions (from_int_batch, from_float_batch,
// from_decimal_batch and their strict_mode counterparts) for all datelike types:
//   DateV1, DateTimeV1, DateV2, DateTimeV2, TimeV2
//
// The primary goal is to verify that the non-strict batch path uses NON_STRICT parse
// mode (returning OK with null for invalid rows) and the strict batch path uses STRICT
// parse mode (returning error Status on the first invalid row).
//
// The DCHECK(IsStrict == params.is_strict) guards added inside each cast helper will
// also fire on any mismatch in debug/ASAN builds, providing a compile-time-like safety
// net against accidental mode swaps.

#include <gtest/gtest.h>

#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type_serde/data_type_date_or_datetime_serde.h"
#include "core/data_type_serde/data_type_datetimev2_serde.h"
#include "core/data_type_serde/data_type_datev2_serde.h"
#include "core/data_type_serde/data_type_time_serde.h"

namespace doris {

class DatelikeSerDeBatchTest : public ::testing::Test {};

// ============================================================================
// Helper: build a ColumnDecimal64 with given raw int64 values at a fixed scale.
// Each value is stored as value * 10^scale, so get_intergral_part gives `value`
// and get_fractional_part gives 0.
// ============================================================================
static ColumnDecimal64::MutablePtr build_decimal64_column(const std::vector<int64_t>& int_parts,
                                                          uint32_t scale) {
    auto col = ColumnDecimal64::create(0, scale);
    int64_t multiplier = common::exp10_i64(scale);
    for (auto v : int_parts) {
        col->insert_value(Decimal64(v * multiplier));
    }
    return col;
}

// Helper: build a ColumnInt64 with given values.
static ColumnInt64::MutablePtr build_int64_column(const std::vector<int64_t>& values) {
    auto col = ColumnInt64::create();
    for (auto v : values) {
        col->insert_value(v);
    }
    return col;
}

// Helper: build a ColumnFloat64 with given values.
static ColumnFloat64::MutablePtr build_float64_column(const std::vector<double>& values) {
    auto col = ColumnFloat64::create();
    for (auto v : values) {
        col->insert_value(v);
    }
    return col;
}

// ============================================================================
// DateV1 / DateTimeV1 (DataTypeDateSerDe<TYPE_DATE / TYPE_DATETIME>)
// ============================================================================

// --- from_decimal_batch (non-strict) ---
// Valid: 20230115 (8 digits → 2023-01-15)
// Invalid: -1 (negative, always rejected)
TEST_F(DatelikeSerDeBatchTest, datev1_from_decimal_batch) {
    DataTypeDateSerDe<TYPE_DATE> serde;
    auto dec_col = build_decimal64_column({20230115, -1}, /*scale=*/0);

    auto data_col = ColumnDate::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_decimal_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

// --- from_decimal_strict_mode_batch (strict) ---
TEST_F(DatelikeSerDeBatchTest, datev1_from_decimal_strict_mode_batch) {
    DataTypeDateSerDe<TYPE_DATE> serde;
    auto dec_col = build_decimal64_column({-1}, /*scale=*/0);

    auto target = ColumnDate::create();
    auto st = serde.from_decimal_strict_mode_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_FALSE(st.ok());
}

TEST_F(DatelikeSerDeBatchTest, datetimev1_from_decimal_batch) {
    DataTypeDateSerDe<TYPE_DATETIME> serde;
    auto dec_col = build_decimal64_column({20230115143059, -1}, /*scale=*/0);

    auto data_col = ColumnDateTime::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_decimal_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, datetimev1_from_decimal_strict_mode_batch) {
    DataTypeDateSerDe<TYPE_DATETIME> serde;
    auto dec_col = build_decimal64_column({-1}, /*scale=*/0);

    auto target = ColumnDateTime::create();
    auto st = serde.from_decimal_strict_mode_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_FALSE(st.ok());
}

// --- from_int_batch / from_int_strict_mode_batch ---
TEST_F(DatelikeSerDeBatchTest, datev1_from_int_batch) {
    DataTypeDateSerDe<TYPE_DATE> serde;
    auto int_col = build_int64_column({20230115, -1});

    auto data_col = ColumnDate::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_int_batch<DataTypeInt64>(*int_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, datev1_from_int_strict_mode_batch) {
    DataTypeDateSerDe<TYPE_DATE> serde;
    auto int_col = build_int64_column({-1});

    auto target = ColumnDate::create();
    auto st = serde.from_int_strict_mode_batch<DataTypeInt64>(*int_col, *target);
    ASSERT_FALSE(st.ok());
}

// --- from_float_batch / from_float_strict_mode_batch ---
TEST_F(DatelikeSerDeBatchTest, datev1_from_float_batch) {
    DataTypeDateSerDe<TYPE_DATE> serde;
    auto float_col = build_float64_column({20230115.0, -1.0});

    auto data_col = ColumnDate::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_float_batch<DataTypeFloat64>(*float_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, datev1_from_float_strict_mode_batch) {
    DataTypeDateSerDe<TYPE_DATE> serde;
    auto float_col = build_float64_column({-1.0});

    auto target = ColumnDate::create();
    auto st = serde.from_float_strict_mode_batch<DataTypeFloat64>(*float_col, *target);
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// DateV2 (DataTypeDateV2SerDe)
// ============================================================================
TEST_F(DatelikeSerDeBatchTest, datev2_from_decimal_batch) {
    DataTypeDateV2SerDe serde;
    auto dec_col = build_decimal64_column({20230115, -1}, /*scale=*/0);

    auto data_col = ColumnDateV2::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_decimal_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, datev2_from_decimal_strict_mode_batch) {
    DataTypeDateV2SerDe serde;
    auto dec_col = build_decimal64_column({-1}, /*scale=*/0);

    auto target = ColumnDateV2::create();
    auto st = serde.from_decimal_strict_mode_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_FALSE(st.ok());
}

TEST_F(DatelikeSerDeBatchTest, datev2_from_int_batch) {
    DataTypeDateV2SerDe serde;
    auto int_col = build_int64_column({20230115, -1});

    auto data_col = ColumnDateV2::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_int_batch<DataTypeInt64>(*int_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, datev2_from_int_strict_mode_batch) {
    DataTypeDateV2SerDe serde;
    auto int_col = build_int64_column({-1});

    auto target = ColumnDateV2::create();
    auto st = serde.from_int_strict_mode_batch<DataTypeInt64>(*int_col, *target);
    ASSERT_FALSE(st.ok());
}

TEST_F(DatelikeSerDeBatchTest, datev2_from_float_batch) {
    DataTypeDateV2SerDe serde;
    auto float_col = build_float64_column({20230115.0, -1.0});

    auto data_col = ColumnDateV2::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_float_batch<DataTypeFloat64>(*float_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, datev2_from_float_strict_mode_batch) {
    DataTypeDateV2SerDe serde;
    auto float_col = build_float64_column({-1.0});

    auto target = ColumnDateV2::create();
    auto st = serde.from_float_strict_mode_batch<DataTypeFloat64>(*float_col, *target);
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// DateTimeV2 (DataTypeDateTimeV2SerDe)
// ============================================================================
TEST_F(DatelikeSerDeBatchTest, datetimev2_from_decimal_batch) {
    DataTypeDateTimeV2SerDe serde(/*scale=*/0);
    auto dec_col = build_decimal64_column({20230115143059, -1}, /*scale=*/0);

    auto data_col = ColumnDateTimeV2::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_decimal_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, datetimev2_from_decimal_strict_mode_batch) {
    DataTypeDateTimeV2SerDe serde(/*scale=*/0);
    auto dec_col = build_decimal64_column({-1}, /*scale=*/0);

    auto target = ColumnDateTimeV2::create();
    auto st = serde.from_decimal_strict_mode_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_FALSE(st.ok());
}

TEST_F(DatelikeSerDeBatchTest, datetimev2_from_int_batch) {
    DataTypeDateTimeV2SerDe serde(/*scale=*/0);
    auto int_col = build_int64_column({20230115143059, -1});

    auto data_col = ColumnDateTimeV2::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_int_batch<DataTypeInt64>(*int_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, datetimev2_from_int_strict_mode_batch) {
    DataTypeDateTimeV2SerDe serde(/*scale=*/0);
    auto int_col = build_int64_column({-1});

    auto target = ColumnDateTimeV2::create();
    auto st = serde.from_int_strict_mode_batch<DataTypeInt64>(*int_col, *target);
    ASSERT_FALSE(st.ok());
}

TEST_F(DatelikeSerDeBatchTest, datetimev2_from_float_batch) {
    DataTypeDateTimeV2SerDe serde(/*scale=*/0);
    auto float_col = build_float64_column({20230115143059.0, -1.0});

    auto data_col = ColumnDateTimeV2::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_float_batch<DataTypeFloat64>(*float_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, datetimev2_from_float_strict_mode_batch) {
    DataTypeDateTimeV2SerDe serde(/*scale=*/0);
    auto float_col = build_float64_column({-1.0});

    auto target = ColumnDateTimeV2::create();
    auto st = serde.from_float_strict_mode_batch<DataTypeFloat64>(*float_col, *target);
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// TimeV2 (DataTypeTimeV2SerDe)
// ============================================================================
TEST_F(DatelikeSerDeBatchTest, timev2_from_decimal_batch) {
    DataTypeTimeV2SerDe serde(/*scale=*/0);
    // 123 → 00:01:23, valid 3-digit time
    // A value that overflows int64 range won't fit in Decimal64 so use a huge number
    // that won't parse as valid time: 99999999 (8 digits, > 7 not valid for time)
    auto dec_col = build_decimal64_column({123, 99999999}, /*scale=*/0);

    auto data_col = ColumnTimeV2::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_decimal_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, timev2_from_decimal_strict_mode_batch) {
    DataTypeTimeV2SerDe serde(/*scale=*/0);
    auto dec_col = build_decimal64_column({99999999}, /*scale=*/0);

    auto target = ColumnTimeV2::create();
    auto st = serde.from_decimal_strict_mode_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_FALSE(st.ok());
}

TEST_F(DatelikeSerDeBatchTest, timev2_from_int_batch) {
    DataTypeTimeV2SerDe serde(/*scale=*/0);
    auto int_col = build_int64_column({123, 99999999});

    auto data_col = ColumnTimeV2::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_int_batch<DataTypeInt64>(*int_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, timev2_from_int_strict_mode_batch) {
    DataTypeTimeV2SerDe serde(/*scale=*/0);
    auto int_col = build_int64_column({99999999});

    auto target = ColumnTimeV2::create();
    auto st = serde.from_int_strict_mode_batch<DataTypeInt64>(*int_col, *target);
    ASSERT_FALSE(st.ok());
}

TEST_F(DatelikeSerDeBatchTest, timev2_from_float_batch) {
    DataTypeTimeV2SerDe serde(/*scale=*/0);
    auto float_col = build_float64_column({123.0, 99999999.0});

    auto data_col = ColumnTimeV2::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_float_batch<DataTypeFloat64>(*float_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 2);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
}

TEST_F(DatelikeSerDeBatchTest, timev2_from_float_strict_mode_batch) {
    DataTypeTimeV2SerDe serde(/*scale=*/0);
    auto float_col = build_float64_column({99999999.0});

    auto target = ColumnTimeV2::create();
    auto st = serde.from_float_strict_mode_batch<DataTypeFloat64>(*float_col, *target);
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// Mixed valid+invalid batch: ensures non-strict handles multiple rows correctly,
// setting null only on invalid rows while correctly populating valid ones.
// Also acts as regression test for the original bug where STRICT mode was
// accidentally used in from_decimal_batch for DateV1/DateTimeV1.
// ============================================================================
TEST_F(DatelikeSerDeBatchTest, datev1_from_decimal_batch_mixed) {
    DataTypeDateSerDe<TYPE_DATE> serde;
    // valid, invalid, valid, invalid
    auto dec_col = build_decimal64_column({20230115, -1, 20001231, 0}, /*scale=*/0);

    auto data_col = ColumnDate::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_decimal_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 4);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
    EXPECT_FALSE(target->is_null_at(2));
    EXPECT_TRUE(target->is_null_at(3));
}

TEST_F(DatelikeSerDeBatchTest, datetimev1_from_decimal_batch_mixed) {
    DataTypeDateSerDe<TYPE_DATETIME> serde;
    auto dec_col = build_decimal64_column({20230115143059, -1, 20001231235959, 0}, /*scale=*/0);

    auto data_col = ColumnDateTime::create();
    auto null_col = ColumnUInt8::create();
    auto target = ColumnNullable::create(std::move(data_col), std::move(null_col));

    auto st = serde.from_decimal_batch<DataTypeDecimal64>(*dec_col, *target);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(target->size(), 4);
    EXPECT_FALSE(target->is_null_at(0));
    EXPECT_TRUE(target->is_null_at(1));
    EXPECT_FALSE(target->is_null_at(2));
    EXPECT_TRUE(target->is_null_at(3));
}

} // namespace doris
