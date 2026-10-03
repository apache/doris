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

#include "storage/storage_layout.h"

#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <limits>

#include "core/types.h"
#include "core/value/decimalv2_value.h"
#include "core/value/vdatetime_value.h"
#include "storage/types.h"

namespace doris {

TEST(StorageLayoutTest, StorageValueSizeIsFieldTypeSize) {
#define CHECK_SIZE(FT)                                                                            \
    EXPECT_EQ(sizeof(StorageLayout<FieldType::FT>::StorageValue), field_type_size(FieldType::FT)) \
            << #FT;
    CHECK_SIZE(OLAP_FIELD_TYPE_BOOL)
    CHECK_SIZE(OLAP_FIELD_TYPE_TINYINT)
    CHECK_SIZE(OLAP_FIELD_TYPE_SMALLINT)
    CHECK_SIZE(OLAP_FIELD_TYPE_INT)
    CHECK_SIZE(OLAP_FIELD_TYPE_BIGINT)
    CHECK_SIZE(OLAP_FIELD_TYPE_LARGEINT)
    CHECK_SIZE(OLAP_FIELD_TYPE_UNSIGNED_INT)
    CHECK_SIZE(OLAP_FIELD_TYPE_UNSIGNED_BIGINT)
    CHECK_SIZE(OLAP_FIELD_TYPE_FLOAT)
    CHECK_SIZE(OLAP_FIELD_TYPE_DOUBLE)
    CHECK_SIZE(OLAP_FIELD_TYPE_DECIMAL)
    CHECK_SIZE(OLAP_FIELD_TYPE_DECIMAL32)
    CHECK_SIZE(OLAP_FIELD_TYPE_DECIMAL64)
    CHECK_SIZE(OLAP_FIELD_TYPE_DECIMAL128I)
    CHECK_SIZE(OLAP_FIELD_TYPE_DECIMAL256)
    CHECK_SIZE(OLAP_FIELD_TYPE_DATE)
    CHECK_SIZE(OLAP_FIELD_TYPE_DATETIME)
    CHECK_SIZE(OLAP_FIELD_TYPE_DATEV2)
    CHECK_SIZE(OLAP_FIELD_TYPE_DATETIMEV2)
    CHECK_SIZE(OLAP_FIELD_TYPE_TIMESTAMP_NS)
    CHECK_SIZE(OLAP_FIELD_TYPE_TIMESTAMPTZ)
    CHECK_SIZE(OLAP_FIELD_TYPE_IPV4)
    CHECK_SIZE(OLAP_FIELD_TYPE_IPV6)
    CHECK_SIZE(OLAP_FIELD_TYPE_UUID)
#undef CHECK_SIZE
}

TEST(StorageLayoutTest, DateV1PacksYearMonthDayIntoThreeBytes) {
    VecDateTimeValue value;
    ASSERT_TRUE(value.from_date_int64(20200101));
    const uint24_t stored = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>::to_storage(value);
    EXPECT_EQ(static_cast<uint32_t>(stored), (2020U << 9) | (1U << 5) | 1U);
}

TEST(StorageLayoutTest, DateTimeV1PacksDecimalDigits) {
    VecDateTimeValue value;
    ASSERT_TRUE(value.from_date_int64(20200101123045));
    EXPECT_EQ(StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME>::to_storage(value),
              20200101123045);
}

TEST(StorageLayoutTest, DecimalV1SplitsIntegerAndFraction) {
    using Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL>;
    decimal12_t stored = Layout::to_storage(DecimalV2Value(123, 456000000));
    EXPECT_EQ(stored.integer, 123);
    EXPECT_EQ(stored.fraction, 456000000);

    stored = Layout::to_storage(DecimalV2Value(-123, -456000000));
    EXPECT_EQ(stored.integer, -123);
    EXPECT_EQ(stored.fraction, -456000000);
}

TEST(StorageLayoutTest, FloatingPointCanonicalisesEveryNaN) {
    using FloatLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_FLOAT>;
    using DoubleLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DOUBLE>;

    const auto payload_nan = std::bit_cast<float>(0x7FC12345U);
    EXPECT_EQ(std::bit_cast<uint32_t>(FloatLayout::canonicalize_nan(payload_nan)), 0x7FC00000U);
    EXPECT_EQ(std::bit_cast<uint32_t>(FloatLayout::canonicalize_nan(-payload_nan)), 0x7FC00000U);
    EXPECT_EQ(std::bit_cast<uint32_t>(FloatLayout::canonicalize_nan(-0.0F)), 0x80000000U);
    EXPECT_EQ(FloatLayout::canonicalize_nan(1.5F), 1.5F);
    EXPECT_EQ(std::bit_cast<uint32_t>(FloatLayout::to_storage(-payload_nan)), 0x7FC00000U);

    const auto payload_nan64 = std::bit_cast<double>(0xFFF8000000000001ULL);
    EXPECT_EQ(std::bit_cast<uint64_t>(DoubleLayout::canonicalize_nan(payload_nan64)),
              0x7FF8000000000000ULL);
    EXPECT_EQ(DoubleLayout::canonicalize_nan(-2.25), -2.25);
    EXPECT_EQ(DoubleLayout::canonicalize_nan(std::numeric_limits<double>::infinity()),
              std::numeric_limits<double>::infinity());
    EXPECT_EQ(std::bit_cast<uint64_t>(DoubleLayout::to_storage(payload_nan64)),
              0x7FF8000000000000ULL);
}

TEST(StorageLayoutTest, BitCastStoresThePrimitiveBytes) {
    DateV2Value<DateV2ValueType> date;
    ASSERT_TRUE(date.from_date_int64(20200101));
    EXPECT_EQ(StorageLayout<FieldType::OLAP_FIELD_TYPE_DATEV2>::to_storage(date),
              date.to_date_int_val());

    EXPECT_EQ(StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL32>::to_storage(Decimal32(-12345)),
              -12345);
}

} // namespace doris
