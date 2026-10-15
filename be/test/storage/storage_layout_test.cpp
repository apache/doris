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
#include <cstring>
#include <limits>
#include <string>

#include "common/config.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/pod_array.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/decimalv2_value.h"
#include "core/value/jsonb_value.h"
#include "core/value/vdatetime_value.h"
#include "storage/types.h"
#include "util/defer_op.h"

namespace doris {

TEST(StorageLayoutTest, StorageValueSizeIsFieldTypeSize) {
#define CHECK_SIZE(FT) \
    EXPECT_EQ(sizeof(CppTypeTraits<FieldType::FT>::CppType), field_type_size(FieldType::FT)) << #FT;
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

TEST(StorageLayoutTest, StorageAtConvertsOneRowOfAColumn) {
    auto ints = ColumnInt32::create();
    ints->insert_value(7);
    ints->insert_value(-3);
    EXPECT_EQ(StorageLayout<FieldType::OLAP_FIELD_TYPE_INT>::storage_at(*ints, 1), -3);

    auto floats = ColumnFloat32::create();
    floats->insert_value(std::bit_cast<float>(0x7FC12345U));
    EXPECT_EQ(std::bit_cast<uint32_t>(
                      StorageLayout<FieldType::OLAP_FIELD_TYPE_FLOAT>::storage_at(*floats, 0)),
              0x7FC00000U);

    VecDateTimeValue date;
    ASSERT_TRUE(date.from_date_int64(20200101));
    auto dates = ColumnDate::create();
    dates->insert_value(date);
    EXPECT_EQ(static_cast<uint32_t>(
                      StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>::storage_at(*dates, 0)),
              (2020U << 9) | (1U << 5) | 1U);

    VecDateTimeValue datetime;
    ASSERT_TRUE(datetime.from_date_int64(20200101123045));
    auto datetimes = ColumnDateTime::create();
    datetimes->insert_value(datetime);
    EXPECT_EQ(StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME>::storage_at(*datetimes, 0),
              20200101123045);

    auto decimals = ColumnDecimal128V2::create(0, 9);
    decimals->insert_value(DecimalV2Value(123, 456000000));
    const decimal12_t stored =
            StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL>::storage_at(*decimals, 0);
    EXPECT_EQ(stored.integer, 123);
    EXPECT_EQ(stored.fraction, 456000000);
}

TEST(StorageLayoutTest, CharIsPaddedToTheDeclaredLength) {
    using Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>;
    std::string out = "key:";
    Layout::append_padded(StringRef("ab"), 5, &out);
    EXPECT_EQ(out, std::string("key:ab\0\0\0", 9));

    out.clear();
    Layout::append_padded(StringRef("hello"), 5, &out);
    EXPECT_EQ(out, "hello");

    out.clear();
    Layout::append_padded(StringRef(), 5, &out);
    EXPECT_EQ(out, std::string(5, '\0'));
}

TEST(StorageLayoutTest, OtherStringsKeepTheirBytes) {
    auto column = ColumnString::create();
    column->insert_data("hello", 5);
    PaddedPODArray<char> tmp_buffer;
    const StringRef stored =
            StorageLayout<FieldType::OLAP_FIELD_TYPE_VARCHAR>::storage_at(*column, 0, tmp_buffer);
    EXPECT_EQ(stored.data, column->get_data_at(0).data);
    EXPECT_EQ(stored.size, 5);
    EXPECT_TRUE(tmp_buffer.empty());
}

// column_to_storage writes each row's stored value back to back: a copy of the
// column's bytes where they already are the stored values, a conversion where they
// are not, and never a change to the source column.
TEST(StorageLayoutTest, ColumnToStorageCopiesOrConverts) {
    auto ints = ColumnInt32::create();
    ints->insert_value(7);
    ints->insert_value(8);
    int32_t int_out[1];
    StorageLayout<FieldType::OLAP_FIELD_TYPE_INT>::column_to_storage(
            *ints, 1, 1, reinterpret_cast<uint8_t*>(int_out));
    EXPECT_EQ(int_out[0], 8);

    const auto payload_nan = std::bit_cast<float>(0x7FC12345U);
    auto floats = ColumnFloat32::create();
    floats->insert_value(1.5F);
    floats->insert_value(payload_nan);
    uint8_t float_out[2 * sizeof(float)];
    StorageLayout<FieldType::OLAP_FIELD_TYPE_FLOAT>::column_to_storage(*floats, 0, 2, float_out);
    float stored[2];
    memcpy(stored, float_out, sizeof(stored));
    EXPECT_EQ(stored[0], 1.5F);
    EXPECT_EQ(std::bit_cast<uint32_t>(stored[1]), 0x7FC00000U) << "a NaN is stored canonical";
    EXPECT_EQ(std::bit_cast<uint32_t>(floats->get_data()[1]), 0x7FC12345U)
            << "the source column is left alone";

    auto dates = ColumnDate::create();
    VecDateTimeValue day;
    ASSERT_TRUE(day.from_date_int64(20200101));
    dates->insert_value(day);
    dates->insert_value(day);
    using DateLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>;
    using StoredDate = CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>::CppType;
    // One byte of slack in front: the stored values land unaligned, as in a page.
    uint8_t date_out[1 + 2 * sizeof(StoredDate)];
    DateLayout::column_to_storage(*dates, 0, 2, date_out + 1);
    for (size_t i = 0; i < 2; ++i) {
        StoredDate value;
        memcpy(&value, date_out + 1 + i * sizeof(value), sizeof(value));
        EXPECT_EQ(value, DateLayout::to_storage(day)) << "row " << i;
    }
}

TEST(StorageLayoutTest, AdmitsOnlyStringsWithinTheSoftLimit) {
    auto strings = ColumnString::create();
    const std::string value = "longer than the limit set below";
    strings->insert_data(value.data(), value.size());
    JsonBinaryValue document;
    ASSERT_TRUE(document.from_json_string(R"({"key": "longer than the limit set below"})").ok());
    strings->insert_data(document.value(), document.size());

    const auto saved_limit = config::string_type_length_soft_limit_bytes;
    config::string_type_length_soft_limit_bytes = 8;
    Defer restore {[&] { config::string_type_length_soft_limit_bytes = saved_limit; }};

    EXPECT_FALSE(
            admit_storage_rows(FieldType::OLAP_FIELD_TYPE_STRING, *strings, 0, 1, nullptr).ok());
    // A well-formed document is held to the limit too.
    EXPECT_FALSE(
            admit_storage_rows(FieldType::OLAP_FIELD_TYPE_JSONB, *strings, 1, 1, nullptr).ok());
    // VARCHAR has its own declared length and is not checked here.
    EXPECT_TRUE(
            admit_storage_rows(FieldType::OLAP_FIELD_TYPE_VARCHAR, *strings, 0, 1, nullptr).ok());
    // A NULL row is not looked at.
    const uint8_t null_map[1] = {1};
    EXPECT_TRUE(
            admit_storage_rows(FieldType::OLAP_FIELD_TYPE_STRING, *strings, 0, 1, null_map).ok());
}

TEST(StorageLayoutTest, AdmitsOnlyWellFormedJsonb) {
    JsonBinaryValue document;
    ASSERT_TRUE(document.from_json_string(R"({"key": [1, 2, 3]})").ok());
    auto jsonb = ColumnString::create();
    jsonb->insert_data(document.value(), document.size());
    const std::string not_a_document = "not a jsonb document";
    jsonb->insert_data(not_a_document.data(), not_a_document.size());

    EXPECT_TRUE(admit_storage_rows(FieldType::OLAP_FIELD_TYPE_JSONB, *jsonb, 0, 1, nullptr).ok());
    const Status status =
            admit_storage_rows(FieldType::OLAP_FIELD_TYPE_JSONB, *jsonb, 1, 1, nullptr);
    EXPECT_NE(status.to_string().find("Invalid JSONB document"), std::string::npos)
            << status.to_string();
    // A NULL row is not looked at, and a STRING is not a document.
    const uint8_t null_map[1] = {1};
    EXPECT_TRUE(admit_storage_rows(FieldType::OLAP_FIELD_TYPE_JSONB, *jsonb, 1, 1, null_map).ok());
    EXPECT_TRUE(admit_storage_rows(FieldType::OLAP_FIELD_TYPE_STRING, *jsonb, 1, 1, nullptr).ok());
}

} // namespace doris
