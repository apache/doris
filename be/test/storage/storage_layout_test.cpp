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
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/value/decimalv2_value.h"
#include "core/value/jsonb_value.h"
#include "core/value/vdatetime_value.h"
#include "storage/types.h"
#include "util/defer_op.h"

namespace doris {

TEST(StorageLayoutTest, FixedWidthStorageValueSizeIsFieldTypeSize) {
#define CHECK_SIZE(FT)                                                                            \
    EXPECT_EQ(sizeof(StorageLayout<FieldType::FT>::StorageValue), field_type_size(FieldType::FT)) \
            << #FT;
    DORIS_APPLY_FOR_FIXED_WIDTH_STORAGE_LAYOUT_TYPES(CHECK_SIZE)
#undef CHECK_SIZE
}

TEST(StorageLayoutTest, DateV1PacksYearMonthDayIntoThreeBytes) {
    using Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>;
    VecDateTimeValue value;
    ASSERT_TRUE(value.from_date_int64(20200101));
    const uint24_t stored = Layout::to_storage(value);
    EXPECT_EQ(static_cast<uint32_t>(stored), (2020U << 9) | (1U << 5) | 1U);

    const VecDateTimeValue back = Layout::to_primitive(stored);
    EXPECT_EQ(back.year(), 2020);
    EXPECT_EQ(back.month(), 1);
    EXPECT_EQ(back.day(), 1);
    EXPECT_EQ(back.type(), TIME_DATE);
}

TEST(StorageLayoutTest, DateTimeV1PacksDecimalDigits) {
    using Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME>;
    VecDateTimeValue value;
    ASSERT_TRUE(value.from_date_int64(20200101123045));
    const int64_t stored = Layout::to_storage(value);
    EXPECT_EQ(stored, 20200101123045);

    const VecDateTimeValue back = Layout::to_primitive(stored);
    EXPECT_EQ(back.to_olap_datetime(), 20200101123045U);
    EXPECT_EQ(back.type(), TIME_DATETIME);
}

TEST(StorageLayoutTest, DecimalV1SplitsIntegerAndFraction) {
    using Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL>;
    const DecimalV2Value positive(123, 456000000);
    decimal12_t stored = Layout::to_storage(positive);
    EXPECT_EQ(stored.integer, 123);
    EXPECT_EQ(stored.fraction, 456000000);
    EXPECT_EQ(Layout::to_primitive(stored), positive);

    const DecimalV2Value negative(-123, -456000000);
    stored = Layout::to_storage(negative);
    EXPECT_EQ(stored.integer, -123);
    EXPECT_EQ(stored.fraction, -456000000);
    EXPECT_EQ(Layout::to_primitive(stored), negative);
}

TEST(StorageLayoutTest, FloatingPointCanonicalisesEveryNaN) {
    using FloatLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_FLOAT>;
    using DoubleLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DOUBLE>;

    const auto payload_nan = std::bit_cast<float>(0x7FC12345U);
    EXPECT_EQ(std::bit_cast<uint32_t>(FloatLayout::to_storage(payload_nan)), 0x7FC00000U);
    EXPECT_EQ(std::bit_cast<uint32_t>(FloatLayout::to_storage(-payload_nan)), 0x7FC00000U);
    EXPECT_EQ(std::bit_cast<uint32_t>(FloatLayout::to_storage(-0.0F)), 0x80000000U);
    EXPECT_EQ(FloatLayout::to_storage(1.5F), 1.5F);

    const auto payload_nan64 = std::bit_cast<double>(0xFFF8000000000001ULL);
    EXPECT_EQ(std::bit_cast<uint64_t>(DoubleLayout::to_storage(payload_nan64)),
              0x7FF8000000000000ULL);
    EXPECT_EQ(DoubleLayout::to_storage(-2.25), -2.25);
    EXPECT_EQ(DoubleLayout::to_primitive(std::numeric_limits<double>::infinity()),
              std::numeric_limits<double>::infinity());
}

TEST(StorageLayoutTest, BitCastStoresThePrimitiveBytes) {
    using DateV2Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATEV2>;
    DateV2Value<DateV2ValueType> date;
    ASSERT_TRUE(date.from_date_int64(20200101));
    const uint32_t stored = DateV2Layout::to_storage(date);
    EXPECT_EQ(stored, date.to_date_int_val());
    EXPECT_EQ(DateV2Layout::to_primitive(stored), date);

    using Decimal32Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL32>;
    EXPECT_EQ(Decimal32Layout::to_storage(Decimal32(-12345)), -12345);
    EXPECT_EQ(Decimal32Layout::to_primitive(-12345).value, -12345);
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

// A CHAR StorageValue is the row's own bytes, whatever their length: nothing is copied
// and the tmp_buffer stays untouched. Only a key encoding pads, through append_padded.
TEST(StorageLayoutTest, CharStoresTheRowsOwnBytes) {
    using Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>;
    const std::string value = "Allemande";
    auto input = ColumnString::create();
    const size_t rows = value.length();
    for (size_t i = 0; i < rows; i++) {
        input->insert_data(value.data(), value.length() - i);
    }
    PaddedPODArray<char> tmp_buffer;
    for (size_t i = 0; i < rows; i++) {
        const StringRef stored = Layout::storage_at(*input, i, tmp_buffer);
        EXPECT_EQ(stored.size, value.length() - i) << "row " << i;
        EXPECT_EQ(stored.data, input->get_data_at(i).data) << "row " << i << " was copied";
    }
    EXPECT_TRUE(tmp_buffer.empty());
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

// column_to_storage writes each row's StorageValue back to back: a copy of the
// column's bytes where they already are the StorageValues, a conversion where
// they are not, and never a change to the source column.
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
    // One byte of slack in front: the StorageValues land unaligned, as in a page.
    uint8_t date_out[1 + 2 * sizeof(DateLayout::StorageValue)];
    DateLayout::column_to_storage(*dates, 0, 2, date_out + 1);
    for (size_t i = 0; i < 2; ++i) {
        DateLayout::StorageValue value;
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

TEST(StorageLayoutTest, ReadToColumnRepacksTheV1LayoutsFromUnalignedBytes) {
    using DateLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>;
    using DateTimeLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME>;
    using DecimalLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL>;

    VecDateTimeValue day1;
    VecDateTimeValue day2;
    ASSERT_TRUE(day1.from_date_int64(20200101));
    ASSERT_TRUE(day2.from_date_int64(20211231));
    VecDateTimeValue moment;
    ASSERT_TRUE(moment.from_date_int64(20200101123045));
    const DecimalV2Value amount(-123, -456000000);

    // A page holds the StorageValues after a header, so they are not aligned.
    std::string page(1, 'h');
    auto put = [&](const auto& value) {
        page.append(reinterpret_cast<const char*>(&value), sizeof(value));
    };
    put(DateLayout::to_storage(day1));
    put(DateLayout::to_storage(day2));
    put(DateTimeLayout::to_storage(moment));
    put(DecimalLayout::to_storage(amount));
    const auto* stored = reinterpret_cast<const uint8_t*>(page.data()) + 1;

    auto dates = ColumnDate::create();
    read_to_column<FieldType::OLAP_FIELD_TYPE_DATE>(stored, 2, *dates);
    ASSERT_EQ(dates->size(), 2);
    EXPECT_EQ(dates->get_data()[0], day1);
    EXPECT_EQ(dates->get_data()[1], day2);
    stored += 2 * sizeof(uint24_t);

    auto moments = ColumnDateTime::create();
    read_to_column<FieldType::OLAP_FIELD_TYPE_DATETIME>(stored, 1, *moments);
    ASSERT_EQ(moments->size(), 1);
    EXPECT_EQ(moments->get_data()[0], moment);
    stored += sizeof(int64_t);

    auto amounts = ColumnDecimal128V2::create(0, 9);
    read_to_column<FieldType::OLAP_FIELD_TYPE_DECIMAL>(stored, 1, *amounts);
    ASSERT_EQ(amounts->size(), 1);
    EXPECT_EQ(amounts->get_data()[0], amount);
}

TEST(StorageLayoutTest, ReadToColumnIntoNullableMarksTheRowsNotNull) {
    auto column = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
    column->insert_default();
    const int32_t stored[] = {7, 8};
    read_to_column<FieldType::OLAP_FIELD_TYPE_INT>(reinterpret_cast<const uint8_t*>(stored), 2,
                                                   *column);
    ASSERT_EQ(column->size(), 3);
    EXPECT_TRUE(column->is_null_at(0));
    EXPECT_FALSE(column->is_null_at(1));
    EXPECT_FALSE(column->is_null_at(2));
    const auto& values = assert_cast<const ColumnInt32&>(column->get_nested_column()).get_data();
    EXPECT_EQ(values[1], 7);
    EXPECT_EQ(values[2], 8);
}

} // namespace doris
