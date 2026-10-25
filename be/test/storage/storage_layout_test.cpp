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
#include <type_traits>

#include "common/config.h"
#include "common/exception.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_nullable.h"
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
    StorageBuffers buffers;
    const auto* cells = reinterpret_cast<const Slice*>(
            StorageLayout<FieldType::OLAP_FIELD_TYPE_VARCHAR>::column_to_storage(*column, 0, 1,
                                                                                 buffers));
    EXPECT_EQ(cells, buffers.cells.data());
    EXPECT_EQ(cells[0].data, column->get_data_at(0).data);
    EXPECT_EQ(cells[0].size, 5);
    EXPECT_TRUE(buffers.bytes.empty());
}

TEST(StorageLayoutTest, FixedBatchViewBorrowsTheRequestedRange) {
    auto column = ColumnInt32::create();
    column->insert_value(7);
    column->insert_value(11);
    column->insert_value(13);
    StorageBuffers buffers;
    const auto* values = StorageLayout<FieldType::OLAP_FIELD_TYPE_INT>::column_to_storage(
            *column, 1, 2, buffers);
    EXPECT_EQ(values, reinterpret_cast<const uint8_t*>(column->get_data().data() + 1));
    EXPECT_TRUE(buffers.bytes.empty());
    const auto* cells = reinterpret_cast<const int32_t*>(values);
    EXPECT_EQ(cells[0], 11);
    EXPECT_EQ(cells[1], 13);
}

TEST(StorageLayoutTest, StringBatchViewBorrowsPayloadsAndCarriesLengths) {
    auto column = ColumnString::create();
    for (const std::string value : {"outside", "", "hello"}) {
        column->insert_data(value.data(), value.size());
    }
    StorageBuffers buffers;
    const auto* values = StorageLayout<FieldType::OLAP_FIELD_TYPE_STRING>::column_to_storage(
            *column, 1, 2, buffers);
    const auto* cells = reinterpret_cast<const Slice*>(values);
    EXPECT_EQ(cells, buffers.cells.data());
    ASSERT_EQ(buffers.cells.size(), 2);
    EXPECT_EQ(cells[0].size, 0);
    EXPECT_EQ(cells[1].size, 5);
    EXPECT_EQ(cells[1].data, column->get_data_at(2).data);
    EXPECT_EQ(cells[1].to_string(), "hello");
}

// column_to_storage converts rows to contiguous storage values without changing
// the source column.
TEST(StorageLayoutTest, ColumnToStoragePreservesSource) {
    const auto payload_nan = std::bit_cast<float>(0x7FC12345U);
    auto floats = ColumnFloat32::create();
    floats->insert_value(1.5F);
    floats->insert_value(payload_nan);
    StorageBuffers float_buffers;
    const auto* float_out = StorageLayout<FieldType::OLAP_FIELD_TYPE_FLOAT>::column_to_storage(
            *floats, 0, 2, float_buffers);
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
    StorageBuffers date_buffers;
    const auto* date_out = DateLayout::column_to_storage(*dates, 0, 2, date_buffers);
    for (size_t i = 0; i < 2; ++i) {
        StoredDate value;
        memcpy(&value, date_out + i * sizeof(value), sizeof(value));
        EXPECT_EQ(value, DateLayout::to_storage(day)) << "row " << i;
    }
}

namespace {

template <FieldType FT>
void expect_floating_point_range_views() {
    using Value = typename CppTypeTraits<FT>::CppType;
    using UInt = std::conditional_t<sizeof(Value) == sizeof(uint32_t), uint32_t, uint64_t>;
    using Column =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
    const UInt payload_bits =
            sizeof(Value) == sizeof(float) ? UInt(0x7FC12345U) : UInt(0xFFF8000000000001ULL);
    const auto payload_nan = std::bit_cast<Value>(payload_bits);
    auto column = Column::create();
    for (Value value : {payload_nan, Value(1.5), Value(-0.0),
                        std::numeric_limits<Value>::infinity(), payload_nan}) {
        column->insert_value(value);
    }
    StorageBuffers buffers;
    const auto* source = reinterpret_cast<const uint8_t*>(column->get_data().data());
    EXPECT_EQ(StorageLayout<FT>::column_to_storage(*column, 1, 3, buffers), source + sizeof(Value));
    EXPECT_EQ(buffers.bytes.allocated_bytes(), 0);

    const auto* converted = StorageLayout<FT>::column_to_storage(*column, 0, 2, buffers);
    EXPECT_NE(converted, source);
    Value stored[2];
    memcpy(stored, converted, sizeof(stored));
    EXPECT_EQ(std::bit_cast<UInt>(stored[0]),
              std::bit_cast<UInt>(std::numeric_limits<Value>::quiet_NaN()));
    EXPECT_EQ(stored[1], Value(1.5));
    EXPECT_EQ(std::bit_cast<UInt>(column->get_data()[0]), payload_bits);
    EXPECT_EQ(std::bit_cast<UInt>(column->get_data()[4]), payload_bits);

    // The prefix is copied bit for bit, including -0 and infinity, when
    // the final row in [1, 5) needs NaN normalization.
    converted = StorageLayout<FT>::column_to_storage(*column, 1, 4, buffers);
    Value with_prefix[4];
    memcpy(with_prefix, converted, sizeof(with_prefix));
    EXPECT_EQ(memcmp(with_prefix, column->get_data().data() + 1, 3 * sizeof(Value)), 0);
    EXPECT_EQ(std::bit_cast<UInt>(with_prefix[3]),
              std::bit_cast<UInt>(std::numeric_limits<Value>::quiet_NaN()));

    // Reusing a buffer from an earlier NaN range must not force later clean rows
    // through it. The NaNs surrounding [1, 4) are outside the requested range.
    const std::string previous_buffer(buffers.bytes.begin(), buffers.bytes.end());
    const auto allocated_bytes = buffers.bytes.allocated_bytes();
    EXPECT_EQ(StorageLayout<FT>::column_to_storage(*column, 1, 3, buffers), source + sizeof(Value));
    EXPECT_EQ(buffers.bytes.allocated_bytes(), allocated_bytes);
    EXPECT_EQ(std::string(buffers.bytes.begin(), buffers.bytes.end()), previous_buffer);
}

} // namespace

TEST(StorageLayoutTest, FloatingPointBorrowsOnlyTheSelectedRange) {
    expect_floating_point_range_views<FieldType::OLAP_FIELD_TYPE_FLOAT>();
    expect_floating_point_range_views<FieldType::OLAP_FIELD_TYPE_DOUBLE>();
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

TEST(StorageLayoutTest, CharIsPaddedIntoReusedBytes) {
    using Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>;
    auto strings = ColumnString::create();
    for (const std::string value : {"outside", "abcd", "xy", "a"}) {
        strings->insert_data(value.data(), value.size());
    }
    StorageBuffers buffers;
    const auto* cells = reinterpret_cast<const Slice*>(
            Layout::column_to_storage(*strings, 1, 2, 4, nullptr, buffers));
    EXPECT_EQ(cells, buffers.cells.data());
    EXPECT_EQ(cells[0].to_string(), "abcd");
    EXPECT_EQ(cells[1].to_string(), std::string("xy\0\0", 4));
    // A reused buffer must not keep the bytes of a longer earlier value.
    const auto* bytes = buffers.bytes.data();
    cells = reinterpret_cast<const Slice*>(
            Layout::column_to_storage(*strings, 3, 1, 4, nullptr, buffers));
    EXPECT_EQ(buffers.bytes.data(), bytes);
    EXPECT_EQ(cells[0].to_string(), std::string("a\0\0\0", 4));

    // A NULL row keeps an empty, zeroed slot, and its value is never read: "outside" is
    // longer than CHAR(4).
    const uint8_t null_map[] = {1, 0, 1};
    cells = reinterpret_cast<const Slice*>(
            Layout::column_to_storage(*strings, 0, 3, 4, null_map, buffers));
    EXPECT_TRUE(cells[0].empty());
    EXPECT_EQ(cells[1].to_string(), "abcd");
    EXPECT_TRUE(cells[2].empty());
    EXPECT_EQ(std::string(buffers.bytes.begin(), buffers.bytes.end()),
              std::string("\0\0\0\0abcd\0\0\0\0", 12));

    // The Bytes overload stores a value as it is, which the SNII index is fed.
    cells = reinterpret_cast<const Slice*>(Layout::column_to_storage(*strings, 3, 1, buffers));
    EXPECT_EQ(cells[0].data, strings->get_data_at(3).data);
    EXPECT_EQ(cells[0].size, 1);
}

TEST(StorageLayoutTest, AggregateStateObjectsAreBorrowed) {
    using Layout = StorageLayout<FieldType::OLAP_FIELD_TYPE_AGG_STATE>;
    auto objects = ColumnFixedLengthObject::create(sizeof(int64_t));
    for (int64_t value : {11, 22, 33, 44}) {
        objects->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    StorageBuffers buffers;
    const auto* cells =
            reinterpret_cast<const Slice*>(Layout::column_to_storage(*objects, 1, 2, buffers));
    EXPECT_EQ(cells, buffers.cells.data());
    EXPECT_EQ(cells[0].data,
              reinterpret_cast<const char*>(objects->get_data().data()) + sizeof(int64_t));
    EXPECT_EQ(cells[1].data, cells[0].data + sizeof(int64_t));
    EXPECT_EQ(cells[1].size, sizeof(int64_t));
    int64_t stored = 0;
    memcpy(&stored, cells[1].data, sizeof(stored));
    EXPECT_EQ(stored, 33);
    EXPECT_TRUE(buffers.bytes.empty());
}

TEST(StorageLayoutTest, RuntimeTypeConvertsThroughItsLayout) {
    StorageBuffers buffers;
    auto ints = ColumnInt32::create();
    ints->insert_value(7);
    ints->insert_value(11);
    EXPECT_EQ(column_to_storage(FieldType::OLAP_FIELD_TYPE_INT, *ints, 1, 1, buffers),
              reinterpret_cast<const uint8_t*>(ints->get_data().data() + 1));

    auto dates = ColumnDate::create();
    VecDateTimeValue day;
    ASSERT_TRUE(day.from_date_int64(20200101));
    dates->insert_value(day);
    using StoredDate = CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>::CppType;
    StoredDate stored;
    memcpy(&stored, column_to_storage(FieldType::OLAP_FIELD_TYPE_DATE, *dates, 0, 1, buffers),
           sizeof(stored));
    EXPECT_EQ(stored, StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>::to_storage(day));

    auto strings = ColumnString::create();
    strings->insert_data("ab", 2);
    const auto* cells = reinterpret_cast<const Slice*>(
            column_to_storage(FieldType::OLAP_FIELD_TYPE_VARCHAR, *strings, 0, 1, buffers));
    EXPECT_EQ(cells[0].data, strings->get_data_at(0).data);
    // Whether CHAR is padded is the caller's choice, made through StorageLayout<CHAR>.
    EXPECT_THROW(column_to_storage(FieldType::OLAP_FIELD_TYPE_CHAR, *strings, 0, 1, buffers),
                 Exception);
}

TEST(StorageLayoutTest, ToPrimitiveInvertsToStorage) {
    using DateLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>;
    VecDateTimeValue day;
    ASSERT_TRUE(day.from_date_int64(20200229));
    const VecDateTimeValue read_day = DateLayout::to_primitive(DateLayout::to_storage(day));
    EXPECT_EQ(read_day, day);
    EXPECT_EQ(read_day.to_olap_date(), day.to_olap_date());
    VecDateTimeValue last_day;
    ASSERT_TRUE(last_day.from_date_int64(99991231));
    EXPECT_EQ(DateLayout::to_primitive(DateLayout::to_storage(last_day)), last_day);

    using DateTimeLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME>;
    VecDateTimeValue moment;
    ASSERT_TRUE(moment.from_date_int64(20200101123045));
    EXPECT_EQ(DateTimeLayout::to_primitive(DateTimeLayout::to_storage(moment)), moment);

    using DecimalLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL>;
    const DecimalV2Value amount(123, 456000000);
    EXPECT_EQ(DecimalLayout::to_primitive(DecimalLayout::to_storage(amount)), amount);
    const DecimalV2Value debt(-123, -456000000);
    EXPECT_EQ(DecimalLayout::to_primitive(DecimalLayout::to_storage(debt)), debt);
    const DecimalV2Value read_debt = DecimalLayout::to_primitive(decimal12_t {-123, -456000000});
    EXPECT_EQ(read_debt.int_value(), -123);
    EXPECT_EQ(read_debt.frac_value(), -456000000);

    // Reading does not canonicalise: a stored NaN keeps its payload.
    using FloatLayout = StorageLayout<FieldType::OLAP_FIELD_TYPE_FLOAT>;
    const auto payload_nan = std::bit_cast<float>(0x7FC12345U);
    EXPECT_EQ(std::bit_cast<uint32_t>(FloatLayout::to_primitive(payload_nan)), 0x7FC12345U);
    EXPECT_EQ(FloatLayout::to_primitive(FloatLayout::to_storage(1.5F)), 1.5F);

    DateV2Value<DateV2ValueType> date;
    ASSERT_TRUE(date.from_date_int64(20200101));
    EXPECT_EQ(
            StorageLayout<FieldType::OLAP_FIELD_TYPE_DATEV2>::to_primitive(date.to_date_int_val()),
            date);
    EXPECT_EQ(StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL32>::to_primitive(-12345),
              Decimal32(-12345));
    EXPECT_EQ(StorageLayout<FieldType::OLAP_FIELD_TYPE_BOOL>::to_primitive(uint8_t(1)), UInt8(1));
}

TEST(StorageLayoutTest, StorageToColumnRepacksTheV1LayoutsFromUnalignedBytes) {
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

    // A page holds the stored values after a header, so they are not aligned.
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
    dates->insert_value(day2);
    DateLayout::storage_to_column(stored, 2, *dates);
    ASSERT_EQ(dates->size(), 3);
    EXPECT_EQ(dates->get_data()[1], day1);
    EXPECT_EQ(dates->get_data()[2], day2);
    stored += 2 * sizeof(uint24_t);

    auto moments = ColumnDateTime::create();
    DateTimeLayout::storage_to_column(stored, 1, *moments);
    ASSERT_EQ(moments->size(), 1);
    EXPECT_EQ(moments->get_data()[0], moment);
    stored += sizeof(int64_t);

    auto amounts = ColumnDecimal128V2::create(0, 9);
    DecimalLayout::storage_to_column(stored, 1, *amounts);
    ASSERT_EQ(amounts->size(), 1);
    EXPECT_EQ(amounts->get_data()[0], amount);
}

TEST(StorageLayoutTest, StorageToColumnCopiesStoredValuesAsTheyAre) {
    const float stored_floats[] = {1.5F, std::bit_cast<float>(0x7FC12345U)};
    auto floats = ColumnFloat32::create();
    StorageLayout<FieldType::OLAP_FIELD_TYPE_FLOAT>::storage_to_column(
            reinterpret_cast<const uint8_t*>(stored_floats), 2, *floats);
    ASSERT_EQ(floats->size(), 2);
    EXPECT_EQ(floats->get_data()[0], 1.5F);
    EXPECT_EQ(std::bit_cast<uint32_t>(floats->get_data()[1]), 0x7FC12345U);

    const int32_t stored_decimals[] = {-12345, 67890};
    auto decimals = ColumnDecimal32::create(0, 3);
    StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL32>::storage_to_column(
            reinterpret_cast<const uint8_t*>(stored_decimals), 2, *decimals);
    ASSERT_EQ(decimals->size(), 2);
    EXPECT_EQ(decimals->get_data()[0], Decimal32(-12345));
    EXPECT_EQ(decimals->get_data()[1], Decimal32(67890));
}

TEST(StorageLayoutTest, StorageToColumnIntoNullableMarksTheRowsNotNull) {
    auto ints = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
    ints->insert_default();
    const int32_t stored_ints[] = {7, 8};
    StorageLayout<FieldType::OLAP_FIELD_TYPE_INT>::storage_to_column(
            reinterpret_cast<const uint8_t*>(stored_ints), 2, *ints);
    ASSERT_EQ(ints->size(), 3);
    EXPECT_TRUE(ints->is_null_at(0));
    EXPECT_FALSE(ints->is_null_at(1));
    EXPECT_FALSE(ints->is_null_at(2));
    const auto& values = assert_cast<const ColumnInt32&>(ints->get_nested_column()).get_data();
    EXPECT_EQ(values[1], 7);
    EXPECT_EQ(values[2], 8);

    auto dates = ColumnNullable::create(ColumnDate::create(), ColumnUInt8::create());
    VecDateTimeValue day;
    ASSERT_TRUE(day.from_date_int64(20200101));
    const uint24_t stored_date = StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>::to_storage(day);
    StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>::storage_to_column(
            reinterpret_cast<const uint8_t*>(&stored_date), 1, *dates);
    ASSERT_EQ(dates->size(), 1);
    EXPECT_FALSE(dates->is_null_at(0));
    EXPECT_EQ(assert_cast<const ColumnDate&>(dates->get_nested_column()).get_data()[0], day);
}

TEST(StorageLayoutTest, ObjectBatchSerialisesEveryValue) {
    auto bitmaps = ColumnBitmap::create();
    for (size_t i = 0; i < 3; ++i) {
        BitmapValue bitmap;
        for (size_t j = 0; j <= i * 3; ++j) {
            bitmap.add(i * 100 + j);
        }
        bitmaps->insert_value(bitmap);
    }
    StorageBuffers buffers;
    const auto* cells = reinterpret_cast<const Slice*>(
            StorageLayout<FieldType::OLAP_FIELD_TYPE_BITMAP>::column_to_storage(*bitmaps, 0, 3,
                                                                                buffers));
    EXPECT_EQ(cells, buffers.cells.data());
    const auto& values = bitmaps->get_data();
    // The payloads sit back to back in the batch's bytes.
    const auto* bytes = reinterpret_cast<const char*>(buffers.bytes.data());
    EXPECT_EQ(cells[0].data, bytes);
    size_t total_size = 0;
    for (size_t row = 0; row < 3; ++row) {
        EXPECT_EQ(cells[row].data, bytes + total_size) << "row " << row;
        EXPECT_EQ(cells[row].size, values[row].getSizeInBytes()) << "row " << row;
        EXPECT_EQ(BitmapValue(cells[row].data).to_string(), values[row].to_string());
        total_size += cells[row].size;
    }
    EXPECT_EQ(buffers.bytes.size(), total_size);

    auto hlls = ColumnHLL::create();
    hlls->insert_value(HyperLogLog(uint64_t(7)));
    hlls->insert_value(HyperLogLog());
    cells = reinterpret_cast<const Slice*>(
            StorageLayout<FieldType::OLAP_FIELD_TYPE_HLL>::column_to_storage(*hlls, 0, 2, buffers));
    for (size_t row = 0; row < 2; ++row) {
        std::string expected(hlls->get_data()[row].max_serialized_size(), '\0');
        expected.resize(
                hlls->get_data()[row].serialize(reinterpret_cast<uint8_t*>(expected.data())));
        EXPECT_EQ(cells[row].to_string(), expected) << "row " << row;
    }
    EXPECT_EQ(cells[1].data, cells[0].data + cells[0].size);
}

} // namespace doris
