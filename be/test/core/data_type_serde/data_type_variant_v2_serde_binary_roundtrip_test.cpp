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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/binary_cast.hpp"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/decimalv2_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_canonical.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "gen_cpp/data.pb.h"
#include "util/variant/variant_test_utils.h"

namespace doris {
namespace {

VariantField encode_json(std::string_view json) {
    JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    encoder.add_json({json.data(), json.size()});
    VariantBatchBuilder block = encoder.finish_batch();
    return VariantField::encode(block.value_at(0));
}

ColumnVariantV2::MutablePtr encoded(std::string_view json) {
    auto column = ColumnVariantV2::create();
    insert_encoded_field(*column, encode_json(json));
    return column;
}

void append_unsigned(std::string& output, unsigned __int128 value, uint8_t width) {
    for (uint8_t byte = 0; byte < width; ++byte) {
        output.push_back(static_cast<char>(value >> (byte * 8)));
    }
}

VariantField noncanonical_object() {
    std::string metadata;
    metadata.push_back(static_cast<char>(VARIANT_ENCODING_VERSION));
    append_unsigned(metadata, 3, 1);
    append_unsigned(metadata, 0, 1);
    append_unsigned(metadata, 1, 1);
    append_unsigned(metadata, 2, 1);
    append_unsigned(metadata, 8, 1);
    metadata.append("baunused");

    std::string value;
    value.push_back(static_cast<char>(VariantBasicType::OBJECT));
    append_unsigned(value, 2, 1);
    append_unsigned(value, 1, 1);
    append_unsigned(value, 0, 1);
    append_unsigned(value, 1, 1);
    append_unsigned(value, 0, 1);
    append_unsigned(value, 2, 1);
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::NULL_VALUE)
                                      << VARIANT_VALUE_HEADER_SHIFT));
    value.push_back(static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::TRUE_VALUE)
                                      << VARIANT_VALUE_HEADER_SHIFT));

    std::string field;
    append_unsigned(field, metadata.size(), sizeof(uint32_t));
    field.append(metadata);
    field.append(value);
    return VariantField::decode({field.data(), field.size()});
}

std::vector<char> serialize(const IColumn& source) {
    const DataTypeVariantV2 type;
    const int64_t max_size = type.get_uncompressed_serialized_bytes(source, 10);
    EXPECT_GT(max_size, 0);
    std::vector<char> bytes(max_size);
    char* end = type.serialize(source, bytes.data(), 10);
    bytes.resize(end - bytes.data());
    return bytes;
}

struct DecodedVariant {
    MutableColumnPtr column;

    bool is_constant() const { return check_and_get_column<ColumnConst>(column.get()) != nullptr; }

    ColumnVariantV2* operator->() {
        IColumn* physical = column.get();
        if (auto* constant = check_and_get_column<ColumnConst>(physical)) {
            physical = const_cast<IColumn*>(&constant->get_data_column());
        }
        return &assert_cast<ColumnVariantV2&>(*physical);
    }
    const ColumnVariantV2* operator->() const {
        const IColumn* physical = column.get();
        if (const auto* constant = check_and_get_column<ColumnConst>(physical)) {
            physical = &constant->get_data_column();
        }
        return &assert_cast<const ColumnVariantV2&>(*physical);
    }
    const ColumnVariantV2& operator*() const { return *operator->(); }
};

DecodedVariant round_trip(const IColumn& source) {
    const DataTypeVariantV2 type;
    const std::vector<char> bytes = serialize(source);
    MutableColumnPtr destination = type.create_column();
    EXPECT_EQ(type.deserialize(bytes.data(), &destination, 10), bytes.data() + bytes.size());
    return {.column = std::move(destination)};
}

MutableColumnPtr wrap_nullable(MutableColumnPtr nested, std::span<const uint8_t> nullmap) {
    EXPECT_EQ(nested->size(), nullmap.size());
    auto null_column = ColumnUInt8::create();
    null_column->get_data().insert(nullmap.begin(), nullmap.end());
    return ColumnNullable::create(std::move(nested), std::move(null_column));
}

template <typename Column, typename Value>
MutableColumnPtr fixed_column(std::initializer_list<Value> values) {
    auto column = Column::create();
    for (const Value& value : values) {
        column->insert_value(value);
    }
    return column;
}

template <typename Column, typename Value>
MutableColumnPtr decimal_column(uint32_t scale, std::initializer_list<Value> values) {
    auto column = Column::create(0, scale);
    for (const Value& value : values) {
        column->insert_value(value);
    }
    return column;
}

ColumnVariantV2::MutablePtr typed(MutableColumnPtr nested, DataTypePtr type,
                                  std::span<const uint8_t> nullmap) {
    return ColumnVariantV2::create_typed(wrap_nullable(std::move(nested), nullmap),
                                         std::move(type));
}

void expect_type_identity(const DataTypePtr& expected, const DataTypePtr& actual) {
    ASSERT_NE(expected, nullptr);
    ASSERT_NE(actual, nullptr);
    EXPECT_EQ(actual->get_primitive_type(), expected->get_primitive_type());
    EXPECT_EQ(actual->get_precision(), expected->get_precision());
    EXPECT_EQ(actual->get_scale(), expected->get_scale());
    if (is_string_type(expected->get_primitive_type())) {
        EXPECT_EQ(assert_cast<const DataTypeString&>(*actual).len(),
                  assert_cast<const DataTypeString&>(*expected).len());
    }
    if (expected->get_primitive_type() == TYPE_DECIMALV2) {
        const auto& expected_decimal = assert_cast<const DataTypeDecimalV2&>(*expected);
        const auto& actual_decimal = assert_cast<const DataTypeDecimalV2&>(*actual);
        EXPECT_EQ(actual_decimal.get_original_precision(),
                  expected_decimal.get_original_precision());
        EXPECT_EQ(actual_decimal.get_original_scale(), expected_decimal.get_original_scale());
    }
}

void expect_typed_equal(const ColumnVariantV2& expected, const ColumnVariantV2& actual) {
    ASSERT_TRUE(expected.is_typed());
    ASSERT_TRUE(actual.is_typed());
    ASSERT_EQ(actual.size(), expected.size());
    expect_type_identity(expected.typed_type(), actual.typed_type());
    const auto& expected_nullable = assert_cast<const ColumnNullable&>(expected.typed_column());
    const auto& actual_nullable = assert_cast<const ColumnNullable&>(actual.typed_column());
    ASSERT_EQ(actual_nullable.size(), expected_nullable.size());
    for (size_t row = 0; row < expected_nullable.size(); ++row) {
        EXPECT_EQ(actual_nullable.get_null_map_data()[row],
                  expected_nullable.get_null_map_data()[row])
                << row;
        const StringRef expected_value = expected_nullable.get_nested_column().get_data_at(row);
        const StringRef actual_value = actual_nullable.get_nested_column().get_data_at(row);
        ASSERT_EQ(actual_value.size, expected_value.size) << row;
        EXPECT_EQ(std::string_view(actual_value.data, actual_value.size),
                  std::string_view(expected_value.data, expected_value.size))
                << row;
    }
}

void expect_typed_round_trip(const ColumnVariantV2& source) {
    const auto destination = round_trip(source);
    expect_typed_equal(source, *destination);
}

} // namespace

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, ComputeVariantTypeRoundTripsEncodedColumn) {
    auto source = encoded(R"({"adapter":[1,true,null]})");
    const auto destination = round_trip(*source);
    ASSERT_EQ(destination->size(), source->size());
    EXPECT_TRUE(canonical_equals(source->read_view().value_at(0),
                                 destination->read_view().value_at(0)));
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, ExecutionTypeSelectsPhysicalColumn) {
    DataTypeVariant legacy;
    DataTypeVariantV2 compute_v2;

    MutableColumnPtr legacy_column = legacy.create_column();
    MutableColumnPtr compute_v2_column = compute_v2.create_column();
    EXPECT_NE(dynamic_cast<ColumnVariant*>(legacy_column.get()), nullptr);
    EXPECT_EQ(dynamic_cast<ColumnVariantV2*>(legacy_column.get()), nullptr);
    EXPECT_NE(dynamic_cast<ColumnVariantV2*>(compute_v2_column.get()), nullptr);
    EXPECT_TRUE(legacy.check_column(*legacy_column).ok());
    EXPECT_FALSE(legacy.check_column(*compute_v2_column).ok());
    EXPECT_TRUE(compute_v2.check_column(*compute_v2_column).ok());
    EXPECT_FALSE(compute_v2.check_column(*legacy_column).ok());
    EXPECT_FALSE(legacy.equals(compute_v2));
    EXPECT_FALSE(compute_v2.equals(legacy));
    EXPECT_TRUE(compute_v2.equals(DataTypeVariantV2 {}));
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, ExecutionMarkerRoundTripsThroughDescriptors) {
    DataTypeVariantV2 compute_v2(12, true);

    PColumnMeta column_meta;
    compute_v2.to_pb_column_meta(&column_meta);
    DataTypePtr from_protobuf = DataTypeFactory::instance().create_data_type(column_meta);
    ASSERT_NE(from_protobuf, nullptr);
    const auto* protobuf_variant = dynamic_cast<const DataTypeVariantV2*>(from_protobuf.get());
    ASSERT_NE(protobuf_variant, nullptr);
    EXPECT_NE(dynamic_cast<ColumnVariantV2*>(from_protobuf->create_column().get()), nullptr);

    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::VARIANT);
    scalar_type.__set_variant_max_subcolumns_count(12);
    scalar_type.__set_variant_enable_doc_mode(true);
    scalar_type.__set_variant_is_v2(true);
    TTypeNode type_node;
    type_node.__set_type(TTypeNodeType::SCALAR);
    type_node.__set_scalar_type(scalar_type);
    TTypeDesc type_desc;
    type_desc.types.push_back(type_node);
    DataTypePtr from_thrift = DataTypeFactory::instance().create_data_type(type_desc);
    ASSERT_NE(from_thrift, nullptr);
    const auto* thrift_variant = dynamic_cast<const DataTypeVariantV2*>(from_thrift.get());
    ASSERT_NE(thrift_variant, nullptr);
    EXPECT_NE(dynamic_cast<ColumnVariantV2*>(from_thrift->create_column().get()), nullptr);
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, EncodedRowsPreserveStateOrderAndRowBytes) {
    auto source = ColumnVariantV2::create();
    insert_encoded_field(*source, encode_json("null"));
    insert_encoded_field(*source, noncanonical_object());
    insert_encoded_field(*source, encode_json(R"({"z":1,"a":[true,null]})"));
    insert_encoded_field(*source, encode_json(R"("text")"));
    insert_encoded_field(*source, encode_json(R"({"z":2,"a":[]})"));

    const auto destination = round_trip(*source);
    ASSERT_FALSE(destination->is_typed());
    ASSERT_EQ(destination->size(), source->size());
    for (size_t row = 0; row < source->size(); ++row) {
        const VariantField expected = VariantField::encode(source->read_view().value_at(row));
        const VariantField actual = VariantField::encode(destination->read_view().value_at(row));
        EXPECT_EQ(std::string_view(actual.bytes().data, actual.bytes().size),
                  std::string_view(expected.bytes().data, expected.bytes().size))
                << row;
    }
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, EncodedColumnsPreserveDictionaryEntries) {
    auto source = ColumnVariantV2::create();
    insert_encoded_field(*source, encode_json(R"({"a":1})"));
    insert_encoded_field(*source, encode_json(R"({"b":2})"));
    insert_encoded_field(*source, encode_json(R"({"c":3})"));
    const size_t source_metadata_count = source->read_view().metadata_count();
    ASSERT_GT(source_metadata_count, 1);

    const IColumn::Filter filter {0, 1, 0};
    ColumnPtr filtered = source->filter(filter, 1);
    const IColumn::Permutation permutation {0};
    MutableColumnPtr selected_base = filtered->permute(permutation, 0);
    const auto& selected = assert_cast<const ColumnVariantV2&>(*selected_base);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected.read_view().metadata_count(), source_metadata_count);

    const auto decoded = round_trip(selected);
    ASSERT_FALSE(decoded->is_typed());
    EXPECT_EQ(decoded->read_view().metadata_count(), source_metadata_count);
    EXPECT_EQ(source->read_view().metadata_count(), source_metadata_count);
    EXPECT_FALSE(source->is_typed());
    EXPECT_EQ(selected.read_view().metadata_count(), source_metadata_count);
}

// NOLINTNEXTLINE(readability-function-size) -- one row matrix covers every fixed scalar identity.
TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, TypedNumericAndDecimalPhysicalBits) {
    constexpr std::array<uint8_t, 3> NULLS {0, 1, 0};
    expect_typed_round_trip(*typed(fixed_column<ColumnUInt8, UInt8>({0, 255, 1}),
                                   std::make_shared<DataTypeBool>(), NULLS));
    expect_typed_round_trip(*typed(fixed_column<ColumnInt8, Int8>({-128, 17, 127}),
                                   std::make_shared<DataTypeInt8>(), NULLS));
    expect_typed_round_trip(*typed(fixed_column<ColumnInt16, Int16>({-32768, 123, 32767}),
                                   std::make_shared<DataTypeInt16>(), NULLS));
    expect_typed_round_trip(
            *typed(fixed_column<ColumnInt32, Int32>({std::numeric_limits<Int32>::min(), 0x12345678,
                                                     std::numeric_limits<Int32>::max()}),
                   std::make_shared<DataTypeInt32>(), NULLS));
    expect_typed_round_trip(*typed(
            fixed_column<ColumnInt64, Int64>({std::numeric_limits<Int64>::min(), 0x123456789ABCDEF,
                                              std::numeric_limits<Int64>::max()}),
            std::make_shared<DataTypeInt64>(), NULLS));
    const Int128 large_positive = (static_cast<Int128>(1) << 120) + 0x1234;
    expect_typed_round_trip(
            *typed(fixed_column<ColumnInt128, Int128>(
                           {-large_positive, static_cast<Int128>(-1), large_positive}),
                   std::make_shared<DataTypeInt128>(), NULLS));

    expect_typed_round_trip(
            *typed(fixed_column<ColumnFloat32, Float32>({std::bit_cast<Float32>(0x80000000U),
                                                         std::bit_cast<Float32>(0x7FC12345U),
                                                         std::bit_cast<Float32>(0x00000000U)}),
                   std::make_shared<DataTypeFloat32>(), NULLS));
    expect_typed_round_trip(*typed(
            fixed_column<ColumnFloat64, Float64>({std::bit_cast<Float64>(0x8000000000000000ULL),
                                                  std::bit_cast<Float64>(0x7FF8123456789ABCULL),
                                                  std::bit_cast<Float64>(0x0000000000000000ULL)}),
            std::make_shared<DataTypeFloat64>(), NULLS));

    const auto decimal_v2_type = std::make_shared<DataTypeDecimalV2>(DecimalV2Value::PRECISION,
                                                                     DecimalV2Value::SCALE, 12, 4);
    expect_typed_round_trip(*typed(
            decimal_column<ColumnDecimal128V2, DecimalV2Value>(
                    DecimalV2Value::SCALE, {DecimalV2Value(-static_cast<Int128>(123456789)),
                                            DecimalV2Value(static_cast<Int128>(0x123456789ABCDEF)),
                                            DecimalV2Value(static_cast<Int128>(987654321))}),
            decimal_v2_type, NULLS));
    expect_typed_round_trip(
            *typed(decimal_column<ColumnDecimal32, Decimal32>(
                           3, {Decimal32(-12345), Decimal32(678), Decimal32(99999)}),
                   std::make_shared<DataTypeDecimal32>(9, 3), NULLS));
    expect_typed_round_trip(*typed(
            decimal_column<ColumnDecimal64, Decimal64>(
                    6, {Decimal64(static_cast<Int64>(-1234567890123LL)), Decimal64(Int64 {7}),
                        Decimal64(static_cast<Int64>(999999999999LL))}),
            std::make_shared<DataTypeDecimal64>(18, 6), NULLS));
    expect_typed_round_trip(*typed(decimal_column<ColumnDecimal128V3, Decimal128V3>(
                                           9, {Decimal128V3(-large_positive), Decimal128V3(-1),
                                               Decimal128V3(large_positive)}),
                                   std::make_shared<DataTypeDecimal128>(38, 9), NULLS));
}

// NOLINTNEXTLINE(readability-function-size) -- one row matrix covers every remaining identity.
TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, TypedTemporalStringAndIpPhysicalBits) {
    constexpr std::array<uint8_t, 3> NULLS {0, 1, 0};
    const VecDateTimeValue old_date_a =
            binary_cast<Int64, VecDateTimeValue>(static_cast<Int64>(0x0123456789ABCDEFULL));
    const VecDateTimeValue old_date_b =
            binary_cast<Int64, VecDateTimeValue>(static_cast<Int64>(0xFEDCBA9876543210ULL));
    expect_typed_round_trip(*typed(fixed_column<ColumnDate, VecDateTimeValue>(
                                           {old_date_a, old_date_b, VecDateTimeValue()}),
                                   std::make_shared<DataTypeDate>(), NULLS));
    expect_typed_round_trip(*typed(fixed_column<ColumnDateTime, VecDateTimeValue>(
                                           {old_date_b, old_date_a, VecDateTimeValue()}),
                                   std::make_shared<DataTypeDateTime>(), NULLS));
    expect_typed_round_trip(*typed(
            fixed_column<ColumnDateV2, DateV2Value<DateV2ValueType>>(
                    {DateV2Value<DateV2ValueType>(0xFFFFFFFFU),
                     DateV2Value<DateV2ValueType>(0x12345678U), DateV2Value<DateV2ValueType>(0U)}),
            std::make_shared<DataTypeDateV2>(), NULLS));
    expect_typed_round_trip(
            *typed(fixed_column<ColumnDateTimeV2, DateV2Value<DateTimeV2ValueType>>(
                           {DateV2Value<DateTimeV2ValueType>(
                                    DateV2Value<DateTimeV2ValueType>::underlying_value {
                                            0xFFFFFFFFFFFFFFFFULL}),
                            DateV2Value<DateTimeV2ValueType>(
                                    DateV2Value<DateTimeV2ValueType>::underlying_value {
                                            0x0123456789ABCDEFULL}),
                            DateV2Value<DateTimeV2ValueType>(
                                    DateV2Value<DateTimeV2ValueType>::underlying_value {0})}),
                   std::make_shared<DataTypeDateTimeV2>(6), NULLS));
    expect_typed_round_trip(
            *typed(fixed_column<ColumnTimeStampTz, TimestampTzValue>(
                           {TimestampTzValue(0xFEDCBA9876543210ULL),
                            TimestampTzValue(0x0123456789ABCDEFULL), TimestampTzValue(0ULL)}),
                   std::make_shared<DataTypeTimeStampTz>(4), NULLS));

    const std::array<std::string_view, 3> strings {std::string_view("", 0),
                                                   std::string_view("a\0b", 3),
                                                   std::string_view("under-null", 10)};
    const std::array<DataTypePtr, 3> string_types {
            std::make_shared<DataTypeString>(8, TYPE_CHAR),
            std::make_shared<DataTypeString>(32, TYPE_VARCHAR), std::make_shared<DataTypeString>()};
    for (const DataTypePtr& type : string_types) {
        auto nested = ColumnString::create();
        for (std::string_view value : strings) {
            nested->insert_data(value.data(), value.size());
        }
        expect_typed_round_trip(*typed(std::move(nested), type, NULLS));
    }

    expect_typed_round_trip(*typed(fixed_column<ColumnIPv4, IPv4>({0x01020304U, 0xAABBCCDDU, 0U}),
                                   std::make_shared<DataTypeIPv4>(), NULLS));
    const IPv6 ipv6_a = (static_cast<IPv6>(0x0123456789ABCDEFULL) << 64) |
                        static_cast<IPv6>(0xFEDCBA9876543210ULL);
    const IPv6 ipv6_b = (static_cast<IPv6>(0xFFEEDDCCBBAA9988ULL) << 64) |
                        static_cast<IPv6>(0x7766554433221100ULL);
    expect_typed_round_trip(*typed(fixed_column<ColumnIPv6, IPv6>({ipv6_a, ipv6_b, 0}),
                                   std::make_shared<DataTypeIPv6>(), NULLS));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, EmptyAndConstColumnsPreserveWholeColumnState) {
    auto empty_encoded = ColumnVariantV2::create();
    const auto decoded_empty_encoded = round_trip(*empty_encoded);
    EXPECT_FALSE(decoded_empty_encoded->is_typed());
    EXPECT_EQ(decoded_empty_encoded->size(), 0);

    const std::array<uint8_t, 0> NO_NULLS {};
    auto empty_strings = typed(ColumnString::create(),
                               std::make_shared<DataTypeString>(17, TYPE_VARCHAR), NO_NULLS);
    const auto decoded_empty_typed = round_trip(*empty_strings);
    ASSERT_TRUE(decoded_empty_typed->is_typed());
    EXPECT_EQ(decoded_empty_typed->size(), 0);
    expect_type_identity(empty_strings->typed_type(), decoded_empty_typed->typed_type());

    auto empty_fixed = typed(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), NO_NULLS);
    const auto decoded_empty_fixed = round_trip(*empty_fixed);
    ASSERT_TRUE(decoded_empty_fixed->is_typed());
    EXPECT_EQ(decoded_empty_fixed->size(), 0);
    expect_type_identity(empty_fixed->typed_type(), decoded_empty_fixed->typed_type());

    auto one_encoded = ColumnVariantV2::create();
    insert_encoded_field(*one_encoded, encode_json(R"({"const":true})"));
    ColumnPtr one_encoded_ptr = std::move(one_encoded);
    const auto& one_encoded_ref = assert_cast<const ColumnVariantV2&>(*one_encoded_ptr);
    ColumnPtr encoded_const = ColumnConst::create(one_encoded_ptr, 4);
    const auto decoded_encoded_const = round_trip(*encoded_const);
    ASSERT_TRUE(decoded_encoded_const.is_constant());
    ASSERT_EQ(decoded_encoded_const.column->size(), 4);
    ASSERT_FALSE(decoded_encoded_const->is_typed());
    ASSERT_EQ(decoded_encoded_const->size(), 1);
    EXPECT_EQ(VariantField::encode(decoded_encoded_const->read_view().value_at(0)).bytes(),
              VariantField::encode(one_encoded_ref.read_view().value_at(0)).bytes());

    constexpr std::array<uint8_t, 1> NOT_NULL {0};
    auto one_typed = typed(fixed_column<ColumnInt32, Int32>({0x12345678}),
                           std::make_shared<DataTypeInt32>(), NOT_NULL);
    ColumnPtr one_typed_ptr = std::move(one_typed);
    const auto& one_typed_ref = assert_cast<const ColumnVariantV2&>(*one_typed_ptr);
    ColumnPtr typed_const = ColumnConst::create(one_typed_ptr, 5);
    const auto decoded_typed_const = round_trip(*typed_const);
    ASSERT_TRUE(decoded_typed_const.is_constant());
    ASSERT_EQ(decoded_typed_const.column->size(), 5);
    ASSERT_TRUE(decoded_typed_const->is_typed());
    ASSERT_EQ(decoded_typed_const->size(), 1);
    const auto& decoded_nullable =
            assert_cast<const ColumnNullable&>(decoded_typed_const->typed_column());
    EXPECT_EQ(assert_cast<const ColumnInt32&>(decoded_nullable.get_nested_column()).get_data()[0],
              0x12345678);

    ColumnPtr encoded_const_zero = ColumnConst::create(one_encoded_ptr, 0);
    const auto decoded_encoded_const_zero = round_trip(*encoded_const_zero);
    EXPECT_TRUE(decoded_encoded_const_zero.is_constant());
    EXPECT_EQ(decoded_encoded_const_zero.column->size(), 0);
    EXPECT_FALSE(decoded_encoded_const_zero->is_typed());
    EXPECT_EQ(decoded_encoded_const_zero->size(), 1);
    ColumnPtr typed_const_zero = ColumnConst::create(one_typed_ptr, 0);
    const auto decoded_typed_const_zero = round_trip(*typed_const_zero);
    EXPECT_TRUE(decoded_typed_const_zero.is_constant());
    EXPECT_EQ(decoded_typed_const_zero.column->size(), 0);
    EXPECT_TRUE(decoded_typed_const_zero->is_typed());
    EXPECT_EQ(decoded_typed_const_zero->size(), 1);
    expect_type_identity(one_typed_ref.typed_type(), decoded_typed_const_zero->typed_type());
}

TEST(DataTypeVariantV2SerDeBinaryRoundTripTest, EncodedAndTypedDecodeToCanonicalEquality) {
    auto encoded_source = encoded(R"(42)");
    constexpr std::array<uint8_t, 1> NOT_NULL {0};
    auto typed_source = typed(fixed_column<ColumnInt32, Int32>({42}),
                              std::make_shared<DataTypeInt32>(), NOT_NULL);
    const auto decoded_encoded = round_trip(*encoded_source);
    auto decoded_typed = round_trip(*typed_source);
    decoded_typed->ensure_encoded();
    ASSERT_FALSE(decoded_typed->is_typed());
    EXPECT_TRUE(canonical_equals(decoded_encoded->read_view().value_at(0),
                                 decoded_typed->read_view().value_at(0)));
}

} // namespace doris
