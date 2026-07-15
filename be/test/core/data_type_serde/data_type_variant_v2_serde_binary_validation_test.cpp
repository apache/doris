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
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "util/variant/variant_block_builder.h"
#include "util/variant/variant_encoding.h"
#include "util/variant/variant_json.h"
#include "util/variant/variant_test_utils.h"

namespace doris {
namespace {

constexpr size_t HEADER_BYTES = 24;
constexpr size_t ENCODED_SECTIONS = 48;
constexpr size_t TYPED_TYPE_META = 44;
constexpr size_t TYPED_PAYLOAD = 60;

VariantField encode_json(std::string_view json) {
    JsonToVariantEncoder encoder({.max_json_key_length = 255,
                                  .throw_on_invalid_json = true,
                                  .check_duplicate_json_path = false});
    encoder.add_json({json.data(), json.size()});
    VariantEncodedBlock block = encoder.finish_block();
    return VariantField::encode(block.value_at(0));
}

uint16_t read_u16(const std::vector<uint8_t>& bytes, size_t offset) {
    return static_cast<uint16_t>(bytes[offset]) | (static_cast<uint16_t>(bytes[offset + 1]) << 8);
}

uint32_t read_u32(const std::vector<uint8_t>& bytes, size_t offset) {
    uint32_t value = 0;
    for (uint8_t index = 0; index < sizeof(value); ++index) {
        value |= static_cast<uint32_t>(bytes[offset + index]) << (index * 8);
    }
    return value;
}

void write_u16(std::vector<uint8_t>& bytes, size_t offset, uint16_t value) {
    for (uint8_t index = 0; index < sizeof(value); ++index) {
        bytes[offset + index] = static_cast<uint8_t>(value >> (index * 8));
    }
}

void write_u32(std::vector<uint8_t>& bytes, size_t offset, uint32_t value) {
    for (uint8_t index = 0; index < sizeof(value); ++index) {
        bytes[offset + index] = static_cast<uint8_t>(value >> (index * 8));
    }
}

void write_u64(std::vector<uint8_t>& bytes, size_t offset, uint64_t value) {
    for (uint8_t index = 0; index < sizeof(value); ++index) {
        bytes[offset + index] = static_cast<uint8_t>(value >> (index * 8));
    }
}

void append_u32(std::vector<uint8_t>& bytes, uint32_t value) {
    const size_t offset = bytes.size();
    bytes.resize(offset + sizeof(value));
    write_u32(bytes, offset, value);
}

void append_unsigned(std::string& output, uint64_t value, uint8_t width) {
    for (uint8_t byte = 0; byte < width; ++byte) {
        output.push_back(static_cast<char>(value >> (byte * 8)));
    }
}

std::string primitive(VariantPrimitiveId id) {
    return {1, static_cast<char>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT)};
}

std::string array(const std::string& child) {
    uint8_t offset_width = 1;
    if (child.size() > std::numeric_limits<uint8_t>::max()) {
        offset_width = child.size() <= std::numeric_limits<uint16_t>::max() ? 2 : 4;
    }
    const auto value_header =
            static_cast<uint8_t>((offset_width - 1) << VARIANT_ARRAY_OFFSET_SIZE_SHIFT);
    std::string result;
    result.push_back(static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                       static_cast<uint8_t>(VariantBasicType::ARRAY)));
    result.push_back(1);
    append_unsigned(result, 0, offset_width);
    append_unsigned(result, child.size(), offset_width);
    result.append(child);
    return result;
}

std::string nested_arrays(uint32_t count) {
    std::string result = primitive(VariantPrimitiveId::NULL_VALUE);
    for (uint32_t depth = 0; depth < count; ++depth) {
        result = array(result);
    }
    return result;
}

std::vector<uint8_t> serialize(const IColumn& source) {
    const Result<size_t> size = DataTypeVariantV2SerDe::serialized_size(source);
    EXPECT_TRUE(size.has_value()) << size.error();
    if (!size.has_value()) {
        return {};
    }
    std::vector<uint8_t> frame(*size);
    EXPECT_TRUE(DataTypeVariantV2SerDe::serialize_binary(source, frame).ok());
    return frame;
}

ColumnVariantV2::MutablePtr encoded(std::string_view json) {
    auto column = ColumnVariantV2::create();
    insert_encoded_field(*column, encode_json(json));
    return column;
}

ColumnVariantV2::MutablePtr typed_int32(std::span<const Int32> values,
                                        std::span<const uint8_t> nullmap) {
    auto nested = ColumnInt32::create();
    nested->get_data().insert(values.begin(), values.end());
    auto null_column = ColumnUInt8::create();
    null_column->get_data().insert(nullmap.begin(), nullmap.end());
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(nested), std::move(null_column)),
            std::make_shared<DataTypeInt32>());
}

ColumnVariantV2::MutablePtr typed_strings(std::span<const std::string_view> values,
                                          std::span<const uint8_t> nullmap) {
    auto nested = ColumnString::create();
    for (std::string_view value : values) {
        nested->insert_data(value.data(), value.size());
    }
    auto null_column = ColumnUInt8::create();
    null_column->get_data().insert(nullmap.begin(), nullmap.end());
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(nested), std::move(null_column)),
            std::make_shared<DataTypeString>());
}

ColumnVariantV2::MutablePtr typed_decimal128(__int128 value) {
    auto nested = ColumnDecimal128V3::create(0, 0);
    nested->insert_value(Decimal128V3(value));
    auto null_column = ColumnUInt8::create();
    null_column->insert_value(0);
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(nested), std::move(null_column)),
            std::make_shared<DataTypeDecimal128>(38, 0));
}

void expect_corruption(const std::vector<uint8_t>& frame, std::string_view expected_message = {}) {
    auto sentinel = ColumnInt32::create();
    sentinel->get_data().push_back(123456);
    MutableColumnPtr destination = std::move(sentinel);
    const IColumn* original = destination.get();
    const Status status = DataTypeVariantV2SerDe::deserialize_binary(frame, &destination);
    EXPECT_EQ(status.code(), ErrorCode::CORRUPTION) << status;
    if (!expected_message.empty()) {
        EXPECT_NE(status.to_string().find(expected_message), std::string::npos) << status;
    }
    ASSERT_EQ(destination.get(), original);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(*destination).get_data()[0], 123456);
}

struct EncodedLayout {
    size_t metadata_offsets;
    size_t metadata;
    size_t meta_ids;
    size_t value_offsets;
    size_t values;
    uint32_t metadata_size;
    uint32_t values_size;
};

EncodedLayout encoded_layout(const std::vector<uint8_t>& frame) {
    EXPECT_EQ(frame[5], 0);
    const uint32_t metadata_offsets_size = read_u32(frame, 24);
    const uint32_t metadata_size = read_u32(frame, 28);
    const uint32_t meta_ids_size = read_u32(frame, 32);
    const uint32_t value_offsets_size = read_u32(frame, 36);
    const uint32_t values_size = read_u32(frame, 40);
    const size_t metadata_offsets = ENCODED_SECTIONS;
    const size_t metadata = metadata_offsets + metadata_offsets_size;
    const size_t meta_ids = metadata + metadata_size;
    const size_t value_offsets = meta_ids + meta_ids_size;
    const size_t values = value_offsets + value_offsets_size;
    EXPECT_EQ(values + values_size, frame.size());
    return {.metadata_offsets = metadata_offsets,
            .metadata = metadata,
            .meta_ids = meta_ids,
            .value_offsets = value_offsets,
            .values = values,
            .metadata_size = metadata_size,
            .values_size = values_size};
}

std::vector<uint8_t> replace_single_encoded_value(const std::vector<uint8_t>& frame,
                                                  std::string_view value) {
    const EncodedLayout layout = encoded_layout(frame);
    EXPECT_EQ(read_u32(frame, 32), 4);
    std::vector<uint8_t> result(frame.begin(), frame.begin() + layout.value_offsets);
    append_u32(result, 0);
    append_u32(result, static_cast<uint32_t>(value.size()));
    result.insert(result.end(), reinterpret_cast<const uint8_t*>(value.data()),
                  reinterpret_cast<const uint8_t*>(value.data() + value.size()));
    write_u32(result, 36, 2 * sizeof(uint32_t));
    write_u32(result, 40, static_cast<uint32_t>(value.size()));
    write_u64(result, 8, result.size());
    return result;
}

void erase_section_tail(std::vector<uint8_t>& frame, size_t section, uint32_t old_size,
                        size_t bytes_to_erase, size_t size_field) {
    ASSERT_LE(bytes_to_erase, old_size);
    frame.erase(frame.begin() + section + old_size - bytes_to_erase,
                frame.begin() + section + old_size);
    write_u32(frame, size_field, old_size - bytes_to_erase);
    write_u64(frame, 8, frame.size());
}

void insert_section(std::vector<uint8_t>& frame, size_t section, std::span<const uint8_t> bytes,
                    size_t size_field) {
    const uint32_t old_size = read_u32(frame, size_field);
    frame.insert(frame.begin() + section, bytes.begin(), bytes.end());
    write_u32(frame, size_field, old_size + static_cast<uint32_t>(bytes.size()));
    write_u64(frame, 8, frame.size());
}

std::vector<uint8_t> keep_first_row_with_all_metadata(const std::vector<uint8_t>& frame) {
    const EncodedLayout layout = encoded_layout(frame);
    const uint32_t metadata_offsets_size = read_u32(frame, 24);
    const uint32_t meta_ids_size = read_u32(frame, 32);
    const uint32_t first_value_end = read_u32(frame, layout.value_offsets + sizeof(uint32_t));
    EXPECT_GE(meta_ids_size, 2 * sizeof(uint32_t));

    std::vector<uint8_t> result(frame.begin(), frame.begin() + ENCODED_SECTIONS);
    result.insert(result.end(), frame.begin() + layout.metadata_offsets,
                  frame.begin() + layout.metadata_offsets + metadata_offsets_size);
    result.insert(result.end(), frame.begin() + layout.metadata,
                  frame.begin() + layout.metadata + layout.metadata_size);
    result.insert(result.end(), frame.begin() + layout.meta_ids,
                  frame.begin() + layout.meta_ids + sizeof(uint32_t));
    result.insert(result.end(), frame.begin() + layout.value_offsets,
                  frame.begin() + layout.value_offsets + 2 * sizeof(uint32_t));
    result.insert(result.end(), frame.begin() + layout.values,
                  frame.begin() + layout.values + first_value_end);
    write_u64(result, 8, result.size());
    write_u64(result, 16, 1);
    write_u32(result, 32, sizeof(uint32_t));
    write_u32(result, 36, 2 * sizeof(uint32_t));
    write_u32(result, 40, first_value_end);
    return result;
}

} // namespace

TEST(DataTypeVariantV2SerDeBinaryValidationTest, HeaderBoundsAndExactFrameSpanAreRecoverable) {
    const std::vector<uint8_t> valid = serialize(*ColumnVariantV2::create());
    ASSERT_GE(valid.size(), ENCODED_SECTIONS);
    for (const size_t length : {size_t {0}, size_t {1}, HEADER_BYTES - 1, valid.size() - 1}) {
        expect_corruption({valid.begin(), valid.begin() + length});
    }

    auto trailing = valid;
    trailing.push_back(0xA5);
    expect_corruption(trailing);
    auto declared_internal_trailing = valid;
    declared_internal_trailing.push_back(0x5A);
    write_u64(declared_internal_trailing, 8, declared_internal_trailing.size());
    expect_corruption(declared_internal_trailing);

    auto wrong_magic = valid;
    wrong_magic[0] ^= 0xFF;
    expect_corruption(wrong_magic);
    auto wrong_version = valid;
    wrong_version[4] = 2;
    expect_corruption(wrong_version);
    auto wrong_state = valid;
    wrong_state[5] = 2;
    expect_corruption(wrong_state);
    auto header_flags = valid;
    header_flags[6] = 1;
    expect_corruption(header_flags);
    auto wrong_frame_size = valid;
    write_u64(wrong_frame_size, 8, valid.size() + 1);
    expect_corruption(wrong_frame_size);
    auto impossible_rows = valid;
    write_u64(impossible_rows, 16, std::numeric_limits<uint64_t>::max());
    expect_corruption(impossible_rows);
}

TEST(DataTypeVariantV2SerDeBinaryValidationTest, EncodedSectionsValidateOffsetsIdsAndFullRows) {
    const std::vector<uint8_t> object_frame = serialize(*encoded(R"({"a":[1,true]})"));
    const EncodedLayout object = encoded_layout(object_frame);

    auto extension = object_frame;
    write_u32(extension, 44, 1);
    expect_corruption(extension);
    auto oversized_section = object_frame;
    write_u32(oversized_section, 24, std::numeric_limits<uint32_t>::max());
    expect_corruption(oversized_section);
    auto metadata_start = object_frame;
    write_u32(metadata_start, object.metadata_offsets, 1);
    expect_corruption(metadata_start);
    auto value_start = object_frame;
    write_u32(value_start, object.value_offsets, 1);
    expect_corruption(value_start);
    auto invalid_id = object_frame;
    write_u32(invalid_id, object.meta_ids, 1);
    expect_corruption(invalid_id);

    auto invalid_key_utf8 = object_frame;
    const auto metadata_begin = invalid_key_utf8.begin() + object.metadata;
    const auto key = std::find(metadata_begin, metadata_begin + object.metadata_size,
                               static_cast<uint8_t>('a'));
    ASSERT_NE(key, metadata_begin + object.metadata_size);
    *key = 0xFF;
    expect_corruption(invalid_key_utf8);

    const std::vector<uint8_t> string_frame = serialize(*encoded(R"("x")"));
    const EncodedLayout string = encoded_layout(string_frame);
    ASSERT_GE(string.values_size, 2);
    auto invalid_string_utf8 = string_frame;
    invalid_string_utf8[string.values + string.values_size - 1] = 0xFF;
    expect_corruption(invalid_string_utf8);
    auto root_with_trailing_bytes = string_frame;
    root_with_trailing_bytes[string.values] = static_cast<uint8_t>(VariantPrimitiveId::NULL_VALUE)
                                              << VARIANT_VALUE_HEADER_SHIFT;
    expect_corruption(root_with_trailing_bytes);
}

TEST(DataTypeVariantV2SerDeBinaryValidationTest, EncodedSectionShapesMatchRowsAndPayloads) {
    auto source = ColumnVariantV2::create();
    insert_encoded_field(*source, encode_json(R"({"a":1})"));
    insert_encoded_field(*source, encode_json(R"({"b":2})"));
    insert_encoded_field(*source, encode_json(R"({"c":3})"));
    const std::vector<uint8_t> valid = serialize(*source);
    const EncodedLayout layout = encoded_layout(valid);
    const uint32_t metadata_offsets_size = read_u32(valid, 24);
    const uint32_t meta_ids_size = read_u32(valid, 32);
    const uint32_t value_offsets_size = read_u32(valid, 36);
    ASSERT_EQ(meta_ids_size, 3 * sizeof(uint32_t));
    ASSERT_EQ(value_offsets_size, 4 * sizeof(uint32_t));

    auto metadata_offsets_not_u32 = valid;
    erase_section_tail(metadata_offsets_not_u32, layout.metadata_offsets, metadata_offsets_size, 1,
                       24);
    expect_corruption(metadata_offsets_not_u32);
    auto metadata_offsets_decrease = valid;
    write_u32(metadata_offsets_decrease, layout.metadata_offsets + sizeof(uint32_t),
              layout.metadata_size);
    expect_corruption(metadata_offsets_decrease);
    auto metadata_end_mismatch = valid;
    write_u32(metadata_end_mismatch,
              layout.metadata_offsets + metadata_offsets_size - sizeof(uint32_t),
              layout.metadata_size - 1);
    expect_corruption(metadata_end_mismatch);

    auto ids_row_mismatch = valid;
    erase_section_tail(ids_row_mismatch, layout.meta_ids, meta_ids_size, sizeof(uint32_t), 32);
    expect_corruption(ids_row_mismatch);
    auto value_offsets_not_u32 = valid;
    erase_section_tail(value_offsets_not_u32, layout.value_offsets, value_offsets_size, 1, 36);
    expect_corruption(value_offsets_not_u32);
    auto value_offsets_row_mismatch = valid;
    erase_section_tail(value_offsets_row_mismatch, layout.value_offsets, value_offsets_size,
                       sizeof(uint32_t), 36);
    expect_corruption(value_offsets_row_mismatch);
    auto value_offsets_decrease = valid;
    write_u32(value_offsets_decrease, layout.value_offsets + sizeof(uint32_t),
              layout.values_size - 1);
    write_u32(value_offsets_decrease, layout.value_offsets + 2 * sizeof(uint32_t), 1);
    expect_corruption(value_offsets_decrease);
    auto value_end_mismatch = valid;
    write_u32(value_end_mismatch, layout.value_offsets + value_offsets_size - sizeof(uint32_t),
              layout.values_size - 1);
    expect_corruption(value_end_mismatch);
}

TEST(DataTypeVariantV2SerDeBinaryValidationTest, EncodedPayloadValidationIsRecursiveAndComplete) {
    const std::vector<uint8_t> null_frame = serialize(*encoded("null"));
    std::string truncated_child = array(primitive(VariantPrimitiveId::NULL_VALUE));
    truncated_child.pop_back();
    expect_corruption(replace_single_encoded_value(null_frame, truncated_child));
    expect_corruption(
            replace_single_encoded_value(null_frame, nested_arrays(VARIANT_MAX_NESTING_DEPTH + 1)));

    const std::vector<uint8_t> object_frame = serialize(*encoded(R"({"a":1,"b":2})"));
    const std::string overlapping_spans {char {0x02}, char {0x02}, char {0x00}, char {0x01},
                                         char {0x00}, char {0x00}, char {0x01}, char {0x00}};
    const std::string object_gap {char {0x02}, char {0x02}, char {0x00}, char {0x01}, char {0x00},
                                  char {0x02}, char {0x03}, char {0x00}, char {0x00}, char {0x00}};
    expect_corruption(replace_single_encoded_value(object_frame, overlapping_spans));
    expect_corruption(replace_single_encoded_value(object_frame, object_gap));
}

TEST(DataTypeVariantV2SerDeBinaryValidationTest, EncodedUnusedMetadataIsStillValidated) {
    auto source = ColumnVariantV2::create();
    insert_encoded_field(*source, encode_json(R"({"a":1})"));
    insert_encoded_field(*source, encode_json(R"({"b":2})"));
    const std::vector<uint8_t> with_unused = keep_first_row_with_all_metadata(serialize(*source));
    MutableColumnPtr decoded;
    ASSERT_TRUE(DataTypeVariantV2SerDe::deserialize_binary(with_unused, &decoded).ok());

    const EncodedLayout layout = encoded_layout(with_unused);
    const uint32_t second_begin = read_u32(with_unused, layout.metadata_offsets + sizeof(uint32_t));
    const uint32_t second_end =
            read_u32(with_unused, layout.metadata_offsets + 2 * sizeof(uint32_t));
    ASSERT_LT(second_begin, second_end);
    auto invalid_structure = with_unused;
    invalid_structure[layout.metadata + second_begin] = 0xFF;
    expect_corruption(invalid_structure);

    auto invalid_utf8 = with_unused;
    auto key = std::find(invalid_utf8.begin() + layout.metadata + second_begin,
                         invalid_utf8.begin() + layout.metadata + second_end,
                         static_cast<uint8_t>('b'));
    ASSERT_NE(key, invalid_utf8.begin() + layout.metadata + second_end);
    *key = 0xFF;
    expect_corruption(invalid_utf8);
}

TEST(DataTypeVariantV2SerDeBinaryValidationTest, TypedDescriptorTypeAndNullMapAreStrict) {
    constexpr std::array<Int32, 2> VALUES {0x12345678, -7};
    constexpr std::array<uint8_t, 2> NULLS {0, 1};
    const std::vector<uint8_t> valid = serialize(*typed_int32(VALUES, NULLS));
    ASSERT_EQ(valid[5], 1);
    ASSERT_EQ(read_u32(valid, 24), 16);
    ASSERT_EQ(read_u16(valid, TYPED_TYPE_META), 4);

    auto wrong_type_meta_size = valid;
    write_u32(wrong_type_meta_size, 24, 15);
    expect_corruption(wrong_type_meta_size);
    auto extension = valid;
    constexpr std::array<uint8_t, 1> EXTENSION {0xA5};
    insert_section(extension, TYPED_PAYLOAD, EXTENSION, 28);
    expect_corruption(extension);
    auto wrong_nullmap_size = valid;
    write_u32(wrong_nullmap_size, 32, 1);
    expect_corruption(wrong_nullmap_size);
    auto fixed_offsets = valid;
    constexpr std::array<uint8_t, 4> ZERO_OFFSET {0, 0, 0, 0};
    insert_section(fixed_offsets, TYPED_PAYLOAD + NULLS.size(), ZERO_OFFSET, 36);
    expect_corruption(fixed_offsets);
    auto wrong_values_size = valid;
    write_u32(wrong_values_size, 40, 7);
    expect_corruption(wrong_values_size);
    auto unknown_type = valid;
    write_u16(unknown_type, TYPED_TYPE_META, 99);
    expect_corruption(unknown_type);
    auto removed_type_code = valid;
    write_u16(removed_type_code, TYPED_TYPE_META, 13);
    expect_corruption(removed_type_code);
    auto type_flags = valid;
    write_u16(type_flags, TYPED_TYPE_META + 2, 1);
    expect_corruption(type_flags);
    auto irrelevant_precision = valid;
    write_u32(irrelevant_precision, TYPED_TYPE_META + 4, 1);
    expect_corruption(irrelevant_precision);
    auto invalid_nullmap = valid;
    invalid_nullmap[TYPED_PAYLOAD] = 2;
    expect_corruption(invalid_nullmap);

    auto invalid_decimal = valid;
    write_u16(invalid_decimal, TYPED_TYPE_META, 10);
    write_u32(invalid_decimal, TYPED_TYPE_META + 4, 2);
    write_u32(invalid_decimal, TYPED_TYPE_META + 8, 3);
    expect_corruption(invalid_decimal);
    auto invalid_datetime_scale = valid;
    write_u16(invalid_datetime_scale, TYPED_TYPE_META, 17);
    write_u32(invalid_datetime_scale, TYPED_TYPE_META + 8, 7);
    expect_corruption(invalid_datetime_scale);
}

TEST(DataTypeVariantV2SerDeBinaryValidationTest, TypedDecimalValueMustFitDeclaredPrecision) {
    std::vector<uint8_t> invalid = serialize(*typed_decimal128(0));
    ASSERT_EQ(invalid[5], 1);
    ASSERT_EQ(read_u16(invalid, TYPED_TYPE_META), 12);
    ASSERT_EQ(read_u32(invalid, TYPED_TYPE_META + 4), 38);
    ASSERT_EQ(read_u32(invalid, 32), 1);
    ASSERT_EQ(read_u32(invalid, 40), 16);

    const size_t values = TYPED_PAYLOAD + 1;
    std::fill(invalid.begin() + values, invalid.begin() + values + 16, 0);
    invalid[values + 15] = 0x80;
    expect_corruption(invalid);
}

TEST(DataTypeVariantV2SerDeBinaryValidationTest, TypedDecimalSourceMustFitDeclaredPrecision) {
    const Result<size_t> size = DataTypeVariantV2SerDe::serialized_size(
            *typed_decimal128(std::numeric_limits<__int128>::min()));
    ASSERT_FALSE(size.has_value());
    EXPECT_EQ(size.error().code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_NE(size.error().msg().find("39 digits, exceeding declared precision 38"),
              std::string::npos);
}

TEST(DataTypeVariantV2SerDeBinaryValidationTest, TypedStringOffsetsAndParametersAreStrict) {
    const std::array<std::string_view, 3> VALUES {
            std::string_view("", 0), std::string_view("a\0b", 3), std::string_view("under", 5)};
    constexpr std::array<uint8_t, 3> NULLS {0, 0, 1};
    const std::vector<uint8_t> valid = serialize(*typed_strings(VALUES, NULLS));
    const uint32_t offsets_size = read_u32(valid, 36);
    ASSERT_EQ(offsets_size, 16);
    const size_t offsets = TYPED_PAYLOAD + NULLS.size();
    ASSERT_EQ(read_u32(valid, offsets), 0);

    auto nonzero_start = valid;
    write_u32(nonzero_start, offsets, 1);
    expect_corruption(nonzero_start);
    auto decreasing = valid;
    write_u32(decreasing, offsets + sizeof(uint32_t), 5);
    expect_corruption(decreasing);
    auto wrong_end = valid;
    write_u32(wrong_end, offsets + 3 * sizeof(uint32_t), 4);
    expect_corruption(wrong_end);
    auto string_length_parameter = valid;
    write_u32(string_length_parameter, TYPED_TYPE_META + 12, 1);
    expect_corruption(string_length_parameter);
    auto invalid_char_length = valid;
    write_u16(invalid_char_length, TYPED_TYPE_META, 19);
    write_u32(invalid_char_length, TYPED_TYPE_META + 12, 1);
    expect_corruption(invalid_char_length);

    auto offset_bytes_not_u32 = valid;
    erase_section_tail(offset_bytes_not_u32, offsets, offsets_size, 1, 36);
    expect_corruption(offset_bytes_not_u32);
    auto offset_row_mismatch = valid;
    erase_section_tail(offset_row_mismatch, offsets, offsets_size, sizeof(uint32_t), 36);
    expect_corruption(offset_row_mismatch);
}

TEST(DataTypeVariantV2SerDeBinaryValidationTest, PublicApisRejectBadInputsWithoutPartialWrites) {
    auto encoded_empty = ColumnVariantV2::create();
    const Result<size_t> exact_size = DataTypeVariantV2SerDe::serialized_size(*encoded_empty);
    ASSERT_TRUE(exact_size.has_value()) << exact_size.error();

    std::vector<uint8_t> short_buffer(*exact_size - 1, 0xA5);
    const auto original_short = short_buffer;
    EXPECT_FALSE(DataTypeVariantV2SerDe::serialize_binary(*encoded_empty, short_buffer).ok());
    EXPECT_EQ(short_buffer, original_short);
    std::vector<uint8_t> long_buffer(*exact_size + 1, 0x5A);
    const auto original_long = long_buffer;
    EXPECT_FALSE(DataTypeVariantV2SerDe::serialize_binary(*encoded_empty, long_buffer).ok());
    EXPECT_EQ(long_buffer, original_long);

    auto wrong_source = ColumnInt32::create();
    wrong_source->get_data().push_back(7);
    const Result<size_t> wrong_size = DataTypeVariantV2SerDe::serialized_size(*wrong_source);
    ASSERT_FALSE(wrong_size.has_value());
    EXPECT_EQ(wrong_size.error().code(), ErrorCode::INVALID_ARGUMENT);
    std::array<uint8_t, 32> untouched {};
    untouched.fill(0xCC);
    const auto original_untouched = untouched;
    const Status wrong_serialize =
            DataTypeVariantV2SerDe::serialize_binary(*wrong_source, untouched);
    EXPECT_EQ(wrong_serialize.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(untouched, original_untouched);

    auto empty_nullmap = ColumnUInt8::create();
    auto outer_nullable =
            ColumnNullable::create(ColumnVariantV2::create(), std::move(empty_nullmap));
    const Result<size_t> outer_size = DataTypeVariantV2SerDe::serialized_size(*outer_nullable);
    ASSERT_FALSE(outer_size.has_value());
    EXPECT_EQ(outer_size.error().code(), ErrorCode::INVALID_ARGUMENT);

    ColumnPtr malformed_const = ColumnConst::create(ColumnVariantV2::create(), 0, true);
    const Result<size_t> malformed_const_size =
            DataTypeVariantV2SerDe::serialized_size(*malformed_const);
    ASSERT_FALSE(malformed_const_size.has_value());
    EXPECT_EQ(malformed_const_size.error().code(), ErrorCode::INVALID_ARGUMENT);

    const std::array<uint8_t, 1> truncated {0};
    EXPECT_EQ(DataTypeVariantV2SerDe::deserialize_binary(truncated, nullptr).code(),
              ErrorCode::INVALID_ARGUMENT);
}

} // namespace doris
