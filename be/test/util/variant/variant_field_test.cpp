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

#include "core/value/variant/variant_field.h"

#include <gtest/gtest.h>

#include <bit>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/field.h"
#include "core/value/variant/variant_encoding.h"
#include "exprs/function/parse/variant_json.h"
#include "util/json/json_parser.h"
#include "util/json/simd_json_parser.h"

namespace doris {
namespace {

void append_unsigned(std::string& output, uint64_t value, uint8_t width) {
    for (uint8_t byte = 0; byte < width; ++byte) {
        output.push_back(static_cast<char>(value >> (byte * 8)));
    }
}

void append_signed128(std::string& output, __int128 value, uint8_t width) {
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    for (uint8_t byte = 0; byte < width; ++byte) {
        output.push_back(static_cast<char>(unsigned_value >> (byte * 8)));
    }
}

__int128 power_of_ten(uint8_t exponent) {
    __int128 result = 1;
    for (uint8_t digit = 0; digit < exponent; ++digit) {
        result *= 10;
    }
    return result;
}

uint32_t read_u32(const char* data) {
    uint32_t result = 0;
    for (uint8_t byte = 0; byte < sizeof(uint32_t); ++byte) {
        result |= static_cast<uint32_t>(static_cast<uint8_t>(data[byte])) << (byte * 8);
    }
    return result;
}

std::string metadata(const std::vector<std::string>& keys, bool sorted) {
    std::string result;
    result.push_back(static_cast<char>(VARIANT_ENCODING_VERSION |
                                       (sorted ? VARIANT_METADATA_SORTED_STRINGS_MASK : 0)));
    append_unsigned(result, keys.size(), 1);
    size_t offset = 0;
    append_unsigned(result, offset, 1);
    for (const std::string& key : keys) {
        offset += key.size();
        append_unsigned(result, offset, 1);
    }
    for (const std::string& key : keys) {
        result.append(key);
    }
    return result;
}

std::string primitive(VariantPrimitiveId id, std::string_view payload = {}) {
    std::string result;
    result.push_back(static_cast<char>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT));
    result.append(payload);
    return result;
}

std::string integer(int64_t value, VariantPrimitiveId id, uint8_t width) {
    std::string payload;
    append_unsigned(payload, std::bit_cast<uint64_t>(value), width);
    return primitive(id, payload);
}

std::string short_string(std::string_view value) {
    std::string result;
    result.push_back(static_cast<char>((value.size() << VARIANT_VALUE_HEADER_SHIFT) |
                                       static_cast<uint8_t>(VariantBasicType::SHORT_STRING)));
    result.append(value);
    return result;
}

std::string long_string(std::string_view value) {
    std::string payload;
    append_unsigned(payload, value.size(), sizeof(uint32_t));
    payload.append(value);
    return primitive(VariantPrimitiveId::STRING, payload);
}

std::string binary(std::string_view value) {
    std::string payload;
    append_unsigned(payload, value.size(), sizeof(uint32_t));
    payload.append(value);
    return primitive(VariantPrimitiveId::BINARY, payload);
}

std::string decimal(VariantPrimitiveId id, __int128 unscaled, uint8_t scale, uint8_t width) {
    std::string payload(1, static_cast<char>(scale));
    append_signed128(payload, unscaled, width);
    return primitive(id, payload);
}

std::string array(const std::vector<std::string>& children) {
    std::string values;
    for (const std::string& child : children) {
        values.append(child);
    }
    uint8_t offset_width = 1;
    if (values.size() > std::numeric_limits<uint8_t>::max()) {
        offset_width = values.size() <= std::numeric_limits<uint16_t>::max() ? 2 : 4;
    }
    const auto value_header =
            static_cast<uint8_t>((offset_width - 1) << VARIANT_ARRAY_OFFSET_SIZE_SHIFT);
    std::string result;
    result.push_back(static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                       static_cast<uint8_t>(VariantBasicType::ARRAY)));
    append_unsigned(result, children.size(), 1);
    size_t offset = 0;
    append_unsigned(result, offset, offset_width);
    for (const std::string& child : children) {
        offset += child.size();
        append_unsigned(result, offset, offset_width);
    }
    result.append(values);
    return result;
}

std::string nested_single_element_arrays(uint32_t array_count) {
    std::string result = primitive(VariantPrimitiveId::NULL_VALUE);
    for (uint32_t depth = 0; depth < array_count; ++depth) {
        result = array({result});
    }
    return result;
}

std::string object(const std::vector<uint32_t>& field_ids, const std::vector<uint32_t>& offsets,
                   const std::vector<std::string>& physical_values) {
    std::string result;
    result.push_back(static_cast<char>(VariantBasicType::OBJECT));
    append_unsigned(result, field_ids.size(), 1);
    for (uint32_t id : field_ids) {
        append_unsigned(result, id, 1);
    }
    for (uint32_t offset : offsets) {
        append_unsigned(result, offset, 1);
    }
    size_t values_size = 0;
    for (const std::string& value : physical_values) {
        values_size += value.size();
    }
    append_unsigned(result, values_size, 1);
    for (const std::string& value : physical_values) {
        result.append(value);
    }
    return result;
}

VariantRef value_ref(const std::string& metadata_bytes, const std::string& value_bytes) {
    return {.metadata = {.data = metadata_bytes.data(), .size = metadata_bytes.size()},
            .value = {value_bytes.data(), value_bytes.size()}};
}

std::string raw_field(const std::string& metadata_bytes, const std::string& value_bytes) {
    std::string result;
    append_unsigned(result, metadata_bytes.size(), sizeof(uint32_t));
    result.append(metadata_bytes);
    result.append(value_bytes);
    return result;
}

std::string_view as_view(StringRef bytes) {
    return {bytes.data, bytes.size};
}

void expect_encode_and_decode_failure(const std::string& metadata_bytes,
                                      const std::string& value_bytes) {
    EXPECT_THROW(VariantField::encode(value_ref(metadata_bytes, value_bytes)), Exception);
    const std::string raw = raw_field(metadata_bytes, value_bytes);
    EXPECT_THROW(VariantField::decode({raw.data(), raw.size()}), Exception);
}

void expect_encode_and_decode_preservation(const std::string& metadata_bytes,
                                           const std::string& value_bytes) {
    const std::string raw = raw_field(metadata_bytes, value_bytes);
    VariantField encoded = VariantField::encode(value_ref(metadata_bytes, value_bytes));
    EXPECT_EQ(as_view(encoded.bytes()), raw);
    VariantField decoded = VariantField::decode({raw.data(), raw.size()});
    EXPECT_EQ(as_view(decoded.bytes()), raw);
}

VariantField encode_json(std::string_view json) {
    JsonToVariantEncoder encoder({.max_json_key_length = 255,
                                  .throw_on_invalid_json = true,
                                  .check_duplicate_json_path = false});
    encoder.add_json({json.data(), json.size()});
    VariantEncodedBlock block = encoder.finish_block();
    return VariantField::encode(block.value_at(0));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions are the matrix.
TEST(VariantFieldTest, ScalarObjectAndArrayRoundTrip) {
    const std::string empty_metadata = metadata({}, true);
    const std::string int_value = integer(-12345, VariantPrimitiveId::INT16, 2);

    VariantField scalar = VariantField::encode(value_ref(empty_metadata, int_value));
    ASSERT_EQ(scalar.bytes().size, sizeof(uint32_t) + empty_metadata.size() + int_value.size());
    EXPECT_EQ(read_u32(scalar.bytes().data), empty_metadata.size());
    EXPECT_EQ(scalar.ref().get_int(), -12345);

    VariantField decoded = VariantField::decode(scalar.bytes());
    EXPECT_EQ(as_view(decoded.bytes()), as_view(scalar.bytes()));
    EXPECT_EQ(as_view(VariantField::encode(decoded.ref()).bytes()), as_view(decoded.bytes()));

    const std::string object_metadata = metadata({"a", "b"}, true);
    const std::string array_value =
            array({primitive(VariantPrimitiveId::NULL_VALUE), short_string("x")});
    const std::string object_value =
            object({0, 1}, {0, static_cast<uint32_t>(int_value.size())}, {int_value, array_value});
    VariantField nested = VariantField::encode(value_ref(object_metadata, object_value));

    VariantRef a;
    ASSERT_TRUE(nested.ref().object_find(StringRef("a"), &a));
    EXPECT_EQ(a.get_int(), -12345);
    VariantRef b;
    ASSERT_TRUE(nested.ref().object_find(StringRef("b"), &b));
    ASSERT_EQ(b.num_elements(), 2);
    EXPECT_TRUE(b.array_at(0).is_null());
    EXPECT_EQ(b.array_at(1).get_string(), StringRef("x"));
}

TEST(VariantFieldTest, CopyAndMoveOwnTheirBytes) {
    const std::string metadata_bytes = metadata({}, true);
    const std::string value_bytes = short_string("owned");
    VariantField original = VariantField::encode(value_ref(metadata_bytes, value_bytes));

    VariantField copied(original);
    EXPECT_EQ(as_view(copied.bytes()), as_view(original.bytes()));
    EXPECT_NE(copied.bytes().data, original.bytes().data);

    VariantField copy_assigned;
    copy_assigned = original;
    EXPECT_EQ(as_view(copy_assigned.bytes()), as_view(original.bytes()));
    EXPECT_NE(copy_assigned.bytes().data, original.bytes().data);
    const VariantField* self = &copy_assigned;
    copy_assigned = *self;
    EXPECT_EQ(copy_assigned.ref().get_string(), StringRef("owned"));

    VariantField moved(std::move(copied));
    EXPECT_EQ(moved.ref().get_string(), StringRef("owned"));
    // These accesses intentionally verify the class's documented moved-from contract.
    // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    EXPECT_EQ(copied.bytes().size, 0);
    // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    EXPECT_THROW(copied.ref(), Exception);

    VariantField move_assigned;
    move_assigned = std::move(copy_assigned);
    EXPECT_EQ(move_assigned.ref().get_string(), StringRef("owned"));
    // These accesses intentionally verify the class's documented moved-from contract.
    // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    EXPECT_EQ(copy_assigned.bytes().size, 0);
    // NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
    EXPECT_THROW(copy_assigned.ref(), Exception);

    VariantField decoded;
    std::string decoded_snapshot;
    {
        std::string source = raw_field(metadata_bytes, value_bytes);
        decoded = VariantField::decode({source.data(), source.size()});
        decoded_snapshot.assign(decoded.bytes().data, decoded.bytes().size);
        source.assign(source.size(), '\x7f');
        EXPECT_EQ(as_view(decoded.bytes()), decoded_snapshot);
    }
    EXPECT_EQ(as_view(decoded.bytes()), decoded_snapshot);
    EXPECT_EQ(decoded.ref().get_string(), StringRef("owned"));
}

TEST(VariantFieldTest, PreservesLegalNonCanonicalBytes) {
    const std::string unsorted_metadata = metadata({"b", "a"}, false);
    const std::string null_value = primitive(VariantPrimitiveId::NULL_VALUE);
    const std::string true_value = primitive(VariantPrimitiveId::TRUE_VALUE);
    // Logical key order is a,b through ids 1,0. Physical children are b,a, hence offsets 1,0.
    const std::string nonmonotonic_object = object({1, 0}, {1, 0}, {null_value, true_value});
    const std::string expected = raw_field(unsorted_metadata, nonmonotonic_object);

    VariantField encoded = VariantField::encode(value_ref(unsorted_metadata, nonmonotonic_object));
    EXPECT_EQ(as_view(encoded.bytes()), expected);
    VariantField decoded = VariantField::decode({expected.data(), expected.size()});
    EXPECT_EQ(as_view(decoded.bytes()), expected);

    VariantRef a;
    ASSERT_TRUE(decoded.ref().object_find(StringRef("a"), &a));
    EXPECT_TRUE(a.get_bool());
    VariantRef b;
    ASSERT_TRUE(decoded.ref().object_find(StringRef("b"), &b));
    EXPECT_TRUE(b.is_null());

    std::string metadata_with_ignored_bit = metadata({}, true);
    metadata_with_ignored_bit[0] =
            static_cast<char>(static_cast<uint8_t>(metadata_with_ignored_bit[0]) | 0x20U);
    const std::string ignored_bit_raw = raw_field(metadata_with_ignored_bit, null_value);
    VariantField ignored_bit =
            VariantField::decode({ignored_bit_raw.data(), ignored_bit_raw.size()});
    EXPECT_EQ(as_view(ignored_bit.bytes()), ignored_bit_raw);
    EXPECT_EQ(as_view(VariantField::encode(ignored_bit.ref()).bytes()), ignored_bit_raw);

    const std::string empty_metadata = metadata({}, true);
    std::string reserved_array = array({});
    reserved_array[0] = static_cast<char>(static_cast<uint8_t>(reserved_array[0]) |
                                          (0x38U << VARIANT_VALUE_HEADER_SHIFT));
    expect_encode_and_decode_preservation(empty_metadata, reserved_array);

    std::string reserved_object = object({}, {}, {});
    reserved_object[0] = static_cast<char>(static_cast<uint8_t>(reserved_object[0]) |
                                           (0x20U << VARIANT_VALUE_HEADER_SHIFT));
    expect_encode_and_decode_preservation(empty_metadata, reserved_object);

    const std::string nested_metadata = metadata({"nested"}, true);
    std::string nested_object = object({0}, {0}, {reserved_array});
    nested_object[0] = static_cast<char>(static_cast<uint8_t>(nested_object[0]) |
                                         (0x20U << VARIANT_VALUE_HEADER_SHIFT));
    expect_encode_and_decode_preservation(nested_metadata, nested_object);
}

TEST(VariantFieldTest, ValidatesUtf8BySemanticType) {
    const std::string empty_metadata = metadata({}, true);
    const std::string invalid_utf8(1, '\xff');
    const std::string null_value = primitive(VariantPrimitiveId::NULL_VALUE);

    expect_encode_and_decode_failure(metadata({invalid_utf8}, true), null_value);
    expect_encode_and_decode_failure(empty_metadata, short_string(invalid_utf8));
    expect_encode_and_decode_failure(empty_metadata, long_string(invalid_utf8));

    const std::string binary_value = binary(invalid_utf8);
    VariantField encoded = VariantField::encode(value_ref(empty_metadata, binary_value));
    EXPECT_EQ(as_view(encoded.ref().get_binary()), invalid_utf8);
    VariantField decoded = VariantField::decode(encoded.bytes());
    EXPECT_EQ(as_view(decoded.ref().get_binary()), invalid_utf8);
}

TEST(VariantFieldTest, ValidatesDecimalPrecisionByPhysicalWidth) {
    const std::string empty_metadata = metadata({}, true);

    expect_encode_and_decode_failure(empty_metadata,
                                     decimal(VariantPrimitiveId::DECIMAL4, 1'000'000'000, 0, 4));
    expect_encode_and_decode_failure(
            empty_metadata, decimal(VariantPrimitiveId::DECIMAL8,
                                    static_cast<__int128>(1'000'000'000'000'000'000), 0, 8));
    expect_encode_and_decode_failure(
            empty_metadata, decimal(VariantPrimitiveId::DECIMAL16, power_of_ten(38), 0, 16));
    expect_encode_and_decode_failure(
            empty_metadata,
            decimal(VariantPrimitiveId::DECIMAL16, std::numeric_limits<__int128>::min(), 0, 16));
    expect_encode_and_decode_failure(empty_metadata,
                                     decimal(VariantPrimitiveId::DECIMAL4, 1, 39, 4));

    for (const auto [id, width] : {std::pair {VariantPrimitiveId::DECIMAL8, uint8_t {8}},
                                   std::pair {VariantPrimitiveId::DECIMAL16, uint8_t {16}}}) {
        const std::string value = decimal(id, 1, 38, width);
        VariantField encoded = VariantField::encode(value_ref(empty_metadata, value));
        EXPECT_EQ(encoded.ref().get_decimal(), (VariantDecimal {1, 38, width}));
        VariantField decoded = VariantField::decode(encoded.bytes());
        EXPECT_EQ(decoded.ref().get_decimal(), (VariantDecimal {1, 38, width}));
    }
}

TEST(VariantFieldTest, ValidatesDepthAndActualObjectKeyOrder) {
    const std::string empty_metadata = metadata({}, true);
    const std::string maximum_depth = nested_single_element_arrays(VARIANT_MAX_NESTING_DEPTH);
    VariantField encoded = VariantField::encode(value_ref(empty_metadata, maximum_depth));
    VariantField decoded = VariantField::decode(encoded.bytes());
    EXPECT_EQ(as_view(decoded.bytes()), as_view(encoded.bytes()));

    expect_encode_and_decode_failure(empty_metadata,
                                     nested_single_element_arrays(VARIANT_MAX_NESTING_DEPTH + 1));

    const std::string null_value = primitive(VariantPrimitiveId::NULL_VALUE);
    const std::string true_value = primitive(VariantPrimitiveId::TRUE_VALUE);
    const std::string two_values = object({0, 1}, {0, 1}, {null_value, true_value});
    // IDs are ascending, but the actual keys are b,a.
    expect_encode_and_decode_failure(metadata({"b", "a"}, false), two_values);
    // IDs are distinct and ascending, but both dictionary entries contain the same actual key.
    expect_encode_and_decode_failure(metadata({"a", "a"}, false), two_values);
}

TEST(VariantFieldTest, RejectsInvalidObjectValuePartition) {
    const std::string metadata_bytes = metadata({"a", "b"}, true);
    const std::string null_value = primitive(VariantPrimitiveId::NULL_VALUE);
    const std::string true_value = primitive(VariantPrimitiveId::TRUE_VALUE);

    const std::string duplicate_offset = object({0, 1}, {0, 0}, {null_value, true_value});
    expect_encode_and_decode_failure(metadata_bytes, duplicate_offset);

    const std::string overlapping_spans = object({0, 1}, {0, 1}, {long_string("x")});
    expect_encode_and_decode_failure(metadata_bytes, overlapping_spans);

    const std::string internal_gap = object({0, 1}, {0, 2}, {null_value, null_value, true_value});
    expect_encode_and_decode_failure(metadata_bytes, internal_gap);

    const std::string trailing_unreferenced =
            object({0, 1}, {0, 1}, {null_value, true_value, null_value});
    expect_encode_and_decode_failure(metadata_bytes, trailing_unreferenced);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- Each corruption is independent.
TEST(VariantFieldTest, RejectsMalformedFramingMetadataAndValue) {
    VariantField empty;
    EXPECT_EQ(empty.bytes().size, 0);
    EXPECT_THROW(empty.ref(), Exception);
    EXPECT_THROW(VariantField::decode({}), Exception);
    EXPECT_THROW(VariantField::decode({static_cast<const char*>(nullptr), 1}), Exception);

    for (size_t size = 1; size < sizeof(uint32_t); ++size) {
        const std::string truncated(size, '\0');
        EXPECT_THROW(VariantField::decode({truncated.data(), truncated.size()}), Exception);
    }

    std::string bad_meta_size(sizeof(uint32_t), '\0');
    bad_meta_size[0] = 5;
    EXPECT_THROW(VariantField::decode({bad_meta_size.data(), bad_meta_size.size()}), Exception);

    std::string zero_metadata(sizeof(uint32_t), '\0');
    zero_metadata.push_back(primitive(VariantPrimitiveId::NULL_VALUE)[0]);
    EXPECT_THROW(VariantField::decode({zero_metadata.data(), zero_metadata.size()}), Exception);

    const std::string empty_metadata = metadata({}, true);
    std::string bad_version = empty_metadata;
    bad_version[0] = 2;
    const std::string null_value = primitive(VariantPrimitiveId::NULL_VALUE);
    std::string raw = raw_field(bad_version, null_value);
    EXPECT_THROW(VariantField::decode({raw.data(), raw.size()}), Exception);

    std::string truncated_metadata = empty_metadata;
    truncated_metadata.pop_back();
    raw = raw_field(truncated_metadata, null_value);
    EXPECT_THROW(VariantField::decode({raw.data(), raw.size()}), Exception);

    raw = raw_field(empty_metadata, {});
    EXPECT_THROW(VariantField::decode({raw.data(), raw.size()}), Exception);

    const std::string truncated_int64 = integer(1, VariantPrimitiveId::INT64, 1);
    raw = raw_field(empty_metadata, truncated_int64);
    EXPECT_THROW(VariantField::decode({raw.data(), raw.size()}), Exception);

    const std::string unknown_id(
            1, static_cast<char>((VARIANT_MAX_PRIMITIVE_ID + 1) << VARIANT_VALUE_HEADER_SHIFT));
    raw = raw_field(empty_metadata, unknown_id);
    EXPECT_THROW(VariantField::decode({raw.data(), raw.size()}), Exception);

    std::string trailing_value = null_value;
    trailing_value.push_back('\0');
    raw = raw_field(empty_metadata, trailing_value);
    EXPECT_THROW(VariantField::decode({raw.data(), raw.size()}), Exception);

    const std::string truncated_array(1, static_cast<char>(VariantBasicType::ARRAY));
    raw = raw_field(empty_metadata, truncated_array);
    EXPECT_THROW(VariantField::decode({raw.data(), raw.size()}), Exception);

    const std::string bad_object_id = object({0}, {0}, {null_value});
    raw = raw_field(empty_metadata, bad_object_id);
    EXPECT_THROW(VariantField::decode({raw.data(), raw.size()}), Exception);

    const std::string nested_unknown = array({unknown_id});
    raw = raw_field(empty_metadata, nested_unknown);
    EXPECT_THROW(VariantField::decode({raw.data(), raw.size()}), Exception);

    const char one_byte = 0;
    EXPECT_THROW(VariantField::encode({{nullptr, 1}, {&one_byte, 1}}), Exception);
    EXPECT_THROW(VariantField::encode({{empty_metadata.data(), empty_metadata.size()},
                                       {static_cast<const char*>(nullptr), 1}}),
                 Exception);
    EXPECT_THROW(VariantField::encode({{empty_metadata.data(), empty_metadata.size()},
                                       {&one_byte, std::numeric_limits<size_t>::max()}}),
                 Exception);
    if constexpr (sizeof(size_t) > sizeof(uint32_t)) {
        EXPECT_THROW(VariantField::encode(
                             {{empty_metadata.data(),
                               static_cast<size_t>(std::numeric_limits<uint32_t>::max()) + 1},
                              {&one_byte, 1}}),
                     Exception);
    }
}

TEST(VariantFieldTest, AllComparisonsThrow) {
    const std::string metadata_bytes = metadata({}, true);
    const std::string value_bytes = primitive(VariantPrimitiveId::NULL_VALUE);
    const VariantField left = VariantField::encode(value_ref(metadata_bytes, value_bytes));
    const VariantField right = VariantField::encode(value_ref(metadata_bytes, value_bytes));

    EXPECT_THROW(static_cast<void>(left < right), Exception);
    EXPECT_THROW(static_cast<void>(left <= right), Exception);
    EXPECT_THROW(static_cast<void>(left == right), Exception);
    EXPECT_THROW(static_cast<void>(left != right), Exception);
    EXPECT_THROW(static_cast<void>(left >= right), Exception);
    EXPECT_THROW(static_cast<void>(left > right), Exception);
}

// This is a narrow current-source comparison, not the deferred T0.2 semantics baseline.
TEST(VariantFieldTest, LegacyJsonDataParserStableSubsetDoesNotReplaceT02) {
    JSONDataParser<SimdJSONParser> legacy;
    ParseConfig config;

    auto old_scalar = legacy.parse("123", 3, config);
    ASSERT_TRUE(old_scalar.has_value());
    ASSERT_EQ(old_scalar->values.size(), 1);
    EXPECT_EQ(old_scalar->values[0].get<TYPE_BIGINT>(), 123);
    EXPECT_EQ(encode_json("123").ref().get_int(), 123);

    old_scalar = legacy.parse("\"text\"", 6, config);
    ASSERT_TRUE(old_scalar.has_value());
    EXPECT_EQ(old_scalar->values[0].get<TYPE_STRING>(), "text");
    EXPECT_EQ(encode_json("\"text\"").ref().get_string(), StringRef("text"));

    const std::string object_json = R"({"a":1,"b":"x"})";
    auto old_object = legacy.parse(object_json.data(), object_json.size(), config);
    ASSERT_TRUE(old_object.has_value());
    ASSERT_EQ(old_object->paths.size(), 2);
    EXPECT_EQ(old_object->paths[0].get_path(), "a");
    EXPECT_EQ(old_object->paths[1].get_path(), "b");
    VariantField new_object = encode_json(object_json);
    VariantRef a;
    ASSERT_TRUE(new_object.ref().object_find(StringRef("a"), &a));
    EXPECT_EQ(a.get_int(), 1);
    VariantRef b;
    ASSERT_TRUE(new_object.ref().object_find(StringRef("b"), &b));
    EXPECT_EQ(b.get_string(), StringRef("x"));

    const std::string array_json = R"([1,null,"x"])";
    auto old_array = legacy.parse(array_json.data(), array_json.size(), config);
    ASSERT_TRUE(old_array.has_value());
    const auto& old_elements = old_array->values[0].get<TYPE_ARRAY>();
    ASSERT_EQ(old_elements.size(), 3);
    EXPECT_EQ(old_elements[0].get<TYPE_BIGINT>(), 1);
    EXPECT_TRUE(old_elements[1].is_null());
    EXPECT_EQ(old_elements[2].get<TYPE_STRING>(), "x");
    VariantField new_array_field = encode_json(array_json);
    VariantRef new_array = new_array_field.ref();
    ASSERT_EQ(new_array.num_elements(), 3);
    EXPECT_EQ(new_array.array_at(0).get_int(), 1);
    EXPECT_TRUE(new_array.array_at(1).is_null());
    EXPECT_EQ(new_array.array_at(2).get_string(), StringRef("x"));

    auto old_empty_object = legacy.parse("{}", 2, config);
    ASSERT_TRUE(old_empty_object.has_value());
    EXPECT_TRUE(old_empty_object->paths.empty());
    EXPECT_EQ(encode_json("{}").ref().num_elements(), 0);

    auto old_empty_array = legacy.parse("[]", 2, config);
    ASSERT_TRUE(old_empty_array.has_value());
    EXPECT_TRUE(old_empty_array->values[0].get<TYPE_ARRAY>().empty());
    EXPECT_EQ(encode_json("[]").ref().num_elements(), 0);
}

} // namespace
} // namespace doris
