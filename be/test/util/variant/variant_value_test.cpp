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

#include "util/variant/variant_value.h"

#include <gtest/gtest.h>

#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "common/exception.h"

namespace doris {
namespace {

void append_unsigned(std::string& output, uint64_t value, uint8_t width) {
    for (uint8_t i = 0; i < width; ++i) {
        output.push_back(static_cast<char>(value >> (i * 8)));
    }
}

std::string primitive(VariantPrimitiveId id, std::string_view payload = {}) {
    std::string result;
    result.push_back(static_cast<char>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT));
    result.append(payload);
    return result;
}

std::string integer_payload(int64_t value, uint8_t width) {
    std::string result;
    append_unsigned(result, std::bit_cast<uint64_t>(value), width);
    return result;
}

std::string int128_payload(__int128 value, uint8_t width) {
    std::string result;
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    for (uint8_t i = 0; i < width; ++i) {
        result.push_back(static_cast<char>(unsigned_value >> (i * 8)));
    }
    return result;
}

std::string variable_payload(std::string_view value) {
    std::string result;
    append_unsigned(result, value.size(), sizeof(uint32_t));
    result.append(value);
    return result;
}

std::string metadata(const std::vector<std::string>& keys, bool sorted, uint8_t width = 1) {
    std::string result;
    result.push_back(static_cast<char>(VARIANT_ENCODING_VERSION |
                                       (sorted ? VARIANT_METADATA_SORTED_STRINGS_MASK : 0) |
                                       ((width - 1) << VARIANT_METADATA_OFFSET_SIZE_SHIFT)));
    append_unsigned(result, keys.size(), width);
    size_t offset = 0;
    append_unsigned(result, offset, width);
    for (const std::string& key : keys) {
        offset += key.size();
        append_unsigned(result, offset, width);
    }
    for (const std::string& key : keys) {
        result.append(key);
    }
    return result;
}

VariantMetadataRef metadata_ref(const std::string& bytes) {
    return {.data = bytes.data(), .size = bytes.size()};
}

VariantValueRef value_ref(const std::string& metadata_bytes, const std::string& value) {
    return {.metadata = metadata_ref(metadata_bytes), .data = value.data(), .size = value.size()};
}

std::string short_string(std::string_view value) {
    std::string result;
    result.push_back(static_cast<char>((value.size() << VARIANT_VALUE_HEADER_SHIFT) |
                                       static_cast<uint8_t>(VariantBasicType::SHORT_STRING)));
    result.append(value);
    return result;
}

std::string array_value(const std::vector<std::string>& values, uint8_t offset_width, bool large) {
    const auto value_header =
            static_cast<uint8_t>(((offset_width - 1) << VARIANT_ARRAY_OFFSET_SIZE_SHIFT) |
                                 (large ? VARIANT_ARRAY_LARGE_MASK : 0));
    std::string result;
    result.push_back(static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                       static_cast<uint8_t>(VariantBasicType::ARRAY)));
    append_unsigned(result, values.size(), large ? sizeof(uint32_t) : sizeof(uint8_t));
    size_t offset = 0;
    append_unsigned(result, offset, offset_width);
    for (const std::string& value : values) {
        offset += value.size();
        append_unsigned(result, offset, offset_width);
    }
    for (const std::string& value : values) {
        result.append(value);
    }
    return result;
}

std::string object_value(const std::vector<uint32_t>& ordered_ids,
                         const std::vector<uint32_t>& ordered_offsets,
                         const std::vector<std::string>& physical_values, uint8_t id_width = 1,
                         uint8_t offset_width = 1, bool large = false) {
    const auto value_header =
            static_cast<uint8_t>(((offset_width - 1) << VARIANT_OBJECT_OFFSET_SIZE_SHIFT) |
                                 ((id_width - 1) << VARIANT_OBJECT_ID_SIZE_SHIFT) |
                                 (large ? VARIANT_OBJECT_LARGE_MASK : 0));
    std::string result;
    result.push_back(static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                       static_cast<uint8_t>(VariantBasicType::OBJECT)));
    append_unsigned(result, ordered_ids.size(), large ? sizeof(uint32_t) : sizeof(uint8_t));
    for (uint32_t id : ordered_ids) {
        append_unsigned(result, id, id_width);
    }
    for (uint32_t offset : ordered_offsets) {
        append_unsigned(result, offset, offset_width);
    }
    size_t values_size = 0;
    for (const std::string& value : physical_values) {
        values_size += value.size();
    }
    append_unsigned(result, values_size, offset_width);
    for (const std::string& value : physical_values) {
        result.append(value);
    }
    return result;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros expand assertions.
TEST(VariantValueTest, PrimitiveRoundTrips) {
    const std::string empty_metadata = metadata({}, true);

    const std::string null_value = primitive(VariantPrimitiveId::NULL_VALUE);
    EXPECT_TRUE(value_ref(empty_metadata, null_value).is_null());
    EXPECT_EQ(value_ref(empty_metadata, null_value).value_size(), 1);

    EXPECT_TRUE(value_ref(empty_metadata, primitive(VariantPrimitiveId::TRUE_VALUE)).get_bool());
    EXPECT_FALSE(value_ref(empty_metadata, primitive(VariantPrimitiveId::FALSE_VALUE)).get_bool());

    const std::array<std::pair<VariantPrimitiveId, uint8_t>, 4> integer_types {{
            {VariantPrimitiveId::INT8, 1},
            {VariantPrimitiveId::INT16, 2},
            {VariantPrimitiveId::INT32, 4},
            {VariantPrimitiveId::INT64, 8},
    }};
    for (const auto& [id, width] : integer_types) {
        const int64_t expected = width == 1 ? -100 : -12345;
        const std::string encoded = primitive(id, integer_payload(expected, width));
        EXPECT_EQ(value_ref(empty_metadata, encoded).get_int(), expected);
        EXPECT_EQ(value_ref(empty_metadata, encoded).value_size(), encoded.size());
    }

    const float expected_float = -1.25F;
    std::string float_payload;
    append_unsigned(float_payload, std::bit_cast<uint32_t>(expected_float), sizeof(uint32_t));
    EXPECT_EQ(value_ref(empty_metadata, primitive(VariantPrimitiveId::FLOAT, float_payload))
                      .get_float(),
              expected_float);

    const double expected_double = 123.5;
    std::string double_payload;
    append_unsigned(double_payload, std::bit_cast<uint64_t>(expected_double), sizeof(uint64_t));
    EXPECT_EQ(value_ref(empty_metadata, primitive(VariantPrimitiveId::DOUBLE, double_payload))
                      .get_double(),
              expected_double);

    const __int128 expected_decimal = -123456789012345678LL;
    const std::array<std::pair<VariantPrimitiveId, uint8_t>, 3> decimal_types {{
            {VariantPrimitiveId::DECIMAL4, 4},
            {VariantPrimitiveId::DECIMAL8, 8},
            {VariantPrimitiveId::DECIMAL16, 16},
    }};
    for (const auto& [id, width] : decimal_types) {
        const __int128 unscaled = width == 4 ? -123456 : expected_decimal;
        std::string payload(1, static_cast<char>(7));
        payload.append(int128_payload(unscaled, width));
        EXPECT_EQ(value_ref(empty_metadata, primitive(id, payload)).get_decimal(),
                  (VariantDecimal {unscaled, 7, width}));
    }

    EXPECT_EQ(value_ref(empty_metadata,
                        primitive(VariantPrimitiveId::DATE, integer_payload(-20000, 4)))
                      .get_date(),
              -20000);
    EXPECT_EQ(value_ref(empty_metadata, primitive(VariantPrimitiveId::TIMESTAMP_MICROS,
                                                  integer_payload(-1234567890, 8)))
                      .get_timestamp_micros(),
              -1234567890);
    EXPECT_EQ(value_ref(empty_metadata, primitive(VariantPrimitiveId::TIMESTAMP_NTZ_MICROS,
                                                  integer_payload(2234567890, 8)))
                      .get_timestamp_ntz_micros(),
              2234567890);
    EXPECT_EQ(value_ref(empty_metadata, primitive(VariantPrimitiveId::TIME_NTZ_MICROS,
                                                  integer_payload(3234567890, 8)))
                      .get_time_ntz_micros(),
              3234567890);
    EXPECT_EQ(value_ref(empty_metadata, primitive(VariantPrimitiveId::TIMESTAMP_NANOS,
                                                  integer_payload(-4234567890, 8)))
                      .get_timestamp_nanos(),
              -4234567890);
    EXPECT_EQ(value_ref(empty_metadata, primitive(VariantPrimitiveId::TIMESTAMP_NTZ_NANOS,
                                                  integer_payload(5234567890, 8)))
                      .get_timestamp_ntz_nanos(),
              5234567890);

    const std::string binary_bytes("\0\xFF\x01", 3);
    const std::string binary_value =
            primitive(VariantPrimitiveId::BINARY, variable_payload(binary_bytes));
    EXPECT_EQ(value_ref(empty_metadata, binary_value).get_binary(), StringRef(binary_bytes));

    const std::string long_text(64, 's');
    const std::string long_string =
            primitive(VariantPrimitiveId::STRING, variable_payload(long_text));
    EXPECT_EQ(value_ref(empty_metadata, long_string).get_string(), StringRef(long_text));
    const std::string short_text = "short";
    const std::string encoded_short = short_string(short_text);
    EXPECT_EQ(value_ref(empty_metadata, encoded_short).get_string(), StringRef(short_text));

    std::string uuid_payload;
    std::array<uint8_t, 16> expected_uuid {};
    for (uint8_t i = 0; i < expected_uuid.size(); ++i) {
        expected_uuid[i] = i;
        uuid_payload.push_back(static_cast<char>(i));
    }
    EXPECT_EQ(
            value_ref(empty_metadata, primitive(VariantPrimitiveId::UUID, uuid_payload)).get_uuid(),
            expected_uuid);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros expand assertions.
TEST(VariantValueTest, PrimitiveTruncationAndInvalidTypesFail) {
    const std::string empty_metadata = metadata({}, true);
    std::vector<std::string> values {
            primitive(VariantPrimitiveId::NULL_VALUE),
            primitive(VariantPrimitiveId::TRUE_VALUE),
            primitive(VariantPrimitiveId::FALSE_VALUE),
            primitive(VariantPrimitiveId::INT8, integer_payload(-1, 1)),
            primitive(VariantPrimitiveId::INT16, integer_payload(-1, 2)),
            primitive(VariantPrimitiveId::INT32, integer_payload(-1, 4)),
            primitive(VariantPrimitiveId::INT64, integer_payload(-1, 8)),
            primitive(VariantPrimitiveId::DOUBLE, integer_payload(0, 8)),
            primitive(VariantPrimitiveId::DECIMAL4, std::string(5, '\0')),
            primitive(VariantPrimitiveId::DECIMAL8, std::string(9, '\0')),
            primitive(VariantPrimitiveId::DECIMAL16, std::string(17, '\0')),
            primitive(VariantPrimitiveId::DATE, integer_payload(0, 4)),
            primitive(VariantPrimitiveId::TIMESTAMP_MICROS, integer_payload(0, 8)),
            primitive(VariantPrimitiveId::TIMESTAMP_NTZ_MICROS, integer_payload(0, 8)),
            primitive(VariantPrimitiveId::FLOAT, integer_payload(0, 4)),
            primitive(VariantPrimitiveId::BINARY, variable_payload("binary")),
            primitive(VariantPrimitiveId::STRING, variable_payload("string")),
            primitive(VariantPrimitiveId::TIME_NTZ_MICROS, integer_payload(0, 8)),
            primitive(VariantPrimitiveId::TIMESTAMP_NANOS, integer_payload(0, 8)),
            primitive(VariantPrimitiveId::TIMESTAMP_NTZ_NANOS, integer_payload(0, 8)),
            primitive(VariantPrimitiveId::UUID, std::string(16, '\0')),
            short_string("short"),
    };
    for (std::string& value : values) {
        value.pop_back();
        EXPECT_THROW(value_ref(empty_metadata, value).value_size(), Exception);
    }

    std::string unknown_primitive(1, static_cast<char>(21 << VARIANT_VALUE_HEADER_SHIFT));
    EXPECT_THROW(value_ref(empty_metadata, unknown_primitive).primitive_id(), Exception);
    EXPECT_THROW(value_ref(empty_metadata, unknown_primitive).value_size(), Exception);
    EXPECT_THROW(value_ref(empty_metadata, short_string("x")).get_int(), Exception);

    std::string invalid_decimal_payload(1, static_cast<char>(39));
    invalid_decimal_payload.append(4, '\0');
    EXPECT_THROW(value_ref(empty_metadata,
                           primitive(VariantPrimitiveId::DECIMAL4, invalid_decimal_payload))
                         .get_decimal(),
                 Exception);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros expand assertions.
TEST(VariantValueTest, MetadataWidthsBoundariesAndErrors) {
    const std::array<size_t, 4> key_sizes {255, 256, 65536, 16777216};
    for (uint8_t width = 1; width <= 4; ++width) {
        const std::string expected(key_sizes[width - 1], static_cast<char>('a' + width));
        const std::string encoded = metadata({expected}, true, width);
        const VariantMetadataRef ref = metadata_ref(encoded);
        EXPECT_EQ(ref.version(), VARIANT_ENCODING_VERSION);
        EXPECT_TRUE(ref.sorted_strings());
        EXPECT_EQ(ref.offset_size(), width);
        EXPECT_EQ(ref.dict_size(), 1);
        EXPECT_EQ(ref.key_at(0), StringRef(expected));
        EXPECT_EQ(ref.find_key(StringRef(expected)), 0);
        EXPECT_NO_THROW(ref.validate());
    }

    const std::string unsorted = metadata({"z", "a", "m"}, false);
    const VariantMetadataRef unsorted_ref = metadata_ref(unsorted);
    EXPECT_FALSE(unsorted_ref.sorted_strings());
    EXPECT_EQ(unsorted_ref.find_key(StringRef("a", 1)), 1);
    EXPECT_EQ(unsorted_ref.find_key(StringRef("missing", 7)), -1);
    EXPECT_NO_THROW(unsorted_ref.validate());

    const std::string valid = metadata({"a", "b"}, true);
    for (size_t truncated_size = 0; truncated_size < valid.size(); ++truncated_size) {
        EXPECT_THROW((VariantMetadataRef {valid.data(), truncated_size}).validate(), Exception);
    }
    EXPECT_THROW(metadata_ref(valid).key_at(2), Exception);

    std::string invalid_version = valid;
    invalid_version[0] = static_cast<char>((invalid_version[0] & 0xF0) | 2);
    EXPECT_THROW(metadata_ref(invalid_version).validate(), Exception);

    std::string invalid_offset = valid;
    invalid_offset[3] = 3;
    EXPECT_THROW(metadata_ref(invalid_offset).validate(), Exception);

    EXPECT_THROW(metadata_ref(metadata({"b", "a"}, true)).validate(), Exception);
    EXPECT_THROW(metadata_ref(metadata({"a", "a"}, true)).validate(), Exception);
}

TEST(VariantValueTest, OffsetWidthsAndElementCountBoundaries) {
    const std::string empty_metadata = metadata({}, true);
    EXPECT_EQ(static_cast<uint8_t>(array_value({}, 4, true)[0]), 0x1F);
    EXPECT_EQ(static_cast<uint8_t>(object_value({}, {}, {}, 4, 4, true)[0]), 0x7E);

    const std::array<size_t, 4> child_sizes {1, 256, 65536, 16777216};
    for (uint8_t width = 1; width <= 4; ++width) {
        std::string child;
        if (width == 1) {
            child = primitive(VariantPrimitiveId::NULL_VALUE);
        } else {
            const std::string bytes(child_sizes[width - 1] - 1 - sizeof(uint32_t), 'x');
            child = primitive(VariantPrimitiveId::BINARY, variable_payload(bytes));
        }
        ASSERT_EQ(child.size(), child_sizes[width - 1]);
        const std::string encoded = array_value({child}, width, false);
        const VariantValueRef ref = value_ref(empty_metadata, encoded);
        EXPECT_EQ(ref.value_size(), encoded.size());
        EXPECT_EQ(ref.array_at(0).value_size(), child.size());
    }

    const std::string null_value = primitive(VariantPrimitiveId::NULL_VALUE);
    const std::vector<std::string> small_values(255, null_value);
    const std::string small_array = array_value(small_values, 1, false);
    const VariantValueRef small_ref = value_ref(empty_metadata, small_array);
    EXPECT_EQ(small_ref.num_elements(), 255);
    EXPECT_TRUE(small_ref.array_at(254).is_null());

    const std::vector<std::string> large_values(256, null_value);
    const std::string large_array = array_value(large_values, 2, true);
    const VariantValueRef large_ref = value_ref(empty_metadata, large_array);
    EXPECT_EQ(large_ref.num_elements(), 256);
    EXPECT_TRUE(large_ref.array_at(255).is_null());
}

TEST(VariantValueTest, ObjectLookupSortedAndUnsortedMetadata) {
    const std::string sorted_metadata = metadata({"a", "b"}, true);
    const std::string false_value = primitive(VariantPrimitiveId::FALSE_VALUE);
    const std::string true_value = primitive(VariantPrimitiveId::TRUE_VALUE);
    const std::string physically_reordered =
            object_value({0, 1}, {1, 0}, {false_value, true_value});
    const VariantValueRef sorted_ref = value_ref(sorted_metadata, physically_reordered);
    EXPECT_EQ(sorted_ref.value_size(), physically_reordered.size());

    VariantValueRef found;
    ASSERT_TRUE(sorted_ref.object_find(StringRef("a", 1), &found));
    EXPECT_TRUE(found.get_bool());
    ASSERT_TRUE(sorted_ref.object_find_by_id(1, &found));
    EXPECT_FALSE(found.get_bool());
    EXPECT_FALSE(sorted_ref.object_find(StringRef("missing", 7), &found));

    const std::string unsorted_metadata = metadata({"z", "a", "m"}, false);
    const std::string unsorted_object =
            object_value({1, 2, 0}, {0, 1, 2},
                         {primitive(VariantPrimitiveId::NULL_VALUE), true_value, false_value});
    const VariantValueRef unsorted_ref = value_ref(unsorted_metadata, unsorted_object);
    ASSERT_TRUE(unsorted_ref.object_find(StringRef("a", 1), &found));
    EXPECT_TRUE(found.is_null());
    ASSERT_TRUE(unsorted_ref.object_find(StringRef("m", 1), &found));
    EXPECT_TRUE(found.get_bool());
    ASSERT_TRUE(unsorted_ref.object_find(StringRef("z", 1), &found));
    EXPECT_FALSE(found.get_bool());

    uint32_t id = std::numeric_limits<uint32_t>::max();
    EXPECT_TRUE(unsorted_ref.object_value_at(0, &id).is_null());
    EXPECT_EQ(id, 1);
}

TEST(VariantValueTest, ObjectFindRejectsInvalidReceivers) {
    const std::string empty_metadata = metadata({}, true);
    VariantValueRef found;
    EXPECT_THROW(value_ref(empty_metadata, primitive(VariantPrimitiveId::NULL_VALUE))
                         .object_find(StringRef("missing", 7), &found),
                 Exception);
    EXPECT_THROW(value_ref(empty_metadata, array_value({}, 1, false))
                         .object_find(StringRef("missing", 7), &found),
                 Exception);

    const std::string truncated_object(1, static_cast<char>(VariantBasicType::OBJECT));
    EXPECT_THROW(value_ref(empty_metadata, truncated_object)
                         .object_find(StringRef("missing", 7), &found),
                 Exception);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros expand assertions.
TEST(VariantValueTest, ObjectIdWidthsAndInvalidId) {
    std::vector<std::string> keys;
    keys.reserve(257);
    for (uint32_t id = 0; id < 257; ++id) {
        keys.push_back("k" + std::to_string(1000 + id));
    }
    const std::string wide_metadata = metadata(keys, true, 2);
    const std::string null_value = primitive(VariantPrimitiveId::NULL_VALUE);
    for (uint8_t id_width = 1; id_width <= 4; ++id_width) {
        const uint32_t id = id_width == 1 ? 255 : 256;
        const std::string encoded = object_value({id}, {0}, {null_value}, id_width);
        const VariantValueRef ref = value_ref(wide_metadata, encoded);
        uint32_t decoded_id = 0;
        EXPECT_TRUE(ref.object_value_at(0, &decoded_id).is_null());
        EXPECT_EQ(decoded_id, id);
    }

    const std::string one_key_metadata = metadata({"a"}, true);
    const std::string invalid_id_object = object_value({1}, {0}, {null_value});
    EXPECT_THROW(value_ref(one_key_metadata, invalid_id_object).object_value_at(0, nullptr),
                 Exception);
    VariantValueRef found;
    EXPECT_THROW(
            value_ref(one_key_metadata, invalid_id_object).object_find(StringRef("a", 1), &found),
            Exception);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros expand assertions.
TEST(VariantValueTest, ContainerBoundsAndTruncationFail) {
    const std::string empty_metadata = metadata({}, true);
    const std::string null_value = primitive(VariantPrimitiveId::NULL_VALUE);
    const std::string valid_array = array_value({null_value}, 1, false);
    for (size_t truncated_size = 0; truncated_size < valid_array.size(); ++truncated_size) {
        EXPECT_THROW(
                (VariantValueRef {metadata_ref(empty_metadata), valid_array.data(), truncated_size})
                        .value_size(),
                Exception);
    }
    EXPECT_THROW(value_ref(empty_metadata, valid_array).array_at(1), Exception);

    std::string invalid_first_offset = valid_array;
    invalid_first_offset[2] = 1;
    EXPECT_THROW(value_ref(empty_metadata, invalid_first_offset).value_size(), Exception);

    std::string invalid_child_boundary = valid_array;
    invalid_child_boundary[3] = 2;
    invalid_child_boundary.push_back(static_cast<char>(VariantPrimitiveId::NULL_VALUE));
    EXPECT_THROW(value_ref(empty_metadata, invalid_child_boundary).array_at(0), Exception);

    const std::string one_key_metadata = metadata({"a"}, true);
    const std::string valid_object = object_value({0}, {0}, {null_value});
    for (size_t truncated_size = 0; truncated_size < valid_object.size(); ++truncated_size) {
        EXPECT_THROW((VariantValueRef {metadata_ref(one_key_metadata), valid_object.data(),
                                       truncated_size})
                             .value_size(),
                     Exception);
    }

    std::string empty_object_with_values = object_value({}, {}, {null_value});
    EXPECT_THROW(value_ref(empty_metadata, empty_object_with_values).value_size(), Exception);
}

} // namespace
} // namespace doris
