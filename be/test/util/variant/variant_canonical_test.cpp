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

#include "util/variant/variant_canonical.h"

#include <crc32c/crc32c.h>
#include <gtest/gtest.h>

#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "exec/common/sip_hash.h"
#include "util/variant/variant_encoding.h"
#include "variant_test_utils.h"

namespace doris {
namespace {

struct OwnedValue {
    std::string metadata;
    std::string value;

    VariantValueRef ref() const {
        return {.metadata = {.data = metadata.data(), .size = metadata.size()},
                .data = value.data(),
                .size = value.size()};
    }
};

struct Hashes {
    uint64_t sip;
    uint64_t xx;
    uint32_t crc;
    uint32_t crc32c;
};

void append_unsigned(std::string& output, unsigned __int128 value, uint8_t width) {
    for (uint8_t index = 0; index < width; ++index) {
        output.push_back(static_cast<char>(value >> (index * 8)));
    }
}

void append_signed(std::string& output, __int128 value, uint8_t width) {
    append_unsigned(output, static_cast<unsigned __int128>(value), width);
}

uint32_t read_u32(const char* data) {
    uint32_t value = 0;
    for (uint8_t index = 0; index < sizeof(uint32_t); ++index) {
        value |= static_cast<uint32_t>(static_cast<uint8_t>(data[index])) << (index * 8);
    }
    return value;
}

uint32_t read_width(const char* data, uint8_t width) {
    uint32_t value = 0;
    for (uint8_t index = 0; index < width; ++index) {
        value |= static_cast<uint32_t>(static_cast<uint8_t>(data[index])) << (index * 8);
    }
    return value;
}

void write_u32(char* data, uint32_t value) {
    for (uint8_t index = 0; index < sizeof(uint32_t); ++index) {
        data[index] = static_cast<char>(value >> (index * 8));
    }
}

std::string encode_metadata(const std::vector<std::string>& keys, bool sorted) {
    size_t strings_size = 0;
    for (const std::string& key : keys) {
        strings_size += key.size();
    }
    EXPECT_LE(keys.size(), std::numeric_limits<uint8_t>::max());
    EXPECT_LE(strings_size, std::numeric_limits<uint8_t>::max());

    std::string metadata;
    metadata.push_back(static_cast<char>(VARIANT_ENCODING_VERSION |
                                         (sorted ? VARIANT_METADATA_SORTED_STRINGS_MASK : 0)));
    metadata.push_back(static_cast<char>(keys.size()));
    metadata.push_back(0);
    uint8_t offset = 0;
    for (const std::string& key : keys) {
        offset += static_cast<uint8_t>(key.size());
        metadata.push_back(static_cast<char>(offset));
    }
    for (const std::string& key : keys) {
        metadata.append(key);
    }
    return metadata;
}

std::string empty_metadata() {
    return encode_metadata({}, true);
}

OwnedValue scalar(std::string value) {
    return {.metadata = empty_metadata(), .value = std::move(value)};
}

std::string primitive(VariantPrimitiveId id, std::string payload = {}) {
    std::string value;
    value.push_back(static_cast<char>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT));
    value.append(payload);
    return value;
}

OwnedValue integer_value(int64_t value, uint8_t width) {
    VariantPrimitiveId id = VariantPrimitiveId::INT64;
    if (width == 1) {
        id = VariantPrimitiveId::INT8;
    } else if (width == 2) {
        id = VariantPrimitiveId::INT16;
    } else if (width == 4) {
        id = VariantPrimitiveId::INT32;
    }
    std::string payload;
    append_signed(payload, value, width);
    return scalar(primitive(id, payload));
}

OwnedValue decimal_value(__int128 unscaled, uint8_t scale, uint8_t width = 16) {
    VariantPrimitiveId id = VariantPrimitiveId::DECIMAL16;
    if (width == 4) {
        id = VariantPrimitiveId::DECIMAL4;
    } else if (width == 8) {
        id = VariantPrimitiveId::DECIMAL8;
    }
    std::string payload(1, static_cast<char>(scale));
    append_signed(payload, unscaled, width);
    return scalar(primitive(id, payload));
}

OwnedValue double_value(double value) {
    std::string payload;
    append_unsigned(payload, std::bit_cast<uint64_t>(value), sizeof(uint64_t));
    return scalar(primitive(VariantPrimitiveId::DOUBLE, payload));
}

OwnedValue double_bits(uint64_t bits) {
    std::string payload;
    append_unsigned(payload, bits, sizeof(uint64_t));
    return scalar(primitive(VariantPrimitiveId::DOUBLE, payload));
}

OwnedValue float_value(float value) {
    std::string payload;
    append_unsigned(payload, std::bit_cast<uint32_t>(value), sizeof(uint32_t));
    return scalar(primitive(VariantPrimitiveId::FLOAT, payload));
}

OwnedValue float_bits(uint32_t bits) {
    std::string payload;
    append_unsigned(payload, bits, sizeof(uint32_t));
    return scalar(primitive(VariantPrimitiveId::FLOAT, payload));
}

OwnedValue fixed_value(VariantPrimitiveId id, int64_t value, uint8_t width) {
    std::string payload;
    append_signed(payload, value, width);
    return scalar(primitive(id, payload));
}

OwnedValue string_value(std::string_view text, bool force_long = false) {
    std::string value;
    if (!force_long && text.size() <= VARIANT_MAX_SHORT_STRING_SIZE) {
        value.push_back(static_cast<char>((text.size() << VARIANT_VALUE_HEADER_SHIFT) |
                                          static_cast<uint8_t>(VariantBasicType::SHORT_STRING)));
        value.append(text);
        return scalar(std::move(value));
    }
    value = primitive(VariantPrimitiveId::STRING);
    append_unsigned(value, text.size(), sizeof(uint32_t));
    value.append(text);
    return scalar(std::move(value));
}

OwnedValue binary_value(std::string_view bytes) {
    std::string value = primitive(VariantPrimitiveId::BINARY);
    append_unsigned(value, bytes.size(), sizeof(uint32_t));
    value.append(bytes);
    return scalar(std::move(value));
}

OwnedValue uuid_value(const std::array<uint8_t, 16>& uuid) {
    std::string payload;
    for (uint8_t byte : uuid) {
        payload.push_back(static_cast<char>(byte));
    }
    return scalar(primitive(VariantPrimitiveId::UUID, payload));
}

std::string object_bytes(const std::vector<uint32_t>& field_ids,
                         const std::vector<std::string>& children,
                         const std::vector<uint32_t>& physical_order) {
    EXPECT_EQ(field_ids.size(), children.size());
    EXPECT_EQ(field_ids.size(), physical_order.size());
    EXPECT_LE(children.size(), std::numeric_limits<uint8_t>::max());

    std::vector<uint8_t> offsets(children.size());
    std::string values;
    for (uint32_t logical_index : physical_order) {
        EXPECT_LT(logical_index, children.size());
        EXPECT_LE(values.size(), std::numeric_limits<uint8_t>::max());
        offsets[logical_index] = static_cast<uint8_t>(values.size());
        values.append(children[logical_index]);
    }
    EXPECT_LE(values.size(), std::numeric_limits<uint8_t>::max());

    std::string object;
    object.push_back(static_cast<char>(VariantBasicType::OBJECT));
    object.push_back(static_cast<char>(children.size()));
    for (uint32_t id : field_ids) {
        EXPECT_LE(id, std::numeric_limits<uint8_t>::max());
        object.push_back(static_cast<char>(id));
    }
    for (uint8_t offset : offsets) {
        object.push_back(static_cast<char>(offset));
    }
    object.push_back(static_cast<char>(values.size()));
    object.append(values);
    return object;
}

OwnedValue object_value(std::vector<std::string> dictionary, bool sorted,
                        const std::vector<uint32_t>& field_ids,
                        const std::vector<std::string>& children,
                        const std::vector<uint32_t>& physical_order) {
    return {.metadata = encode_metadata(dictionary, sorted),
            .value = object_bytes(field_ids, children, physical_order)};
}

std::string array_bytes(const std::vector<std::string>& children) {
    std::string values;
    std::vector<uint32_t> offsets;
    offsets.push_back(0);
    for (const std::string& child : children) {
        values.append(child);
        EXPECT_LE(values.size(), std::numeric_limits<uint32_t>::max());
        offsets.push_back(static_cast<uint32_t>(values.size()));
    }
    uint8_t offset_width = 1;
    if (values.size() > std::numeric_limits<uint8_t>::max()) {
        offset_width = values.size() <= std::numeric_limits<uint16_t>::max() ? 2 : 4;
    }
    const auto value_header =
            static_cast<uint8_t>((offset_width - 1) << VARIANT_ARRAY_OFFSET_SIZE_SHIFT);
    std::string array;
    array.push_back(static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                      static_cast<uint8_t>(VariantBasicType::ARRAY)));
    array.push_back(static_cast<char>(children.size()));
    for (uint32_t offset : offsets) {
        append_unsigned(array, offset, offset_width);
    }
    array.append(values);
    return array;
}

OwnedValue array_value(const std::vector<std::string>& children) {
    return {.metadata = empty_metadata(), .value = array_bytes(children)};
}

Hashes hashes(VariantValueRef value) {
    SipHash sip;
    canonical_hash(value, sip);
    VariantXxHashSink xx(0x123456789ABCDEF0ULL);
    canonical_hash(value, xx);
    VariantCrc32HashSink crc(0x13579BDFU);
    canonical_hash(value, crc);
    VariantCrc32cHashSink crc32c(0x2468ACE0U);
    canonical_hash(value, crc32c);
    return {.sip = sip.get64(), .xx = xx.digest(), .crc = crc.digest(), .crc32c = crc32c.digest()};
}

Hashes hashes(VariantCanonicalScalarRef value) {
    SipHash sip;
    canonical_hash(value, sip);
    VariantXxHashSink xx(0x123456789ABCDEF0ULL);
    canonical_hash(value, xx);
    VariantCrc32HashSink crc(0x13579BDFU);
    canonical_hash(value, crc);
    VariantCrc32cHashSink crc32c(0x2468ACE0U);
    canonical_hash(value, crc32c);
    return {.sip = sip.get64(), .xx = xx.digest(), .crc = crc.digest(), .crc32c = crc32c.digest()};
}

std::string arena(VariantValueRef value) {
    std::string encoded;
    const size_t appended = canonical_serialize(value, encoded);
    EXPECT_EQ(appended, encoded.size());
    EXPECT_GE(encoded.size(), sizeof(uint32_t));
    EXPECT_EQ(read_u32(encoded.data()), encoded.size() - sizeof(uint32_t));
    return encoded;
}

std::string arena(VariantCanonicalScalarRef value) {
    const CanonicalScalarSerializationPlan plan = prepare_canonical_serialize(value);
    std::string encoded(plan.size(), '\0');
    plan.write(encoded.data(), encoded.size());
    EXPECT_GE(encoded.size(), sizeof(uint32_t));
    EXPECT_EQ(read_u32(encoded.data()), encoded.size() - sizeof(uint32_t));
    return encoded;
}

VariantValueRef arena_value_ref(const std::string& encoded) {
    EXPECT_GE(encoded.size(), sizeof(uint32_t) + 3);
    EXPECT_EQ(read_u32(encoded.data()), encoded.size() - sizeof(uint32_t));
    const char* metadata = encoded.data() + sizeof(uint32_t);
    const auto header = static_cast<uint8_t>(metadata[0]);
    const auto width = static_cast<uint8_t>(
            ((header >> VARIANT_METADATA_OFFSET_SIZE_SHIFT) & VARIANT_METADATA_OFFSET_SIZE_MASK) +
            1);
    const uint32_t count = read_width(metadata + 1, width);
    const size_t offsets_position = 1 + width;
    const uint32_t strings_size =
            read_width(metadata + offsets_position + static_cast<size_t>(count) * width, width);
    const size_t metadata_size =
            offsets_position + (static_cast<size_t>(count) + 1) * width + strings_size;
    EXPECT_LT(sizeof(uint32_t) + metadata_size, encoded.size());
    return {.metadata = {.data = metadata, .size = metadata_size},
            .data = metadata + metadata_size,
            .size = encoded.size() - sizeof(uint32_t) - metadata_size};
}

void expect_canonical_parse_error(const std::string& encoded, int expected) {
    try {
        static_cast<void>(parse_canonical_serialized(StringRef(encoded.data(), encoded.size())));
        ADD_FAILURE() << "expected canonical cell parse failure";
    } catch (const Exception& exception) {
        EXPECT_EQ(exception.code(), expected) << exception.message();
    }
}

void expect_equivalent(const OwnedValue& left, const OwnedValue& right) {
    EXPECT_TRUE(canonical_equals(left.ref(), right.ref()));
    const Hashes left_hashes = hashes(left.ref());
    const Hashes right_hashes = hashes(right.ref());
    EXPECT_EQ(left_hashes.sip, right_hashes.sip);
    EXPECT_EQ(left_hashes.xx, right_hashes.xx);
    EXPECT_EQ(left_hashes.crc, right_hashes.crc);
    EXPECT_EQ(left_hashes.crc32c, right_hashes.crc32c);
    EXPECT_EQ(arena(left.ref()), arena(right.ref()));
}

void expect_distinct(const OwnedValue& left, const OwnedValue& right) {
    EXPECT_FALSE(canonical_equals(left.ref(), right.ref()));
    EXPECT_NE(arena(left.ref()), arena(right.ref()));
}

void expect_scalar_equivalent(VariantCanonicalScalarRef scalar_ref,
                              const OwnedValue& encoded_value) {
    EXPECT_TRUE(canonical_equals(scalar_ref, scalar_ref));
    const Hashes scalar_hashes = hashes(scalar_ref);
    const Hashes encoded_hashes = hashes(encoded_value.ref());
    EXPECT_EQ(scalar_hashes.sip, encoded_hashes.sip);
    EXPECT_EQ(scalar_hashes.xx, encoded_hashes.xx);
    EXPECT_EQ(scalar_hashes.crc, encoded_hashes.crc);
    EXPECT_EQ(scalar_hashes.crc32c, encoded_hashes.crc32c);
    EXPECT_EQ(arena(scalar_ref), arena(encoded_value.ref()));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- exhaustive typed/encoded parity matrix.
TEST(VariantCanonicalTest, TypedScalarCanonicalMatchesVariantValueRef) {
    expect_scalar_equivalent(VariantCanonicalScalarRef::null_value(),
                             scalar(primitive(VariantPrimitiveId::NULL_VALUE)));
    expect_scalar_equivalent(VariantCanonicalScalarRef::boolean(false),
                             scalar(primitive(VariantPrimitiveId::FALSE_VALUE)));
    expect_scalar_equivalent(VariantCanonicalScalarRef::boolean(true),
                             scalar(primitive(VariantPrimitiveId::TRUE_VALUE)));
    expect_scalar_equivalent(VariantCanonicalScalarRef::exact_integer(42), integer_value(42, 1));
    expect_scalar_equivalent(
            VariantCanonicalScalarRef::exact_integer(
                    static_cast<__int128>(1'000'000'000'000'000'000LL)),
            decimal_value(static_cast<__int128>(1'000'000'000'000'000'000LL), 0, 16));
    expect_scalar_equivalent(VariantCanonicalScalarRef::decimal(12300, 4),
                             decimal_value(123, 2, 4));
    expect_scalar_equivalent(VariantCanonicalScalarRef::decimal(4200, 2), integer_value(42, 1));
    expect_scalar_equivalent(VariantCanonicalScalarRef::float32(1.5F), float_value(1.5F));
    expect_scalar_equivalent(VariantCanonicalScalarRef::float64(42.0), integer_value(42, 1));
    expect_scalar_equivalent(VariantCanonicalScalarRef::float64(-0.0), integer_value(0, 1));
    expect_scalar_equivalent(
            VariantCanonicalScalarRef::float64(std::numeric_limits<double>::quiet_NaN()),
            double_bits(0x7FF0000000000001ULL));

    const std::string short_text = "typed string";
    const std::string long_text(64, 'L');
    const std::string binary("\0\xFF\x01", 3);
    expect_scalar_equivalent(VariantCanonicalScalarRef::string(StringRef(short_text)),
                             string_value(short_text));
    expect_scalar_equivalent(VariantCanonicalScalarRef::string(StringRef(long_text)),
                             string_value(long_text, true));
    expect_scalar_equivalent(VariantCanonicalScalarRef::binary(StringRef(binary)),
                             binary_value(binary));
    expect_scalar_equivalent(VariantCanonicalScalarRef::date(-20000),
                             fixed_value(VariantPrimitiveId::DATE, -20000, 4));
    expect_scalar_equivalent(VariantCanonicalScalarRef::timestamp_micros(-1234567890, true),
                             fixed_value(VariantPrimitiveId::TIMESTAMP_MICROS, -1234567890, 8));
    expect_scalar_equivalent(VariantCanonicalScalarRef::timestamp_micros(2234567890, false),
                             fixed_value(VariantPrimitiveId::TIMESTAMP_NTZ_MICROS, 2234567890, 8));
    expect_scalar_equivalent(VariantCanonicalScalarRef::timestamp_nanos(-3234567891, true),
                             fixed_value(VariantPrimitiveId::TIMESTAMP_NANOS, -3234567891, 8));
    expect_scalar_equivalent(VariantCanonicalScalarRef::timestamp_nanos(4234567891, false),
                             fixed_value(VariantPrimitiveId::TIMESTAMP_NTZ_NANOS, 4234567891, 8));
    expect_scalar_equivalent(VariantCanonicalScalarRef::time_ntz_micros(5234567890),
                             fixed_value(VariantPrimitiveId::TIME_NTZ_MICROS, 5234567890, 8));

    std::array<uint8_t, 16> uuid {};
    for (uint8_t index = 0; index < uuid.size(); ++index) {
        uuid[index] = index;
    }
    expect_scalar_equivalent(VariantCanonicalScalarRef::uuid(uuid), uuid_value(uuid));

    const VariantCanonicalScalarRef integer_42 = VariantCanonicalScalarRef::exact_integer(42);
    const VariantCanonicalScalarRef decimal_42 = VariantCanonicalScalarRef::decimal(4200, 2);
    const VariantCanonicalScalarRef double_42 = VariantCanonicalScalarRef::float64(42.0);
    EXPECT_TRUE(canonical_equals(integer_42, decimal_42));
    EXPECT_TRUE(canonical_equals(decimal_42, double_42));
    EXPECT_FALSE(canonical_equals(integer_42, VariantCanonicalScalarRef::string(StringRef("42"))));
    EXPECT_FALSE(canonical_equals(integer_42, VariantCanonicalScalarRef::float64(42.5)));

    std::string borrowed = "borrowed";
    const VariantCanonicalScalarRef borrowed_ref =
            VariantCanonicalScalarRef::string(StringRef(borrowed));
    const CanonicalScalarSerializationPlan borrowed_plan =
            prepare_canonical_serialize(borrowed_ref);
    borrowed.front() = 'B';
    std::string borrowed_arena(borrowed_plan.size(), '\0');
    borrowed_plan.write(borrowed_arena.data(), borrowed_arena.size());
    EXPECT_EQ(borrowed_arena, arena(string_value(borrowed).ref()));

    const CanonicalScalarSerializationPlan null_plan =
            prepare_canonical_serialize(VariantCanonicalScalarRef::null_value());
    std::array<char, 16> unchanged {};
    unchanged.fill('x');
    EXPECT_THROW(null_plan.write(nullptr, null_plan.size()), Exception);
    EXPECT_THROW(null_plan.write(unchanged.data(), null_plan.size() - 1), Exception);
    EXPECT_TRUE(std::ranges::all_of(unchanged, [](char value) { return value == 'x'; }));

    const std::string invalid_utf8("\xC3\x28", 2);
    EXPECT_THROW(VariantCanonicalScalarRef::string(StringRef(invalid_utf8)), Exception);
    EXPECT_THROW(VariantCanonicalScalarRef::decimal(1, 39), Exception);
    const __int128 outside_decimal38 = [] {
        __int128 value = 1;
        for (uint8_t digit = 0; digit < 38; ++digit) {
            value *= 10;
        }
        return value;
    }();
    EXPECT_THROW(VariantCanonicalScalarRef::decimal(outside_decimal38, 0), Exception);
    EXPECT_THROW(VariantCanonicalScalarRef::exact_integer(outside_decimal38), Exception);
    const StringRef null_bytes(static_cast<const char*>(nullptr), 1);
    EXPECT_THROW(VariantCanonicalScalarRef::string(null_bytes), Exception);
    EXPECT_THROW(VariantCanonicalScalarRef::binary(null_bytes), Exception);
}

TEST(VariantCanonicalTest, NumericEquivalenceMatrix) {
    const OwnedValue int8 = integer_value(42, 1);
    const OwnedValue int16 = integer_value(42, 2);
    const OwnedValue int32 = integer_value(42, 4);
    const OwnedValue int64 = integer_value(42, 8);
    const OwnedValue decimal_integer = decimal_value(4200, 2, 16);
    const OwnedValue double_integer = double_value(42.0);
    const OwnedValue float_integer = float_value(42.0F);
    expect_equivalent(int8, int16);
    expect_equivalent(int8, int32);
    expect_equivalent(int8, int64);
    expect_equivalent(int8, decimal_integer);
    expect_equivalent(int8, double_integer);
    expect_equivalent(int8, float_integer);

    const OwnedValue decimal_trailing = decimal_value(12300, 4, 16);
    const OwnedValue decimal_stripped = decimal_value(123, 2, 4);
    expect_equivalent(decimal_trailing, decimal_stripped);
    expect_distinct(decimal_stripped, double_value(1.23));

    expect_equivalent(float_value(1.5F), double_value(1.5));
    expect_distinct(float_value(0.1F), double_value(0.1));

    const OwnedValue double_nan_a = double_bits(0x7FF0000000000001ULL);
    const OwnedValue double_nan_b = double_bits(0xFFFABCDE12345678ULL);
    const OwnedValue float_nan = float_bits(0x7FC12345U);
    expect_equivalent(double_nan_a, double_nan_b);
    expect_equivalent(double_nan_a, float_nan);

    expect_equivalent(double_value(0.0), double_value(-0.0));
    expect_equivalent(double_value(-0.0), float_value(-0.0F));
    expect_equivalent(double_value(-0.0), integer_value(0, 1));

    // The binary64 value spelled 1E38 is slightly below mathematical 10^38 and is encodable;
    // its next representable value toward +infinity is the first value outside decimal38.
    const double inside_decimal38 = 1E38;
    const OwnedValue inside = double_value(inside_decimal38);
    const std::string inside_arena = arena(inside.ref());
    EXPECT_EQ(arena_value_ref(inside_arena).primitive_id(), VariantPrimitiveId::DECIMAL16);
    expect_equivalent(inside, decimal_value(static_cast<__int128>(inside_decimal38), 0, 16));
    const OwnedValue outside =
            double_value(std::nextafter(1E38, std::numeric_limits<double>::infinity()));
    const std::string outside_arena = arena(outside.ref());
    EXPECT_EQ(arena_value_ref(outside_arena).primitive_id(), VariantPrimitiveId::DOUBLE);
    expect_distinct(inside, outside);
    EXPECT_EQ(arena_value_ref(
                      arena(double_value(
                                    std::nextafter(-1E38, -std::numeric_limits<double>::infinity()))
                                    .ref()))
                      .primitive_id(),
              VariantPrimitiveId::DOUBLE);
}

TEST(VariantCanonicalTest, PrimitiveTypeClasses) {
    const OwnedValue null_a = scalar(primitive(VariantPrimitiveId::NULL_VALUE));
    const OwnedValue null_b = scalar(primitive(VariantPrimitiveId::NULL_VALUE));
    expect_equivalent(null_a, null_b);

    const OwnedValue true_value = scalar(primitive(VariantPrimitiveId::TRUE_VALUE));
    const OwnedValue false_value = scalar(primitive(VariantPrimitiveId::FALSE_VALUE));
    expect_distinct(true_value, false_value);

    const OwnedValue short_string = string_value("same bytes");
    const OwnedValue long_form = string_value("same bytes", true);
    expect_equivalent(short_string, long_form);
    expect_distinct(short_string, binary_value("same bytes"));

    expect_equivalent(fixed_value(VariantPrimitiveId::DATE, -20000, 4),
                      fixed_value(VariantPrimitiveId::DATE, -20000, 4));
    expect_equivalent(fixed_value(VariantPrimitiveId::TIME_NTZ_MICROS, 1234567, 8),
                      fixed_value(VariantPrimitiveId::TIME_NTZ_MICROS, 1234567, 8));
    expect_distinct(fixed_value(VariantPrimitiveId::DATE, 1, 4), integer_value(1, 1));

    std::array<uint8_t, 16> uuid {};
    for (uint8_t index = 0; index < uuid.size(); ++index) {
        uuid[index] = index;
    }
    expect_equivalent(uuid_value(uuid), uuid_value(uuid));
    uuid.back() ^= 1;
    expect_distinct(uuid_value(std::array<uint8_t, 16> {}), uuid_value(uuid));
}

TEST(VariantCanonicalTest, TimestampUnitsNormalizeByLogicalTime) {
    const OwnedValue micros = fixed_value(VariantPrimitiveId::TIMESTAMP_MICROS, 1, 8);
    const OwnedValue nanos = fixed_value(VariantPrimitiveId::TIMESTAMP_NANOS, 1000, 8);
    expect_equivalent(micros, nanos);
    EXPECT_EQ(arena_value_ref(arena(nanos.ref())).primitive_id(),
              VariantPrimitiveId::TIMESTAMP_MICROS);

    const OwnedValue sub_micros = fixed_value(VariantPrimitiveId::TIMESTAMP_NANOS, 1001, 8);
    expect_distinct(micros, sub_micros);
    EXPECT_EQ(arena_value_ref(arena(sub_micros.ref())).primitive_id(),
              VariantPrimitiveId::TIMESTAMP_NANOS);

    expect_distinct(micros, fixed_value(VariantPrimitiveId::TIMESTAMP_NTZ_NANOS, 1000, 8));
    expect_equivalent(fixed_value(VariantPrimitiveId::TIMESTAMP_NTZ_MICROS, -7, 8),
                      fixed_value(VariantPrimitiveId::TIMESTAMP_NTZ_NANOS, -7000, 8));
    for (int64_t extreme :
         {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()}) {
        const OwnedValue value = fixed_value(VariantPrimitiveId::TIMESTAMP_MICROS, extreme, 8);
        const std::string encoded = arena(value.ref());
        const VariantValueRef canonical = arena_value_ref(encoded);
        EXPECT_EQ(canonical.primitive_id(), VariantPrimitiveId::TIMESTAMP_MICROS);
        EXPECT_EQ(canonical.get_timestamp_micros(), extreme);
        EXPECT_TRUE(canonical_equals(value.ref(), canonical));
    }
}

TEST(VariantCanonicalTest, ObjectUsesKeyBytesAcrossMetadataAndPhysicalOrder) {
    const std::string one = integer_value(1, 1).value;
    const std::string two = integer_value(2, 8).value;
    const OwnedValue canonical = object_value({"a", "b"}, true, {0, 1}, {one, two}, {0, 1});
    const OwnedValue external =
            object_value({"b", "a", "unused"}, false, {1, 0}, {one, two}, {1, 0});
    expect_equivalent(canonical, external);

    const std::string nested_a = object_bytes({0}, {one}, {0});
    const OwnedValue nested_left =
            object_value({"a", "inner", "z"}, true, {1, 2}, {nested_a, two}, {0, 1});
    const std::string nested_b = object_bytes({1}, {one}, {0});
    const OwnedValue nested_right =
            object_value({"z", "a", "inner"}, false, {2, 0}, {nested_b, two}, {1, 0});
    expect_equivalent(nested_left, nested_right);

    const OwnedValue bad_order = object_value({"b", "a"}, false, {0, 1}, {one, two}, {0, 1});
    VariantXxHashSink sink;
    EXPECT_THROW(canonical_hash(bad_order.ref(), sink), Exception);
    std::string destination = "unchanged";
    EXPECT_THROW(canonical_serialize(bad_order.ref(), destination), Exception);
    EXPECT_EQ(destination, "unchanged");
}

TEST(VariantCanonicalTest, ArrayOrderIsSignificant) {
    const std::string one = integer_value(1, 1).value;
    const std::string two = integer_value(2, 8).value;
    const OwnedValue forward = array_value({one, two});
    const OwnedValue same =
            array_value({integer_value(1, 8).value, decimal_value(200, 2, 16).value});
    const OwnedValue reverse = array_value({two, one});
    expect_equivalent(forward, same);
    expect_distinct(forward, reverse);
}

TEST(VariantCanonicalTest, Crc32cSinkUsesCanonicalTokensAndCallerSeed) {
    const OwnedValue narrow = integer_value(42, 1);
    const OwnedValue wide = decimal_value(4200, 2, 16);
    constexpr uint32_t SEED = 0x89ABCDEFU;
    VariantCrc32cHashSink narrow_sink(SEED);
    canonical_hash(narrow.ref(), narrow_sink);
    VariantCrc32cHashSink wide_sink(SEED);
    canonical_hash(wide.ref(), wide_sink);
    EXPECT_EQ(narrow_sink.digest(), wide_sink.digest());
    EXPECT_NE(narrow_sink.digest(), SEED);

    const std::string fixed_bytes = "variant-crc32c-sink";
    VariantCrc32cHashSink identity_sink(SEED);
    identity_sink.update(fixed_bytes.data(), fixed_bytes.size());
    EXPECT_EQ(identity_sink.digest(),
              crc32c_extend(SEED, reinterpret_cast<const uint8_t*>(fixed_bytes.data()),
                            fixed_bytes.size()));
}

TEST(VariantCanonicalTest, ArenaPrefixAndCanonicalEncoding) {
    const OwnedValue input = object_value(
            {"b", "a", "unused"}, false, {1, 0},
            {decimal_value(4200, 2, 16).value, string_value("short", true).value}, {1, 0});
    std::string destination = "prefix";
    const size_t appended = canonical_serialize(input.ref(), destination);
    ASSERT_GT(appended, sizeof(uint32_t));
    EXPECT_EQ(destination.substr(0, 6), "prefix");
    const std::string encoded = destination.substr(6);
    EXPECT_EQ(read_u32(encoded.data()), appended - sizeof(uint32_t));
    const VariantValueRef canonical = arena_value_ref(encoded);
    validate_canonical(canonical);
    EXPECT_TRUE(canonical.metadata.sorted_strings());
    EXPECT_EQ(canonical.metadata.dict_size(), 2);
    EXPECT_EQ(canonical.metadata.key_at(0), StringRef("a", 1));
    EXPECT_EQ(canonical.metadata.key_at(1), StringRef("b", 1));

    uint32_t field_id = 0;
    EXPECT_EQ(canonical.object_value_at(0, &field_id).primitive_id(), VariantPrimitiveId::INT8);
    EXPECT_EQ(field_id, 0);
    EXPECT_EQ(canonical.object_value_at(1, &field_id).basic_type(), VariantBasicType::SHORT_STRING);
    EXPECT_EQ(field_id, 1);
}

TEST(VariantCanonicalTest, ParseCanonicalCellRejectsInvalidBoundaries) {
    const OwnedValue input = object_value(
            {"a", "nested"}, true, {0, 1},
            {integer_value(7, 8).value,
             array_bytes({string_value("value", true).value, integer_value(9, 1).value})},
            {0, 1});
    const std::string encoded = arena(input.ref());
    const VariantValueRef parsed =
            parse_canonical_serialized(StringRef(encoded.data(), encoded.size()));
    EXPECT_EQ(parsed.value_size(), parsed.size);
    EXPECT_TRUE(canonical_equals(input.ref(), parsed));

    expect_canonical_parse_error(encoded.substr(0, sizeof(uint32_t) - 1), ErrorCode::CORRUPTION);

    std::string payload_mismatch = encoded;
    write_u32(payload_mismatch.data(), read_u32(payload_mismatch.data()) - 1);
    expect_canonical_parse_error(payload_mismatch, ErrorCode::CORRUPTION);

    std::string truncated_metadata(sizeof(uint32_t) + 1, '\0');
    write_u32(truncated_metadata.data(), 1);
    truncated_metadata[sizeof(uint32_t)] = static_cast<char>(VARIANT_ENCODING_VERSION);
    expect_canonical_parse_error(truncated_metadata, ErrorCode::CORRUPTION);

    std::string unsupported_version = encoded;
    auto& metadata_header = unsupported_version[sizeof(uint32_t)];
    metadata_header = static_cast<char>(
            (static_cast<uint8_t>(metadata_header) & ~VARIANT_METADATA_VERSION_MASK) | 2);
    expect_canonical_parse_error(unsupported_version, ErrorCode::INVALID_ARGUMENT);

    std::string truncated_value = encoded;
    truncated_value.pop_back();
    write_u32(truncated_value.data(), truncated_value.size() - sizeof(uint32_t));
    expect_canonical_parse_error(truncated_value, ErrorCode::CORRUPTION);

    std::string trailing_value = encoded;
    trailing_value.push_back('\0');
    write_u32(trailing_value.data(), trailing_value.size() - sizeof(uint32_t));
    expect_canonical_parse_error(trailing_value, ErrorCode::CORRUPTION);

    VariantValueRef nested;
    ASSERT_TRUE(parsed.object_find(StringRef("nested"), &nested));
    ASSERT_EQ(nested.basic_type(), VariantBasicType::ARRAY);
    const size_t nested_offset = nested.data - encoded.data();
    ASSERT_EQ(static_cast<uint8_t>(encoded[nested_offset + 2]), 0);
    std::string invalid_nested_boundary = encoded;
    invalid_nested_boundary[nested_offset + 2] = 1;
    const VariantValueRef valid_outer_layout = arena_value_ref(invalid_nested_boundary);
    EXPECT_EQ(valid_outer_layout.value_size(), valid_outer_layout.size);
    expect_canonical_parse_error(invalid_nested_boundary, ErrorCode::CORRUPTION);

    const OwnedValue two_fields = object_value(
            {"a", "b"}, true, {0, 1},
            {primitive(VariantPrimitiveId::NULL_VALUE), primitive(VariantPrimitiveId::TRUE_VALUE)},
            {0, 1});
    std::string overlapping_object = arena(two_fields.ref());
    const VariantValueRef object_ref = arena_value_ref(overlapping_object);
    ASSERT_EQ(object_ref.num_elements(), 2);
    const size_t object_offset = object_ref.data - overlapping_object.data();
    // Small-object layout: header, count, two ids, then three one-byte value offsets.
    ASSERT_GE(object_ref.size, 9);
    overlapping_object[object_offset + 5] = 0;
    expect_canonical_parse_error(overlapping_object, ErrorCode::CORRUPTION);

    std::string invalid_metadata_utf8 = arena(two_fields.ref());
    VariantValueRef metadata_ref = arena_value_ref(invalid_metadata_utf8);
    const size_t metadata_offset = metadata_ref.metadata.data - invalid_metadata_utf8.data();
    ASSERT_GE(metadata_ref.metadata.size, 7);
    invalid_metadata_utf8[metadata_offset + metadata_ref.metadata.size - 1] =
            static_cast<char>(0xFF);
    expect_canonical_parse_error(invalid_metadata_utf8, ErrorCode::CORRUPTION);
}

TEST(VariantCanonicalTest, CallerOwnedBufferMatchesStringAndRejectsInsufficientCapacity) {
    const OwnedValue input = object_value(
            {"nested", "value", "unused"}, false, {0, 1},
            {array_bytes({integer_value(7, 8).value, string_value("text", true).value}),
             decimal_value(12300, 4, 16).value},
            {1, 0});
    const std::string string_encoded = arena(input.ref());
    CanonicalSerializationPlan plan = prepare_canonical_serialize(input.ref());
    ASSERT_EQ(plan.size(), string_encoded.size());

    std::vector<char> caller_owned(plan.size(), static_cast<char>(0x5A));
    plan.write(caller_owned.data(), caller_owned.size());
    EXPECT_EQ(std::string(caller_owned.begin(), caller_owned.end()), string_encoded);
    EXPECT_EQ(read_u32(caller_owned.data()), plan.size() - sizeof(uint32_t));

    std::vector<char> insufficient(plan.size(), static_cast<char>(0x33));
    const std::vector<char> before = insufficient;
    ASSERT_GT(plan.size(), 0);
    EXPECT_THROW(plan.write(insufficient.data(), plan.size() - 1), Exception);
    EXPECT_EQ(insufficient, before);
    EXPECT_THROW(plan.write(nullptr, plan.size()), Exception);
}

TEST(VariantCanonicalTest, RejectsTruncatedOverflowingAndTooDeepInputs) {
    OwnedValue truncated = double_value(1.5);
    truncated.value.resize(4);
    EXPECT_THROW(canonical_equals(truncated.ref(), double_value(1.5).ref()), Exception);
    VariantCrc32HashSink crc;
    EXPECT_THROW(canonical_hash(truncated.ref(), crc), Exception);
    std::string destination = "unchanged";
    EXPECT_THROW(canonical_serialize(truncated.ref(), destination), Exception);
    EXPECT_EQ(destination, "unchanged");

    std::string maximal_count;
    maximal_count.push_back(
            static_cast<char>((VARIANT_ARRAY_LARGE_MASK << VARIANT_VALUE_HEADER_SHIFT) |
                              static_cast<uint8_t>(VariantBasicType::ARRAY)));
    append_unsigned(maximal_count, std::numeric_limits<uint32_t>::max(), sizeof(uint32_t));
    const OwnedValue overflowing_layout = scalar(maximal_count);
    EXPECT_THROW(canonical_serialize(overflowing_layout.ref(), destination), Exception);
    EXPECT_EQ(destination, "unchanged");

    const unsigned __int128 int128_max = (static_cast<unsigned __int128>(1) << 127) - 1;
    const OwnedValue invalid_decimal = decimal_value(static_cast<__int128>(int128_max), 0, 16);
    EXPECT_THROW(canonical_serialize(invalid_decimal.ref(), destination), Exception);

    const OwnedValue invalid_utf8 = string_value(std::string("\xFF", 1));
    EXPECT_THROW(canonical_serialize(invalid_utf8.ref(), destination), Exception);

    std::string deep = integer_value(1, 1).value;
    for (uint32_t depth = 0; depth < 130; ++depth) {
        deep = array_bytes({deep});
    }
    const OwnedValue too_deep = scalar(deep);
    EXPECT_THROW(canonical_serialize(too_deep.ref(), destination), Exception);
}

OwnedValue equivalent_integer(int64_t value, uint32_t encoding) {
    switch (encoding % 5) {
    case 0:
        return integer_value(value, 8);
    case 1:
        return decimal_value(static_cast<__int128>(value) * 100, 2, 16);
    case 2:
        return double_value(static_cast<double>(value));
    case 3:
        return float_value(static_cast<float>(value));
    case 4:
        return value >= std::numeric_limits<int8_t>::min() &&
                               value <= std::numeric_limits<int8_t>::max()
                       ? integer_value(value, 1)
                       : integer_value(value, 4);
    }
    return integer_value(value, 8);
}

std::pair<OwnedValue, OwnedValue> random_pair(std::mt19937_64& random, uint32_t index,
                                              bool equivalent) {
    const uint32_t mode = index % 5;
    int64_t left_number = static_cast<int64_t>(random() % 2'000'001) - 1'000'000;
    int64_t right_number =
            equivalent ? left_number : static_cast<int64_t>(random() % 2'000'001) - 1'000'000;
    if (!equivalent && right_number == left_number) {
        ++right_number;
    }
    if (mode == 0) {
        return {equivalent_integer(left_number, random()),
                equivalent_integer(right_number, random())};
    }
    if (mode == 1) {
        int64_t left_unscaled = left_number * 10 + 3;
        int64_t right_unscaled = equivalent ? left_unscaled : right_number * 10 + 7;
        return {decimal_value(left_unscaled * 100, 5, 16), decimal_value(right_unscaled, 3, 8)};
    }
    if (mode == 2) {
        const std::string left = "value_" + std::to_string(left_number);
        const std::string right = equivalent ? left : "value_" + std::to_string(right_number);
        return {string_value(left, (random() & 1U) != 0),
                string_value(right, (random() & 1U) != 0)};
    }
    if (mode == 3) {
        const OwnedValue left_first = equivalent_integer(left_number, random());
        const OwnedValue right_first = equivalent_integer(right_number, random());
        const OwnedValue left_second = equivalent_integer(left_number + 1, random());
        const OwnedValue right_second = equivalent_integer(right_number + 1, random());
        return {array_value({left_first.value, left_second.value}),
                array_value({right_first.value, right_second.value})};
    }

    const OwnedValue left_first = equivalent_integer(left_number, random());
    const OwnedValue right_first = equivalent_integer(right_number, random());
    const OwnedValue left_second = equivalent_integer(left_number + 1, random());
    const OwnedValue right_second = equivalent_integer(right_number + 1, random());
    return {object_value({"a", "b"}, true, {0, 1}, {left_first.value, left_second.value}, {0, 1}),
            object_value({"b", "a", "unused"}, false, {1, 0},
                         {right_first.value, right_second.value}, {1, 0})};
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertion macros inflate it.
TEST(VariantCanonicalTest, RandomizedEqualsHashAndArenaProperties) {
    std::mt19937_64 random(0xC0FFEE123456789ULL);
    uint32_t unequal_pairs = 0;
    uint32_t observed_all_family_collisions = 0;
    for (uint32_t index = 0; index < 10'000; ++index) {
        SCOPED_TRACE(index);
        const bool expected_equal = index % 2 == 0;
        auto [left, right] = random_pair(random, index, expected_equal);
        const bool equal = canonical_equals(left.ref(), right.ref());
        EXPECT_EQ(equal, expected_equal);

        const std::string left_arena = arena(left.ref());
        const std::string right_arena = arena(right.ref());
        EXPECT_EQ(left_arena == right_arena, equal);

        const Hashes left_hashes = hashes(left.ref());
        const Hashes right_hashes = hashes(right.ref());
        if (equal) {
            EXPECT_EQ(left_hashes.sip, right_hashes.sip);
            EXPECT_EQ(left_hashes.xx, right_hashes.xx);
            EXPECT_EQ(left_hashes.crc, right_hashes.crc);
            EXPECT_EQ(left_hashes.crc32c, right_hashes.crc32c);
        } else {
            ++unequal_pairs;
            if (left_hashes.sip == right_hashes.sip && left_hashes.xx == right_hashes.xx &&
                left_hashes.crc == right_hashes.crc && left_hashes.crc32c == right_hashes.crc32c) {
                ++observed_all_family_collisions;
            }
        }
    }
    EXPECT_EQ(unequal_pairs, 5'000);
    // This is an observation for the fixed sample, not a claim that finite hashes are injective.
    EXPECT_EQ(observed_all_family_collisions, 0);
}

} // namespace
} // namespace doris
