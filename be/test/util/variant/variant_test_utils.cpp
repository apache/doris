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

#include "variant_test_utils.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "common/exception.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "util/utf8_check.h"

namespace doris {
namespace {

constexpr unsigned __int128 max_decimal38() {
    unsigned __int128 value = 1;
    for (uint8_t digit = 0; digit < 38; ++digit) {
        value *= 10;
    }
    return value - 1;
}

constexpr unsigned __int128 MAX_DECIMAL38 = max_decimal38();
constexpr unsigned __int128 DECIMAL4_MAX = 999'999'999;
constexpr unsigned __int128 DECIMAL8_MAX = 999'999'999'999'999'999;

[[noreturn]] void fail(const char* reason) {
    throw Exception(ErrorCode::CORRUPTION, "Non-canonical Variant encoding: {}", reason);
}

void require_bytes(size_t available, size_t required) {
    if (available < required) {
        fail("truncated bytes");
    }
}

uint64_t read_unsigned(const char* data, uint8_t width) {
    uint64_t result = 0;
    for (uint8_t byte = 0; byte < width; ++byte) {
        result |= static_cast<uint64_t>(static_cast<uint8_t>(data[byte])) << (byte * 8);
    }
    return result;
}

unsigned __int128 magnitude(__int128 value) {
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    return value < 0 ? ~unsigned_value + 1 : unsigned_value;
}

uint8_t minimum_unsigned_width(uint64_t value) {
    if (value <= std::numeric_limits<uint8_t>::max()) {
        return 1;
    }
    if (value <= std::numeric_limits<uint16_t>::max()) {
        return 2;
    }
    if (value <= 0xFFFFFFU) {
        return 3;
    }
    return 4;
}

VariantPrimitiveId minimum_integer_id(int64_t value) {
    if (value >= std::numeric_limits<int8_t>::min() &&
        value <= std::numeric_limits<int8_t>::max()) {
        return VariantPrimitiveId::INT8;
    }
    if (value >= std::numeric_limits<int16_t>::min() &&
        value <= std::numeric_limits<int16_t>::max()) {
        return VariantPrimitiveId::INT16;
    }
    if (value >= std::numeric_limits<int32_t>::min() &&
        value <= std::numeric_limits<int32_t>::max()) {
        return VariantPrimitiveId::INT32;
    }
    return VariantPrimitiveId::INT64;
}

uint8_t minimum_decimal_width(__int128 value) {
    const unsigned __int128 absolute = magnitude(value);
    if (absolute <= DECIMAL4_MAX) {
        return 4;
    }
    if (absolute <= DECIMAL8_MAX) {
        return 8;
    }
    return 16;
}

void require_valid_utf8(StringRef value, const char* description) {
    if (value.size != 0 && !validate_utf8_naive(value.data, value.size)) {
        fail(description);
    }
}

bool unsigned_bytes_less(StringRef left, StringRef right) {
    return std::ranges::lexicographical_compare(left, right, [](char left_byte, char right_byte) {
        return static_cast<uint8_t>(left_byte) < static_cast<uint8_t>(right_byte);
    });
}

void validate_node(VariantRef value, std::vector<bool>& referenced_keys);

void validate_primitive(VariantRef value) {
    const VariantPrimitiveId id = value.primitive_id();
    switch (id) {
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        if (id != minimum_integer_id(value.get_int())) {
            fail("integer does not use its minimum width");
        }
        return;
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16: {
        const VariantDecimal decimal = value.get_decimal();
        if (magnitude(decimal.unscaled) > MAX_DECIMAL38) {
            fail("decimal exceeds precision 38");
        }
        if (decimal.width < minimum_decimal_width(decimal.unscaled)) {
            fail("decimal width is below its implied precision");
        }
        return;
    }
    case VariantPrimitiveId::STRING: {
        const StringRef string = value.get_string();
        if (string.size <= VARIANT_MAX_SHORT_STRING_SIZE) {
            fail("long string form used for a short string");
        }
        require_valid_utf8(string, "string is not valid UTF-8");
        return;
    }
    case VariantPrimitiveId::NULL_VALUE:
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
    case VariantPrimitiveId::DOUBLE:
    case VariantPrimitiveId::DATE:
    case VariantPrimitiveId::TIMESTAMP_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
    case VariantPrimitiveId::FLOAT:
    case VariantPrimitiveId::BINARY:
    case VariantPrimitiveId::TIME_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
    case VariantPrimitiveId::UUID:
        return;
    }
    fail("unknown primitive id");
}

// NOLINTNEXTLINE(readability-function-size): Kept contiguous to mirror the encoded container layout.
void validate_container(VariantRef value, std::vector<bool>& referenced_keys, bool is_object) {
    const uint8_t value_header =
            static_cast<uint8_t>(value.value.data[0]) >> VARIANT_VALUE_HEADER_SHIFT;
    if ((is_object && (value_header & 0x20U) != 0) || (!is_object && (value_header & 0x38U) != 0)) {
        fail("container reserved bits are nonzero");
    }

    const auto offset_width = static_cast<uint8_t>((value_header & 0x03U) + 1);
    const uint8_t id_width =
            is_object ? static_cast<uint8_t>(((value_header >> 2) & 0x03U) + 1) : 0;
    const bool is_large = (value_header &
                           (is_object ? VARIANT_OBJECT_LARGE_MASK : VARIANT_ARRAY_LARGE_MASK)) != 0;
    const uint8_t count_width = is_large ? sizeof(uint32_t) : sizeof(uint8_t);
    require_bytes(value.value.size - 1, count_width);
    const auto count = static_cast<uint32_t>(read_unsigned(value.value.data + 1, count_width));
    if (is_large != (count > std::numeric_limits<uint8_t>::max())) {
        fail("container count does not use its minimum form");
    }

    size_t position = 1 + count_width;
    if (is_object) {
        if (count > (value.value.size - position) / id_width) {
            fail("truncated object id table");
        }
    }
    const size_t ids_offset = position;
    position += static_cast<size_t>(count) * id_width;
    if (static_cast<uint64_t>(count) + 1 > (value.value.size - position) / offset_width) {
        fail("truncated container offset table");
    }
    const size_t offsets_offset = position;
    position += (static_cast<size_t>(count) + 1) * offset_width;
    const size_t values_offset = position;

    uint32_t maximum_id = 0;
    uint32_t previous_id = 0;
    for (uint32_t index = 0; index < count && is_object; ++index) {
        const auto id = static_cast<uint32_t>(read_unsigned(
                value.value.data + ids_offset + static_cast<size_t>(index) * id_width, id_width));
        if (id >= referenced_keys.size()) {
            fail("object field id is outside metadata");
        }
        if (index != 0 && id <= previous_id) {
            fail("object field ids are not strictly increasing");
        }
        referenced_keys[id] = true;
        previous_id = id;
        maximum_id = id;
    }
    if (is_object && id_width != minimum_unsigned_width(maximum_id)) {
        fail("object field ids do not use their minimum width");
    }

    const auto final_offset = static_cast<uint32_t>(read_unsigned(
            value.value.data + offsets_offset + static_cast<size_t>(count) * offset_width,
            offset_width));
    if (offset_width != minimum_unsigned_width(final_offset)) {
        fail("container offsets do not use their minimum width");
    }
    if (final_offset != value.value.size - values_offset) {
        fail("container final offset does not end at the value boundary");
    }

    uint32_t expected_offset = 0;
    for (uint32_t index = 0; index < count; ++index) {
        const auto offset = static_cast<uint32_t>(read_unsigned(
                value.value.data + offsets_offset + static_cast<size_t>(index) * offset_width,
                offset_width));
        const auto next_offset = static_cast<uint32_t>(read_unsigned(
                value.value.data + offsets_offset + (static_cast<size_t>(index) + 1) * offset_width,
                offset_width));
        if (offset != expected_offset || next_offset <= offset || next_offset > final_offset) {
            fail("container offsets are not tightly increasing");
        }
        VariantRef child {
                .metadata = value.metadata,
                .value = {value.value.data + values_offset + offset, next_offset - offset}};
        validate_node(child, referenced_keys);
        expected_offset = next_offset;
    }
    if (expected_offset != final_offset) {
        fail("container values contain an unreferenced gap");
    }
}

void validate_node(VariantRef value, std::vector<bool>& referenced_keys) {
    if (value.value_size() != value.value.size) {
        fail("value contains trailing bytes");
    }
    switch (value.basic_type()) {
    case VariantBasicType::PRIMITIVE:
        validate_primitive(value);
        return;
    case VariantBasicType::SHORT_STRING:
        require_valid_utf8(value.get_string(), "short string is not valid UTF-8");
        return;
    case VariantBasicType::OBJECT:
        validate_container(value, referenced_keys, true);
        return;
    case VariantBasicType::ARRAY:
        validate_container(value, referenced_keys, false);
        return;
    }
    fail("unknown basic type");
}

} // namespace

void validate_canonical(VariantMetadataRef metadata, std::span<const VariantRef> rows) {
    metadata.validate();
    if (metadata.version() != VARIANT_ENCODING_VERSION || !metadata.sorted_strings()) {
        fail("metadata must use version 1 and a sorted dictionary");
    }
    if ((static_cast<uint8_t>(metadata.data[0]) & 0x20U) != 0) {
        fail("metadata reserved bit is nonzero");
    }

    const uint32_t key_count = metadata.dict_size();
    uint64_t strings_size = 0;
    StringRef previous;
    for (uint32_t id = 0; id < key_count; ++id) {
        const StringRef key = metadata.key_at(id);
        require_valid_utf8(key, "metadata key is not valid UTF-8");
        strings_size += key.size;
        if (strings_size > std::numeric_limits<uint32_t>::max()) {
            fail("metadata dictionary strings exceed uint32");
        }
        if (id != 0 && !unsigned_bytes_less(previous, key)) {
            fail("metadata dictionary is not unsigned-byte sorted and unique");
        }
        previous = key;
    }
    if (metadata.offset_size() !=
        minimum_unsigned_width(std::max<uint64_t>(key_count, strings_size))) {
        fail("metadata does not use its minimum offset width");
    }

    std::vector<bool> referenced_keys(key_count, false);
    for (VariantRef row : rows) {
        if (row.metadata.size != metadata.size ||
            std::memcmp(row.metadata.data, metadata.data, metadata.size) != 0) {
            fail("rows do not share the encoding unit metadata");
        }
        validate_node(row, referenced_keys);
    }
    if (std::ranges::find(referenced_keys, false) != referenced_keys.end()) {
        fail("metadata contains an unreferenced key");
    }
}

void validate_canonical(VariantRef row) {
    const std::array<VariantRef, 1> rows {row};
    validate_canonical(row.metadata, rows);
}

} // namespace doris
