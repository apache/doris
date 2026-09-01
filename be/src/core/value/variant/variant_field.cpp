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

#include <algorithm>
#include <cstring>
#include <limits>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/field.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "core/value/variant/variant_scalar.h"
#include "util/json/path_in_data.h"
#include "util/utf8_check.h"

namespace doris {
namespace {

constexpr size_t METADATA_SIZE_PREFIX = sizeof(uint32_t);

uint32_t read_u32(const char* data) noexcept {
    uint32_t result = 0;
    for (uint8_t byte = 0; byte < METADATA_SIZE_PREFIX; ++byte) {
        result |= static_cast<uint32_t>(static_cast<uint8_t>(data[byte])) << (byte * 8);
    }
    return result;
}

void write_u32(char* destination, uint32_t value) noexcept {
    for (uint8_t byte = 0; byte < METADATA_SIZE_PREFIX; ++byte) {
        destination[byte] = static_cast<char>(value >> (byte * 8));
    }
}

void require_non_null(StringRef bytes, const char* description) {
    if (bytes.size != 0 && bytes.data == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "VariantField {} has a null pointer for {} bytes", description, bytes.size);
    }
}

void require_valid_utf8(StringRef value, const char* description) {
    if (value.size != 0 && !validate_utf8(value.data, value.size)) {
        throw Exception(ErrorCode::CORRUPTION, "VariantField {} is not valid UTF-8", description);
    }
}

void require_decimal_in_range(const VariantDecimal& decimal) {
    unsigned __int128 maximum = VARIANT_DECIMAL16_MAX;
    if (decimal.width == 4) {
        maximum = VARIANT_DECIMAL4_MAX;
    } else if (decimal.width == 8) {
        maximum = VARIANT_DECIMAL8_MAX;
    }
    if (variant_unsigned_magnitude(decimal.unscaled) > maximum) {
        throw Exception(ErrorCode::CORRUPTION,
                        "VariantField decimal unscaled value exceeds precision for width {}",
                        decimal.width);
    }
}

void require_valid_primitive(VariantRef value) {
    switch (value.primitive_id()) {
    case VariantPrimitiveId::NULL_VALUE:
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return;
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        static_cast<void>(value.get_int());
        return;
    case VariantPrimitiveId::DOUBLE:
        static_cast<void>(value.get_double());
        return;
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16:
        require_decimal_in_range(value.get_decimal());
        return;
    case VariantPrimitiveId::DATE:
        static_cast<void>(value.get_date());
        return;
    case VariantPrimitiveId::TIMESTAMP_MICROS:
        static_cast<void>(value.get_timestamp_micros());
        return;
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
        static_cast<void>(value.get_timestamp_ntz_micros());
        return;
    case VariantPrimitiveId::FLOAT:
        static_cast<void>(value.get_float());
        return;
    case VariantPrimitiveId::BINARY:
        static_cast<void>(value.get_binary());
        return;
    case VariantPrimitiveId::STRING:
        require_valid_utf8(value.get_string(), "string");
        return;
    case VariantPrimitiveId::TIME_NTZ_MICROS:
        static_cast<void>(value.get_time_ntz_micros());
        return;
    case VariantPrimitiveId::TIMESTAMP_NANOS:
        static_cast<void>(value.get_timestamp_nanos());
        return;
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        static_cast<void>(value.get_timestamp_ntz_nanos());
        return;
    case VariantPrimitiveId::UUID:
        static_cast<void>(value.get_uuid());
        return;
    }
    throw Exception(ErrorCode::CORRUPTION, "VariantField contains an unknown primitive type");
}

void require_exact_value(VariantRef value, uint32_t depth);

struct ObjectValueSpan {
    size_t offset;
    size_t size;
};

size_t object_values_offset(VariantRef value, uint32_t count) {
    const uint8_t value_header =
            static_cast<uint8_t>(value.value.data[0]) >> VARIANT_VALUE_HEADER_SHIFT;
    const auto offset_width =
            static_cast<uint8_t>(((value_header >> VARIANT_OBJECT_OFFSET_SIZE_SHIFT) & 0x03U) + 1);
    const auto id_width =
            static_cast<uint8_t>(((value_header >> VARIANT_OBJECT_ID_SIZE_SHIFT) & 0x03U) + 1);
    const size_t count_width =
            (value_header & VARIANT_OBJECT_LARGE_MASK) != 0 ? sizeof(uint32_t) : sizeof(uint8_t);
    return 1 + count_width + static_cast<size_t>(count) * id_width +
           (static_cast<size_t>(count) + 1) * offset_width;
}

void require_valid_object(VariantRef value, uint32_t depth) {
    const uint32_t count = value.num_elements();
    const size_t values_offset = object_values_offset(value, count);
    const char* values_begin = value.value.data + values_offset;
    const size_t values_size = value.value.size - values_offset;
    std::vector<ObjectValueSpan> spans;
    spans.reserve(count);

    StringRef previous_key;
    for (uint32_t index = 0; index < count; ++index) {
        uint32_t field_id = 0;
        const VariantRef child = value.object_value_at(index, &field_id);
        const StringRef key = value.metadata.key_at(field_id);
        if (index != 0 && previous_key.compare(key) >= 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "VariantField object keys are not strictly ordered at field {}", index);
        }
        require_exact_value(child, depth + 1);
        spans.push_back({.offset = static_cast<size_t>(child.value.data - values_begin),
                         .size = child.value.size});
        previous_key = key;
    }

    std::ranges::sort(spans, [](const ObjectValueSpan& left, const ObjectValueSpan& right) {
        return left.offset < right.offset;
    });
    size_t consumed = 0;
    for (const ObjectValueSpan& span : spans) {
        if (span.offset < consumed) {
            throw Exception(ErrorCode::CORRUPTION,
                            "VariantField object value spans overlap or reuse an offset");
        }
        if (span.offset > consumed) {
            throw Exception(ErrorCode::CORRUPTION,
                            "VariantField object values contain an unreferenced gap");
        }
        consumed += span.size;
    }
    if (consumed != values_size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "VariantField object values contain unreferenced trailing bytes");
    }
}

void require_valid_array(VariantRef value, uint32_t depth) {
    const uint32_t count = value.num_elements();
    for (uint32_t index = 0; index < count; ++index) {
        require_exact_value(value.array_at(index), depth + 1);
    }
}

void require_exact_value(VariantRef value, uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::CORRUPTION,
                        "VariantField value exceeds maximum nesting depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
    const size_t encoded_size = value.value_size();
    if (encoded_size != value.value.size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "VariantField value has {} trailing bytes after its {} byte root",
                        value.value.size - encoded_size, encoded_size);
    }

    switch (value.basic_type()) {
    case VariantBasicType::PRIMITIVE:
        require_valid_primitive(value);
        return;
    case VariantBasicType::SHORT_STRING:
        require_valid_utf8(value.get_string(), "short string");
        return;
    case VariantBasicType::OBJECT:
        require_valid_object(value, depth);
        return;
    case VariantBasicType::ARRAY:
        require_valid_array(value, depth);
        return;
    }
    throw Exception(ErrorCode::CORRUPTION, "VariantField contains an unknown value type");
}

struct RowSlices {
    VariantMetadataRef metadata;
    VariantRef value;
};

RowSlices split_untrusted(StringRef bytes) {
    require_non_null(bytes, "encoded row");
    if (bytes.size < METADATA_SIZE_PREFIX) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Truncated VariantField metadata-size prefix: need {} bytes, have {}",
                        METADATA_SIZE_PREFIX, bytes.size);
    }
    const uint32_t metadata_size = read_u32(bytes.data);
    const size_t payload_size = bytes.size - METADATA_SIZE_PREFIX;
    if (metadata_size > payload_size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "VariantField metadata size {} exceeds {} payload bytes", metadata_size,
                        payload_size);
    }
    VariantMetadataRef metadata {.data = bytes.data + METADATA_SIZE_PREFIX, .size = metadata_size};
    VariantRef value {.metadata = metadata,
                      .value = {metadata.data + metadata.size, payload_size - metadata.size}};
    return {.metadata = metadata, .value = value};
}

std::unique_ptr<char[]> copy_bytes(StringRef bytes) {
    auto result = std::make_unique<char[]>(bytes.size);
    std::memcpy(result.get(), bytes.data, bytes.size);
    return result;
}

[[noreturn]] void throw_comparison_not_supported() {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Comparison between VariantField values is not supported");
}

} // namespace

void validate_variant_metadata(VariantMetadataRef metadata) {
    require_non_null({metadata.data, metadata.size}, "metadata");
    metadata.validate();
    const uint32_t key_count = metadata.dict_size();
    for (uint32_t id = 0; id < key_count; ++id) {
        require_valid_utf8(metadata.key_at(id), "metadata key");
    }
}

void validate_variant_payload(VariantRef value) {
    require_non_null(value.value, "value");
    require_exact_value(value, 0);
}

VariantField::VariantField(std::unique_ptr<char[]> data, size_t size) noexcept
        : _data(std::move(data)), _size(size) {}

VariantField::VariantField() noexcept = default;

VariantField::~VariantField() = default;

VariantField::VariantField(VariantMap legacy)
        : _legacy_representation(true), _legacy(std::make_unique<VariantMap>(std::move(legacy))) {}

bool VariantField::is_legacy() const noexcept {
    return _legacy_representation;
}

// NOLINTNEXTLINE(readability-make-member-function-const) -- non-const overload exposes a writable V1 map.
VariantMap& VariantField::legacy_map() {
    if (!_legacy_representation) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Encoded VariantField does not contain a legacy VariantMap");
    }
    if (_legacy == nullptr) {
        _legacy = std::make_unique<VariantMap>();
    }
    return *_legacy;
}

const VariantMap& VariantField::legacy_map() const {
    if (!_legacy_representation) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Encoded VariantField does not contain a legacy VariantMap");
    }
    if (_legacy == nullptr) {
        static const VariantMap EMPTY;
        return EMPTY;
    }
    return *_legacy;
}

VariantField::VariantField(const VariantField& other)
        : _legacy_representation(other._legacy_representation), _size(other._size) {
    if (_legacy_representation) {
        _legacy = other._legacy != nullptr ? std::make_unique<VariantMap>(*other._legacy)
                                           : std::make_unique<VariantMap>();
    }
    if (_size != 0) {
        _data = std::make_unique<char[]>(_size);
        std::memcpy(_data.get(), other._data.get(), _size);
    }
}

VariantField::VariantField(VariantField&& other) noexcept
        : _legacy_representation(other._legacy_representation),
          _legacy(std::move(other._legacy)),
          _data(std::move(other._data)),
          _size(std::exchange(other._size, 0)) {}

VariantField& VariantField::operator=(const VariantField& other) {
    VariantField copy(other);
    swap(copy);
    return *this;
}

VariantField& VariantField::operator=(VariantField&& other) noexcept {
    if (this != &other) {
        _legacy_representation = other._legacy_representation;
        _legacy = std::move(other._legacy);
        _data = std::move(other._data);
        _size = std::exchange(other._size, 0);
    }
    return *this;
}

VariantField VariantField::from_ref(VariantRef value) {
    if (value.metadata.size > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "VariantField metadata size {} exceeds uint32 limit", value.metadata.size);
    }
    if (value.metadata.size > std::numeric_limits<size_t>::max() - METADATA_SIZE_PREFIX) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "VariantField metadata size exceeds the addressable row size");
    }
    const size_t value_offset = METADATA_SIZE_PREFIX + value.metadata.size;
    if (value.value.size > std::numeric_limits<size_t>::max() - value_offset) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "VariantField value size exceeds the addressable row size");
    }

    validate_variant_metadata(value.metadata);
    validate_variant_payload(value);
    const size_t total_size = value_offset + value.value.size;
    auto data = std::make_unique<char[]>(total_size);
    write_u32(data.get(), static_cast<uint32_t>(value.metadata.size));
    std::memcpy(data.get() + METADATA_SIZE_PREFIX, value.metadata.data, value.metadata.size);
    std::memcpy(data.get() + value_offset, value.value.data, value.value.size);
    return {std::move(data), total_size};
}

VariantField VariantField::from_bytes(StringRef bytes) {
    const RowSlices slices = split_untrusted(bytes);
    validate_variant_metadata(slices.metadata);
    validate_variant_payload(slices.value);
    return {copy_bytes(bytes), bytes.size};
}

VariantField VariantField::from_scalar(const VariantScalarRef& scalar) {
    const size_t metadata_size = VARIANT_EMPTY_METADATA.size();
    const size_t value_offset = METADATA_SIZE_PREFIX + metadata_size;
    const size_t value_size = scalar.encoded_size();
    auto data = std::make_unique<char[]>(value_offset + value_size);
    write_u32(data.get(), static_cast<uint32_t>(metadata_size));
    std::memcpy(data.get() + METADATA_SIZE_PREFIX, VARIANT_EMPTY_METADATA.data(), metadata_size);
    scalar.write_physical(data.get() + value_offset, value_size);
    return VariantField(std::move(data), value_offset + value_size);
}

VariantRef VariantField::ref() const {
    if (_legacy_representation) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Legacy VariantField does not contain an encoded Variant row");
    }
    if (_size == 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot reference an empty or moved-from VariantField");
    }
    DCHECK(_data != nullptr);
    DCHECK_GE(_size, METADATA_SIZE_PREFIX);
    const uint32_t metadata_size = read_u32(_data.get());
    DCHECK_LE(metadata_size, _size - METADATA_SIZE_PREFIX);
    VariantMetadataRef metadata {.data = _data.get() + METADATA_SIZE_PREFIX, .size = metadata_size};
    return {.metadata = metadata,
            .value = {metadata.data + metadata.size, _size - METADATA_SIZE_PREFIX - metadata.size}};
}

VariantMetadataRef VariantField::metadata() const {
    return ref().metadata;
}

StringRef VariantField::value() const {
    return ref().value;
}

StringRef VariantField::bytes() const noexcept {
    return {_data.get(), _size};
}

bool VariantField::operator<(const VariantField&) const {
    throw_comparison_not_supported();
}

bool VariantField::operator<=(const VariantField&) const {
    throw_comparison_not_supported();
}

bool VariantField::operator==(const VariantField&) const {
    throw_comparison_not_supported();
}

bool VariantField::operator!=(const VariantField&) const {
    throw_comparison_not_supported();
}

bool VariantField::operator>=(const VariantField&) const {
    throw_comparison_not_supported();
}

bool VariantField::operator>(const VariantField&) const {
    throw_comparison_not_supported();
}

void VariantField::swap(VariantField& other) noexcept {
    std::swap(_legacy_representation, other._legacy_representation);
    _legacy.swap(other._legacy);
    _data.swap(other._data);
    std::swap(_size, other._size);
}

} // namespace doris
