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

#include "core/value/variant/variant_scalar.h"

#include <bit>
#include <cstring>
#include <limits>

#include "common/exception.h"
#include "util/utf8_check.h"

namespace doris {
namespace {

constexpr uint8_t integer_width(VariantPrimitiveId id) noexcept {
    switch (id) {
    case VariantPrimitiveId::INT8:
        return 1;
    case VariantPrimitiveId::INT16:
        return 2;
    case VariantPrimitiveId::INT32:
        return 4;
    case VariantPrimitiveId::INT64:
        return 8;
    default:
        return 0;
    }
}

constexpr uint8_t decimal_width(VariantPrimitiveId id) noexcept {
    switch (id) {
    case VariantPrimitiveId::DECIMAL4:
        return 4;
    case VariantPrimitiveId::DECIMAL8:
        return 8;
    case VariantPrimitiveId::DECIMAL16:
        return 16;
    default:
        return 0;
    }
}

uint8_t minimum_integer_width(int64_t value) noexcept {
    if (value >= std::numeric_limits<int8_t>::min() &&
        value <= std::numeric_limits<int8_t>::max()) {
        return 1;
    }
    if (value >= std::numeric_limits<int16_t>::min() &&
        value <= std::numeric_limits<int16_t>::max()) {
        return 2;
    }
    if (value >= std::numeric_limits<int32_t>::min() &&
        value <= std::numeric_limits<int32_t>::max()) {
        return 4;
    }
    return 8;
}

VariantPrimitiveId integer_id(uint8_t width) {
    switch (width) {
    case 1:
        return VariantPrimitiveId::INT8;
    case 2:
        return VariantPrimitiveId::INT16;
    case 4:
        return VariantPrimitiveId::INT32;
    case 8:
        return VariantPrimitiveId::INT64;
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant integer width {} must be one of 1, 2, 4, or 8", width);
    }
}

VariantPrimitiveId decimal_id(uint8_t width) {
    switch (width) {
    case 4:
        return VariantPrimitiveId::DECIMAL4;
    case 8:
        return VariantPrimitiveId::DECIMAL8;
    case 16:
        return VariantPrimitiveId::DECIMAL16;
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal width {} must be one of 4, 8, or 16", width);
    }
}

void write_unsigned(char*& output, unsigned __int128 value, uint8_t width) noexcept {
    for (uint8_t byte = 0; byte < width; ++byte) {
        *output++ = static_cast<char>(value >> (byte * 8));
    }
}

void write_primitive_header(char*& output, VariantPrimitiveId id) noexcept {
    *output++ = static_cast<char>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT);
}

void copy_bytes(StringRef bytes, char*& output) noexcept {
    if (bytes.size != 0) {
        std::memcpy(output, bytes.data, bytes.size);
        output += bytes.size;
    }
}

} // namespace

VariantScalarRef VariantScalarRef::null_value() noexcept {
    return VariantScalarRef(VariantPrimitiveId::NULL_VALUE);
}

VariantScalarRef VariantScalarRef::boolean(bool value) noexcept {
    return VariantScalarRef(value ? VariantPrimitiveId::TRUE_VALUE
                                  : VariantPrimitiveId::FALSE_VALUE);
}

VariantScalarRef VariantScalarRef::integer(int64_t value, uint8_t width) {
    const uint8_t required_width = minimum_integer_width(value);
    if (width == 0) {
        width = required_width;
    } else if (integer_width(integer_id(width)) < required_width) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant integer value does not fit requested width {}", width);
    }
    VariantScalarRef result(integer_id(width));
    result._signed_value = value;
    return result;
}

VariantScalarRef VariantScalarRef::decimal(__int128 unscaled, uint8_t scale, uint8_t width) {
    if (scale > 38) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant decimal scale {} is outside [0, 38]",
                        scale);
    }
    const unsigned __int128 absolute = variant_unsigned_magnitude(unscaled);
    if (absolute > VARIANT_DECIMAL16_MAX) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal unscaled value exceeds precision 38");
    }
    if (width == 0) {
        width = absolute <= VARIANT_DECIMAL4_MAX ? 4 : (absolute <= VARIANT_DECIMAL8_MAX ? 8 : 16);
    }
    const VariantPrimitiveId id = decimal_id(width);
    const unsigned __int128 width_max =
            width == 4 ? VARIANT_DECIMAL4_MAX
                       : (width == 8 ? VARIANT_DECIMAL8_MAX : VARIANT_DECIMAL16_MAX);
    if (absolute > width_max) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal unscaled value exceeds precision for width {}", width);
    }
    VariantScalarRef result(id);
    result._signed_value = unscaled;
    result._scale = scale;
    return result;
}

VariantScalarRef VariantScalarRef::float32(float value) noexcept {
    VariantScalarRef result(VariantPrimitiveId::FLOAT);
    result._floating_bits = std::bit_cast<uint32_t>(value);
    return result;
}

VariantScalarRef VariantScalarRef::float64(double value) noexcept {
    VariantScalarRef result(VariantPrimitiveId::DOUBLE);
    result._floating_bits = std::bit_cast<uint64_t>(value);
    return result;
}

VariantScalarRef VariantScalarRef::string(StringRef value) {
    if (value.data == nullptr && value.size != 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant string has a null data pointer");
    }
    if (value.size != 0 && !validate_utf8(value.data, value.size)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant string is not valid UTF-8");
    }
    if (value.size > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant string exceeds the uint32 byte limit");
    }
    VariantScalarRef result(VariantPrimitiveId::STRING);
    result._bytes = value;
    return result;
}

VariantScalarRef VariantScalarRef::binary(StringRef value) {
    if (value.data == nullptr && value.size != 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant binary has a null data pointer");
    }
    if (value.size > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant binary exceeds the uint32 byte limit");
    }
    VariantScalarRef result(VariantPrimitiveId::BINARY);
    result._bytes = value;
    return result;
}

VariantScalarRef VariantScalarRef::date(int32_t days_since_epoch) noexcept {
    VariantScalarRef result(VariantPrimitiveId::DATE);
    result._signed_value = days_since_epoch;
    return result;
}

VariantScalarRef VariantScalarRef::timestamp_micros(int64_t value, bool utc_adjusted) noexcept {
    VariantScalarRef result(utc_adjusted ? VariantPrimitiveId::TIMESTAMP_MICROS
                                         : VariantPrimitiveId::TIMESTAMP_NTZ_MICROS);
    result._signed_value = value;
    return result;
}

VariantScalarRef VariantScalarRef::timestamp_nanos(int64_t value, bool utc_adjusted) noexcept {
    VariantScalarRef result(utc_adjusted ? VariantPrimitiveId::TIMESTAMP_NANOS
                                         : VariantPrimitiveId::TIMESTAMP_NTZ_NANOS);
    result._signed_value = value;
    return result;
}

VariantScalarRef VariantScalarRef::time_ntz_micros(int64_t value) noexcept {
    VariantScalarRef result(VariantPrimitiveId::TIME_NTZ_MICROS);
    result._signed_value = value;
    return result;
}

VariantScalarRef VariantScalarRef::uuid(const std::array<uint8_t, 16>& value) noexcept {
    VariantScalarRef result(VariantPrimitiveId::UUID);
    result._uuid = value;
    return result;
}

size_t VariantScalarRef::encoded_size() const noexcept {
    switch (_physical_id) {
    case VariantPrimitiveId::NULL_VALUE:
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return 1;
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        return 1 + integer_width(_physical_id);
    case VariantPrimitiveId::FLOAT:
        return 1 + sizeof(float);
    case VariantPrimitiveId::DOUBLE:
        return 1 + sizeof(double);
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16:
        return 2 + decimal_width(_physical_id);
    case VariantPrimitiveId::DATE:
        return 1 + sizeof(int32_t);
    case VariantPrimitiveId::TIMESTAMP_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
    case VariantPrimitiveId::TIME_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        return 1 + sizeof(int64_t);
    case VariantPrimitiveId::BINARY:
        return 1 + sizeof(uint32_t) + _bytes.size;
    case VariantPrimitiveId::STRING:
        return _bytes.size <= VARIANT_MAX_SHORT_STRING_SIZE ? 1 + _bytes.size
                                                            : 1 + sizeof(uint32_t) + _bytes.size;
    case VariantPrimitiveId::UUID:
        return 1 + _uuid.size();
    }
    __builtin_unreachable();
}

void VariantScalarRef::write_physical(char* destination, size_t capacity) const {
    const size_t required = encoded_size();
    if (destination == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant scalar destination must not be null");
    }
    if (capacity < required) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant scalar destination capacity {} is smaller than required {}",
                        capacity, required);
    }

    char* output = destination;
    if (_physical_id == VariantPrimitiveId::STRING &&
        _bytes.size <= VARIANT_MAX_SHORT_STRING_SIZE) {
        *output++ = static_cast<char>((_bytes.size << VARIANT_VALUE_HEADER_SHIFT) |
                                      static_cast<uint8_t>(VariantBasicType::SHORT_STRING));
        copy_bytes(_bytes, output);
        DCHECK_EQ(output, destination + required);
        return;
    }

    write_primitive_header(output, _physical_id);
    switch (_physical_id) {
    case VariantPrimitiveId::NULL_VALUE:
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        break;
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        write_unsigned(output, static_cast<unsigned __int128>(_signed_value),
                       integer_width(_physical_id));
        break;
    case VariantPrimitiveId::FLOAT:
        write_unsigned(output, _floating_bits, sizeof(float));
        break;
    case VariantPrimitiveId::DOUBLE:
        write_unsigned(output, _floating_bits, sizeof(double));
        break;
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16:
        *output++ = static_cast<char>(_scale);
        write_unsigned(output, static_cast<unsigned __int128>(_signed_value),
                       decimal_width(_physical_id));
        break;
    case VariantPrimitiveId::DATE:
        write_unsigned(output, static_cast<unsigned __int128>(_signed_value), sizeof(int32_t));
        break;
    case VariantPrimitiveId::TIMESTAMP_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
    case VariantPrimitiveId::TIME_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        write_unsigned(output, static_cast<unsigned __int128>(_signed_value), sizeof(int64_t));
        break;
    case VariantPrimitiveId::BINARY:
    case VariantPrimitiveId::STRING:
        write_unsigned(output, _bytes.size, sizeof(uint32_t));
        copy_bytes(_bytes, output);
        break;
    case VariantPrimitiveId::UUID:
        std::memcpy(output, _uuid.data(), _uuid.size());
        output += _uuid.size();
        break;
    }
    DCHECK_EQ(output, destination + required);
}

} // namespace doris
