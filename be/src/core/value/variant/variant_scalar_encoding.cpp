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

#include "core/value/variant/variant_scalar_encoding.h"

#include <bit>
#include <cstring>
#include <limits>

#include "common/exception.h"
#include "core/value/variant/variant_encoding.h"
#include "util/utf8_check.h"

namespace doris {
namespace {

constexpr unsigned __int128 DECIMAL4_MAX = 999'999'999;
constexpr unsigned __int128 DECIMAL8_MAX = 999'999'999'999'999'999;

constexpr unsigned __int128 max_decimal38() {
    unsigned __int128 value = 1;
    for (uint8_t digit = 0; digit < 38; ++digit) {
        value *= 10;
    }
    return value - 1;
}

constexpr unsigned __int128 MAX_DECIMAL38 = max_decimal38();

unsigned __int128 magnitude(__int128 value) {
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    return value < 0 ? ~unsigned_value + 1 : unsigned_value;
}

constexpr uint8_t primitive_header(VariantPrimitiveId id) noexcept {
    return static_cast<uint8_t>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT);
}

constexpr VariantPrimitiveId boolean_primitive_id(bool value) noexcept {
    return value ? VariantPrimitiveId::TRUE_VALUE : VariantPrimitiveId::FALSE_VALUE;
}

struct IntegerEncoding {
    VariantPrimitiveId id;
    uint8_t width;
};

IntegerEncoding resolve_integer_encoding(int64_t value, uint8_t requested_width) {
    uint8_t width = requested_width;
    if (width == 0) {
        if (value >= std::numeric_limits<int8_t>::min() &&
            value <= std::numeric_limits<int8_t>::max()) {
            width = 1;
        } else if (value >= std::numeric_limits<int16_t>::min() &&
                   value <= std::numeric_limits<int16_t>::max()) {
            width = 2;
        } else if (value >= std::numeric_limits<int32_t>::min() &&
                   value <= std::numeric_limits<int32_t>::max()) {
            width = 4;
        } else {
            width = 8;
        }
    }

    VariantPrimitiveId id = VariantPrimitiveId::INT64;
    bool fits = true;
    if (width == 1) {
        id = VariantPrimitiveId::INT8;
        fits = value >= std::numeric_limits<int8_t>::min() &&
               value <= std::numeric_limits<int8_t>::max();
    } else if (width == 2) {
        id = VariantPrimitiveId::INT16;
        fits = value >= std::numeric_limits<int16_t>::min() &&
               value <= std::numeric_limits<int16_t>::max();
    } else if (width == 4) {
        id = VariantPrimitiveId::INT32;
        fits = value >= std::numeric_limits<int32_t>::min() &&
               value <= std::numeric_limits<int32_t>::max();
    } else if (width != 8) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant integer width {} must be one of 1, 2, 4, or 8", width);
    }
    if (!fits) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant integer value does not fit requested width {}", width);
    }
    return {.id = id, .width = width};
}

void write_unsigned(char*& output, uint64_t value, uint8_t width) noexcept {
    for (uint8_t byte = 0; byte < width; ++byte) {
        *output++ = static_cast<char>(value >> (byte * 8));
    }
}

void write_signed128(char*& output, __int128 value, uint8_t width) noexcept {
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    for (uint8_t byte = 0; byte < width; ++byte) {
        *output++ = static_cast<char>(unsigned_value >> (byte * 8));
    }
}

void validate_string_ref(StringRef value, const char* description) {
    if (value.data == nullptr && value.size != 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant {} has a null data pointer",
                        description);
    }
}

void validate_utf8_string_ref(StringRef value, const char* description) {
    validate_string_ref(value, description);
    if (value.size != 0 && !validate_utf8(value.data, value.size)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant {} is not valid UTF-8", description);
    }
}

} // namespace

VariantScalarEncodingPlan VariantScalarEncodingPlan::null_value() noexcept {
    VariantScalarEncodingPlan plan;
    plan._header = primitive_header(VariantPrimitiveId::NULL_VALUE);
    plan._encoded_size = 1;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::boolean(bool value) noexcept {
    VariantScalarEncodingPlan plan;
    plan._header = primitive_header(boolean_primitive_id(value));
    plan._encoded_size = 1;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::integer(int64_t value, uint8_t width) {
    const IntegerEncoding encoding = resolve_integer_encoding(value, width);

    VariantScalarEncodingPlan plan;
    plan._header = primitive_header(encoding.id);
    plan._unsigned_value = std::bit_cast<uint64_t>(value);
    plan._payload_width = encoding.width;
    plan._encoded_size = static_cast<size_t>(encoding.width) + 1;
    plan._kind = PayloadKind::UNSIGNED;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::float32(float value) noexcept {
    VariantScalarEncodingPlan plan;
    plan._header = static_cast<uint8_t>(VariantPrimitiveId::FLOAT) << VARIANT_VALUE_HEADER_SHIFT;
    plan._unsigned_value = std::bit_cast<uint32_t>(value);
    plan._payload_width = sizeof(float);
    plan._encoded_size = sizeof(float) + 1;
    plan._kind = PayloadKind::UNSIGNED;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::float64(double value) noexcept {
    VariantScalarEncodingPlan plan;
    plan._header = static_cast<uint8_t>(VariantPrimitiveId::DOUBLE) << VARIANT_VALUE_HEADER_SHIFT;
    plan._unsigned_value = std::bit_cast<uint64_t>(value);
    plan._payload_width = sizeof(double);
    plan._encoded_size = sizeof(double) + 1;
    plan._kind = PayloadKind::UNSIGNED;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::decimal(__int128 unscaled, uint8_t scale,
                                                             uint8_t width) {
    if (scale > 38) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant decimal scale {} is outside [0, 38]",
                        scale);
    }
    const unsigned __int128 absolute = magnitude(unscaled);
    if (absolute > MAX_DECIMAL38) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal unscaled value exceeds precision 38");
    }
    if (width == 0) {
        if (absolute <= DECIMAL4_MAX) {
            width = 4;
        } else if (absolute <= DECIMAL8_MAX) {
            width = 8;
        } else {
            width = 16;
        }
    }

    VariantPrimitiveId id = VariantPrimitiveId::DECIMAL16;
    unsigned __int128 width_max = MAX_DECIMAL38;
    if (width == 4) {
        id = VariantPrimitiveId::DECIMAL4;
        width_max = DECIMAL4_MAX;
    } else if (width == 8) {
        id = VariantPrimitiveId::DECIMAL8;
        width_max = DECIMAL8_MAX;
    } else if (width != 16) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal width {} must be one of 4, 8, or 16", width);
    }
    if (absolute > width_max) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal unscaled value exceeds precision for width {}", width);
    }

    VariantScalarEncodingPlan plan;
    plan._header = static_cast<uint8_t>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT);
    plan._signed_value = unscaled;
    plan._payload_width = width;
    plan._scale = scale;
    plan._encoded_size = static_cast<size_t>(width) + 2;
    plan._kind = PayloadKind::SIGNED;
    plan._has_scale = true;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::date(int32_t days_since_epoch) noexcept {
    VariantScalarEncodingPlan plan;
    plan._header = static_cast<uint8_t>(VariantPrimitiveId::DATE) << VARIANT_VALUE_HEADER_SHIFT;
    plan._unsigned_value = std::bit_cast<uint32_t>(days_since_epoch);
    plan._payload_width = sizeof(int32_t);
    plan._encoded_size = sizeof(int32_t) + 1;
    plan._kind = PayloadKind::UNSIGNED;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::timestamp_micros(int64_t value,
                                                                      bool utc_adjusted) noexcept {
    VariantScalarEncodingPlan plan;
    plan._header = static_cast<uint8_t>(
            static_cast<uint8_t>(utc_adjusted ? VariantPrimitiveId::TIMESTAMP_MICROS
                                              : VariantPrimitiveId::TIMESTAMP_NTZ_MICROS)
            << VARIANT_VALUE_HEADER_SHIFT);
    plan._unsigned_value = std::bit_cast<uint64_t>(value);
    plan._payload_width = sizeof(int64_t);
    plan._encoded_size = sizeof(int64_t) + 1;
    plan._kind = PayloadKind::UNSIGNED;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::timestamp_nanos(int64_t value,
                                                                     bool utc_adjusted) noexcept {
    VariantScalarEncodingPlan plan;
    plan._header = static_cast<uint8_t>(
            static_cast<uint8_t>(utc_adjusted ? VariantPrimitiveId::TIMESTAMP_NANOS
                                              : VariantPrimitiveId::TIMESTAMP_NTZ_NANOS)
            << VARIANT_VALUE_HEADER_SHIFT);
    plan._unsigned_value = std::bit_cast<uint64_t>(value);
    plan._payload_width = sizeof(int64_t);
    plan._encoded_size = sizeof(int64_t) + 1;
    plan._kind = PayloadKind::UNSIGNED;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::time_ntz_micros(int64_t value) noexcept {
    VariantScalarEncodingPlan plan;
    plan._header = static_cast<uint8_t>(VariantPrimitiveId::TIME_NTZ_MICROS)
                   << VARIANT_VALUE_HEADER_SHIFT;
    plan._unsigned_value = std::bit_cast<uint64_t>(value);
    plan._payload_width = sizeof(int64_t);
    plan._encoded_size = sizeof(int64_t) + 1;
    plan._kind = PayloadKind::UNSIGNED;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::binary(StringRef value) {
    validate_string_ref(value, "binary");
    if (value.size > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant binary exceeds the uint32 byte limit");
    }
    VariantScalarEncodingPlan plan;
    plan._header = static_cast<uint8_t>(VariantPrimitiveId::BINARY) << VARIANT_VALUE_HEADER_SHIFT;
    plan._borrowed = value;
    plan._encoded_size = value.size + sizeof(uint32_t) + 1;
    plan._kind = PayloadKind::BORROWED_BYTES;
    plan._has_length = true;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::string(StringRef value) {
    validate_utf8_string_ref(value, "string");
    if (value.size > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant string exceeds the uint32 byte limit");
    }
    VariantScalarEncodingPlan plan;
    plan._borrowed = value;
    plan._kind = PayloadKind::BORROWED_BYTES;
    if (value.size <= VARIANT_MAX_SHORT_STRING_SIZE) {
        plan._header = static_cast<uint8_t>((value.size << VARIANT_VALUE_HEADER_SHIFT) |
                                            static_cast<uint8_t>(VariantBasicType::SHORT_STRING));
        plan._encoded_size = value.size + 1;
    } else {
        plan._header = static_cast<uint8_t>(VariantPrimitiveId::STRING)
                       << VARIANT_VALUE_HEADER_SHIFT;
        plan._encoded_size = value.size + sizeof(uint32_t) + 1;
        plan._has_length = true;
    }
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::uuid(
        const std::array<uint8_t, 16>& value) noexcept {
    VariantScalarEncodingPlan plan;
    plan._header = static_cast<uint8_t>(VariantPrimitiveId::UUID) << VARIANT_VALUE_HEADER_SHIFT;
    plan._uuid = value;
    plan._encoded_size = value.size() + 1;
    plan._kind = PayloadKind::UUID;
    return plan;
}

VariantScalarEncodingPlan VariantScalarEncodingPlan::largeint(__int128 value) noexcept {
    if (magnitude(value) <= MAX_DECIMAL38) {
        VariantScalarEncodingPlan plan;
        plan._header = static_cast<uint8_t>(VariantPrimitiveId::DECIMAL16)
                       << VARIANT_VALUE_HEADER_SHIFT;
        plan._signed_value = value;
        plan._payload_width = 16;
        plan._encoded_size = 18;
        plan._kind = PayloadKind::SIGNED;
        plan._has_scale = true;
        return plan;
    }

    VariantScalarEncodingPlan plan;
    char reversed[39];
    size_t digit_count = 0;
    unsigned __int128 remaining = magnitude(value);
    do {
        reversed[digit_count++] = static_cast<char>('0' + remaining % 10);
        remaining /= 10;
    } while (remaining != 0);
    if (value < 0) {
        plan._inline_string[plan._inline_size++] = '-';
    }
    while (digit_count != 0) {
        plan._inline_string[plan._inline_size++] = reversed[--digit_count];
    }
    plan._header = static_cast<uint8_t>(
            (static_cast<size_t>(plan._inline_size) << VARIANT_VALUE_HEADER_SHIFT) |
            static_cast<uint8_t>(VariantBasicType::SHORT_STRING));
    plan._encoded_size = static_cast<size_t>(plan._inline_size) + 1;
    plan._kind = PayloadKind::INLINE_STRING;
    plan._used_string_fallback = true;
    return plan;
}

void VariantScalarEncodingPlan::write(char* destination, size_t capacity) const {
    if (destination == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant scalar destination must not be null");
    }
    if (capacity < _encoded_size) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant scalar destination capacity {} is smaller than required {}",
                        capacity, _encoded_size);
    }
    char* output = destination;
    *output++ = static_cast<char>(_header);
    if (_has_scale) {
        *output++ = static_cast<char>(_scale);
    }
    if (_has_length) {
        write_unsigned(output, _borrowed.size, sizeof(uint32_t));
    }
    switch (_kind) {
    case PayloadKind::HEADER_ONLY:
        break;
    case PayloadKind::UNSIGNED:
        write_unsigned(output, _unsigned_value, _payload_width);
        break;
    case PayloadKind::SIGNED:
        write_signed128(output, _signed_value, _payload_width);
        break;
    case PayloadKind::BORROWED_BYTES:
        if (_borrowed.size != 0) {
            std::memcpy(output, _borrowed.data, _borrowed.size);
            output += _borrowed.size;
        }
        break;
    case PayloadKind::UUID:
        std::memcpy(output, _uuid.data(), _uuid.size());
        output += _uuid.size();
        break;
    case PayloadKind::INLINE_STRING:
        std::memcpy(output, _inline_string.data(), _inline_size);
        output += _inline_size;
        break;
    }
    DCHECK_EQ(output, destination + _encoded_size);
}

} // namespace doris
