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

#include "core/value/variant/variant_batch_builder.h"

#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <ranges>
#include <string>
#include <utility>

#include "common/exception.h"
#include "core/custom_allocator.h"
#include "core/pod_array.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "util/utf8_check.h"

namespace doris {
namespace {

constexpr uint32_t INVALID_INDEX = std::numeric_limits<uint32_t>::max();
constexpr uint64_t FINISHED_ROW_GENERATION = std::numeric_limits<uint64_t>::max();
constexpr uint64_t ABORTED_ROW_GENERATION = FINISHED_ROW_GENERATION - 1;
constexpr size_t INLINE_SCALAR_CAPACITY = sizeof(uint32_t);
constexpr size_t SMALL_OBJECT_SORT_THRESHOLD = 16;

constexpr unsigned __int128 max_decimal38() {
    unsigned __int128 value = 1;
    for (uint8_t digit = 0; digit < 38; ++digit) {
        value *= 10;
    }
    return value - 1;
}

constexpr unsigned __int128 MAX_DECIMAL38 = max_decimal38();

template <typename Container, typename Less>
void sort_object_entries(Container& entries, size_t begin, size_t count, Less less) {
    if (count < 2) {
        return;
    }
    if (count <= SMALL_OBJECT_SORT_THRESHOLD) {
        for (size_t index = begin + 1; index < begin + count; ++index) {
            typename Container::value_type current = entries[index];
            size_t insertion = index;
            while (insertion > begin && less(current, entries[insertion - 1])) {
                entries[insertion] = entries[insertion - 1];
                --insertion;
            }
            entries[insertion] = current;
        }
        return;
    }
    std::sort(entries.begin() + begin, entries.begin() + begin + count, less);
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

void write_unsigned(char*& output, uint64_t value, uint8_t width) noexcept {
    for (uint8_t byte = 0; byte < width; ++byte) {
        *output++ = static_cast<char>(value >> (byte * 8));
    }
}

void validate_string_ref(StringRef value, const char* description) {
    if (value.data == nullptr && value.size != 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant {} has a null data pointer",
                        description);
    }
}

void validate_import_root(VariantRef value) {
    if (value.metadata.data == nullptr && value.metadata.size != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant imported metadata has a null data pointer for {} bytes",
                        value.metadata.size);
    }
    if (value.value.data == nullptr && value.value.size != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant imported value has a null data pointer for {} bytes",
                        value.value.size);
    }
    value.metadata.validate();
}

void require_import_depth(uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant import exceeds maximum nesting depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
}

void require_exact_import_value(VariantRef value) {
    const size_t encoded_size = value.value_size();
    if (encoded_size != value.value.size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant imported value has {} trailing bytes after its {} byte root",
                        value.value.size - encoded_size, encoded_size);
    }
}

void require_import_utf8(StringRef value, const char* description) {
    if (value.size != 0 && !validate_utf8(value.data, value.size)) {
        throw Exception(ErrorCode::CORRUPTION, "Variant imported {} is not valid UTF-8",
                        description);
    }
}

constexpr unsigned __int128 DECIMAL4_MAX = 999'999'999;
constexpr unsigned __int128 DECIMAL8_MAX = 999'999'999'999'999'999;

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

void write_scalar_unsigned(char*& output, uint64_t value, uint8_t width) noexcept {
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

void validate_utf8_string_ref(StringRef value, const char* description) {
    validate_string_ref(value, description);
    if (value.size != 0 && !validate_utf8(value.data, value.size)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant {} is not valid UTF-8", description);
    }
}

template <typename String>
void append_unsigned(String& output, uint64_t value, uint8_t width) {
    for (uint8_t byte = 0; byte < width; ++byte) {
        output.push_back(static_cast<char>(value >> (byte * 8)));
    }
}

template <typename Container>
void release_batch_container(Container& container) noexcept {
    Container().swap(container);
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
        write_scalar_unsigned(output, _borrowed.size, sizeof(uint32_t));
    }
    switch (_kind) {
    case PayloadKind::HEADER_ONLY:
        break;
    case PayloadKind::UNSIGNED:
        write_scalar_unsigned(output, _unsigned_value, _payload_width);
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

struct VariantMetadataBuilder::Impl {
    using KeyMapValue = std::pair<const StringRef, uint32_t>;
    using KeyMap = phmap::flat_hash_map<StringRef, uint32_t, StringRefHash, std::equal_to<>,
                                        CustomStdAllocator<KeyMapValue>>;
    using Keys = std::deque<PaddedPODArray<char>, CustomStdAllocator<PaddedPODArray<char>>>;

    Keys keys;
    KeyMap key_to_temporary_id;
    DorisVector<uint64_t> key_use_counts;
    DorisVector<uint32_t> temporary_to_final_id;
    PaddedPODArray<char> encoded;
    uint64_t active_collectors = 0;
    size_t key_capacity_growths = 0;
    uint32_t sealed_key_count = 0;
    bool sealed = false;
};

VariantMetadataBuilder::VariantMetadataBuilder() : _impl(std::make_unique<Impl>()) {}

VariantMetadataBuilder::~VariantMetadataBuilder() = default;

uint32_t VariantMetadataBuilder::register_key(StringRef key) {
    if (_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot register a Variant metadata key after seal");
    }
    validate_utf8_string_ref(key, "metadata key");
    const auto existing = _impl->key_to_temporary_id.find(key);
    if (existing != _impl->key_to_temporary_id.end()) {
        return existing->second;
    }
    if (_impl->keys.size() == std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant metadata dictionary exceeds the uint32 key limit");
    }

    const auto temporary_id = static_cast<uint32_t>(_impl->keys.size());
    _impl->keys.emplace_back();
    if (key.size != 0) {
        _impl->keys.back().insert(key.data, key.data + key.size);
    }
    const size_t old_capacity = _impl->key_use_counts.capacity();
    _impl->key_use_counts.push_back(0);
    if (_impl->key_use_counts.capacity() != old_capacity) {
        ++_impl->key_capacity_growths;
    }
    const StringRef owned_key {_impl->keys.back().data(), _impl->keys.back().size()};
    const auto [unused, inserted] = _impl->key_to_temporary_id.emplace(owned_key, temporary_id);
    static_cast<void>(unused);
    DCHECK(inserted);
    return temporary_id;
}

void VariantMetadataBuilder::_begin_row() {
    if (_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot create a Variant row builder after metadata seal");
    }
    ++_impl->active_collectors;
}

void VariantMetadataBuilder::_retain_key(uint32_t temporary_id) noexcept {
    DCHECK_LT(temporary_id, _impl->key_use_counts.size());
    DCHECK(!_impl->sealed);
    DCHECK_LT(_impl->key_use_counts[temporary_id], std::numeric_limits<uint64_t>::max());
    ++_impl->key_use_counts[temporary_id];
}

void VariantMetadataBuilder::_complete_row() noexcept {
    DCHECK(!_impl->sealed);
    DCHECK_GT(_impl->active_collectors, 0);
    --_impl->active_collectors;
}

void VariantMetadataBuilder::_abort_row(const uint32_t* temporary_ids, size_t count,
                                        bool was_collecting) noexcept {
    DCHECK(!_impl->sealed);
    for (size_t index = 0; index < count; ++index) {
        DCHECK_LT(temporary_ids[index], _impl->key_use_counts.size());
        DCHECK_GT(_impl->key_use_counts[temporary_ids[index]], 0);
        --_impl->key_use_counts[temporary_ids[index]];
    }
    if (was_collecting) {
        DCHECK_GT(_impl->active_collectors, 0);
        --_impl->active_collectors;
    }
}

void VariantMetadataBuilder::_reserve_keys(size_t count) {
    if (_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot reserve Variant metadata keys after seal");
    }
    _impl->key_use_counts.reserve(count);
    _impl->key_to_temporary_id.reserve(count);
}

StringRef VariantMetadataBuilder::_temporary_key(uint32_t temporary_id) const noexcept {
    DCHECK_LT(temporary_id, _impl->keys.size());
    return {_impl->keys[temporary_id].data(), _impl->keys[temporary_id].size()};
}

size_t VariantMetadataBuilder::_key_capacity() const noexcept {
    return _impl->key_use_counts.capacity();
}

size_t VariantMetadataBuilder::_key_capacity_growths() const noexcept {
    return _impl->key_capacity_growths;
}

void VariantMetadataBuilder::seal() {
    if (_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant metadata is already sealed");
    }
    if (_impl->active_collectors != 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot seal Variant metadata while {} row builders are incomplete",
                        _impl->active_collectors);
    }

    DorisVector<uint32_t> sorted_temporary_ids;
    sorted_temporary_ids.reserve(_impl->keys.size());
    for (uint32_t temporary_id = 0; temporary_id < _impl->keys.size(); ++temporary_id) {
        if (_impl->key_use_counts[temporary_id] != 0) {
            sorted_temporary_ids.push_back(temporary_id);
        }
    }
    std::ranges::sort(sorted_temporary_ids, [this](uint32_t left, uint32_t right) {
        const StringRef left_key {_impl->keys[left].data(), _impl->keys[left].size()};
        const StringRef right_key {_impl->keys[right].data(), _impl->keys[right].size()};
        return left_key.compare(right_key) < 0;
    });

    uint64_t strings_size = 0;
    for (uint32_t temporary_id : sorted_temporary_ids) {
        strings_size += _impl->keys[temporary_id].size();
        if (strings_size > std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant metadata dictionary strings exceed the uint32 byte limit");
        }
    }

    const auto count = static_cast<uint32_t>(sorted_temporary_ids.size());
    _impl->temporary_to_final_id.assign(_impl->keys.size(), INVALID_INDEX);
    for (uint32_t final_id = 0; final_id < count; ++final_id) {
        _impl->temporary_to_final_id[sorted_temporary_ids[final_id]] = final_id;
    }

    const uint8_t offset_width = minimum_unsigned_width(std::max<uint64_t>(count, strings_size));
    const uint64_t encoded_size =
            1 + offset_width + (static_cast<uint64_t>(count) + 1) * offset_width + strings_size;
    if (encoded_size > std::numeric_limits<size_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant metadata exceeds the addressable output size");
    }
    _impl->encoded.reserve(static_cast<size_t>(encoded_size));
    const uint8_t header =
            VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK |
            static_cast<uint8_t>((offset_width - 1) << VARIANT_METADATA_OFFSET_SIZE_SHIFT);
    _impl->encoded.push_back(static_cast<char>(header));
    append_unsigned(_impl->encoded, count, offset_width);

    uint32_t offset = 0;
    append_unsigned(_impl->encoded, offset, offset_width);
    for (uint32_t temporary_id : sorted_temporary_ids) {
        offset += static_cast<uint32_t>(_impl->keys[temporary_id].size());
        append_unsigned(_impl->encoded, offset, offset_width);
    }
    for (uint32_t temporary_id : sorted_temporary_ids) {
        const auto& key = _impl->keys[temporary_id];
        _impl->encoded.insert(key.data(), key.data() + key.size());
    }
    DCHECK_EQ(_impl->encoded.size(), encoded_size);
    _impl->sealed_key_count = count;
    _impl->sealed = true;
}

bool VariantMetadataBuilder::is_sealed() const noexcept {
    return _impl->sealed;
}

size_t VariantMetadataBuilder::num_keys() const noexcept {
    return _impl->sealed ? _impl->sealed_key_count : _impl->keys.size();
}

uint32_t VariantMetadataBuilder::final_id(uint32_t temporary_id) const {
    if (!_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot resolve a Variant metadata id before seal");
    }
    if (temporary_id >= _impl->temporary_to_final_id.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant temporary metadata id {} is out of range [0, {})", temporary_id,
                        _impl->temporary_to_final_id.size());
    }
    if (_impl->temporary_to_final_id[temporary_id] == INVALID_INDEX) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant temporary metadata id {} is not referenced by this encoding unit",
                        temporary_id);
    }
    return _impl->temporary_to_final_id[temporary_id];
}

StringRef VariantMetadataBuilder::encoded_metadata() const {
    if (!_impl->sealed) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot access Variant metadata bytes before seal");
    }
    return {_impl->encoded.data(), _impl->encoded.size()};
}

PaddedPODArray<char> VariantMetadataBuilder::_take_encoded_metadata() noexcept {
    DCHECK(_impl->sealed);
    PaddedPODArray<char> encoded = std::move(_impl->encoded);
    release_batch_container(_impl->keys);
    release_batch_container(_impl->key_to_temporary_id);
    release_batch_container(_impl->key_use_counts);
    release_batch_container(_impl->temporary_to_final_id);
    return encoded;
}

VariantMetadataRef VariantMetadataBuilder::metadata_ref() const {
    const StringRef bytes = encoded_metadata();
    return {.data = bytes.data, .size = bytes.size};
}

class VariantCollectionCore {
public:
    enum class NodeKind : uint8_t { SCALAR, INLINE_SCALAR, OBJECT, ARRAY };
    enum class State : uint8_t { COLLECTING, COLLECTED, FINISHED, ABORTED };

    struct Node {
        Node() noexcept : kind(NodeKind::SCALAR), payload_index(0), payload_size(0) {}
        Node(NodeKind kind_, uint32_t payload_index_, uint32_t payload_size_) noexcept
                : kind(kind_), payload_index(payload_index_), payload_size(payload_size_) {}
        Node(std::array<char, INLINE_SCALAR_CAPACITY> inline_bytes_,
             uint32_t payload_size_) noexcept
                : kind(NodeKind::INLINE_SCALAR),
                  inline_bytes(inline_bytes_),
                  payload_size(payload_size_) {}

        NodeKind kind;
        union {
            uint32_t payload_index;
            std::array<char, INLINE_SCALAR_CAPACITY> inline_bytes;
        };
        uint32_t payload_size;
    };
    static_assert(sizeof(Node) == 12);

    struct Child {
        uint32_t node_index;
        uint32_t temporary_field_id;
        uint32_t next;
    };

    struct Container {
        explicit Container(NodeKind kind_) : kind(kind_) {}

        NodeKind kind;
        uint32_t first_child = INVALID_INDEX;
        uint32_t last_child = INVALID_INDEX;
        uint32_t child_count = 0;
        uint32_t pending_field_id = 0;
        uint32_t previous_or_source_token = INVALID_INDEX;
        uint32_t previous_child_cursor = INVALID_INDEX;
        bool has_pending_field = false;
    };

    struct Checkpoint {
        size_t scalar_bytes;
        size_t nodes;
        size_t containers;
        size_t children;
        size_t key_references;
    };

    struct CapacitySnapshot {
        size_t scalar_bytes;
        size_t nodes;
        size_t containers;
        size_t children;
        size_t scope_stack;
        size_t object_id_scratch;
        size_t key_references;
        size_t container_plans;
        size_t planned_object_children;
    };

    struct FinalChild {
        uint32_t node_index;
        uint32_t final_field_id;
    };

    struct ContainerPlan {
        size_t encoded_size = 0;
        size_t object_children_begin = 0;
        uint32_t values_size = 0;
        uint8_t count_width = 0;
        uint8_t offset_width = 0;
        uint8_t id_width = 0;
    };

    explicit VariantCollectionCore(VariantMetadataBuilder& metadata_) : metadata(metadata_) {}

    void reserve(size_t scalar_byte_count, size_t node_count, size_t container_count,
                 size_t child_count) {
        scalar_bytes.reserve(scalar_byte_count);
        nodes.reserve(node_count);
        containers.reserve(container_count);
        children.reserve(child_count);
        scope_stack.reserve(container_count);
        object_id_scratch.reserve(child_count);
        key_references.reserve(child_count);
        container_plans.reserve(container_count);
        planned_object_children.reserve(child_count);
    }

    CapacitySnapshot capacity_snapshot() const noexcept {
        return {.scalar_bytes = scalar_bytes.capacity(),
                .nodes = nodes.capacity(),
                .containers = containers.capacity(),
                .children = children.capacity(),
                .scope_stack = scope_stack.capacity(),
                .object_id_scratch = object_id_scratch.capacity(),
                .key_references = key_references.capacity(),
                .container_plans = container_plans.capacity(),
                .planned_object_children = planned_object_children.capacity()};
    }

    Checkpoint checkpoint() const noexcept {
        return {.scalar_bytes = scalar_bytes.size(),
                .nodes = nodes.size(),
                .containers = containers.size(),
                .children = children.size(),
                .key_references = key_references.size()};
    }

    void begin_collection(DorisVector<uint32_t>* previous_tokens = nullptr,
                          DorisVector<uint32_t>* pending_tokens = nullptr) noexcept {
        DCHECK(scope_stack.empty());
        DCHECK_EQ(active_container, INVALID_INDEX);
        DCHECK_EQ(previous_tokens == nullptr, pending_tokens == nullptr);
        DCHECK(pending_tokens == nullptr || pending_tokens->empty());
        previous_object_tokens = previous_tokens;
        pending_object_tokens = pending_tokens;
        root_node = INVALID_INDEX;
        state = State::COLLECTING;
    }

    void rollback(const Checkpoint& start) noexcept {
        DCHECK_LE(start.scalar_bytes, scalar_bytes.size());
        DCHECK_LE(start.nodes, nodes.size());
        DCHECK_LE(start.containers, containers.size());
        DCHECK_LE(start.children, children.size());
        DCHECK_LE(start.key_references, key_references.size());
        scalar_bytes.resize(start.scalar_bytes);
        nodes.resize(start.nodes);
        containers.erase(containers.begin() + start.containers, containers.end());
        children.resize(start.children);
        key_references.resize(start.key_references);
        scope_stack.clear();
        active_container = INVALID_INDEX;
        object_id_scratch.clear();
        planned_object_children.clear();
        if (pending_object_tokens != nullptr) {
            pending_object_tokens->clear();
        }
        previous_object_tokens = nullptr;
        pending_object_tokens = nullptr;
        root_node = INVALID_INDEX;
        state = State::ABORTED;
    }

    void publish_object_links() noexcept {
        if (pending_object_tokens == nullptr) {
            return;
        }
        DCHECK(previous_object_tokens != nullptr);
        DCHECK_NE(root_node, INVALID_INDEX);
        previous_object_tokens->swap(*pending_object_tokens);
        pending_object_tokens->clear();
        previous_object_tokens = nullptr;
        pending_object_tokens = nullptr;
    }

    void discard_key_references(size_t begin) noexcept {
        DCHECK_LE(begin, key_references.size());
        key_references.resize(begin);
    }

    const uint32_t* key_reference_data(size_t begin) const noexcept {
        DCHECK_LE(begin, key_references.size());
        return key_references.empty() ? nullptr : key_references.data() + begin;
    }

    size_t key_reference_count(size_t begin) const noexcept {
        DCHECK_LE(begin, key_references.size());
        return key_references.size() - begin;
    }

    void ensure_collecting() const {
        if (state != State::COLLECTING) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot add to a Variant row builder after collection completes");
        }
        if (metadata.is_sealed()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot add a Variant value after metadata seal");
        }
    }

    void ensure_can_add_value() const {
        ensure_collecting();
        if (active_container == INVALID_INDEX) {
            if (root_node != INVALID_INDEX) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant row builder accepts exactly one root value");
            }
            return;
        }

        DCHECK_LT(active_container, containers.size());
        const Container& parent = containers[active_container];
        if (parent.child_count == std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant container exceeds the uint32 element limit");
        }
        if (children.size() == std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant encoding unit exceeds the uint32 temporary child limit");
        }
        if (parent.kind == NodeKind::OBJECT && !parent.has_pending_field) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant object value must be preceded by add_key");
        }
    }

    void prepare_value_node() const {
        ensure_can_add_value();
        if (nodes.size() == std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant encoding unit exceeds the uint32 temporary node limit");
        }
    }

    uint32_t prepare_arena_scalar(size_t encoded_size) const {
        prepare_value_node();
        if (encoded_size > std::numeric_limits<uint32_t>::max() - scalar_bytes.size()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant encoding unit scalar scratch exceeds the uint32 byte limit");
        }
        return static_cast<uint32_t>(scalar_bytes.size());
    }

    void add_scalar(const VariantScalarEncodingPlan& plan) {
        if (plan.size() <= INLINE_SCALAR_CAPACITY) {
            prepare_value_node();
            std::array<char, INLINE_SCALAR_CAPACITY> inline_bytes {};
            plan.write(inline_bytes.data(), plan.size());
            complete_inline_scalar(inline_bytes, plan.size());
            return;
        }

        const uint32_t offset = prepare_arena_scalar(plan.size());
        scalar_bytes.resize(scalar_bytes.size() + plan.size());
        plan.write(scalar_bytes.data() + offset, plan.size());
        complete_scalar(offset);
    }

    void add_null() { add_scalar(VariantScalarEncodingPlan::null_value()); }

    void add_bool(bool value) { add_scalar(VariantScalarEncodingPlan::boolean(value)); }

    void add_int(int64_t value) { add_scalar(VariantScalarEncodingPlan::integer(value)); }

    void add_value(VariantRef value) {
        ensure_can_add_value();
        planned_object_children.clear();
        try {
            validate_import_root(value);
            if (scope_stack.size() > VARIANT_MAX_NESTING_DEPTH) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant import exceeds maximum nesting depth {}",
                                VARIANT_MAX_NESTING_DEPTH);
            }
            import_node(value, static_cast<uint32_t>(scope_stack.size()));
        } catch (...) {
            // The caller still owns row rollback; only release importer scratch here.
            planned_object_children.clear();
            throw;
        }
        DCHECK(planned_object_children.empty());
    }

    void import_primitive(VariantRef value) {
        const VariantPrimitiveId id = value.primitive_id();
        switch (id) {
        case VariantPrimitiveId::NULL_VALUE:
            add_null();
            return;
        case VariantPrimitiveId::TRUE_VALUE:
        case VariantPrimitiveId::FALSE_VALUE:
            add_bool(value.get_bool());
            return;
        case VariantPrimitiveId::INT8:
        case VariantPrimitiveId::INT16:
        case VariantPrimitiveId::INT32:
        case VariantPrimitiveId::INT64:
            add_int(value.get_int());
            return;
        case VariantPrimitiveId::FLOAT:
            add_scalar(VariantScalarEncodingPlan::float32(value.get_float()));
            return;
        case VariantPrimitiveId::DOUBLE:
            add_scalar(VariantScalarEncodingPlan::float64(value.get_double()));
            return;
        case VariantPrimitiveId::DECIMAL4:
        case VariantPrimitiveId::DECIMAL8:
        case VariantPrimitiveId::DECIMAL16: {
            const VariantDecimal decimal = value.get_decimal();
            if (magnitude(decimal.unscaled) > MAX_DECIMAL38) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Variant imported decimal exceeds precision 38");
            }
            add_scalar(VariantScalarEncodingPlan::decimal(decimal.unscaled, decimal.scale));
            return;
        }
        case VariantPrimitiveId::DATE:
            add_scalar(VariantScalarEncodingPlan::date(value.get_date()));
            return;
        case VariantPrimitiveId::TIMESTAMP_MICROS:
            add_scalar(VariantScalarEncodingPlan::timestamp_micros(value.get_timestamp_micros(),
                                                                   true));
            return;
        case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
            add_scalar(VariantScalarEncodingPlan::timestamp_micros(value.get_timestamp_ntz_micros(),
                                                                   false));
            return;
        case VariantPrimitiveId::BINARY:
            add_scalar(VariantScalarEncodingPlan::binary(value.get_binary()));
            return;
        case VariantPrimitiveId::STRING: {
            const StringRef string = value.get_string();
            require_import_utf8(string, "string");
            add_scalar(VariantScalarEncodingPlan::string(string));
            return;
        }
        case VariantPrimitiveId::TIME_NTZ_MICROS:
            add_scalar(VariantScalarEncodingPlan::time_ntz_micros(value.get_time_ntz_micros()));
            return;
        case VariantPrimitiveId::TIMESTAMP_NANOS:
            add_scalar(
                    VariantScalarEncodingPlan::timestamp_nanos(value.get_timestamp_nanos(), true));
            return;
        case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
            add_scalar(VariantScalarEncodingPlan::timestamp_nanos(value.get_timestamp_ntz_nanos(),
                                                                  false));
            return;
        case VariantPrimitiveId::UUID:
            add_scalar(VariantScalarEncodingPlan::uuid(value.get_uuid()));
            return;
        }
    }

    size_t object_values_offset(VariantRef value, uint32_t count) const noexcept {
        const uint8_t value_header =
                static_cast<uint8_t>(value.value.data[0]) >> VARIANT_VALUE_HEADER_SHIFT;
        const auto offset_width = static_cast<uint8_t>(
                ((value_header >> VARIANT_OBJECT_OFFSET_SIZE_SHIFT) & 0x03) + 1);
        const auto id_width =
                static_cast<uint8_t>(((value_header >> VARIANT_OBJECT_ID_SIZE_SHIFT) & 0x03) + 1);
        const uint8_t count_width = (value_header & VARIANT_OBJECT_LARGE_MASK) != 0
                                            ? sizeof(uint32_t)
                                            : sizeof(uint8_t);
        return 1 + count_width + static_cast<size_t>(count) * id_width +
               (static_cast<size_t>(count) + 1) * offset_width;
    }

    void validate_import_object(VariantRef value, uint32_t count) {
        const size_t values_offset = object_values_offset(value, count);
        DCHECK_LE(values_offset, value.value.size);
        const char* values_begin = value.value.data + values_offset;
        const auto values_size = static_cast<uint32_t>(value.value.size - values_offset);
        // Pass-2 planning is idle during collection, so reuse its retained capacity to validate
        // the source object's physical value intervals without adding a per-row scratch buffer.
        planned_object_children.clear();
        planned_object_children.reserve(count);

        StringRef previous_key;
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t field_id = 0;
            const VariantRef child = value.object_value_at(index, &field_id);
            const StringRef key = value.metadata.key_at(field_id);
            require_import_utf8(key, "object key");
            if (index != 0 && previous_key.compare(key) >= 0) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Variant imported object keys are not strictly byte-sorted at "
                                "field {}",
                                index);
            }
            previous_key = key;
            const auto begin = static_cast<uint32_t>(child.value.data - values_begin);
            DCHECK_LE(child.value.size, std::numeric_limits<uint32_t>::max());
            planned_object_children.push_back(
                    {.node_index = begin,
                     .final_field_id = static_cast<uint32_t>(child.value.size)});
        }

        sort_object_entries(planned_object_children, 0, planned_object_children.size(),
                            [](const FinalChild& left, const FinalChild& right) {
                                return left.node_index < right.node_index;
                            });
        uint32_t expected_begin = 0;
        for (const FinalChild& interval : planned_object_children) {
            if (interval.node_index != expected_begin) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Variant imported object values are not a complete non-overlapping "
                                "partition at byte {}",
                                expected_begin);
            }
            expected_begin += interval.final_field_id;
        }
        if (expected_begin != values_size) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant imported object values contain {} unreferenced trailing bytes",
                            values_size - expected_begin);
        }
        planned_object_children.clear();
    }

    void import_object(VariantRef value, uint32_t depth) {
        const uint32_t count = value.num_elements();
        validate_import_object(value, count);
        const uint32_t token = start_container(NodeKind::OBJECT);
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t field_id = 0;
            const VariantRef child = value.object_value_at(index, &field_id);
            add_key(token, value.metadata.key_at(field_id));
            import_node(child, depth + 1);
        }
        finish_container(token, NodeKind::OBJECT);
    }

    void import_array(VariantRef value, uint32_t depth) {
        const uint32_t count = value.num_elements();
        const uint32_t token = start_container(NodeKind::ARRAY);
        for (uint32_t index = 0; index < count; ++index) {
            import_node(value.array_at(index), depth + 1);
        }
        finish_container(token, NodeKind::ARRAY);
    }

    void import_node(VariantRef value, uint32_t depth) {
        require_import_depth(depth);
        require_exact_import_value(value);
        switch (value.basic_type()) {
        case VariantBasicType::PRIMITIVE:
            import_primitive(value);
            return;
        case VariantBasicType::SHORT_STRING: {
            const StringRef string = value.get_string();
            require_import_utf8(string, "string");
            add_scalar(VariantScalarEncodingPlan::string(string));
            return;
        }
        case VariantBasicType::OBJECT:
            import_object(value, depth);
            return;
        case VariantBasicType::ARRAY:
            import_array(value, depth);
            return;
        }
    }

    void attach_node(uint32_t node_index) {
        if (active_container == INVALID_INDEX) {
            DCHECK_EQ(root_node, INVALID_INDEX);
            root_node = node_index;
            if (nodes[node_index].kind == NodeKind::INLINE_SCALAR ||
                nodes[node_index].kind == NodeKind::SCALAR) {
                state = State::COLLECTED;
            }
            return;
        }

        DCHECK_LT(active_container, containers.size());
        Container& parent = containers[active_container];
        const uint32_t field_id = parent.kind == NodeKind::OBJECT ? parent.pending_field_id : 0;
        const auto child_index = static_cast<uint32_t>(children.size());
        children.push_back({node_index, field_id, INVALID_INDEX});
        if (parent.first_child == INVALID_INDEX) {
            parent.first_child = child_index;
        } else {
            DCHECK_NE(parent.last_child, INVALID_INDEX);
            children[parent.last_child].next = child_index;
        }
        parent.last_child = child_index;
        ++parent.child_count;
        parent.has_pending_field = false;
    }

    void complete_scalar(uint32_t offset) {
        DCHECK_LE(scalar_bytes.size() - offset, std::numeric_limits<uint32_t>::max());
        const auto node_index = static_cast<uint32_t>(nodes.size());
        nodes.emplace_back(NodeKind::SCALAR, offset,
                           static_cast<uint32_t>(scalar_bytes.size() - offset));
        attach_node(node_index);
    }

    void complete_inline_scalar(const std::array<char, INLINE_SCALAR_CAPACITY>& inline_bytes,
                                size_t encoded_size) {
        DCHECK_GT(encoded_size, 0);
        DCHECK_LE(encoded_size, INLINE_SCALAR_CAPACITY);
        const auto node_index = static_cast<uint32_t>(nodes.size());
        nodes.emplace_back(inline_bytes, static_cast<uint32_t>(encoded_size));
        attach_node(node_index);
    }

    void add_inline_scalar(const std::array<char, INLINE_SCALAR_CAPACITY>& inline_bytes,
                           size_t encoded_size) {
        prepare_value_node();
        complete_inline_scalar(inline_bytes, encoded_size);
    }

    uint32_t start_container(NodeKind kind) {
        prepare_value_node();
        if (containers.size() == std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant encoding unit exceeds the uint32 temporary container limit");
        }

        const auto token = static_cast<uint32_t>(containers.size());
        uint32_t previous_token = INVALID_INDEX;
        uint32_t previous_child_cursor = INVALID_INDEX;
        if (kind == NodeKind::OBJECT && pending_object_tokens != nullptr) {
            DCHECK(previous_object_tokens != nullptr);
            const size_t object_ordinal = pending_object_tokens->size();
            if (object_ordinal < previous_object_tokens->size()) {
                previous_token = (*previous_object_tokens)[object_ordinal];
                DCHECK_LT(previous_token, token);
                DCHECK(containers[previous_token].kind == NodeKind::OBJECT);
                previous_child_cursor = containers[previous_token].first_child;
            }
#ifdef BE_TEST
            const size_t old_capacity = pending_object_tokens->capacity();
#endif
            try {
                pending_object_tokens->push_back(token);
            } catch (...) {
                state = State::ABORTED;
                throw;
            }
#ifdef BE_TEST
            object_token_capacity_growths += old_capacity != pending_object_tokens->capacity();
#endif
        }
        try {
            containers.emplace_back(kind);
            containers.back().previous_or_source_token = previous_token;
            containers.back().previous_child_cursor = previous_child_cursor;
            const auto node_index = static_cast<uint32_t>(nodes.size());
            nodes.emplace_back(kind, token, 0);
            attach_node(node_index);
            scope_stack.push_back(token);
            active_container = token;
        } catch (...) {
            if (kind == NodeKind::OBJECT && pending_object_tokens != nullptr) {
                DCHECK(!pending_object_tokens->empty());
                DCHECK_EQ(pending_object_tokens->back(), token);
                pending_object_tokens->pop_back();
            }
            if (pending_object_tokens != nullptr) {
                // The active block row owns rollback; keep the state aborted so it releases its
                // metadata references before the next row begins.
                state = State::ABORTED;
            }
            throw;
        }
        return token;
    }

    void ensure_can_add_key(uint32_t token) const {
        ensure_collecting();
        if (token >= containers.size() || active_container != token ||
            containers[token].kind != NodeKind::OBJECT) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant object scope is not the active scope");
        }
        if (containers[token].has_pending_field) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant object key is missing its value");
        }
    }

    void commit_key(uint32_t token, uint32_t temporary_id) noexcept {
        DCHECK_LT(token, containers.size());
        Container& object = containers[token];
        DCHECK(object.kind == NodeKind::OBJECT);
        DCHECK(!object.has_pending_field);
        object.pending_field_id = temporary_id;
        object.has_pending_field = true;
    }

    void add_key(uint32_t token, StringRef key) {
        ensure_can_add_key(token);
        validate_string_ref(key, "metadata key");
        Container& object = containers[token];

        bool key_hit = false;
        uint32_t next_previous_child = INVALID_INDEX;
        uint32_t temporary_id = INVALID_INDEX;
        if (object.previous_or_source_token != INVALID_INDEX &&
            object.previous_child_cursor != INVALID_INDEX) {
            const Child& previous_child = children[object.previous_child_cursor];
            const StringRef previous_key =
                    metadata._temporary_key(previous_child.temporary_field_id);
            if (previous_key.size == key.size &&
                (key.size == 0 || std::memcmp(previous_key.data, key.data, key.size) == 0)) {
                key_hit = true;
                next_previous_child = previous_child.next;
                temporary_id = previous_child.temporary_field_id;
            }
        }
        if (!key_hit) {
            temporary_id = metadata.register_key(key);
        }

        key_references.push_back(temporary_id);
        metadata._retain_key(temporary_id);
        commit_key(token, temporary_id);
        if (key_hit) {
            object.previous_child_cursor = next_previous_child;
        } else {
            object.previous_or_source_token = INVALID_INDEX;
            object.previous_child_cursor = INVALID_INDEX;
        }
    }

    void validate_unique_object_keys(const Container& object) {
        object_id_scratch.clear();
        object_id_scratch.reserve(object.child_count);
        uint32_t child_index = object.first_child;
        for (uint32_t index = 0; index < object.child_count; ++index) {
            DCHECK_NE(child_index, INVALID_INDEX);
            const Child& child = children[child_index];
            object_id_scratch.push_back(child.temporary_field_id);
            child_index = child.next;
        }
        DCHECK_EQ(child_index, INVALID_INDEX);
        sort_object_entries(object_id_scratch, 0, object_id_scratch.size(),
                            [](uint32_t left, uint32_t right) { return left < right; });
        for (size_t index = 1; index < object_id_scratch.size(); ++index) {
            if (object_id_scratch[index - 1] == object_id_scratch[index]) {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Duplicate Variant object key");
            }
        }
    }

    void finish_container(uint32_t token, NodeKind expected_kind) {
        ensure_collecting();
        if (token >= containers.size() || active_container != token ||
            containers[token].kind != expected_kind) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant container scopes must finish in nesting order");
        }
        if (expected_kind == NodeKind::OBJECT && containers[token].has_pending_field) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant object key is missing its value");
        }
        if (expected_kind == NodeKind::OBJECT) {
            Container& object = containers[token];
            if (object.previous_or_source_token != INVALID_INDEX &&
                object.previous_child_cursor == INVALID_INDEX) {
                DCHECK_LT(object.previous_or_source_token, token);
                DCHECK_EQ(containers[object.previous_or_source_token].child_count,
                          object.child_count);
#ifdef BE_TEST
                ++object_schema_hits;
#endif
            } else {
                validate_unique_object_keys(object);
                object.previous_or_source_token = token;
#ifdef BE_TEST
                ++object_schema_fallbacks;
#endif
            }
        }
        DCHECK(!scope_stack.empty());
        DCHECK_EQ(scope_stack.back(), token);
        scope_stack.pop_back();
        if (scope_stack.empty()) {
            active_container = INVALID_INDEX;
            state = State::COLLECTED;
        } else {
            active_container = scope_stack.back();
        }
    }

    void sort_object_children(size_t begin, size_t count) {
        const auto less_by_id = [](const FinalChild& left, const FinalChild& right) {
            return left.final_field_id < right.final_field_id;
        };
        sort_object_entries(planned_object_children, begin, count, less_by_id);
        for (size_t index = begin + 1; index < begin + count; ++index) {
            DCHECK_LT(planned_object_children[index - 1].final_field_id,
                      planned_object_children[index].final_field_id);
        }
    }

    // Recursive pass-2 is a hot path; splitting it adds call layers in ASAN and production.
    // NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
    size_t plan_node(uint32_t node_index) {
        DCHECK_LT(node_index, nodes.size());
        const Node& node = nodes[node_index];
        if (node.kind == NodeKind::INLINE_SCALAR || node.kind == NodeKind::SCALAR) {
            return node.payload_size;
        }

        const Container& container = containers[node.payload_index];
        ContainerPlan& plan = container_plans[node.payload_index];
        const uint32_t count = container.child_count;
        plan.count_width =
                count > std::numeric_limits<uint8_t>::max() ? sizeof(uint32_t) : sizeof(uint8_t);

        if (node.kind == NodeKind::OBJECT) {
            plan.object_children_begin = planned_object_children.size();
            DCHECK_NE(container.previous_or_source_token, INVALID_INDEX);
            if (container.previous_or_source_token != node.payload_index) {
                const uint32_t source_token = container.previous_or_source_token;
                DCHECK_LT(source_token, node.payload_index);
                const Container& source = containers[source_token];
                const ContainerPlan& source_plan = container_plans[source_token];
                DCHECK(source.kind == NodeKind::OBJECT);
                DCHECK_EQ(source.child_count, count);
                DCHECK_GT(source_plan.encoded_size, 0);

                uint32_t child_index = container.first_child;
                for (uint32_t index = 0; index < count; ++index) {
                    DCHECK_NE(child_index, INVALID_INDEX);
                    const Child& child = children[child_index];
                    const uint32_t final_id = metadata.final_id(child.temporary_field_id);
                    DCHECK_LT(final_id, object_id_scratch.size());
                    DCHECK_EQ(object_id_scratch[final_id], INVALID_INDEX);
                    object_id_scratch[final_id] = child.node_index;
                    child_index = child.next;
                }
                DCHECK_EQ(child_index, INVALID_INDEX);

                for (uint32_t index = 0; index < count; ++index) {
                    const uint32_t final_id =
                            planned_object_children[source_plan.object_children_begin + index]
                                    .final_field_id;
                    DCHECK_LT(final_id, object_id_scratch.size());
                    const uint32_t current_node = object_id_scratch[final_id];
                    DCHECK_NE(current_node, INVALID_INDEX);
                    planned_object_children.push_back({current_node, final_id});
                    object_id_scratch[final_id] = INVALID_INDEX;
                }
#ifdef BE_TEST
                ++object_plan_reuses;
#endif
            } else {
                uint32_t child_index = container.first_child;
                for (uint32_t index = 0; index < count; ++index) {
                    DCHECK_NE(child_index, INVALID_INDEX);
                    const Child& child = children[child_index];
                    planned_object_children.push_back(
                            {child.node_index, metadata.final_id(child.temporary_field_id)});
                    child_index = child.next;
                }
                DCHECK_EQ(child_index, INVALID_INDEX);
                sort_object_children(plan.object_children_begin, count);
#ifdef BE_TEST
                ++object_plan_fallbacks;
#endif
            }
        }

        uint32_t values_size = 0;
        uint32_t array_child_index = container.first_child;
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t child_node = 0;
            if (node.kind == NodeKind::OBJECT) {
                child_node = planned_object_children[plan.object_children_begin + index].node_index;
            } else {
                DCHECK_NE(array_child_index, INVALID_INDEX);
                child_node = children[array_child_index].node_index;
                array_child_index = children[array_child_index].next;
            }
            const size_t child_size = plan_node(child_node);
            if (child_size > std::numeric_limits<uint32_t>::max() - values_size) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant container values exceed the uint32 byte limit");
            }
            values_size += static_cast<uint32_t>(child_size);
        }
        if (node.kind == NodeKind::ARRAY) {
            DCHECK_EQ(array_child_index, INVALID_INDEX);
        }
        plan.values_size = values_size;
        plan.offset_width = minimum_unsigned_width(values_size);
        if (node.kind == NodeKind::OBJECT) {
            const uint32_t maximum_id =
                    count == 0 ? 0
                               : planned_object_children[plan.object_children_begin + count - 1]
                                         .final_field_id;
            plan.id_width = minimum_unsigned_width(maximum_id);
        }

        uint64_t encoded_size = 1 + plan.count_width + values_size;
        encoded_size += (static_cast<uint64_t>(count) + 1) * plan.offset_width;
        if (node.kind == NodeKind::OBJECT) {
            encoded_size += static_cast<uint64_t>(count) * plan.id_width;
        }
        if (encoded_size > std::numeric_limits<size_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant row exceeds the addressable output size");
        }
        plan.encoded_size = static_cast<size_t>(encoded_size);
        return plan.encoded_size;
    }

    void reset_plan() {
        container_plans.assign(containers.size(), {});
        planned_object_children.clear();
        size_t object_child_count = 0;
        bool has_object_plan_reuse = false;
        for (size_t index = 0; index < containers.size(); ++index) {
            const Container& container = containers[index];
            if (container.kind != NodeKind::OBJECT) {
                continue;
            }
            DCHECK_NE(container.previous_or_source_token, INVALID_INDEX);
            has_object_plan_reuse |= container.previous_or_source_token != index;
            if (container.child_count > planned_object_children.max_size() - object_child_count) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant object plan exceeds the addressable size");
            }
            object_child_count += container.child_count;
        }
        planned_object_children.reserve(object_child_count);
        if (has_object_plan_reuse) {
            object_id_scratch.assign(metadata.num_keys(), INVALID_INDEX);
        } else {
            object_id_scratch.clear();
        }
    }

    size_t prepare_plan(uint32_t root_index) { return plan_node(root_index); }

    size_t node_encoded_size(uint32_t node_index) const noexcept {
        DCHECK_LT(node_index, nodes.size());
        const Node& node = nodes[node_index];
        return node.kind == NodeKind::INLINE_SCALAR || node.kind == NodeKind::SCALAR
                       ? node.payload_size
                       : container_plans[node.payload_index].encoded_size;
    }

    void write_node(uint32_t node_index, char*& output) const noexcept {
        DCHECK_LT(node_index, nodes.size());
        const Node& node = nodes[node_index];
        if (node.kind == NodeKind::INLINE_SCALAR) {
            std::memcpy(output, node.inline_bytes.data(), node.payload_size);
            output += node.payload_size;
            return;
        }
        if (node.kind == NodeKind::SCALAR) {
            std::memcpy(output, scalar_bytes.data() + node.payload_index, node.payload_size);
            output += node.payload_size;
            return;
        }

        const Container& container = containers[node.payload_index];
        const ContainerPlan& plan = container_plans[node.payload_index];
        const uint32_t count = container.child_count;
        const bool is_large = plan.count_width == sizeof(uint32_t);
        if (node.kind == NodeKind::OBJECT) {
            const auto value_header = static_cast<uint8_t>(
                    ((plan.offset_width - 1) << VARIANT_OBJECT_OFFSET_SIZE_SHIFT) |
                    ((plan.id_width - 1) << VARIANT_OBJECT_ID_SIZE_SHIFT) |
                    (is_large ? VARIANT_OBJECT_LARGE_MASK : 0));
            *output++ = static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                          static_cast<uint8_t>(VariantBasicType::OBJECT));
            write_unsigned(output, count, plan.count_width);
            for (uint32_t index = 0; index < count; ++index) {
                write_unsigned(
                        output,
                        planned_object_children[plan.object_children_begin + index].final_field_id,
                        plan.id_width);
            }
        } else {
            const auto value_header = static_cast<uint8_t>(
                    ((plan.offset_width - 1) << VARIANT_ARRAY_OFFSET_SIZE_SHIFT) |
                    (is_large ? VARIANT_ARRAY_LARGE_MASK : 0));
            *output++ = static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                          static_cast<uint8_t>(VariantBasicType::ARRAY));
            write_unsigned(output, count, plan.count_width);
        }

        uint32_t offset = 0;
        write_unsigned(output, offset, plan.offset_width);
        uint32_t array_child_index = container.first_child;
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t child_node = 0;
            if (node.kind == NodeKind::OBJECT) {
                child_node = planned_object_children[plan.object_children_begin + index].node_index;
            } else {
                DCHECK_NE(array_child_index, INVALID_INDEX);
                child_node = children[array_child_index].node_index;
                array_child_index = children[array_child_index].next;
            }
            offset += static_cast<uint32_t>(node_encoded_size(child_node));
            write_unsigned(output, offset, plan.offset_width);
        }
        DCHECK_EQ(offset, plan.values_size);
        if (node.kind == NodeKind::ARRAY) {
            DCHECK_EQ(array_child_index, INVALID_INDEX);
        }

        array_child_index = container.first_child;
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t child_node = 0;
            if (node.kind == NodeKind::OBJECT) {
                child_node = planned_object_children[plan.object_children_begin + index].node_index;
            } else {
                DCHECK_NE(array_child_index, INVALID_INDEX);
                child_node = children[array_child_index].node_index;
                array_child_index = children[array_child_index].next;
            }
            write_node(child_node, output);
        }
        if (node.kind == NodeKind::ARRAY) {
            DCHECK_EQ(array_child_index, INVALID_INDEX);
        }
    }

    void release_scratch() noexcept {
        release_batch_container(scalar_bytes);
        release_batch_container(nodes);
        release_batch_container(containers);
        release_batch_container(children);
        release_batch_container(scope_stack);
        release_batch_container(object_id_scratch);
        release_batch_container(key_references);
        release_batch_container(container_plans);
        release_batch_container(planned_object_children);
        previous_object_tokens = nullptr;
        pending_object_tokens = nullptr;
        active_container = INVALID_INDEX;
        root_node = INVALID_INDEX;
    }

    VariantMetadataBuilder& metadata;
    DorisVector<char> scalar_bytes;
    DorisVector<Node> nodes;
    DorisVector<Container> containers;
    DorisVector<Child> children;
    DorisVector<uint32_t> scope_stack;
    DorisVector<uint32_t> object_id_scratch;
    DorisVector<uint32_t> key_references;
    DorisVector<ContainerPlan> container_plans;
    DorisVector<FinalChild> planned_object_children;
    DorisVector<uint32_t>* previous_object_tokens = nullptr;
    DorisVector<uint32_t>* pending_object_tokens = nullptr;
#ifdef BE_TEST
    size_t object_token_capacity_growths = 0;
    size_t object_schema_hits = 0;
    size_t object_schema_fallbacks = 0;
    size_t object_plan_reuses = 0;
    size_t object_plan_fallbacks = 0;
#endif
    uint32_t active_container = INVALID_INDEX;
    uint32_t root_node = INVALID_INDEX;
    State state = State::COLLECTING;
};

struct VariantBatchBuilder::Impl {
    Impl() = default;

    void reserve(const ReserveHint& hint) {
        collection.reserve(hint.scalar_bytes, hint.nodes, hint.containers, hint.children);
        roots.reserve(hint.rows);
        previous_object_tokens.reserve(hint.containers);
        pending_object_tokens.reserve(hint.containers);
    }

    void ensure_open() const {
        if (terminal) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant block builder is already finished");
        }
    }

    void ensure_active(uint64_t generation) const {
        ensure_open();
        if (!row_active || generation != active_generation) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant block row handle is not the active row");
        }
    }

#ifdef BE_TEST
    void observe_collection_growth(const VariantCollectionCore::CapacitySnapshot& before) noexcept {
        const VariantCollectionCore::CapacitySnapshot after = collection.capacity_snapshot();
        counters.scalar_capacity_growths += before.scalar_bytes != after.scalar_bytes;
        counters.node_capacity_growths += before.nodes != after.nodes;
        counters.container_capacity_growths += before.containers != after.containers;
        counters.child_capacity_growths += before.children != after.children;
        counters.scope_stack_capacity_growths += before.scope_stack != after.scope_stack;
        counters.object_id_scratch_capacity_growths +=
                before.object_id_scratch != after.object_id_scratch;
        counters.key_reference_capacity_growths += before.key_references != after.key_references;
        counters.container_plan_capacity_growths += before.container_plans != after.container_plans;
        counters.planned_object_child_capacity_growths +=
                before.planned_object_children != after.planned_object_children;
    }

    void observe_root_growth(size_t before) {
        counters.row_root_capacity_growths += before != roots.capacity();
    }

    void refresh_counters(size_t metadata_capacity, size_t metadata_growths,
                          size_t metadata_unique_keys) const noexcept {
        const VariantCollectionCore::CapacitySnapshot capacity = collection.capacity_snapshot();
        counters.metadata_capacity_growths = metadata_growths;
        counters.metadata_unique_keys = metadata_unique_keys;
        counters.metadata_key_capacity = metadata_capacity;
        counters.scalar_byte_capacity = capacity.scalar_bytes;
        counters.node_capacity = capacity.nodes;
        counters.container_capacity = capacity.containers;
        counters.child_capacity = capacity.children;
        counters.scope_stack_capacity = capacity.scope_stack;
        counters.object_id_scratch_capacity = capacity.object_id_scratch;
        counters.key_reference_capacity = capacity.key_references;
        counters.container_plan_capacity = capacity.container_plans;
        counters.planned_object_child_capacity = capacity.planned_object_children;
        counters.row_root_capacity = roots.capacity();
        counters.object_token_capacity_growths = collection.object_token_capacity_growths;
        counters.object_schema_hits = collection.object_schema_hits;
        counters.object_schema_fallbacks = collection.object_schema_fallbacks;
        counters.object_plan_reuses = collection.object_plan_reuses;
        counters.object_plan_fallbacks = collection.object_plan_fallbacks;
        counters.previous_object_token_capacity = previous_object_tokens.capacity();
        counters.pending_object_token_capacity = pending_object_tokens.capacity();
    }

    mutable TestCounters counters;
    VariantCollectionCore::CapacitySnapshot row_capacity_start {};
#endif

    VariantMetadataBuilder metadata;
    VariantCollectionCore collection {metadata};
    DorisVector<uint32_t> roots;
    DorisVector<uint32_t> previous_object_tokens;
    DorisVector<uint32_t> pending_object_tokens;
    VariantCollectionCore::Checkpoint row_start {};
    uint64_t current_generation = 0;
    uint64_t active_generation = 0;
    bool row_active = false;
    bool terminal = false;
#ifdef BE_TEST
    bool counters_finalized = false;
#endif
};

VariantBatchBuilder::VariantBatchBuilder() : VariantBatchBuilder(ReserveHint {}) {}

VariantBatchBuilder::VariantBatchBuilder(ReserveHint hint) : _impl(std::make_unique<Impl>()) {
    _impl->metadata._reserve_keys(hint.metadata_keys);
    _impl->reserve(hint);
#ifdef BE_TEST
    _impl->refresh_counters(_impl->metadata._key_capacity(),
                            _impl->metadata._key_capacity_growths(), _impl->metadata.num_keys());
#endif
}

VariantBatchBuilder::~VariantBatchBuilder() {
    if (_impl != nullptr && _impl->row_active) {
        _abort_row_noexcept(_impl->active_generation);
    }
}

VariantBatchBuilder::VariantBatchBuilder(PaddedPODArray<char> metadata, PaddedPODArray<char> values,
                                         PaddedPODArray<uint32_t> offsets) noexcept
        : _metadata(std::move(metadata)),
          _values(std::move(values)),
          _offsets(std::move(offsets)) {}

VariantBatchBuilder::VariantBatchBuilder(VariantBatchBuilder&& other) noexcept {
    DORIS_CHECK(other._impl == nullptr) << "An active VariantBatchBuilder cannot be moved";
    _metadata = std::move(other._metadata);
    _values = std::move(other._values);
    _offsets = std::move(other._offsets);
}

VariantBatchBuilder& VariantBatchBuilder::operator=(VariantBatchBuilder&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    DORIS_CHECK(_impl == nullptr) << "An active VariantBatchBuilder cannot receive moved data";
    DORIS_CHECK(other._impl == nullptr) << "An active VariantBatchBuilder cannot be moved";
    _metadata = std::move(other._metadata);
    _values = std::move(other._values);
    _offsets = std::move(other._offsets);
    return *this;
}

size_t VariantBatchBuilder::num_rows() const noexcept {
    return _offsets.empty() ? 0 : _offsets.size() - 1;
}

VariantRef VariantBatchBuilder::value_at(size_t row) const {
    if (row >= num_rows()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant encoded batch row {} is outside [0, {})", row, num_rows());
    }
    const uint32_t begin = _offsets[row];
    const uint32_t end = _offsets[row + 1];
    return {.metadata = metadata_ref(), .value = {_values.data() + begin, end - begin}};
}

void VariantBatchBuilder::_add_scalar(uint64_t generation, const VariantScalarEncodingPlan& plan) {
    _impl->ensure_active(generation);
    _impl->collection.add_scalar(plan);
}

void VariantBatchBuilder::_add_null(uint64_t generation) {
    _impl->ensure_active(generation);
    _impl->collection.add_null();
}

void VariantBatchBuilder::_add_bool(uint64_t generation, bool value) {
    _impl->ensure_active(generation);
    _impl->collection.add_bool(value);
}

void VariantBatchBuilder::_add_int(uint64_t generation, int64_t value) {
    _impl->ensure_active(generation);
    _impl->collection.add_int(value);
}

void VariantBatchBuilder::_add_value(uint64_t generation, VariantRef value) {
    _impl->ensure_active(generation);
    _impl->collection.add_value(value);
}

uint32_t VariantBatchBuilder::_start_container(uint64_t generation, bool object) {
    _impl->ensure_active(generation);
    return _impl->collection.start_container(object ? VariantCollectionCore::NodeKind::OBJECT
                                                    : VariantCollectionCore::NodeKind::ARRAY);
}

void VariantBatchBuilder::_add_key(uint64_t generation, uint32_t token, StringRef key) {
    _impl->ensure_active(generation);
    _impl->collection.add_key(token, key);
}

void VariantBatchBuilder::_finish_container(uint64_t generation, uint32_t token, bool object) {
    _impl->ensure_active(generation);
    _impl->collection.finish_container(token, object ? VariantCollectionCore::NodeKind::OBJECT
                                                     : VariantCollectionCore::NodeKind::ARRAY);
}

void VariantBatchBuilder::_finish_row(uint64_t generation) {
    _impl->ensure_active(generation);
    if (_impl->collection.state == VariantCollectionCore::State::COLLECTING) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Cannot finish an incomplete Variant row");
    }
    DCHECK(_impl->collection.state == VariantCollectionCore::State::COLLECTED);
#ifdef BE_TEST
    const size_t root_capacity = _impl->roots.capacity();
#endif
    _impl->roots.push_back(_impl->collection.root_node);
#ifdef BE_TEST
    _impl->observe_root_growth(root_capacity);
#endif
    _impl->metadata._complete_row();
    _impl->collection.discard_key_references(_impl->row_start.key_references);
    _impl->collection.publish_object_links();
    _impl->collection.state = VariantCollectionCore::State::FINISHED;
#ifdef BE_TEST
    _impl->observe_collection_growth(_impl->row_capacity_start);
#endif
    _impl->row_active = false;
}

void VariantBatchBuilder::_abort_row_noexcept(uint64_t generation) noexcept {
    if (!_impl->row_active || generation != _impl->active_generation) {
        return;
    }
    _impl->metadata._abort_row(
            _impl->collection.key_reference_data(_impl->row_start.key_references),
            _impl->collection.key_reference_count(_impl->row_start.key_references), true);
    _impl->collection.rollback(_impl->row_start);
#ifdef BE_TEST
    _impl->observe_collection_growth(_impl->row_capacity_start);
#endif
    _impl->row_active = false;
}

void VariantBatchBuilder::_abort_row(uint64_t generation) {
    _impl->ensure_active(generation);
    _abort_row_noexcept(generation);
}

VariantBatchBuilder::Row VariantBatchBuilder::begin_row() {
    if (_impl == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append rows to a finished Variant batch");
    }
    _impl->ensure_open();
    if (_impl->row_active) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant batch builder already has an active row");
    }
    if (_impl->current_generation >= ABORTED_ROW_GENERATION - 1) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant batch row generation exceeds the uint64 limit");
    }
    ++_impl->current_generation;
    DCHECK(_impl->pending_object_tokens.empty());
#ifdef BE_TEST
    _impl->row_capacity_start = _impl->collection.capacity_snapshot();
#endif
    _impl->collection.begin_collection(&_impl->previous_object_tokens,
                                       &_impl->pending_object_tokens);
    _impl->row_start = _impl->collection.checkpoint();
    _impl->metadata._begin_row();
    _impl->active_generation = _impl->current_generation;
    _impl->row_active = true;
    return {this, _impl->active_generation};
}

VariantBatchBuilder VariantBatchBuilder::finish_batch() {
    if (_impl == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant batch is already materialized");
    }
    _impl->ensure_open();
    if (_impl->row_active) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot finish a Variant batch while a row is active");
    }
    _impl->terminal = true;
    _impl->metadata.seal();
#ifdef BE_TEST
    const VariantCollectionCore::CapacitySnapshot before = _impl->collection.capacity_snapshot();
#endif
    _impl->collection.reset_plan();

    PaddedPODArray<uint32_t> offsets;
    constexpr size_t MAX_OFFSET_COUNT = std::numeric_limits<size_t>::max() / sizeof(uint32_t);
    if (_impl->roots.size() >= MAX_OFFSET_COUNT) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant batch row offsets exceed the addressable size");
    }
    offsets.reserve(_impl->roots.size() + 1);
    offsets.push_back(0);
    uint32_t total_size = 0;
    for (uint32_t root : _impl->roots) {
        const size_t row_size = _impl->collection.prepare_plan(root);
        if (row_size > std::numeric_limits<uint32_t>::max() - total_size) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant batch values exceed the ColumnString uint32 byte limit");
        }
        total_size += static_cast<uint32_t>(row_size);
        offsets.push_back(total_size);
    }
#ifdef BE_TEST
    _impl->observe_collection_growth(before);
#endif
    PaddedPODArray<char> values;
    values.resize(total_size);
    char* output = values.data();
    for (uint32_t root : _impl->roots) {
        _impl->collection.write_node(root, output);
    }
    DCHECK_EQ(output, values.data() + values.size());

#ifdef BE_TEST
    _impl->refresh_counters(_impl->metadata._key_capacity(),
                            _impl->metadata._key_capacity_growths(), _impl->metadata.num_keys());
    _impl->counters_finalized = true;
#endif
    PaddedPODArray<char> metadata = _impl->metadata._take_encoded_metadata();
    _impl->collection.release_scratch();
    release_batch_container(_impl->roots);
    release_batch_container(_impl->previous_object_tokens);
    release_batch_container(_impl->pending_object_tokens);
    return {std::move(metadata), std::move(values), std::move(offsets)};
}

#ifdef BE_TEST
const VariantBatchBuilder::TestCounters& VariantBatchBuilder::test_counters() const noexcept {
    if (!_impl->counters_finalized) {
        _impl->refresh_counters(_impl->metadata._key_capacity(),
                                _impl->metadata._key_capacity_growths(),
                                _impl->metadata.num_keys());
    }
    return _impl->counters;
}
#endif

VariantBatchBuilder::Row::ObjectScope::ObjectScope(VariantBatchBuilder* builder,
                                                   uint64_t generation, uint32_t token) noexcept
        : _builder(builder), _generation(generation), _token(token) {}

VariantBatchBuilder::Row::ObjectScope::ObjectScope(ObjectScope&& other) noexcept
        : _builder(std::exchange(other._builder, nullptr)),
          _generation(other._generation),
          _token(other._token) {}

VariantBatchBuilder::Row::ArrayScope::ArrayScope(VariantBatchBuilder* builder, uint64_t generation,
                                                 uint32_t token) noexcept
        : _builder(builder), _generation(generation), _token(token) {}

VariantBatchBuilder::Row::ArrayScope::ArrayScope(ArrayScope&& other) noexcept
        : _builder(std::exchange(other._builder, nullptr)),
          _generation(other._generation),
          _token(other._token) {}

VariantBatchBuilder::Row::Row(VariantBatchBuilder* builder, uint64_t generation) noexcept
        : _builder(builder), _generation(generation) {}

VariantBatchBuilder::Row::Row(Row&& other) noexcept
        : _builder(std::exchange(other._builder, nullptr)), _generation(other._generation) {}

void VariantBatchBuilder::Row::ObjectScope::add_key(StringRef key) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block object scope is already finished");
    }
    _builder->_add_key(_generation, _token, key);
}

void VariantBatchBuilder::Row::ObjectScope::finish() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block object scope is already finished");
    }
    _builder->_finish_container(_generation, _token, true);
    _builder = nullptr;
}

void VariantBatchBuilder::Row::ArrayScope::finish() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant block array scope is already finished");
    }
    _builder->_finish_container(_generation, _token, false);
    _builder = nullptr;
}

VariantBatchBuilder::Row::~Row() {
    if (_builder != nullptr) {
        _builder->_abort_row_noexcept(_generation);
    }
}

void VariantBatchBuilder::Row::add_null() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_null(_generation);
}

void VariantBatchBuilder::Row::add_bool(bool value) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_bool(_generation, value);
}

void VariantBatchBuilder::Row::add_int(int64_t value) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_int(_generation, value);
}

void VariantBatchBuilder::Row::add_float(float value) {
    _add_scalar(VariantScalarEncodingPlan::float32(value));
}

void VariantBatchBuilder::Row::add_double(double value) {
    _add_scalar(VariantScalarEncodingPlan::float64(value));
}

void VariantBatchBuilder::Row::add_decimal(__int128 unscaled, uint8_t scale) {
    _add_scalar(VariantScalarEncodingPlan::decimal(unscaled, scale));
}

void VariantBatchBuilder::Row::add_decimal(__int128 unscaled, uint8_t scale, uint8_t width) {
    _add_scalar(VariantScalarEncodingPlan::decimal(unscaled, scale, width));
}

void VariantBatchBuilder::Row::add_date(int32_t days_since_epoch) {
    _add_scalar(VariantScalarEncodingPlan::date(days_since_epoch));
}

void VariantBatchBuilder::Row::add_timestamp_micros(int64_t value, bool utc_adjusted) {
    _add_scalar(VariantScalarEncodingPlan::timestamp_micros(value, utc_adjusted));
}

void VariantBatchBuilder::Row::add_timestamp_nanos(int64_t value, bool utc_adjusted) {
    _add_scalar(VariantScalarEncodingPlan::timestamp_nanos(value, utc_adjusted));
}

void VariantBatchBuilder::Row::add_time_ntz_micros(int64_t value) {
    _add_scalar(VariantScalarEncodingPlan::time_ntz_micros(value));
}

void VariantBatchBuilder::Row::add_binary(StringRef value) {
    _add_scalar(VariantScalarEncodingPlan::binary(value));
}

void VariantBatchBuilder::Row::add_string(StringRef value) {
    _add_scalar(VariantScalarEncodingPlan::string(value));
}

void VariantBatchBuilder::Row::add_uuid(const std::array<uint8_t, 16>& value) {
    _add_scalar(VariantScalarEncodingPlan::uuid(value));
}

void VariantBatchBuilder::Row::add_largeint(__int128 value) {
    _add_scalar(VariantScalarEncodingPlan::largeint(value));
}

void VariantBatchBuilder::Row::add_value(VariantRef value) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_value(_generation, value);
}

VariantBatchBuilder::Row::ObjectScope VariantBatchBuilder::Row::start_object() {
    const uint32_t token = _start_object();
    return {_builder, _generation, token};
}

VariantBatchBuilder::Row::ArrayScope VariantBatchBuilder::Row::start_array() {
    const uint32_t token = _start_array();
    return {_builder, _generation, token};
}

void VariantBatchBuilder::Row::finish() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_finish_row(_generation);
    _generation = FINISHED_ROW_GENERATION;
}

void VariantBatchBuilder::Row::abort() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_abort_row(_generation);
    _generation = ABORTED_ROW_GENERATION;
}

bool VariantBatchBuilder::Row::is_finished() const noexcept {
    return _builder != nullptr && _generation == FINISHED_ROW_GENERATION;
}

void VariantBatchBuilder::Row::_add_scalar(const VariantScalarEncodingPlan& plan) {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    _builder->_add_scalar(_generation, plan);
}

uint32_t VariantBatchBuilder::Row::_start_object() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    return _builder->_start_container(_generation, true);
}

uint32_t VariantBatchBuilder::Row::_start_array() {
    if (_builder == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant block row handle is moved-from");
    }
    return _builder->_start_container(_generation, false);
}

} // namespace doris
