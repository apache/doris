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

#include "core/value/variant/variant_value.h"

#include <algorithm>
#include <bit>
#include <cstdint>
#include <limits>

#include "common/exception.h"

namespace doris {
namespace {

void require_bytes(size_t available, size_t required, const char* field) {
    if (available < required) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Truncated Variant value while reading {}: need {} bytes, have {}", field,
                        required, available);
    }
}

uint64_t read_unsigned(const char* data, uint8_t width) {
    uint64_t result = 0;
    for (uint8_t i = 0; i < width; ++i) {
        result |= static_cast<uint64_t>(static_cast<uint8_t>(data[i])) << (i * 8);
    }
    return result;
}

int64_t read_signed(const char* data, uint8_t width) {
    uint64_t value = read_unsigned(data, width);
    if (width < sizeof(value) && (value & (uint64_t {1} << (width * 8 - 1))) != 0) {
        value |= std::numeric_limits<uint64_t>::max() << (width * 8);
    }
    return std::bit_cast<int64_t>(value);
}

__int128 read_signed_128(const char* data, uint8_t width) {
    unsigned __int128 value = 0;
    for (uint8_t i = 0; i < width; ++i) {
        value |= static_cast<unsigned __int128>(static_cast<uint8_t>(data[i])) << (i * 8);
    }
    if (width < sizeof(value) &&
        (value & (static_cast<unsigned __int128>(1) << (width * 8 - 1))) != 0) {
        value |= ~static_cast<unsigned __int128>(0) << (width * 8);
    }
    return static_cast<__int128>(value);
}

size_t primitive_payload_size(VariantPrimitiveId id, const char* payload, size_t available) {
    switch (id) {
    case VariantPrimitiveId::NULL_VALUE:
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return 0;
    case VariantPrimitiveId::INT8:
        return 1;
    case VariantPrimitiveId::INT16:
        return 2;
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::FLOAT:
    case VariantPrimitiveId::DATE:
        return 4;
    case VariantPrimitiveId::INT64:
    case VariantPrimitiveId::DOUBLE:
    case VariantPrimitiveId::TIMESTAMP_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
    case VariantPrimitiveId::TIME_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        return 8;
    case VariantPrimitiveId::DECIMAL4:
        return 5;
    case VariantPrimitiveId::DECIMAL8:
        return 9;
    case VariantPrimitiveId::DECIMAL16:
        return 17;
    case VariantPrimitiveId::BINARY:
    case VariantPrimitiveId::STRING: {
        require_bytes(available, sizeof(uint32_t), "string or binary length");
        const size_t length = read_unsigned(payload, sizeof(uint32_t));
        return sizeof(uint32_t) + length;
    }
    case VariantPrimitiveId::UUID:
        return 16;
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown Variant primitive id {}",
                    static_cast<uint8_t>(id));
}

void require_primitive(VariantPrimitiveId actual, VariantPrimitiveId expected,
                       const char* accessor) {
    if (actual != expected) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant {} accessor cannot read primitive id {}", accessor,
                        static_cast<uint8_t>(actual));
    }
}

} // namespace

VariantBasicType VariantRef::basic_type() const {
    require_bytes(value.size, 1, "header");
    return static_cast<VariantBasicType>(static_cast<uint8_t>(value.data[0]) &
                                         VARIANT_BASIC_TYPE_MASK);
}

VariantPrimitiveId VariantRef::primitive_id() const {
    if (basic_type() != VariantBasicType::PRIMITIVE) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant primitive accessor used for non-primitive basic type {}",
                        static_cast<uint8_t>(basic_type()));
    }
    const uint8_t id = static_cast<uint8_t>(value.data[0]) >> VARIANT_VALUE_HEADER_SHIFT;
    if (id > VARIANT_MAX_PRIMITIVE_ID) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown Variant primitive id {}", id);
    }
    return static_cast<VariantPrimitiveId>(id);
}

size_t VariantRef::value_size() const {
    const VariantBasicType type = basic_type();
    if (type == VariantBasicType::SHORT_STRING) {
        const size_t string_size =
                static_cast<uint8_t>(value.data[0]) >> VARIANT_VALUE_HEADER_SHIFT;
        require_bytes(value.size - 1, string_size, "short string");
        return 1 + string_size;
    }
    if (type == VariantBasicType::PRIMITIVE) {
        const size_t payload_size =
                primitive_payload_size(primitive_id(), value.data + 1, value.size - 1);
        require_bytes(value.size - 1, payload_size, "primitive payload");
        return 1 + payload_size;
    }
    const ContainerLayout layout = _container_layout(type);
    return layout.values_offset + layout.values_size;
}

bool VariantRef::is_null() const {
    return basic_type() == VariantBasicType::PRIMITIVE &&
           primitive_id() == VariantPrimitiveId::NULL_VALUE;
}

bool VariantRef::get_bool() const {
    const VariantPrimitiveId id = primitive_id();
    if (id == VariantPrimitiveId::TRUE_VALUE) {
        return true;
    }
    if (id == VariantPrimitiveId::FALSE_VALUE) {
        return false;
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT,
                    "Variant bool accessor cannot read primitive id {}", static_cast<uint8_t>(id));
}

int64_t VariantRef::get_int() const {
    const VariantPrimitiveId id = primitive_id();
    uint8_t width = 0;
    switch (id) {
    case VariantPrimitiveId::INT8:
        width = 1;
        break;
    case VariantPrimitiveId::INT16:
        width = 2;
        break;
    case VariantPrimitiveId::INT32:
        width = 4;
        break;
    case VariantPrimitiveId::INT64:
        width = 8;
        break;
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant integer accessor cannot read primitive id {}",
                        static_cast<uint8_t>(id));
    }
    require_bytes(value.size - 1, width, "integer");
    return read_signed(value.data + 1, width);
}

float VariantRef::get_float() const {
    require_primitive(primitive_id(), VariantPrimitiveId::FLOAT, "float");
    require_bytes(value.size - 1, sizeof(uint32_t), "float");
    return std::bit_cast<float>(
            static_cast<uint32_t>(read_unsigned(value.data + 1, sizeof(uint32_t))));
}

double VariantRef::get_double() const {
    require_primitive(primitive_id(), VariantPrimitiveId::DOUBLE, "double");
    require_bytes(value.size - 1, sizeof(uint64_t), "double");
    return std::bit_cast<double>(read_unsigned(value.data + 1, sizeof(uint64_t)));
}

VariantDecimal VariantRef::get_decimal() const {
    const VariantPrimitiveId id = primitive_id();
    uint8_t width = 0;
    switch (id) {
    case VariantPrimitiveId::DECIMAL4:
        width = 4;
        break;
    case VariantPrimitiveId::DECIMAL8:
        width = 8;
        break;
    case VariantPrimitiveId::DECIMAL16:
        width = 16;
        break;
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal accessor cannot read primitive id {}",
                        static_cast<uint8_t>(id));
    }
    require_bytes(value.size - 1, static_cast<size_t>(width) + 1, "decimal");
    const auto scale = static_cast<uint8_t>(value.data[1]);
    if (scale > 38) {
        throw Exception(ErrorCode::CORRUPTION, "Invalid Variant decimal scale {}", scale);
    }
    return {.unscaled = read_signed_128(value.data + 2, width), .scale = scale, .width = width};
}

int32_t VariantRef::get_date() const {
    require_primitive(primitive_id(), VariantPrimitiveId::DATE, "date");
    require_bytes(value.size - 1, sizeof(uint32_t), "date");
    return static_cast<int32_t>(read_signed(value.data + 1, sizeof(uint32_t)));
}

int64_t VariantRef::get_timestamp_micros() const {
    require_primitive(primitive_id(), VariantPrimitiveId::TIMESTAMP_MICROS, "timestamp micros");
    require_bytes(value.size - 1, sizeof(uint64_t), "timestamp micros");
    return read_signed(value.data + 1, sizeof(uint64_t));
}

int64_t VariantRef::get_timestamp_ntz_micros() const {
    require_primitive(primitive_id(), VariantPrimitiveId::TIMESTAMP_NTZ_MICROS,
                      "timestamp NTZ micros");
    require_bytes(value.size - 1, sizeof(uint64_t), "timestamp NTZ micros");
    return read_signed(value.data + 1, sizeof(uint64_t));
}

int64_t VariantRef::get_time_ntz_micros() const {
    require_primitive(primitive_id(), VariantPrimitiveId::TIME_NTZ_MICROS, "time NTZ micros");
    require_bytes(value.size - 1, sizeof(uint64_t), "time NTZ micros");
    return read_signed(value.data + 1, sizeof(uint64_t));
}

int64_t VariantRef::get_timestamp_nanos() const {
    require_primitive(primitive_id(), VariantPrimitiveId::TIMESTAMP_NANOS, "timestamp nanos");
    require_bytes(value.size - 1, sizeof(uint64_t), "timestamp nanos");
    return read_signed(value.data + 1, sizeof(uint64_t));
}

int64_t VariantRef::get_timestamp_ntz_nanos() const {
    require_primitive(primitive_id(), VariantPrimitiveId::TIMESTAMP_NTZ_NANOS,
                      "timestamp NTZ nanos");
    require_bytes(value.size - 1, sizeof(uint64_t), "timestamp NTZ nanos");
    return read_signed(value.data + 1, sizeof(uint64_t));
}

StringRef VariantRef::get_binary() const {
    require_primitive(primitive_id(), VariantPrimitiveId::BINARY, "binary");
    const size_t total_size = value_size();
    return {value.data + 1 + sizeof(uint32_t), total_size - 1 - sizeof(uint32_t)};
}

StringRef VariantRef::get_string() const {
    if (basic_type() == VariantBasicType::SHORT_STRING) {
        const size_t total_size = value_size();
        return {value.data + 1, total_size - 1};
    }
    require_primitive(primitive_id(), VariantPrimitiveId::STRING, "string");
    const size_t total_size = value_size();
    return {value.data + 1 + sizeof(uint32_t), total_size - 1 - sizeof(uint32_t)};
}

std::array<uint8_t, 16> VariantRef::get_uuid() const {
    require_primitive(primitive_id(), VariantPrimitiveId::UUID, "UUID");
    require_bytes(value.size - 1, 16, "UUID");
    std::array<uint8_t, 16> result {};
    std::transform(value.data + 1, value.data + 17, result.begin(),
                   [](char byte) { return static_cast<uint8_t>(byte); });
    return result;
}

VariantRef::ContainerLayout VariantRef::_container_layout(VariantBasicType expected_type) const {
    const VariantBasicType actual_type = basic_type();
    if (actual_type != expected_type ||
        (actual_type != VariantBasicType::OBJECT && actual_type != VariantBasicType::ARRAY)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant container accessor expected basic type {}, got {}",
                        static_cast<uint8_t>(expected_type), static_cast<uint8_t>(actual_type));
    }

    const uint8_t value_header = static_cast<uint8_t>(value.data[0]) >> VARIANT_VALUE_HEADER_SHIFT;
    const bool is_object = actual_type == VariantBasicType::OBJECT;
    const auto offset_width =
            static_cast<uint8_t>(((value_header >> (is_object ? VARIANT_OBJECT_OFFSET_SIZE_SHIFT
                                                              : VARIANT_ARRAY_OFFSET_SIZE_SHIFT)) &
                                  0x03) +
                                 1);
    const uint8_t id_width =
            is_object ? static_cast<uint8_t>(
                                ((value_header >> VARIANT_OBJECT_ID_SIZE_SHIFT) & 0x03) + 1)
                      : 0;
    const bool is_large = (value_header &
                           (is_object ? VARIANT_OBJECT_LARGE_MASK : VARIANT_ARRAY_LARGE_MASK)) != 0;
    const uint8_t count_width = is_large ? sizeof(uint32_t) : sizeof(uint8_t);
    require_bytes(value.size - 1, count_width, "container element count");
    const auto count = static_cast<uint32_t>(read_unsigned(value.data + 1, count_width));

    const size_t ids_offset = 1 + count_width;
    if (id_width != 0 && count > (value.size - ids_offset) / id_width) {
        throw Exception(ErrorCode::CORRUPTION, "Variant object field id table is truncated");
    }
    const size_t ids_size = static_cast<size_t>(count) * id_width;
    const size_t offsets_offset = ids_offset + ids_size;
    if (static_cast<uint64_t>(count) + 1 > (value.size - offsets_offset) / offset_width) {
        throw Exception(ErrorCode::CORRUPTION, "Variant container offset table is truncated");
    }
    const size_t offsets_size = (static_cast<size_t>(count) + 1) * offset_width;
    const size_t values_offset = offsets_offset + offsets_size;
    if (!is_object) {
        const auto first_offset =
                static_cast<uint32_t>(read_unsigned(value.data + offsets_offset, offset_width));
        if (first_offset != 0) {
            throw Exception(ErrorCode::CORRUPTION, "Invalid Variant array first offset {}",
                            first_offset);
        }
    }
    const auto values_size = static_cast<uint32_t>(read_unsigned(
            value.data + offsets_offset + static_cast<size_t>(count) * offset_width, offset_width));
    require_bytes(value.size - values_offset, values_size, "container values");
    if (is_object && count == 0 && values_size != 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Empty Variant object has non-empty values region of {} bytes",
                        values_size);
    }
    return {.count = count,
            .offset_width = offset_width,
            .id_width = id_width,
            .ids_offset = ids_offset,
            .offsets_offset = offsets_offset,
            .values_offset = values_offset,
            .values_size = values_size};
}

uint32_t VariantRef::num_elements() const {
    const VariantBasicType type = basic_type();
    if (type != VariantBasicType::OBJECT && type != VariantBasicType::ARRAY) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant element count accessor used for non-container basic type {}",
                        static_cast<uint8_t>(type));
    }
    return _container_layout(type).count;
}

uint32_t VariantRef::_object_field_id(const ContainerLayout& layout, uint32_t index) const {
    if (index >= layout.count) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant object index {} is out of range [0, {})", index, layout.count);
    }
    const auto field_id = static_cast<uint32_t>(read_unsigned(
            value.data + layout.ids_offset + static_cast<size_t>(index) * layout.id_width,
            layout.id_width));
    if (field_id >= metadata.dict_size()) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant object field id {} is outside metadata dictionary of size {}",
                        field_id, metadata.dict_size());
    }
    return field_id;
}

VariantRef VariantRef::_container_value_at(const ContainerLayout& layout, uint32_t index,
                                           bool require_array_boundary) const {
    if (index >= layout.count) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant container index {} is out of range [0, {})", index, layout.count);
    }
    const auto offset = static_cast<uint32_t>(read_unsigned(
            value.data + layout.offsets_offset + static_cast<size_t>(index) * layout.offset_width,
            layout.offset_width));
    if (offset >= layout.values_size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant child offset {} is outside container values of size {}", offset,
                        layout.values_size);
    }
    VariantRef child {
            .metadata = metadata,
            .value = {value.data + layout.values_offset + offset, layout.values_size - offset}};
    const size_t child_size = child.value_size();
    if (require_array_boundary) {
        const auto next_offset = static_cast<uint32_t>(
                read_unsigned(value.data + layout.offsets_offset +
                                      (static_cast<size_t>(index) + 1) * layout.offset_width,
                              layout.offset_width));
        if (next_offset < offset || next_offset > layout.values_size ||
            child_size != next_offset - offset) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Invalid Variant array child bounds [{}, {}) for child size {}", offset,
                            next_offset, child_size);
        }
    }
    child.value.size = child_size;
    return child;
}

VariantRef VariantRef::object_value_at(uint32_t index, uint32_t* field_id_out) const {
    const ContainerLayout layout = _container_layout(VariantBasicType::OBJECT);
    const uint32_t field_id = _object_field_id(layout, index);
    if (field_id_out != nullptr) {
        *field_id_out = field_id;
    }
    return _container_value_at(layout, index, false);
}

VariantRef VariantRef::array_at(uint32_t index) const {
    return _container_value_at(_container_layout(VariantBasicType::ARRAY), index, true);
}

bool VariantRef::object_find(StringRef key, VariantRef* out) const {
    if (out == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant object lookup output is null");
    }
    const ContainerLayout layout = _container_layout(VariantBasicType::OBJECT);
    const int64_t dictionary_id = metadata.find_key(key);
    if (dictionary_id < 0) {
        return false;
    }
    return _object_find_by_id(layout, static_cast<uint32_t>(dictionary_id), out);
}

bool VariantRef::object_find_by_id(uint32_t field_id, VariantRef* out) const {
    if (out == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant object lookup output is null");
    }
    return _object_find_by_id(_container_layout(VariantBasicType::OBJECT), field_id, out);
}

bool VariantRef::_object_find_by_id(const ContainerLayout& layout, uint32_t field_id,
                                    VariantRef* out) const {
    const StringRef target_key = metadata.key_at(field_id);
    uint32_t begin = 0;
    uint32_t end = layout.count;
    while (begin < end) {
        const uint32_t middle = begin + (end - begin) / 2;
        const uint32_t middle_id = _object_field_id(layout, middle);
        const int comparison = metadata.sorted_strings()
                                       ? (middle_id > field_id) - (middle_id < field_id)
                                       : metadata.key_at(middle_id).compare(target_key);
        if (comparison < 0) {
            begin = middle + 1;
        } else {
            end = middle;
        }
    }
    if (begin == layout.count || _object_field_id(layout, begin) != field_id) {
        if (!metadata.sorted_strings() && begin < layout.count &&
            metadata.key_at(_object_field_id(layout, begin)) == target_key) {
            *out = _container_value_at(layout, begin, false);
            return true;
        }
        return false;
    }
    *out = _container_value_at(layout, begin, false);
    return true;
}

} // namespace doris
