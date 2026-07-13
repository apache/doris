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

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "exec/common/sip_hash.h"
#include "util/hash_util.hpp"
#include "util/utf8_check.h"
#include "util/variant/variant_encoding.h"
#include "util/variant/variant_field.h"

namespace doris {
namespace {

constexpr unsigned __int128 DECIMAL4_MAX = 999'999'999;
constexpr unsigned __int128 DECIMAL8_MAX = 999'999'999'999'999'999;
constexpr uint64_t CANONICAL_NAN_BITS = 0x7FF8000000000000ULL;
constexpr size_t CANONICAL_SIZE_PREFIX = sizeof(uint32_t);

uint32_t read_bounded_unsigned(StringRef bytes, size_t offset, uint8_t width, const char* field) {
    if (offset > bytes.size || width > bytes.size - offset) {
        throw Exception(ErrorCode::CORRUPTION, "Truncated Variant canonical cell while reading {}",
                        field);
    }
    uint32_t result = 0;
    for (uint8_t byte = 0; byte < width; ++byte) {
        result |= static_cast<uint32_t>(static_cast<uint8_t>(bytes.data[offset + byte]))
                  << (byte * 8);
    }
    return result;
}

constexpr unsigned __int128 max_decimal38() {
    unsigned __int128 value = 1;
    for (uint8_t digit = 0; digit < 38; ++digit) {
        value *= 10;
    }
    return value - 1;
}

constexpr unsigned __int128 MAX_DECIMAL38 = max_decimal38();

enum class CanonicalKind : uint8_t {
    NULL_VALUE = 0,
    BOOL = 1,
    EXACT_INTEGER = 2,
    DECIMAL = 3,
    FLOATING = 4,
    STRING = 5,
    BINARY = 6,
    DATE = 7,
    TIMESTAMP_TZ = 8,
    TIMESTAMP_NTZ = 9,
    TIME = 10,
    UUID = 11,
    OBJECT = 12,
    ARRAY = 13,
};

struct NormalizedValue {
    __int128 integer = 0;
    uint64_t floating_bits = 0;
    StringRef bytes;
    CanonicalKind kind = CanonicalKind::NULL_VALUE;
    uint8_t scale = 0;
    bool boolean = false;
    std::array<uint8_t, 16> uuid {};
};

struct ObjectEntry {
    StringRef key;
    VariantValueRef value;
};

NormalizedValue normalized_kind(CanonicalKind kind) {
    NormalizedValue result;
    result.kind = kind;
    return result;
}

NormalizedValue normalized_integer(CanonicalKind kind, __int128 value) {
    NormalizedValue result;
    result.kind = kind;
    result.integer = value;
    return result;
}

NormalizedValue normalized_floating(uint64_t bits) {
    NormalizedValue result;
    result.kind = CanonicalKind::FLOATING;
    result.floating_bits = bits;
    return result;
}

NormalizedValue normalized_bytes(CanonicalKind kind, StringRef bytes) {
    NormalizedValue result;
    result.kind = kind;
    result.bytes = bytes;
    return result;
}

unsigned __int128 magnitude(__int128 value) {
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    return value < 0 ? ~unsigned_value + 1 : unsigned_value;
}

void require_depth(uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical traversal exceeds maximum depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
}

void require_exact_value(VariantValueRef value) {
    const size_t encoded_size = value.value_size();
    if (encoded_size != value.size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant value has {} trailing bytes after its {} byte root",
                        value.size - encoded_size, encoded_size);
    }
}

void require_valid_utf8(StringRef value, const char* description) {
    if (value.size != 0 && !validate_utf8(value.data, value.size)) {
        throw Exception(ErrorCode::CORRUPTION, "Variant {} is not valid UTF-8", description);
    }
}

NormalizedValue normalize_floating(double value) {
    constexpr double INT128_UPPER_EXCLUSIVE = 0x1p127;
    if (std::isfinite(value) && std::trunc(value) == value && value >= -INT128_UPPER_EXCLUSIVE &&
        value < INT128_UPPER_EXCLUSIVE) {
        const auto integer = static_cast<__int128>(value);
        if (magnitude(integer) <= MAX_DECIMAL38) {
            return normalized_integer(CanonicalKind::EXACT_INTEGER, integer);
        }
    }
    if (std::isnan(value)) {
        return normalized_floating(CANONICAL_NAN_BITS);
    }
    auto bits = std::bit_cast<uint64_t>(value);
    if (value == 0) {
        bits = 0;
    }
    return normalized_floating(bits);
}

NormalizedValue normalize_primitive(VariantValueRef value) {
    switch (value.primitive_id()) {
    case VariantPrimitiveId::NULL_VALUE:
        return normalized_kind(CanonicalKind::NULL_VALUE);
    case VariantPrimitiveId::TRUE_VALUE: {
        NormalizedValue result = normalized_kind(CanonicalKind::BOOL);
        result.boolean = true;
        return result;
    }
    case VariantPrimitiveId::FALSE_VALUE:
        return normalized_kind(CanonicalKind::BOOL);
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        return normalized_integer(CanonicalKind::EXACT_INTEGER, value.get_int());
    case VariantPrimitiveId::DOUBLE:
        return normalize_floating(value.get_double());
    case VariantPrimitiveId::FLOAT:
        return normalize_floating(static_cast<double>(value.get_float()));
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16: {
        VariantDecimal decimal = value.get_decimal();
        if (magnitude(decimal.unscaled) > MAX_DECIMAL38) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant decimal unscaled value exceeds precision 38");
        }
        while (decimal.scale != 0 && decimal.unscaled % 10 == 0) {
            decimal.unscaled /= 10;
            --decimal.scale;
        }
        if (decimal.scale == 0) {
            return normalized_integer(CanonicalKind::EXACT_INTEGER, decimal.unscaled);
        }
        NormalizedValue result = normalized_integer(CanonicalKind::DECIMAL, decimal.unscaled);
        result.scale = decimal.scale;
        return result;
    }
    case VariantPrimitiveId::DATE:
        return normalized_integer(CanonicalKind::DATE, value.get_date());
    case VariantPrimitiveId::TIMESTAMP_MICROS:
        return normalized_integer(CanonicalKind::TIMESTAMP_TZ,
                                  static_cast<__int128>(value.get_timestamp_micros()) * 1000);
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
        return normalized_integer(CanonicalKind::TIMESTAMP_NTZ,
                                  static_cast<__int128>(value.get_timestamp_ntz_micros()) * 1000);
    case VariantPrimitiveId::TIMESTAMP_NANOS:
        return normalized_integer(CanonicalKind::TIMESTAMP_TZ, value.get_timestamp_nanos());
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        return normalized_integer(CanonicalKind::TIMESTAMP_NTZ, value.get_timestamp_ntz_nanos());
    case VariantPrimitiveId::BINARY:
        return normalized_bytes(CanonicalKind::BINARY, value.get_binary());
    case VariantPrimitiveId::STRING: {
        const StringRef string = value.get_string();
        require_valid_utf8(string, "string");
        return normalized_bytes(CanonicalKind::STRING, string);
    }
    case VariantPrimitiveId::TIME_NTZ_MICROS:
        return normalized_integer(CanonicalKind::TIME, value.get_time_ntz_micros());
    case VariantPrimitiveId::UUID: {
        NormalizedValue result = normalized_kind(CanonicalKind::UUID);
        result.uuid = value.get_uuid();
        return result;
    }
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown Variant primitive id {}",
                    static_cast<uint8_t>(value.primitive_id()));
}

NormalizedValue normalize_value(VariantValueRef value) {
    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING: {
        const StringRef string = value.get_string();
        require_valid_utf8(string, "string");
        return normalized_bytes(CanonicalKind::STRING, string);
    }
    case VariantBasicType::OBJECT:
        return normalized_kind(CanonicalKind::OBJECT);
    case VariantBasicType::ARRAY:
        return normalized_kind(CanonicalKind::ARRAY);
    case VariantBasicType::PRIMITIVE:
        return normalize_primitive(value);
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Unknown Variant basic type");
}

ObjectEntry object_entry_at(VariantValueRef object, uint32_t index, StringRef previous_key,
                            bool has_previous) {
    uint32_t field_id = 0;
    VariantValueRef child = object.object_value_at(index, &field_id);
    const StringRef key = object.metadata.key_at(field_id);
    require_valid_utf8(key, "object key");
    if (has_previous && previous_key.compare(key) >= 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant object keys are not strictly byte-sorted at field {}", index);
    }
    return {.key = key, .value = child};
}

bool scalar_equals(const NormalizedValue& left, const NormalizedValue& right) {
    switch (left.kind) {
    case CanonicalKind::NULL_VALUE:
        return true;
    case CanonicalKind::BOOL:
        return left.boolean == right.boolean;
    case CanonicalKind::EXACT_INTEGER:
    case CanonicalKind::DATE:
    case CanonicalKind::TIMESTAMP_TZ:
    case CanonicalKind::TIMESTAMP_NTZ:
    case CanonicalKind::TIME:
        return left.integer == right.integer;
    case CanonicalKind::DECIMAL:
        return left.integer == right.integer && left.scale == right.scale;
    case CanonicalKind::FLOATING:
        return left.floating_bits == right.floating_bits;
    case CanonicalKind::STRING:
    case CanonicalKind::BINARY:
        return left.bytes == right.bytes;
    case CanonicalKind::UUID:
        return left.uuid == right.uuid;
    case CanonicalKind::OBJECT:
    case CanonicalKind::ARRAY:
        break;
    }
    DCHECK(false) << "Container reached scalar equality";
    return false;
}

bool equals_node(VariantValueRef left, VariantValueRef right, uint32_t depth) {
    require_depth(depth);
    require_exact_value(left);
    require_exact_value(right);
    const NormalizedValue normalized_left = normalize_value(left);
    const NormalizedValue normalized_right = normalize_value(right);
    if (normalized_left.kind != normalized_right.kind) {
        return false;
    }
    if (normalized_left.kind == CanonicalKind::ARRAY) {
        const uint32_t count = left.num_elements();
        if (count != right.num_elements()) {
            return false;
        }
        for (uint32_t index = 0; index < count; ++index) {
            if (!equals_node(left.array_at(index), right.array_at(index), depth + 1)) {
                return false;
            }
        }
        return true;
    }
    if (normalized_left.kind == CanonicalKind::OBJECT) {
        const uint32_t count = left.num_elements();
        if (count != right.num_elements()) {
            return false;
        }
        StringRef previous_left;
        StringRef previous_right;
        for (uint32_t index = 0; index < count; ++index) {
            const ObjectEntry left_entry = object_entry_at(left, index, previous_left, index != 0);
            const ObjectEntry right_entry =
                    object_entry_at(right, index, previous_right, index != 0);
            if (left_entry.key != right_entry.key ||
                !equals_node(left_entry.value, right_entry.value, depth + 1)) {
                return false;
            }
            previous_left = left_entry.key;
            previous_right = right_entry.key;
        }
        return true;
    }
    return scalar_equals(normalized_left, normalized_right);
}

template <typename Sink>
void update_unsigned(Sink& sink, unsigned __int128 value, uint8_t width) {
    std::array<char, 16> bytes {};
    for (uint8_t index = 0; index < width; ++index) {
        bytes[index] = static_cast<char>(value >> (index * 8));
    }
    sink.update(bytes.data(), width);
}

template <typename Sink>
void update_signed(Sink& sink, __int128 value, uint8_t width) {
    update_unsigned(sink, static_cast<unsigned __int128>(value), width);
}

template <typename Sink>
void update_bytes(Sink& sink, StringRef bytes) {
    if (bytes.size > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::CORRUPTION, "Variant byte sequence exceeds uint32 length");
    }
    update_unsigned(sink, bytes.size, sizeof(uint32_t));
    if (bytes.size != 0) {
        sink.update(bytes.data, bytes.size);
    }
}

template <typename Sink>
void hash_normalized_scalar(const NormalizedValue& normalized, Sink& sink) {
    switch (normalized.kind) {
    case CanonicalKind::NULL_VALUE:
        return;
    case CanonicalKind::BOOL: {
        const char boolean = normalized.boolean ? 1 : 0;
        sink.update(&boolean, 1);
        return;
    }
    case CanonicalKind::EXACT_INTEGER:
        update_signed(sink, normalized.integer, 16);
        return;
    case CanonicalKind::DECIMAL:
        update_signed(sink, normalized.integer, 16);
        sink.update(reinterpret_cast<const char*>(&normalized.scale), 1);
        return;
    case CanonicalKind::FLOATING:
        update_unsigned(sink, normalized.floating_bits, sizeof(uint64_t));
        return;
    case CanonicalKind::STRING:
    case CanonicalKind::BINARY:
        update_bytes(sink, normalized.bytes);
        return;
    case CanonicalKind::DATE:
        update_signed(sink, normalized.integer, sizeof(int32_t));
        return;
    case CanonicalKind::TIMESTAMP_TZ:
    case CanonicalKind::TIMESTAMP_NTZ:
        update_signed(sink, normalized.integer, 16);
        return;
    case CanonicalKind::TIME:
        update_signed(sink, normalized.integer, sizeof(int64_t));
        return;
    case CanonicalKind::UUID:
        sink.update(reinterpret_cast<const char*>(normalized.uuid.data()), normalized.uuid.size());
        return;
    case CanonicalKind::OBJECT:
    case CanonicalKind::ARRAY:
        break;
    }
    DCHECK(false) << "Container reached scalar hash";
}

template <typename Sink>
void hash_node(VariantValueRef value, Sink& sink, uint32_t depth) {
    require_depth(depth);
    require_exact_value(value);
    const NormalizedValue normalized = normalize_value(value);
    const char tag = static_cast<char>(normalized.kind);
    sink.update(&tag, 1);

    switch (normalized.kind) {
    case CanonicalKind::OBJECT: {
        const uint32_t count = value.num_elements();
        update_unsigned(sink, count, sizeof(uint32_t));
        StringRef previous;
        for (uint32_t index = 0; index < count; ++index) {
            const ObjectEntry entry = object_entry_at(value, index, previous, index != 0);
            update_bytes(sink, entry.key);
            hash_node(entry.value, sink, depth + 1);
            previous = entry.key;
        }
        return;
    }
    case CanonicalKind::ARRAY: {
        const uint32_t count = value.num_elements();
        update_unsigned(sink, count, sizeof(uint32_t));
        for (uint32_t index = 0; index < count; ++index) {
            hash_node(value.array_at(index), sink, depth + 1);
        }
        return;
    }
    default:
        hash_normalized_scalar(normalized, sink);
        return;
    }
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

uint8_t minimum_integer_width(__int128 value) {
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

bool fits_int64(__int128 value) {
    return value >= std::numeric_limits<int64_t>::min() &&
           value <= std::numeric_limits<int64_t>::max();
}

size_t scalar_encoded_size(const NormalizedValue& value) {
    switch (value.kind) {
    case CanonicalKind::NULL_VALUE:
    case CanonicalKind::BOOL:
        return 1;
    case CanonicalKind::EXACT_INTEGER:
        return fits_int64(value.integer) ? 1 + minimum_integer_width(value.integer) : 18;
    case CanonicalKind::DECIMAL:
        return 2 + minimum_decimal_width(value.integer);
    case CanonicalKind::FLOATING:
        return 1 + sizeof(uint64_t);
    case CanonicalKind::STRING:
        return value.bytes.size <= VARIANT_MAX_SHORT_STRING_SIZE
                       ? 1 + value.bytes.size
                       : 1 + sizeof(uint32_t) + value.bytes.size;
    case CanonicalKind::BINARY:
        return 1 + sizeof(uint32_t) + value.bytes.size;
    case CanonicalKind::DATE:
        return 1 + sizeof(int32_t);
    case CanonicalKind::TIMESTAMP_TZ:
    case CanonicalKind::TIMESTAMP_NTZ:
    case CanonicalKind::TIME:
        return 1 + sizeof(int64_t);
    case CanonicalKind::UUID:
        return 1 + 16;
    case CanonicalKind::OBJECT:
    case CanonicalKind::ARRAY:
        break;
    }
    DCHECK(false) << "Container reached scalar size";
    return 0;
}

struct PlanNode {
    NormalizedValue normalized;
    size_t encoded_size = 0;
    uint32_t values_size = 0;
    size_t children_begin = 0;
    uint32_t child_count = 0;
    uint8_t count_width = 0;
    uint8_t offset_width = 0;
    uint8_t id_width = 0;
};

struct PlanChild {
    uint32_t node_index = 0;
    uint32_t field_id = 0;
    StringRef key;
};

struct SerializePlan {
    std::vector<PlanNode> nodes;
    std::vector<PlanChild> children;
    std::vector<StringRef> keys;
};

uint32_t build_plan_node(SerializePlan& plan, VariantValueRef value, uint32_t depth) {
    require_depth(depth);
    require_exact_value(value);
    if (plan.nodes.size() == std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical plan exceeds uint32 node limit");
    }
    const NormalizedValue normalized = normalize_value(value);
    const auto node_index = static_cast<uint32_t>(plan.nodes.size());
    PlanNode node;
    node.normalized = normalized;
    plan.nodes.push_back(node);
    if (normalized.kind != CanonicalKind::OBJECT && normalized.kind != CanonicalKind::ARRAY) {
        plan.nodes[node_index].encoded_size = scalar_encoded_size(normalized);
        return node_index;
    }

    const uint32_t count = value.num_elements();
    if (count > plan.children.max_size() - plan.children.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical plan exceeds addressable child count");
    }
    const size_t children_begin = plan.children.size();
    plan.children.resize(children_begin + count);
    plan.nodes[node_index].children_begin = children_begin;
    plan.nodes[node_index].child_count = count;

    StringRef previous;
    for (uint32_t index = 0; index < count; ++index) {
        VariantValueRef child;
        StringRef key;
        if (normalized.kind == CanonicalKind::OBJECT) {
            const ObjectEntry entry = object_entry_at(value, index, previous, index != 0);
            child = entry.value;
            key = entry.key;
            if (plan.keys.size() == std::numeric_limits<uint32_t>::max()) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant canonical dictionary exceeds uint32 key references");
            }
            plan.keys.push_back(key);
            previous = key;
        } else {
            child = value.array_at(index);
        }
        const uint32_t child_node = build_plan_node(plan, child, depth + 1);
        PlanChild& planned_child = plan.children[children_begin + index];
        planned_child.node_index = child_node;
        planned_child.key = key;
    }
    return node_index;
}

bool string_ref_less(StringRef left, StringRef right) {
    return left.compare(right) < 0;
}

void finish_plan(SerializePlan& plan) {
    std::ranges::sort(plan.keys, string_ref_less);
    plan.keys.erase(std::ranges::unique(plan.keys).begin(), plan.keys.end());
    if (plan.keys.size() > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical dictionary exceeds uint32 key limit");
    }

    for (PlanNode& node : plan.nodes) {
        if (node.normalized.kind != CanonicalKind::OBJECT) {
            continue;
        }
        uint32_t previous_id = 0;
        for (uint32_t index = 0; index < node.child_count; ++index) {
            PlanChild& child = plan.children[node.children_begin + index];
            const auto position = std::ranges::lower_bound(plan.keys, child.key, string_ref_less);
            DCHECK(position != plan.keys.end() && *position == child.key);
            child.field_id = static_cast<uint32_t>(position - plan.keys.begin());
            if (index != 0) {
                DCHECK_LT(previous_id, child.field_id);
            }
            previous_id = child.field_id;
        }
    }

    for (size_t node_position = plan.nodes.size(); node_position != 0; --node_position) {
        PlanNode& node = plan.nodes[node_position - 1];
        if (node.normalized.kind != CanonicalKind::OBJECT &&
            node.normalized.kind != CanonicalKind::ARRAY) {
            continue;
        }
        node.count_width = node.child_count > std::numeric_limits<uint8_t>::max() ? sizeof(uint32_t)
                                                                                  : sizeof(uint8_t);
        uint32_t values_size = 0;
        for (uint32_t index = 0; index < node.child_count; ++index) {
            const size_t child_size =
                    plan.nodes[plan.children[node.children_begin + index].node_index].encoded_size;
            if (child_size > std::numeric_limits<uint32_t>::max() - values_size) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant canonical container values exceed uint32 byte limit");
            }
            values_size += static_cast<uint32_t>(child_size);
        }
        node.values_size = values_size;
        node.offset_width = minimum_unsigned_width(values_size);
        if (node.normalized.kind == CanonicalKind::OBJECT) {
            const uint32_t maximum_id =
                    node.child_count == 0
                            ? 0
                            : plan.children[node.children_begin + node.child_count - 1].field_id;
            node.id_width = minimum_unsigned_width(maximum_id);
        }

        uint64_t encoded_size = 1 + node.count_width + values_size;
        encoded_size += (static_cast<uint64_t>(node.child_count) + 1) * node.offset_width;
        if (node.normalized.kind == CanonicalKind::OBJECT) {
            encoded_size += static_cast<uint64_t>(node.child_count) * node.id_width;
        }
        if (encoded_size > std::numeric_limits<size_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant canonical value exceeds addressable output size");
        }
        node.encoded_size = static_cast<size_t>(encoded_size);
    }
}

void write_unsigned(char*& output, unsigned __int128 value, uint8_t width) noexcept {
    for (uint8_t index = 0; index < width; ++index) {
        *output++ = static_cast<char>(value >> (index * 8));
    }
}

void write_signed(char*& output, __int128 value, uint8_t width) noexcept {
    write_unsigned(output, static_cast<unsigned __int128>(value), width);
}

void write_primitive_header(char*& output, VariantPrimitiveId id) noexcept {
    *output++ = static_cast<char>(static_cast<uint8_t>(id) << VARIANT_VALUE_HEADER_SHIFT);
}

void write_exact_integer(__int128 value, char*& output) noexcept {
    if (!fits_int64(value)) {
        write_primitive_header(output, VariantPrimitiveId::DECIMAL16);
        *output++ = 0;
        write_signed(output, value, 16);
        return;
    }
    const uint8_t width = minimum_integer_width(value);
    VariantPrimitiveId id = VariantPrimitiveId::INT64;
    if (width == 1) {
        id = VariantPrimitiveId::INT8;
    } else if (width == 2) {
        id = VariantPrimitiveId::INT16;
    } else if (width == 4) {
        id = VariantPrimitiveId::INT32;
    }
    write_primitive_header(output, id);
    write_signed(output, value, width);
}

void write_decimal(const NormalizedValue& value, char*& output) noexcept {
    const uint8_t width = minimum_decimal_width(value.integer);
    VariantPrimitiveId id = VariantPrimitiveId::DECIMAL16;
    if (width == 4) {
        id = VariantPrimitiveId::DECIMAL4;
    } else if (width == 8) {
        id = VariantPrimitiveId::DECIMAL8;
    }
    write_primitive_header(output, id);
    *output++ = static_cast<char>(value.scale);
    write_signed(output, value.integer, width);
}

void copy_bytes(StringRef bytes, char*& output) noexcept {
    if (bytes.size != 0) {
        std::memcpy(output, bytes.data, bytes.size);
        output += bytes.size;
    }
}

void write_string(StringRef string, char*& output) noexcept {
    if (string.size <= VARIANT_MAX_SHORT_STRING_SIZE) {
        *output++ = static_cast<char>((string.size << VARIANT_VALUE_HEADER_SHIFT) |
                                      static_cast<uint8_t>(VariantBasicType::SHORT_STRING));
    } else {
        write_primitive_header(output, VariantPrimitiveId::STRING);
        write_unsigned(output, string.size, sizeof(uint32_t));
    }
    copy_bytes(string, output);
}

void write_binary(StringRef binary, char*& output) noexcept {
    write_primitive_header(output, VariantPrimitiveId::BINARY);
    write_unsigned(output, binary.size, sizeof(uint32_t));
    copy_bytes(binary, output);
}

void write_timestamp(const NormalizedValue& value, char*& output) noexcept {
    const bool use_micros = value.integer % 1000 == 0;
    const bool utc_adjusted = value.kind == CanonicalKind::TIMESTAMP_TZ;
    VariantPrimitiveId id;
    __int128 physical_value;
    if (use_micros) {
        id = utc_adjusted ? VariantPrimitiveId::TIMESTAMP_MICROS
                          : VariantPrimitiveId::TIMESTAMP_NTZ_MICROS;
        physical_value = value.integer / 1000;
    } else {
        id = utc_adjusted ? VariantPrimitiveId::TIMESTAMP_NANOS
                          : VariantPrimitiveId::TIMESTAMP_NTZ_NANOS;
        physical_value = value.integer;
    }
    DCHECK(fits_int64(physical_value));
    write_primitive_header(output, id);
    write_signed(output, physical_value, sizeof(int64_t));
}

void write_scalar(const NormalizedValue& value, char*& output) noexcept {
    switch (value.kind) {
    case CanonicalKind::NULL_VALUE:
        write_primitive_header(output, VariantPrimitiveId::NULL_VALUE);
        return;
    case CanonicalKind::BOOL:
        write_primitive_header(output, value.boolean ? VariantPrimitiveId::TRUE_VALUE
                                                     : VariantPrimitiveId::FALSE_VALUE);
        return;
    case CanonicalKind::EXACT_INTEGER:
        write_exact_integer(value.integer, output);
        return;
    case CanonicalKind::DECIMAL:
        write_decimal(value, output);
        return;
    case CanonicalKind::FLOATING:
        write_primitive_header(output, VariantPrimitiveId::DOUBLE);
        write_unsigned(output, value.floating_bits, sizeof(uint64_t));
        return;
    case CanonicalKind::STRING:
        write_string(value.bytes, output);
        return;
    case CanonicalKind::BINARY:
        write_binary(value.bytes, output);
        return;
    case CanonicalKind::DATE:
        write_primitive_header(output, VariantPrimitiveId::DATE);
        write_signed(output, value.integer, sizeof(int32_t));
        return;
    case CanonicalKind::TIMESTAMP_TZ:
    case CanonicalKind::TIMESTAMP_NTZ:
        write_timestamp(value, output);
        return;
    case CanonicalKind::TIME:
        write_primitive_header(output, VariantPrimitiveId::TIME_NTZ_MICROS);
        write_signed(output, value.integer, sizeof(int64_t));
        return;
    case CanonicalKind::UUID:
        write_primitive_header(output, VariantPrimitiveId::UUID);
        for (uint8_t byte : value.uuid) {
            *output++ = static_cast<char>(byte);
        }
        return;
    case CanonicalKind::OBJECT:
    case CanonicalKind::ARRAY:
        break;
    }
    DCHECK(false) << "Container reached scalar writer";
}

void write_plan_node(const SerializePlan& plan, uint32_t node_index, char*& output) noexcept {
    const PlanNode& node = plan.nodes[node_index];
    if (node.normalized.kind != CanonicalKind::OBJECT &&
        node.normalized.kind != CanonicalKind::ARRAY) {
        write_scalar(node.normalized, output);
        return;
    }

    const bool is_object = node.normalized.kind == CanonicalKind::OBJECT;
    const bool is_large = node.count_width == sizeof(uint32_t);
    if (is_object) {
        const auto value_header =
                static_cast<uint8_t>(((node.offset_width - 1) << VARIANT_OBJECT_OFFSET_SIZE_SHIFT) |
                                     ((node.id_width - 1) << VARIANT_OBJECT_ID_SIZE_SHIFT) |
                                     (is_large ? VARIANT_OBJECT_LARGE_MASK : 0));
        *output++ = static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                      static_cast<uint8_t>(VariantBasicType::OBJECT));
    } else {
        const auto value_header =
                static_cast<uint8_t>(((node.offset_width - 1) << VARIANT_ARRAY_OFFSET_SIZE_SHIFT) |
                                     (is_large ? VARIANT_ARRAY_LARGE_MASK : 0));
        *output++ = static_cast<char>((value_header << VARIANT_VALUE_HEADER_SHIFT) |
                                      static_cast<uint8_t>(VariantBasicType::ARRAY));
    }
    write_unsigned(output, node.child_count, node.count_width);
    if (is_object) {
        for (uint32_t index = 0; index < node.child_count; ++index) {
            write_unsigned(output, plan.children[node.children_begin + index].field_id,
                           node.id_width);
        }
    }

    uint32_t offset = 0;
    write_unsigned(output, offset, node.offset_width);
    for (uint32_t index = 0; index < node.child_count; ++index) {
        offset += static_cast<uint32_t>(
                plan.nodes[plan.children[node.children_begin + index].node_index].encoded_size);
        write_unsigned(output, offset, node.offset_width);
    }
    DCHECK_EQ(offset, node.values_size);
    for (uint32_t index = 0; index < node.child_count; ++index) {
        write_plan_node(plan, plan.children[node.children_begin + index].node_index, output);
    }
}

size_t metadata_encoded_size(const std::vector<StringRef>& keys, uint8_t* width_out) {
    uint64_t strings_size = 0;
    for (StringRef key : keys) {
        strings_size += key.size;
        if (strings_size > std::numeric_limits<uint32_t>::max()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant canonical metadata strings exceed uint32 byte limit");
        }
    }
    const uint8_t width = minimum_unsigned_width(std::max<uint64_t>(keys.size(), strings_size));
    const uint64_t encoded_size = 1 + width + (keys.size() + 1) * width + strings_size;
    if (encoded_size > std::numeric_limits<size_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical metadata exceeds addressable output size");
    }
    *width_out = width;
    return static_cast<size_t>(encoded_size);
}

void write_metadata(const std::vector<StringRef>& keys, uint8_t width, char*& output) noexcept {
    *output++ = static_cast<char>(
            VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK |
            static_cast<uint8_t>((width - 1) << VARIANT_METADATA_OFFSET_SIZE_SHIFT));
    write_unsigned(output, keys.size(), width);
    uint32_t offset = 0;
    write_unsigned(output, offset, width);
    for (StringRef key : keys) {
        offset += static_cast<uint32_t>(key.size);
        write_unsigned(output, offset, width);
    }
    for (StringRef key : keys) {
        if (key.size != 0) {
            std::memcpy(output, key.data, key.size);
            output += key.size;
        }
    }
}

} // namespace

struct VariantCanonicalScalarAdapter {
    static NormalizedValue normalize(VariantCanonicalScalarRef value) {
        switch (value._kind) {
        case VariantCanonicalScalarRef::Kind::NULL_VALUE:
            return normalized_kind(CanonicalKind::NULL_VALUE);
        case VariantCanonicalScalarRef::Kind::BOOL: {
            NormalizedValue normalized = normalized_kind(CanonicalKind::BOOL);
            normalized.boolean = value._boolean;
            return normalized;
        }
        case VariantCanonicalScalarRef::Kind::EXACT_INTEGER:
            return normalized_integer(CanonicalKind::EXACT_INTEGER, value._integer);
        case VariantCanonicalScalarRef::Kind::DECIMAL: {
            __int128 unscaled = value._integer;
            uint8_t scale = value._scale;
            while (scale != 0 && unscaled % 10 == 0) {
                unscaled /= 10;
                --scale;
            }
            if (scale == 0) {
                return normalized_integer(CanonicalKind::EXACT_INTEGER, unscaled);
            }
            NormalizedValue normalized = normalized_integer(CanonicalKind::DECIMAL, unscaled);
            normalized.scale = scale;
            return normalized;
        }
        case VariantCanonicalScalarRef::Kind::FLOATING:
            return normalize_floating(value._floating);
        case VariantCanonicalScalarRef::Kind::STRING:
            return normalized_bytes(CanonicalKind::STRING, value._bytes);
        case VariantCanonicalScalarRef::Kind::BINARY:
            return normalized_bytes(CanonicalKind::BINARY, value._bytes);
        case VariantCanonicalScalarRef::Kind::DATE:
            return normalized_integer(CanonicalKind::DATE, value._integer);
        case VariantCanonicalScalarRef::Kind::TIMESTAMP_TZ:
            return normalized_integer(CanonicalKind::TIMESTAMP_TZ, value._integer);
        case VariantCanonicalScalarRef::Kind::TIMESTAMP_NTZ:
            return normalized_integer(CanonicalKind::TIMESTAMP_NTZ, value._integer);
        case VariantCanonicalScalarRef::Kind::TIME:
            return normalized_integer(CanonicalKind::TIME, value._integer);
        case VariantCanonicalScalarRef::Kind::UUID: {
            NormalizedValue normalized = normalized_kind(CanonicalKind::UUID);
            normalized.uuid = value._uuid;
            return normalized;
        }
        }
        __builtin_unreachable();
    }
};

struct CanonicalSerializationPlan::Impl {
    SerializePlan plan;
    size_t cell_size = 0;
    uint32_t payload_size = 0;
    uint32_t metadata_width = 0;
};

CanonicalSerializationPlan::CanonicalSerializationPlan(std::unique_ptr<Impl> impl) noexcept
        : _impl(std::move(impl)) {}

CanonicalSerializationPlan::CanonicalSerializationPlan(CanonicalSerializationPlan&&) noexcept =
        default;

CanonicalSerializationPlan& CanonicalSerializationPlan::operator=(
        CanonicalSerializationPlan&&) noexcept = default;

CanonicalSerializationPlan::~CanonicalSerializationPlan() = default;

size_t CanonicalSerializationPlan::size() const noexcept {
    return _impl == nullptr ? 0 : _impl->cell_size;
}

void CanonicalSerializationPlan::write(char* destination, size_t capacity) const {
    if (_impl == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot write a moved-from Variant canonical serialization plan");
    }
    if (destination == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical serialization destination is null");
    }
    if (capacity < _impl->cell_size) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical serialization needs {} bytes, but capacity is {}",
                        _impl->cell_size, capacity);
    }

    char* output = destination;
    write_unsigned(output, _impl->payload_size, sizeof(uint32_t));
    write_metadata(_impl->plan.keys, static_cast<uint8_t>(_impl->metadata_width), output);
    write_plan_node(_impl->plan, 0, output);
    DCHECK_EQ(output, destination + _impl->cell_size);
}

void VariantXxHashSink::update(const char* data, size_t size) {
    _state = HashUtil::xxHash64WithSeed(data, size, _state);
}

void VariantCrc32HashSink::update(const char* data, size_t size) {
    while (size != 0) {
        const auto chunk =
                static_cast<uint32_t>(std::min<size_t>(size, std::numeric_limits<uint32_t>::max()));
        _state = HashUtil::zlib_crc_hash(data, chunk, _state);
        data += chunk;
        size -= chunk;
    }
}

void VariantCrc32cHashSink::update(const char* data, size_t size) {
    _state = crc32c_extend(_state, reinterpret_cast<const uint8_t*>(data), size);
}

bool canonical_equals(VariantValueRef left, VariantValueRef right) {
    return equals_node(left, right, 0);
}

template <typename Sink>
void canonical_hash(VariantValueRef value, Sink& sink) {
    hash_node(value, sink, 0);
}

template void canonical_hash<SipHash>(VariantValueRef value, SipHash& sink);
template void canonical_hash<VariantXxHashSink>(VariantValueRef value, VariantXxHashSink& sink);
template void canonical_hash<VariantCrc32HashSink>(VariantValueRef value,
                                                   VariantCrc32HashSink& sink);
template void canonical_hash<VariantCrc32cHashSink>(VariantValueRef value,
                                                    VariantCrc32cHashSink& sink);

VariantCanonicalScalarRef VariantCanonicalScalarRef::null_value() noexcept {
    return VariantCanonicalScalarRef(Kind::NULL_VALUE);
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::boolean(bool value) noexcept {
    VariantCanonicalScalarRef result(Kind::BOOL);
    result._boolean = value;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::exact_integer(__int128 value) {
    if (magnitude(value) > MAX_DECIMAL38) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant exact integer exceeds the decimal38 canonical domain");
    }
    VariantCanonicalScalarRef result(Kind::EXACT_INTEGER);
    result._integer = value;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::decimal(__int128 unscaled, uint8_t scale) {
    if (scale > 38) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant decimal scale {} is outside [0, 38]",
                        scale);
    }
    if (magnitude(unscaled) > MAX_DECIMAL38) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal unscaled value exceeds precision 38");
    }
    VariantCanonicalScalarRef result(Kind::DECIMAL);
    result._integer = unscaled;
    result._scale = scale;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::float32(float value) noexcept {
    VariantCanonicalScalarRef result(Kind::FLOATING);
    result._floating = value;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::float64(double value) noexcept {
    VariantCanonicalScalarRef result(Kind::FLOATING);
    result._floating = value;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::string(StringRef value) {
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
    VariantCanonicalScalarRef result(Kind::STRING);
    result._bytes = value;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::binary(StringRef value) {
    if (value.data == nullptr && value.size != 0) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant binary has a null data pointer");
    }
    if (value.size > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant binary exceeds the uint32 byte limit");
    }
    VariantCanonicalScalarRef result(Kind::BINARY);
    result._bytes = value;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::date(int32_t days_since_epoch) noexcept {
    VariantCanonicalScalarRef result(Kind::DATE);
    result._integer = days_since_epoch;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::timestamp_micros(int64_t value,
                                                                      bool utc_adjusted) noexcept {
    VariantCanonicalScalarRef result(utc_adjusted ? Kind::TIMESTAMP_TZ : Kind::TIMESTAMP_NTZ);
    result._integer = static_cast<__int128>(value) * 1000;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::timestamp_nanos(int64_t value,
                                                                     bool utc_adjusted) noexcept {
    VariantCanonicalScalarRef result(utc_adjusted ? Kind::TIMESTAMP_TZ : Kind::TIMESTAMP_NTZ);
    result._integer = value;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::time_ntz_micros(int64_t value) noexcept {
    VariantCanonicalScalarRef result(Kind::TIME);
    result._integer = value;
    return result;
}

VariantCanonicalScalarRef VariantCanonicalScalarRef::uuid(
        const std::array<uint8_t, 16>& value) noexcept {
    VariantCanonicalScalarRef result(Kind::UUID);
    result._uuid = value;
    return result;
}

bool canonical_equals(VariantCanonicalScalarRef left, VariantCanonicalScalarRef right) {
    const NormalizedValue normalized_left = VariantCanonicalScalarAdapter::normalize(left);
    const NormalizedValue normalized_right = VariantCanonicalScalarAdapter::normalize(right);
    return normalized_left.kind == normalized_right.kind &&
           scalar_equals(normalized_left, normalized_right);
}

template <typename Sink>
void canonical_hash(VariantCanonicalScalarRef value, Sink& sink) {
    const NormalizedValue normalized = VariantCanonicalScalarAdapter::normalize(value);
    const char tag = static_cast<char>(normalized.kind);
    sink.update(&tag, 1);
    hash_normalized_scalar(normalized, sink);
}

template void canonical_hash<SipHash>(VariantCanonicalScalarRef value, SipHash& sink);
template void canonical_hash<VariantXxHashSink>(VariantCanonicalScalarRef value,
                                                VariantXxHashSink& sink);
template void canonical_hash<VariantCrc32HashSink>(VariantCanonicalScalarRef value,
                                                   VariantCrc32HashSink& sink);
template void canonical_hash<VariantCrc32cHashSink>(VariantCanonicalScalarRef value,
                                                    VariantCrc32cHashSink& sink);

void CanonicalScalarSerializationPlan::write(char* destination, size_t capacity) const {
    if (destination == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical serialization destination is null");
    }
    if (capacity < _cell_size) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical serialization needs {} bytes, but capacity is {}",
                        _cell_size, capacity);
    }
    const NormalizedValue normalized = VariantCanonicalScalarAdapter::normalize(_value);
    const size_t value_size = scalar_encoded_size(normalized);
    constexpr size_t EMPTY_METADATA_SIZE = 3;
    DCHECK_EQ(_cell_size, CANONICAL_SIZE_PREFIX + EMPTY_METADATA_SIZE + value_size);

    char* output = destination;
    write_unsigned(output, EMPTY_METADATA_SIZE + value_size, sizeof(uint32_t));
    *output++ = static_cast<char>(VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK);
    *output++ = 0;
    *output++ = 0;
    write_scalar(normalized, output);
    DCHECK_EQ(output, destination + _cell_size);
}

CanonicalScalarSerializationPlan prepare_canonical_serialize(VariantCanonicalScalarRef value) {
    const NormalizedValue normalized = VariantCanonicalScalarAdapter::normalize(value);
    constexpr size_t EMPTY_METADATA_SIZE = 3;
    const size_t value_size = scalar_encoded_size(normalized);
    if (value_size > std::numeric_limits<uint32_t>::max() - EMPTY_METADATA_SIZE) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical arena payload exceeds uint32 byte limit");
    }
    return {value, CANONICAL_SIZE_PREFIX + EMPTY_METADATA_SIZE + value_size};
}

CanonicalSerializationPlan prepare_canonical_serialize(VariantValueRef value) {
    auto implementation = std::make_unique<CanonicalSerializationPlan::Impl>();
    build_plan_node(implementation->plan, value, 0);
    finish_plan(implementation->plan);
    DCHECK(!implementation->plan.nodes.empty());

    uint8_t metadata_width = 0;
    const size_t metadata_size = metadata_encoded_size(implementation->plan.keys, &metadata_width);
    const size_t value_size = implementation->plan.nodes.front().encoded_size;
    if (metadata_size > std::numeric_limits<uint32_t>::max() ||
        value_size > std::numeric_limits<uint32_t>::max() - metadata_size) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical arena payload exceeds uint32 byte limit");
    }
    implementation->payload_size = static_cast<uint32_t>(metadata_size + value_size);
    implementation->metadata_width = metadata_width;
    constexpr size_t PREFIX_SIZE = sizeof(uint32_t);
    implementation->cell_size = PREFIX_SIZE + implementation->payload_size;
    return CanonicalSerializationPlan(std::move(implementation));
}

size_t canonical_serialize(VariantValueRef value, std::string& destination) {
    const CanonicalSerializationPlan plan = prepare_canonical_serialize(value);
    const size_t cell_size = plan.size();
    if (cell_size > destination.max_size() - destination.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical arena exceeds destination address space");
    }

    const size_t original_size = destination.size();
    destination.resize(original_size + cell_size);
    plan.write(destination.data() + original_size, cell_size);
    return cell_size;
}

VariantValueRef parse_canonical_serialized(StringRef serialized) {
    if (serialized.size != 0 && serialized.data == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant canonical cell has a null pointer for {} bytes", serialized.size);
    }
    if (serialized.size < CANONICAL_SIZE_PREFIX) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Truncated Variant canonical cell size prefix: need {} bytes, have {}",
                        CANONICAL_SIZE_PREFIX, serialized.size);
    }

    const uint32_t payload_size =
            read_bounded_unsigned(serialized, 0, CANONICAL_SIZE_PREFIX, "payload size");
    if (payload_size != serialized.size - CANONICAL_SIZE_PREFIX) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant canonical payload size {} does not match {} available bytes",
                        payload_size, serialized.size - CANONICAL_SIZE_PREFIX);
    }

    const StringRef payload(serialized.data + CANONICAL_SIZE_PREFIX, payload_size);
    constexpr size_t METADATA_HEADER_SIZE = 1;
    if (payload.size < METADATA_HEADER_SIZE) {
        throw Exception(ErrorCode::CORRUPTION, "Truncated Variant canonical metadata header");
    }
    const auto metadata_header = static_cast<uint8_t>(payload.data[0]);
    const auto byte_width =
            static_cast<uint8_t>(((metadata_header >> VARIANT_METADATA_OFFSET_SIZE_SHIFT) &
                                  VARIANT_METADATA_OFFSET_SIZE_MASK) +
                                 1);
    constexpr size_t KEY_COUNT_OFFSET = METADATA_HEADER_SIZE;
    const uint32_t key_count = read_bounded_unsigned(payload, KEY_COUNT_OFFSET, byte_width,
                                                     "metadata dictionary size");
    const size_t offsets_offset = METADATA_HEADER_SIZE + byte_width;
    const size_t offsets_count = static_cast<size_t>(key_count) + 1;
    if (offsets_offset > payload.size ||
        offsets_count > (payload.size - offsets_offset) / byte_width) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Truncated Variant canonical metadata dictionary offsets");
    }
    const size_t offsets_size = offsets_count * byte_width;
    const uint32_t strings_size = read_bounded_unsigned(
            payload, offsets_offset + static_cast<size_t>(key_count) * byte_width, byte_width,
            "metadata dictionary strings size");
    const size_t strings_offset = offsets_offset + offsets_size;
    if (strings_size > payload.size - strings_offset) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Truncated Variant canonical metadata strings: need {} bytes, have {}",
                        strings_size, payload.size - strings_offset);
    }
    const size_t metadata_size = strings_offset + strings_size;
    if (metadata_size == payload.size) {
        throw Exception(ErrorCode::CORRUPTION, "Variant canonical cell does not contain a value");
    }

    VariantMetadataRef metadata {.data = payload.data, .size = metadata_size};
    VariantValueRef value {.metadata = metadata,
                           .data = payload.data + metadata_size,
                           .size = payload.size - metadata_size};
    validate_variant_metadata(metadata);
    validate_variant_payload(value);
    return value;
}

} // namespace doris
