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

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

#include "common/check.h"
#include "common/exception.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"

namespace doris::variant_v2_serde_binary_internal {

constexpr std::array<uint8_t, 4> MAGIC {'D', 'V', '2', 'X'};
constexpr uint8_t FORMAT_VERSION = 1;
constexpr size_t HEADER_BYTES = 24;
constexpr size_t ENCODED_DESCRIPTOR_BYTES = 24;
constexpr size_t TYPED_DESCRIPTOR_BYTES = 20;
constexpr size_t TYPE_META_BYTES = 16;

enum class State : uint8_t { ENCODED = 0, TYPED = 1 };

enum class PhysicalTypeCode : uint16_t {
    BOOL = 1,
    TINYINT = 2,
    SMALLINT = 3,
    INT = 4,
    BIGINT = 5,
    LARGEINT = 6,
    FLOAT = 7,
    DOUBLE = 8,
    DECIMALV2 = 9,
    DECIMAL32 = 10,
    DECIMAL64 = 11,
    DECIMAL128I = 12,
    DATE = 14,
    DATEV2 = 15,
    DATETIME = 16,
    DATETIMEV2 = 17,
    TIMESTAMPTZ = 18,
    CHAR = 19,
    VARCHAR = 20,
    STRING = 21,
    IPV4 = 22,
    IPV6 = 23,
};

struct TypeMeta {
    PhysicalTypeCode code;
    uint32_t precision = 0;
    uint32_t scale = 0;
    uint32_t length_code = 0;
};

class Writer {
public:
    explicit Writer(std::span<uint8_t> bytes) : _bytes(bytes) {}

    void u8(uint8_t value) {
        DCHECK_LT(_position, _bytes.size());
        _bytes[_position++] = value;
    }

    void u16(uint16_t value) {
        u8(static_cast<uint8_t>(value));
        u8(static_cast<uint8_t>(value >> 8));
    }

    void u32(uint32_t value) {
        for (uint8_t index = 0; index < sizeof(value); ++index) {
            u8(static_cast<uint8_t>(value >> (index * 8)));
        }
    }

    void u64(uint64_t value) {
        for (uint8_t index = 0; index < sizeof(value); ++index) {
            u8(static_cast<uint8_t>(value >> (index * 8)));
        }
    }

    void raw(std::span<const uint8_t> value) {
        DCHECK_LE(value.size(), _bytes.size() - _position);
        std::ranges::copy(value, _bytes.begin() + _position);
        _position += value.size();
    }

    void raw(StringRef value) {
        raw(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(value.data), value.size));
    }

    size_t position() const noexcept { return _position; }

private:
    std::span<uint8_t> _bytes;
    size_t _position = 0;
};

class Reader {
public:
    explicit Reader(std::span<const uint8_t> bytes) : _bytes(bytes) {}

    uint8_t u8(std::string_view description) {
        require(sizeof(uint8_t), description);
        return _bytes[_position++];
    }

    uint16_t u16(std::string_view description) {
        uint16_t value = 0;
        for (uint8_t index = 0; index < sizeof(value); ++index) {
            value |= static_cast<uint16_t>(u8(description)) << (index * 8);
        }
        return value;
    }

    uint32_t u32(std::string_view description) {
        uint32_t value = 0;
        for (uint8_t index = 0; index < sizeof(value); ++index) {
            value |= static_cast<uint32_t>(u8(description)) << (index * 8);
        }
        return value;
    }

    uint64_t u64(std::string_view description) {
        uint64_t value = 0;
        for (uint8_t index = 0; index < sizeof(value); ++index) {
            value |= static_cast<uint64_t>(u8(description)) << (index * 8);
        }
        return value;
    }

    std::span<const uint8_t> raw(size_t size, std::string_view description) {
        require(size, description);
        const std::span<const uint8_t> result = _bytes.subspan(_position, size);
        _position += size;
        return result;
    }

    size_t position() const noexcept { return _position; }
    size_t remaining() const noexcept { return _bytes.size() - _position; }

private:
    void require(size_t size, std::string_view description) const {
        if (size > _bytes.size() - _position) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Truncated Variant V2 exchange {}: need {} bytes, have {}", description,
                            size, _bytes.size() - _position);
        }
    }

    std::span<const uint8_t> _bytes;
    size_t _position = 0;
};

struct ResolvedColumn {
    ColumnVariantV2::ReadView view;
    size_t logical_rows;
    bool is_constant;

    size_t physical_row(size_t logical_row) const noexcept { return is_constant ? 0 : logical_row; }
};

struct TypedPlan {
    TypeMeta type_meta;
    uint32_t nullmap_bytes = 0;
    uint32_t value_offsets_bytes = 0;
    uint32_t values_bytes = 0;
    size_t frame_bytes = 0;
    DorisVector<uint8_t> nullmap;
    DorisVector<uint32_t> value_offsets;
    DorisVector<StringRef> string_values;
};

size_t checked_add(size_t left, size_t right, std::string_view description);
size_t checked_multiply(size_t left, size_t right, std::string_view description);
uint32_t checked_u32(size_t value, std::string_view description);
size_t checked_size(uint64_t value, std::string_view description);

TypedPlan plan_typed(const ResolvedColumn& source);
void write_typed(const ResolvedColumn& source, const TypedPlan& plan, Writer& writer);
ColumnVariantV2::MutablePtr decode_typed(uint64_t row_count, std::span<const uint8_t> type_meta,
                                         std::span<const uint8_t> extension,
                                         std::span<const uint8_t> nullmap,
                                         std::span<const uint8_t> value_offsets,
                                         std::span<const uint8_t> values);

} // namespace doris::variant_v2_serde_binary_internal
