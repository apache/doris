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

#include <cstddef>
#include <cstdint>

namespace doris {

// Apache Parquet Variant Encoding v1:
// https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
inline constexpr uint8_t VARIANT_ENCODING_VERSION = 1;
inline constexpr size_t VARIANT_MAX_SHORT_STRING_SIZE = 63;
inline constexpr uint8_t VARIANT_MAX_PRIMITIVE_ID = 20;
inline constexpr uint32_t VARIANT_MAX_NESTING_DEPTH = 128;

inline constexpr uint8_t VARIANT_BASIC_TYPE_MASK = 0x03;
inline constexpr uint8_t VARIANT_VALUE_HEADER_SHIFT = 2;

inline constexpr uint8_t VARIANT_METADATA_VERSION_MASK = 0x0F;
inline constexpr uint8_t VARIANT_METADATA_SORTED_STRINGS_MASK = 0x10;
inline constexpr uint8_t VARIANT_METADATA_OFFSET_SIZE_SHIFT = 6;
inline constexpr uint8_t VARIANT_METADATA_OFFSET_SIZE_MASK = 0x03;

// Container positions are relative to the six-bit value_header, after removing basic_type.
inline constexpr uint8_t VARIANT_OBJECT_OFFSET_SIZE_SHIFT = 0;
inline constexpr uint8_t VARIANT_OBJECT_ID_SIZE_SHIFT = 2;
inline constexpr uint8_t VARIANT_OBJECT_LARGE_MASK = 0x10;

inline constexpr uint8_t VARIANT_ARRAY_OFFSET_SIZE_SHIFT = 0;
inline constexpr uint8_t VARIANT_ARRAY_LARGE_MASK = 0x04;

enum class VariantBasicType : uint8_t {
    PRIMITIVE = 0,
    SHORT_STRING = 1,
    OBJECT = 2,
    ARRAY = 3,
};

enum class VariantPrimitiveId : uint8_t {
    NULL_VALUE = 0,
    TRUE_VALUE = 1,
    FALSE_VALUE = 2,
    INT8 = 3,
    INT16 = 4,
    INT32 = 5,
    INT64 = 6,
    DOUBLE = 7,
    DECIMAL4 = 8,
    DECIMAL8 = 9,
    DECIMAL16 = 10,
    DATE = 11,
    TIMESTAMP_MICROS = 12,
    TIMESTAMP_NTZ_MICROS = 13,
    FLOAT = 14,
    BINARY = 15,
    STRING = 16,
    TIME_NTZ_MICROS = 17,
    TIMESTAMP_NANOS = 18,
    TIMESTAMP_NTZ_NANOS = 19,
    UUID = 20,
};

struct VariantDecimal {
    __int128 unscaled = 0;
    uint8_t scale = 0;
    uint8_t width = 0;

    bool operator==(const VariantDecimal&) const = default;
};

} // namespace doris
