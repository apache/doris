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

#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "common/cast_set.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type/storage_field_type.h"
#include "core/decimal12.h"
#include "core/uint24.h"
#include "core/value/decimalv2_value.h"
#include "core/value/vdatetime_value.h"
#include "storage/field_type.h"
#include "storage/types.h"

namespace doris {

// How a compute-layer value becomes what a segment stores for FieldType FT: its own
// bytes, except for the V1 DATE, DATETIME and DECIMAL layouts and NaN, stored as the
// quiet NaN. The two types are PrimitiveTypeTraits<PT>::CppType and
// CppTypeTraits<FT>::CppType; the aliases inside are private shorthand. Declared only,
// so a FieldType without a specialisation is a compile error, not a silent byte copy.
template <FieldType FT>
struct StorageLayout;

namespace storage_layout_detail {

// A StorageValue is the PrimitiveValue's own bytes.
template <FieldType FT>
struct BitCast {
private:
    using StorageValue = typename CppTypeTraits<FT>::CppType;
    using PrimitiveValue =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::CppType;
    static_assert(sizeof(StorageValue) == sizeof(PrimitiveValue));

public:
    static StorageValue to_storage(const PrimitiveValue& value) {
        return std::bit_cast<StorageValue>(value);
    }
};

// Every NaN is stored as the quiet NaN, so equal values have equal stored bytes
// and the bloom filter / dictionary / key encodings that hash or compare bytes
// treat all NaNs as one value.
template <FieldType FT>
struct FloatingPoint {
private:
    using StorageValue = typename CppTypeTraits<FT>::CppType;
    using PrimitiveValue =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::CppType;
    static_assert(std::is_floating_point_v<StorageValue>);
    static_assert(std::is_same_v<StorageValue, PrimitiveValue>);

public:
    static StorageValue canonicalize_nan(StorageValue value) {
        return std::isnan(value) ? std::numeric_limits<StorageValue>::quiet_NaN() : value;
    }
    static StorageValue to_storage(PrimitiveValue value) { return canonicalize_nan(value); }
};

} // namespace storage_layout_detail

#define DORIS_STORAGE_LAYOUT_BIT_CAST(FT) \
    template <>                           \
    struct StorageLayout<FieldType::FT> : storage_layout_detail::BitCast<FieldType::FT> {};

DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_BOOL)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_TINYINT)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_SMALLINT)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_INT)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_BIGINT)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_LARGEINT)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_UNSIGNED_INT)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_UNSIGNED_BIGINT)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_DECIMAL32)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_DECIMAL64)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_DECIMAL128I)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_DECIMAL256)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_DATEV2)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_DATETIMEV2)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_TIMESTAMP_NS)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_TIMESTAMPTZ)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_IPV4)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_IPV6)
DORIS_STORAGE_LAYOUT_BIT_CAST(OLAP_FIELD_TYPE_UUID)
#undef DORIS_STORAGE_LAYOUT_BIT_CAST

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_FLOAT>
        : storage_layout_detail::FloatingPoint<FieldType::OLAP_FIELD_TYPE_FLOAT> {};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_DOUBLE>
        : storage_layout_detail::FloatingPoint<FieldType::OLAP_FIELD_TYPE_DOUBLE> {};

// V1 DATE: three bytes of (year << 9) | (month << 5) | day.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE> {
private:
    using StorageValue = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>::CppType;
    using PrimitiveValue = typename PrimitiveTypeTraits<TYPE_DATE>::CppType;
    static_assert(std::is_same_v<StorageValue, uint24_t>);

public:
    static StorageValue to_storage(const PrimitiveValue& value) {
        return StorageValue(cast_set<uint32_t>(value.to_olap_date()));
    }
};

// V1 DATETIME: the decimal digits YYYYMMDDhhmmss packed into one integer.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME> {
private:
    using StorageValue = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME>::CppType;
    using PrimitiveValue = typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType;
    static_assert(std::is_same_v<StorageValue, int64_t>);

public:
    static StorageValue to_storage(const PrimitiveValue& value) {
        return static_cast<StorageValue>(value.to_olap_datetime());
    }
};

// V1 DECIMAL: the integer and the nine-digit fraction stored side by side.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL> {
private:
    using StorageValue = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL>::CppType;
    using PrimitiveValue = typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType;
    static_assert(std::is_same_v<StorageValue, decimal12_t>);

public:
    static StorageValue to_storage(const PrimitiveValue& value) {
        return {value.int_value(), value.frac_value()};
    }
};

} // namespace doris
