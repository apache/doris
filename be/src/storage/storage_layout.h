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
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <type_traits>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type/storage_field_type.h"
#include "core/decimal12.h"
#include "core/pod_array.h"
#include "core/string_ref.h"
#include "core/uint24.h"
#include "core/value/bitmap_value.h"
#include "core/value/decimalv2_value.h"
#include "core/value/hll.h"
#include "core/value/quantile_state.h"
#include "core/value/vdatetime_value.h"
#include "storage/field_type.h"
#include "storage/types.h"
#include "util/slice.h"

namespace doris {

// A FieldType's StorageValue is what a segment stores for one value of it: what
// the page builders, bloom filters, BKD trees and the KeyCoder are fed, and what
// the page decoders hand back. For most FieldTypes it is the compute-layer value
// itself. The ones where it differs -- the V1 DATE, DATETIME and DECIMAL
// layouts, NaN canonicalisation for FLOAT/DOUBLE and the zero padding a CHAR key
// gets -- are known here and nowhere else: whatever converts a compute-layer
// value to its StorageValue, or back, goes through StorageLayout<FT>, so the two
// sides cannot drift apart.
//
// Which PrimitiveType a FieldType holds is not repeated here:
// core/data_type/storage_field_type.h owns that pairing, and it is constexpr,
// so a template picks its layout with
// StorageLayout<primitive_type_to_storage_field_type(PT)>.
//
// The FieldTypes whose StorageValue is fixed width, for callers that only know
// their FieldType at run time; the others store Slices.
#define DORIS_APPLY_FOR_FIXED_WIDTH_STORAGE_LAYOUT_TYPES(M) \
    M(OLAP_FIELD_TYPE_BOOL)                                 \
    M(OLAP_FIELD_TYPE_TINYINT)                              \
    M(OLAP_FIELD_TYPE_SMALLINT)                             \
    M(OLAP_FIELD_TYPE_INT)                                  \
    M(OLAP_FIELD_TYPE_BIGINT)                               \
    M(OLAP_FIELD_TYPE_LARGEINT)                             \
    M(OLAP_FIELD_TYPE_UNSIGNED_INT)                         \
    M(OLAP_FIELD_TYPE_UNSIGNED_BIGINT)                      \
    M(OLAP_FIELD_TYPE_FLOAT)                                \
    M(OLAP_FIELD_TYPE_DOUBLE)                               \
    M(OLAP_FIELD_TYPE_DECIMAL)                              \
    M(OLAP_FIELD_TYPE_DECIMAL32)                            \
    M(OLAP_FIELD_TYPE_DECIMAL64)                            \
    M(OLAP_FIELD_TYPE_DECIMAL128I)                          \
    M(OLAP_FIELD_TYPE_DECIMAL256)                           \
    M(OLAP_FIELD_TYPE_DATE)                                 \
    M(OLAP_FIELD_TYPE_DATETIME)                             \
    M(OLAP_FIELD_TYPE_DATEV2)                               \
    M(OLAP_FIELD_TYPE_DATETIMEV2)                           \
    M(OLAP_FIELD_TYPE_TIMESTAMP_NS)                         \
    M(OLAP_FIELD_TYPE_TIMESTAMPTZ)                          \
    M(OLAP_FIELD_TYPE_IPV4)                                 \
    M(OLAP_FIELD_TYPE_IPV6)

// Declared only: a FieldType without a specialisation below has no storage
// form, and using it is a compile error rather than a silent byte copy.
//
// Every specialisation gives StorageValue, the C++ type of what is stored on
// disk; a fixed-width one also gives PrimitiveValue, the compute layer's C++
// type for the same value.
//
// Fixed width (StorageValue is a POD)
//   one value, both ways:
//     to_storage(PrimitiveValue) -> StorageValue
//     to_primitive(StorageValue) -> PrimitiveValue
//   a batch, both ways; the storage side is n StorageValues back to back and
//   not necessarily aligned:
//     column_to_storage(column, row_pos, n, dst): rows [row_pos, row_pos + n)
//         of `column` written to `dst` as StorageValues, as one copy when their
//         bytes already are
//     storage_to_column(values, n, dst): n StorageValues appended to `dst`
//   one row:
//     storage_at(column, row): a row's StorageValue, for callers that take one
//         value at a time
// Variable width (StorageValue is a Slice)
//   storage_at(column, row, tmp_buffer): a row's StorageValue, pointing into
//       the column when its bytes already are it, otherwise built in `tmp_buffer`
//   CHAR also has append_padded, to pad a key; the object types have to_storage,
//   to serialise one object
// Either storage_at runs once per row, so its cast skips the release-build type check.
template <FieldType FT>
struct StorageLayout;

namespace storage_layout_detail {

template <FieldType FT>
using ColumnOf = typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;

// A StorageValue is the PrimitiveValue's own bytes, so a run of the column is a
// run of StorageValues.
template <FieldType FT>
struct BitCast {
    using StorageValue = typename CppTypeTraits<FT>::CppType;
    using PrimitiveValue =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::CppType;
    using Column = ColumnOf<FT>;
    static_assert(sizeof(StorageValue) == sizeof(PrimitiveValue));

    static StorageValue to_storage(const PrimitiveValue& value) {
        return std::bit_cast<StorageValue>(value);
    }
    static PrimitiveValue to_primitive(const StorageValue& value) {
        return std::bit_cast<PrimitiveValue>(value);
    }

    static void column_to_storage(const IColumn& column, size_t row_pos, size_t n, uint8_t* dst) {
        memcpy(dst, assert_cast<const Column&>(column).get_data().data() + row_pos,
               n * sizeof(StorageValue));
    }
    static void storage_to_column(const uint8_t* values, size_t n, IColumn& dst) {
        dst.insert_many_raw_data(reinterpret_cast<const char*>(values), n);
    }

    static StorageValue storage_at(const IColumn& column, size_t row) {
        return to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row]);
    }
};

// Every NaN is stored as the quiet NaN, so equal values have equal stored bytes
// and the bloom filter / dictionary / key encodings that hash or compare bytes
// treat all NaNs as one value.
template <FieldType FT>
struct FloatingPoint {
    using StorageValue = typename CppTypeTraits<FT>::CppType;
    using PrimitiveValue = StorageValue;
    using Column = ColumnOf<FT>;
    static_assert(std::is_floating_point_v<StorageValue>);

    static StorageValue to_storage(PrimitiveValue value) {
        return std::isnan(value) ? std::numeric_limits<StorageValue>::quiet_NaN() : value;
    }
    static PrimitiveValue to_primitive(StorageValue value) { return value; }

    // A run without a NaN is copied as is; one holding a NaN is canonicalised
    // value by value.
    static void column_to_storage(const IColumn& column, size_t row_pos, size_t n, uint8_t* dst) {
        const PrimitiveValue* values =
                assert_cast<const Column&>(column).get_data().data() + row_pos;
        if (std::none_of(values, values + n,
                         [](PrimitiveValue value) { return std::isnan(value); })) {
            memcpy(dst, values, n * sizeof(StorageValue));
            return;
        }
        for (size_t i = 0; i < n; ++i) {
            const StorageValue value = to_storage(values[i]);
            memcpy(dst + i * sizeof(StorageValue), &value, sizeof(StorageValue));
        }
    }
    static void storage_to_column(const uint8_t* values, size_t n, IColumn& dst) {
        dst.insert_many_raw_data(reinterpret_cast<const char*>(values), n);
    }

    static StorageValue storage_at(const IColumn& column, size_t row) {
        return to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row]);
    }
};

// A StorageValue packs the PrimitiveValue differently: every value is converted
// on its own.
template <FieldType FT, typename Derived>
struct Repacked {
    using StorageValue = typename CppTypeTraits<FT>::CppType;
    using Column = ColumnOf<FT>;

    static void column_to_storage(const IColumn& column, size_t row_pos, size_t n, uint8_t* dst) {
        const auto& data = assert_cast<const Column&>(column).get_data();
        for (size_t i = 0; i < n; ++i) {
            const StorageValue value = Derived::to_storage(data[row_pos + i]);
            memcpy(dst + i * sizeof(StorageValue), &value, sizeof(StorageValue));
        }
    }
    static void storage_to_column(const uint8_t* values, size_t n, IColumn& dst) {
        auto& data = assert_cast<Column&>(dst).get_data();
        data.reserve(data.size() + n);
        for (size_t i = 0; i < n; ++i) {
            StorageValue value;
            memcpy(&value, values + i * sizeof(StorageValue), sizeof(StorageValue));
            data.push_back_without_reserve(Derived::to_primitive(value));
        }
    }

    static auto storage_at(const IColumn& column, size_t row) {
        return Derived::to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row]);
    }
};

// A StorageValue is the value's own bytes, whatever their length.
struct Bytes {
    using StorageValue = Slice;

    static StringRef storage_at(const IColumn& column, size_t row,
                                PaddedPODArray<char>& /*tmp_buffer*/) {
        return assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                row);
    }
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
#undef DORIS_STORAGE_LAYOUT_BIT_CAST

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_FLOAT>
        : storage_layout_detail::FloatingPoint<FieldType::OLAP_FIELD_TYPE_FLOAT> {};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_DOUBLE>
        : storage_layout_detail::FloatingPoint<FieldType::OLAP_FIELD_TYPE_DOUBLE> {};

// V1 DATE: three bytes of (year << 9) | (month << 5) | day.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>
        : storage_layout_detail::Repacked<FieldType::OLAP_FIELD_TYPE_DATE,
                                          StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>> {
    using StorageValue = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>::CppType;
    using PrimitiveValue = VecDateTimeValue;
    static_assert(std::is_same_v<StorageValue, uint24_t>);

    static StorageValue to_storage(const PrimitiveValue& value) {
        return StorageValue(cast_set<uint32_t>(value.to_olap_date()));
    }
    static PrimitiveValue to_primitive(const StorageValue& value) {
        PrimitiveValue primitive;
        primitive.set_olap_date(value);
        return primitive;
    }
};

// V1 DATETIME: the decimal digits YYYYMMDDhhmmss packed into one integer.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME>
        : storage_layout_detail::Repacked<FieldType::OLAP_FIELD_TYPE_DATETIME,
                                          StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME>> {
    using StorageValue = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME>::CppType;
    using PrimitiveValue = VecDateTimeValue;
    static_assert(std::is_same_v<StorageValue, int64_t>);

    static StorageValue to_storage(const PrimitiveValue& value) {
        return static_cast<StorageValue>(value.to_olap_datetime());
    }
    static PrimitiveValue to_primitive(const StorageValue& value) {
        PrimitiveValue primitive;
        primitive.from_olap_datetime(static_cast<uint64_t>(value));
        return primitive;
    }
};

// V1 DECIMAL: the integer and the nine-digit fraction stored side by side.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL>
        : storage_layout_detail::Repacked<FieldType::OLAP_FIELD_TYPE_DECIMAL,
                                          StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL>> {
    using StorageValue = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL>::CppType;
    using PrimitiveValue = DecimalV2Value;
    static_assert(std::is_same_v<StorageValue, decimal12_t>);

    static StorageValue to_storage(const PrimitiveValue& value) {
        return {value.int_value(), value.frac_value()};
    }
    static PrimitiveValue to_primitive(const StorageValue& value) {
        return PrimitiveValue(value.integer, value.fraction);
    }
};

// Appends n StorageValues of a fixed-width FieldType to `dst`, the column of the
// FieldType or a Nullable over it; none of the rows is NULL.
template <FieldType FT>
void read_to_column(const uint8_t* values, size_t n, IColumn& dst) {
    if (dst.is_nullable()) {
        auto& nullable = assert_cast<ColumnNullable&>(dst);
        nullable.push_false_to_nullmap(n);
        StorageLayout<FT>::storage_to_column(values, n, nullable.get_nested_column());
        return;
    }
    StorageLayout<FT>::storage_to_column(values, n, dst);
}

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_VARCHAR> : storage_layout_detail::Bytes {};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_STRING> : storage_layout_detail::Bytes {};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_JSONB> : storage_layout_detail::Bytes {};

// A VARIANT root column reaches the writer as the strings its rows serialise to.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_VARIANT> : storage_layout_detail::Bytes {};

// A CHAR StorageValue is the value's own bytes, like every other string. Only a key
// encoding pads, through append_padded below: a short key column must span its
// index length, so its bytes cannot stop where the value does.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR> : storage_layout_detail::Bytes {
    // Appends `value` zero padded to `length` bytes. Reserving first grows `out` once for the
    // value and its padding, instead of once for each.
    static void append_padded(StringRef value, size_t length, std::string* out) {
        DCHECK_LE(value.size, length);
        out->reserve(out->size() + length);
        out->append(value.data, value.size);
        out->append(length - value.size, '\0');
    }
};

// The object types are stored serialised.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_BITMAP> {
    using StorageValue = Slice;
    using Column = ColumnBitmap;

    static StringRef to_storage(const BitmapValue& value, PaddedPODArray<char>& tmp_buffer) {
        tmp_buffer.resize(value.getSizeInBytes());
        value.write_to(tmp_buffer.data());
        return {tmp_buffer.data(), tmp_buffer.size()};
    }
    static StringRef storage_at(const IColumn& column, size_t row,
                                PaddedPODArray<char>& tmp_buffer) {
        return to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row],
                tmp_buffer);
    }
};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_HLL> {
    using StorageValue = Slice;
    using Column = ColumnHLL;

    static StringRef to_storage(const HyperLogLog& value, PaddedPODArray<char>& tmp_buffer) {
        tmp_buffer.resize(value.max_serialized_size());
        const size_t size = value.serialize(reinterpret_cast<uint8_t*>(tmp_buffer.data()));
        return {tmp_buffer.data(), size};
    }
    static StringRef storage_at(const IColumn& column, size_t row,
                                PaddedPODArray<char>& tmp_buffer) {
        return to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row],
                tmp_buffer);
    }
};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE> {
    using StorageValue = Slice;
    using Column = ColumnQuantileState;

    static StringRef to_storage(const QuantileState& value, PaddedPODArray<char>& tmp_buffer) {
        tmp_buffer.resize(value.get_serialized_size());
        value.serialize(reinterpret_cast<uint8_t*>(tmp_buffer.data()));
        return {tmp_buffer.data(), tmp_buffer.size()};
    }
    static StringRef storage_at(const IColumn& column, size_t row,
                                PaddedPODArray<char>& tmp_buffer) {
        return to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row],
                tmp_buffer);
    }
};

// An AGG_STATE column's shape is decided at runtime by the aggregate function's
// serialised type: a string, a bitmap, or a fixed-length object.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_AGG_STATE> {
    using StorageValue = Slice;

    static StringRef storage_at(const IColumn& column, size_t row,
                                PaddedPODArray<char>& tmp_buffer) {
        if (const auto* strings = check_and_get_column<ColumnString>(column)) {
            return strings->get_data_at(row);
        }
        if (const auto* bitmaps = check_and_get_column<ColumnBitmap>(column)) {
            return StorageLayout<FieldType::OLAP_FIELD_TYPE_BITMAP>::to_storage(
                    bitmaps->get_data()[row], tmp_buffer);
        }
        const auto& objects = assert_cast<const ColumnFixedLengthObject&>(column);
        const size_t item_size = objects.item_size();
        return {reinterpret_cast<const char*>(objects.get_data().data()) + row * item_size,
                item_size};
    }
};

// The admission rule a writer applies to the rows of a column before they are
// stored: a STRING or JSONB value must fit string_type_length_soft_limit_bytes,
// and a JSONB value must be a well-formed JSONB document. The rows `null_map`
// marks NULL are not checked.
Status admit_storage_rows(FieldType type, const IColumn& column, size_t row_pos, size_t n,
                          const uint8_t* null_map);

} // namespace doris
