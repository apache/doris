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
#include "common/check.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_fixed_length_object.h"
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

namespace doris {

// How a compute-layer value becomes what a segment stores for FieldType FT:
// 1. Unchanged (BitCast): BOOL, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, UNSIGNED_INT,
//    UNSIGNED_BIGINT, DECIMAL32, DECIMAL64, DECIMAL128I, DECIMAL256, DATEV2,
//    DATETIMEV2, TIMESTAMP_NS, TIMESTAMPTZ, IPV4, IPV6, UUID.
// 2. Normalised (FloatingPoint): FLOAT and DOUBLE, every NaN as the quiet NaN.
// 3. Re-encoded (repack_from_column): the V1 DATE, DATETIME and DECIMAL layouts.
// 4. As bytes (Bytes): VARCHAR, STRING, JSONB, VARIANT; CHAR also zero padded to its
//    length.
// 5. Serialised: BITMAP, HLL, QUANTILE_STATE.
// 6. AGG_STATE: a string, a bitmap or a fixed-length object, whichever its aggregate
//    function serialises to.
// Compute-layer types: PrimitiveTypeTraits<PT>; storage-layer types: CppTypeTraits<FT>.
//
// The conversions from a column, what each does and who uses it:
// - column_to_storage: a run of rows. Fixed-width values of a run are contiguous in
//   storage, so they are converted as a batch.
//   - column_to_storage(column, row_pos, n, dst), fixed-width types: n rows as stored.
//     The fixed-width pages and the ANN index.
//   - StorageLayout<CHAR>::column_to_storage(column, length): the column zero padded to
//     length, its rows where they were. The column writer and BUILD INDEX.
// - storage_at: one row. A key is built row by row and a string page adds one value at a
//   time, so they take one row per call. Running per row, its cast skips the
//   release-build type check.
//   - storage_at(column, row), fixed-width types: the value. Key encoding.
//   - storage_at(column, row, tmp_buffer), string and object types: the bytes. The
//     string pages.
//   - StorageLayout<AGG_STATE>::storage_at_of(column): the storage_at for the column,
//     picked once per batch. The string pages.
template <FieldType FT>
struct StorageLayout;

// The FieldTypes stored fixed width, for callers that only know their FieldType at run
// time; the others are stored as bytes.
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
    M(OLAP_FIELD_TYPE_IPV6)                                 \
    M(OLAP_FIELD_TYPE_UUID)

namespace storage_layout_detail {

// Writes rows [row_pos, row_pos + n) of the column of FT's PrimitiveType to `dst` as
// stored values, converting each through StorageLayout<FT>::to_storage.
template <FieldType FT>
void repack_from_column(const IColumn& column, size_t row_pos, size_t n, uint8_t* dst) {
    using StorageValue = typename CppTypeTraits<FT>::CppType;
    using Column =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
    const auto& data = assert_cast<const Column&>(column).get_data();
    for (size_t i = 0; i < n; ++i) {
        const StorageValue value = StorageLayout<FT>::to_storage(data[row_pos + i]);
        memcpy(dst + i * sizeof(StorageValue), &value, sizeof(StorageValue));
    }
}

// A StorageValue is the PrimitiveValue's own bytes, so a run of the column is a run of
// stored values.
template <FieldType FT>
struct BitCast {
private:
    using StorageValue = typename CppTypeTraits<FT>::CppType;
    using PrimitiveValue =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::CppType;
    using Column =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
    static_assert(sizeof(StorageValue) == sizeof(PrimitiveValue));

public:
    static StorageValue to_storage(const PrimitiveValue& value) {
        return std::bit_cast<StorageValue>(value);
    }
    static void column_to_storage(const IColumn& column, size_t row_pos, size_t n, uint8_t* dst) {
        memcpy(dst, assert_cast<const Column&>(column).get_data().data() + row_pos,
               n * sizeof(StorageValue));
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
private:
    using StorageValue = typename CppTypeTraits<FT>::CppType;
    using PrimitiveValue =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::CppType;
    using Column =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
    static_assert(std::is_floating_point_v<StorageValue>);
    static_assert(std::is_same_v<StorageValue, PrimitiveValue>);

public:
    static StorageValue canonicalize_nan(StorageValue value) {
        return std::isnan(value) ? std::numeric_limits<StorageValue>::quiet_NaN() : value;
    }
    static StorageValue to_storage(PrimitiveValue value) { return canonicalize_nan(value); }
    // A run without a NaN is copied as is; one holding a NaN is canonicalised value by
    // value.
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
    static StorageValue storage_at(const IColumn& column, size_t row) {
        return to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row]);
    }
};

// A string is stored as its own bytes, whatever their length.
struct Bytes {
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
    using Column = typename PrimitiveTypeTraits<TYPE_DATE>::ColumnType;
    static_assert(std::is_same_v<StorageValue, uint24_t>);

public:
    static StorageValue to_storage(const PrimitiveValue& value) {
        return StorageValue(cast_set<uint32_t>(value.to_olap_date()));
    }
    static StorageValue storage_at(const IColumn& column, size_t row) {
        return to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row]);
    }
    static void column_to_storage(const IColumn& column, size_t row_pos, size_t n, uint8_t* dst) {
        storage_layout_detail::repack_from_column<FieldType::OLAP_FIELD_TYPE_DATE>(column, row_pos,
                                                                                   n, dst);
    }
};

// V1 DATETIME: the decimal digits YYYYMMDDhhmmss packed into one integer.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME> {
private:
    using StorageValue = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME>::CppType;
    using PrimitiveValue = typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType;
    using Column = typename PrimitiveTypeTraits<TYPE_DATETIME>::ColumnType;
    static_assert(std::is_same_v<StorageValue, int64_t>);

public:
    static StorageValue to_storage(const PrimitiveValue& value) {
        return static_cast<StorageValue>(value.to_olap_datetime());
    }
    static StorageValue storage_at(const IColumn& column, size_t row) {
        return to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row]);
    }
    static void column_to_storage(const IColumn& column, size_t row_pos, size_t n, uint8_t* dst) {
        storage_layout_detail::repack_from_column<FieldType::OLAP_FIELD_TYPE_DATETIME>(
                column, row_pos, n, dst);
    }
};

// V1 DECIMAL: the integer and the nine-digit fraction stored side by side.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL> {
private:
    using StorageValue = typename CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL>::CppType;
    using PrimitiveValue = typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType;
    using Column = typename PrimitiveTypeTraits<TYPE_DECIMALV2>::ColumnType;
    static_assert(std::is_same_v<StorageValue, decimal12_t>);

public:
    static StorageValue to_storage(const PrimitiveValue& value) {
        return {value.int_value(), value.frac_value()};
    }
    static StorageValue storage_at(const IColumn& column, size_t row) {
        return to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row]);
    }
    static void column_to_storage(const IColumn& column, size_t row_pos, size_t n, uint8_t* dst) {
        storage_layout_detail::repack_from_column<FieldType::OLAP_FIELD_TYPE_DECIMAL>(
                column, row_pos, n, dst);
    }
};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_VARCHAR> : storage_layout_detail::Bytes {};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_STRING> : storage_layout_detail::Bytes {};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_JSONB> : storage_layout_detail::Bytes {};

// A VARIANT root column reaches the writer as the JSONB of its rows.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_VARIANT> : storage_layout_detail::Bytes {};

// CHAR(N) is stored zero padded to N bytes, while a block holds it unpadded: the column
// writer pads the columns it stores, and storage_at reads a row of the padded column.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR> : storage_layout_detail::Bytes {
    // `column`'s values, a nullable column's nested ones, zero padded to `length` bytes at their
    // rows: the values themselves when they already total rows * `length` bytes, the values of
    // NULL rows included.
    static ColumnPtr column_to_storage(const IColumn& column, size_t length);

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
private:
    using Column = typename PrimitiveTypeTraits<TYPE_BITMAP>::ColumnType;

public:
    static StringRef storage_at(const IColumn& column, size_t row,
                                PaddedPODArray<char>& tmp_buffer) {
        const BitmapValue& value =
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row];
        tmp_buffer.resize(value.getSizeInBytes());
        value.write_to(tmp_buffer.data());
        return {tmp_buffer.data(), tmp_buffer.size()};
    }
};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_HLL> {
private:
    using Column = typename PrimitiveTypeTraits<TYPE_HLL>::ColumnType;

public:
    static StringRef storage_at(const IColumn& column, size_t row,
                                PaddedPODArray<char>& tmp_buffer) {
        const HyperLogLog& value =
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row];
        tmp_buffer.resize(value.max_serialized_size());
        const size_t size = value.serialize(reinterpret_cast<uint8_t*>(tmp_buffer.data()));
        return {tmp_buffer.data(), size};
    }
};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE> {
private:
    using Column = typename PrimitiveTypeTraits<TYPE_QUANTILE_STATE>::ColumnType;

public:
    static StringRef storage_at(const IColumn& column, size_t row,
                                PaddedPODArray<char>& tmp_buffer) {
        const QuantileState& value =
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[row];
        tmp_buffer.resize(value.get_serialized_size());
        value.serialize(reinterpret_cast<uint8_t*>(tmp_buffer.data()));
        return {tmp_buffer.data(), tmp_buffer.size()};
    }
};

// An AGG_STATE column holds strings, bitmaps or fixed-length objects, whichever its
// aggregate function serialises to, so storage_at_of picks the storage_at once per batch.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_AGG_STATE> {
    using StorageAt = StringRef (*)(const IColumn& column, size_t row,
                                    PaddedPODArray<char>& tmp_buffer);
    static StorageAt storage_at_of(const IColumn& column) {
        if (check_and_get_column<ColumnString>(column) != nullptr) {
            return &storage_layout_detail::Bytes::storage_at;
        }
        if (check_and_get_column<ColumnBitmap>(column) != nullptr) {
            return &StorageLayout<FieldType::OLAP_FIELD_TYPE_BITMAP>::storage_at;
        }
        DORIS_CHECK(check_and_get_column<ColumnFixedLengthObject>(column) != nullptr);
        return &fixed_length_object_at;
    }

private:
    static StringRef fixed_length_object_at(const IColumn& column, size_t row,
                                            PaddedPODArray<char>& /*tmp_buffer*/) {
        const auto& objects =
                assert_cast<const ColumnFixedLengthObject&, TypeCheckOnRelease::DISABLE>(column);
        const size_t item_size = objects.item_size();
        return {reinterpret_cast<const char*>(objects.get_data().data()) + row * item_size,
                item_size};
    }
};

// The admission rule a writer applies to the rows of a column before they are stored: a
// STRING or JSONB value must fit string_type_length_soft_limit_bytes, and a JSONB value
// must be a well-formed JSONB document. The rows `null_map` marks NULL are not checked.
Status admit_storage_rows(FieldType type, const IColumn& column, size_t row_pos, size_t n,
                          const uint8_t* null_map);

} // namespace doris
