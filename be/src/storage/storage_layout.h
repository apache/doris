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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>

#include "common/cast_set.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type/storage_field_type.h"
#include "core/decimal12.h"
#include "core/pod_array.h"
#include "core/string_ref.h"
#include "core/uint24.h"
#include "core/value/decimalv2_value.h"
#include "core/value/vdatetime_value.h"
#include "storage/field_type.h"
#include "storage/types.h"
#include "util/slice.h"

namespace doris {

class IColumn;

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
//    function serialises to; the column holding the states tells which.
// Compute-layer types: PrimitiveTypeTraits<PT>; storage-layer types: CppTypeTraits<FT>.
//
// Every layout converts a batch the same way: column_to_storage(column, row_pos, n, buffers)
// returns the stored cells of rows [row_pos, row_pos + n) back to back, n fixed-width values
// or n Slice descriptors. It returns the column's own bytes when they are stored unchanged
// and otherwise fills the StorageBuffers it is given, which the caller keeps until it has
// consumed the cells. Every row of the range is converted, NULL rows included: whatever a
// NULL row holds converts without harm, and no caller uses a NULL row's cell. CHAR padding is
// the one exception: it needs a range without NULL rows, see StorageLayout<CHAR>. to_storage
// retains a typed result for single-value key and predicate conversions.
template <FieldType FT>
struct StorageLayout;

// Where a batch conversion puts what it cannot borrow: converted fixed-width values and
// serialised payloads in `bytes`, the Slice descriptors of the byte types in `cells`.
struct StorageBuffers {
    PaddedPODArray<uint8_t> bytes;
    PaddedPODArray<Slice> cells;
};

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

// A StorageValue is the PrimitiveValue's own bytes, so a run of the column is a run of
// stored values.
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
    // Always borrows the column's bytes.
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            StorageBuffers& buffers);
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
    // Borrows the column's bytes when [row_pos, row_pos + n) has no NaN, otherwise
    // converts the range into buffers.bytes. NaNs outside the range do not require a
    // copy; the source column is never modified.
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            StorageBuffers& buffers);
};

// A string is stored as its own bytes, whatever their length: the cells are Slice
// descriptors of the column's payloads.
struct Bytes {
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            StorageBuffers& buffers);
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

// The batch conversions of the layouts above are instantiated once, in storage_layout.cpp.
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_BOOL>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_TINYINT>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_SMALLINT>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_INT>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_BIGINT>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_LARGEINT>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL32>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL64>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL256>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DATEV2>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_IPV4>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_IPV6>;
extern template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_UUID>;
extern template struct storage_layout_detail::FloatingPoint<FieldType::OLAP_FIELD_TYPE_FLOAT>;
extern template struct storage_layout_detail::FloatingPoint<FieldType::OLAP_FIELD_TYPE_DOUBLE>;

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
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            StorageBuffers& buffers);
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
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            StorageBuffers& buffers);
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
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            StorageBuffers& buffers);
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

// CHAR(N) is stored zero padded to N bytes, while a block holds it unpadded: the writers
// pad the values they store or index with the length-taking column_to_storage. Padding
// copies a value into an N-byte slot, so it never reads a NULL row: a NULL row can hold a
// value longer than N, as load does not check NULL rows. The Bytes one stays for the SNII
// index, which is fed CHAR as it reads back.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR> : storage_layout_detail::Bytes {
    using storage_layout_detail::Bytes::column_to_storage;

    // Pads every non-NULL row of the range into its own `length` zeroed bytes and points its
    // cell at them; a NULL row keeps an empty cell. null_map[i] is the NULL flag of row
    // row_pos + i, and null_map is nullptr when the range holds no NULL row.
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            size_t length, const uint8_t* null_map,
                                            StorageBuffers& buffers);

    static void append_padded(StringRef value, size_t length, std::string* out) {
        DCHECK_LE(value.size, length);
        out->reserve(out->size() + length);
        out->append(value.data, value.size);
        out->append(length - value.size, '\0');
    }
};

// The object types are stored serialised: every value of the batch into buffers.bytes,
// the cells pointing at them.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_BITMAP> {
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            StorageBuffers& buffers);
};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_HLL> {
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            StorageBuffers& buffers);
};

template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE> {
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            StorageBuffers& buffers);
};

// The fixed-length objects an AGG_STATE column holds when its aggregate function serialises
// to them, each borrowed through a Slice descriptor. String and bitmap states are stored
// through the VARCHAR and BITMAP layouts, which the column writer is given when it is created.
template <>
struct StorageLayout<FieldType::OLAP_FIELD_TYPE_AGG_STATE> {
    static const uint8_t* column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                            StorageBuffers& buffers);
};

// The batch conversion of a FieldType known only at run time: the column_to_storage of
// StorageLayout<type>, for the column writer and the index feeds. CHAR is not accepted:
// whether its values are padded is the caller's choice, made through StorageLayout<CHAR>.
const uint8_t* column_to_storage(FieldType type, const IColumn& column, size_t row_pos, size_t n,
                                 StorageBuffers& buffers);

// The admission rule a writer applies to the rows of a column before they are stored: a
// STRING or JSONB value must fit string_type_length_soft_limit_bytes, and a JSONB value
// must be a well-formed JSONB document. The rows `null_map` marks NULL are not checked.
Status admit_storage_rows(FieldType type, const IColumn& column, size_t row_pos, size_t n,
                          const uint8_t* null_map);

} // namespace doris
