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

#include "storage/storage_layout.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/value/bitmap_value.h"
#include "core/value/hll.h"
#include "core/value/quantile_state.h"
#include "util/jsonb_document.h"

namespace doris {
namespace storage_layout_detail {

template <FieldType FT>
const uint8_t* BitCast<FT>::column_to_storage(const IColumn& column, size_t row_pos, size_t /*n*/,
                                              StorageBuffers& /*buffers*/) {
    using Column =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
    return reinterpret_cast<const uint8_t*>(assert_cast<const Column&>(column).get_data().data() +
                                            row_pos);
}

template <FieldType FT>
void BitCast<FT>::storage_to_column(const uint8_t* values, size_t n, IColumn& dst) {
    dst.insert_many_raw_data(reinterpret_cast<const char*>(values), n);
}

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

// Appends n stored values of a repacked layout, each turned back into its PrimitiveValue.
template <FieldType FT>
void repack_to_column(const uint8_t* values, size_t n, IColumn& dst) {
    if (dst.is_nullable()) {
        // A page holds no NULL row: the n rows it decodes are all not NULL.
        auto& nullable = assert_cast<ColumnNullable&>(dst);
        nullable.push_false_to_nullmap(n);
        repack_to_column<FT>(values, n, nullable.get_nested_column());
        return;
    }
    using StorageValue = typename CppTypeTraits<FT>::CppType;
    using Column =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
    auto& data = assert_cast<Column&>(dst).get_data();
    data.reserve(data.size() + n);
    for (size_t i = 0; i < n; ++i) {
        StorageValue value;
        memcpy(&value, values + i * sizeof(StorageValue), sizeof(StorageValue));
        data.push_back_without_reserve(StorageLayout<FT>::to_primitive(value));
    }
}

template <FieldType FT>
const uint8_t* FloatingPoint<FT>::column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                                    StorageBuffers& buffers) {
    using Column =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
    const PrimitiveValue* values = assert_cast<const Column&>(column).get_data().data() + row_pos;
    const auto* first_nan = std::find_if(values, values + n,
                                         [](PrimitiveValue value) { return std::isnan(value); });
    if (first_nan == values + n) {
        return reinterpret_cast<const uint8_t*>(values);
    }
    auto& bytes = buffers.bytes;
    bytes.resize(n * sizeof(StorageValue));
    const auto prefix_rows = static_cast<size_t>(first_nan - values);
    memcpy(bytes.data(), values, prefix_rows * sizeof(StorageValue));
    for (size_t i = prefix_rows; i < n; ++i) {
        const StorageValue value = to_storage(values[i]);
        memcpy(bytes.data() + i * sizeof(StorageValue), &value, sizeof(StorageValue));
    }
    return bytes.data();
}

template <FieldType FT>
void FloatingPoint<FT>::storage_to_column(const uint8_t* values, size_t n, IColumn& dst) {
    dst.insert_many_raw_data(reinterpret_cast<const char*>(values), n);
}

const uint8_t* Bytes::column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                        StorageBuffers& buffers) {
    const auto& strings = assert_cast<const ColumnString&>(column);
    auto& cells = buffers.cells;
    cells.resize(n);
    for (size_t i = 0; i < n; ++i) {
        const auto value = strings.get_data_at(row_pos + i);
        cells[i] = Slice(value.data, value.size);
    }
    return reinterpret_cast<const uint8_t*>(cells.data());
}

// Serialises the values of an object column into buffers.bytes, sized once from size_of,
// and describes each one with a cell. write returns the bytes a value took, at most
// size_of(value).
template <class Column, class SizeOf, class Write>
const uint8_t* serialize_batch(const IColumn& column, size_t row_pos, size_t n,
                               StorageBuffers& buffers, SizeOf size_of, Write write) {
    const auto* values = assert_cast<const Column&>(column).get_data().data() + row_pos;
    size_t total_size = 0;
    for (size_t i = 0; i < n; ++i) {
        total_size += size_of(values[i]);
    }
    auto& bytes = buffers.bytes;
    auto& cells = buffers.cells;
    bytes.resize(total_size);
    cells.resize(n);
    uint8_t* out = bytes.data();
    for (size_t i = 0; i < n; ++i) {
        const size_t size = write(values[i], out);
        cells[i] = Slice(out, size);
        out += size;
    }
    DCHECK_LE(out, bytes.data() + bytes.size());
    return reinterpret_cast<const uint8_t*>(cells.data());
}

} // namespace storage_layout_detail

// Match the extern template declarations in storage_layout.h.
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_BOOL>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_TINYINT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_SMALLINT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_INT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_BIGINT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_LARGEINT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL32>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL64>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL256>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DATEV2>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_IPV4>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_IPV6>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_UUID>;
template struct storage_layout_detail::FloatingPoint<FieldType::OLAP_FIELD_TYPE_FLOAT>;
template struct storage_layout_detail::FloatingPoint<FieldType::OLAP_FIELD_TYPE_DOUBLE>;

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers) {
    buffers.bytes.resize(n * sizeof(StorageValue));
    storage_layout_detail::repack_from_column<FieldType::OLAP_FIELD_TYPE_DATE>(
            column, row_pos, n, buffers.bytes.data());
    return buffers.bytes.data();
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers) {
    buffers.bytes.resize(n * sizeof(StorageValue));
    storage_layout_detail::repack_from_column<FieldType::OLAP_FIELD_TYPE_DATETIME>(
            column, row_pos, n, buffers.bytes.data());
    return buffers.bytes.data();
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers) {
    buffers.bytes.resize(n * sizeof(StorageValue));
    storage_layout_detail::repack_from_column<FieldType::OLAP_FIELD_TYPE_DECIMAL>(
            column, row_pos, n, buffers.bytes.data());
    return buffers.bytes.data();
}

void StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>::storage_to_column(const uint8_t* values,
                                                                       size_t n, IColumn& dst) {
    storage_layout_detail::repack_to_column<FieldType::OLAP_FIELD_TYPE_DATE>(values, n, dst);
}

void StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME>::storage_to_column(const uint8_t* values,
                                                                           size_t n, IColumn& dst) {
    storage_layout_detail::repack_to_column<FieldType::OLAP_FIELD_TYPE_DATETIME>(values, n, dst);
}

void StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL>::storage_to_column(const uint8_t* values,
                                                                          size_t n, IColumn& dst) {
    storage_layout_detail::repack_to_column<FieldType::OLAP_FIELD_TYPE_DECIMAL>(values, n, dst);
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, size_t length, const uint8_t* null_map,
        StorageBuffers& buffers) {
    ColumnString::check_chars_length(n * length, n);
    buffers.bytes.resize(n * length);
    // Erase old padding when a reused slot changes, e.g. "abcd" -> "a" in CHAR(4).
    memset(buffers.bytes.data(), 0, buffers.bytes.size());
    buffers.cells.resize(n);
    const auto& strings = assert_cast<const ColumnString&>(column);
    for (size_t i = 0; i < n; ++i) {
        if (null_map != nullptr && null_map[i] != 0) {
            buffers.cells[i] = Slice();
            continue;
        }
        const auto value = strings.get_data_at(row_pos + i);
        DCHECK_LE(value.size, length);
        uint8_t* data = buffers.bytes.data() + i * length;
        memcpy(data, value.data, value.size);
        buffers.cells[i] = Slice(data, length);
    }
    return reinterpret_cast<const uint8_t*>(buffers.cells.data());
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_BITMAP>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers) {
    return storage_layout_detail::serialize_batch<ColumnBitmap>(
            column, row_pos, n, buffers,
            [](const BitmapValue& value) { return value.getSizeInBytes(); },
            [](const BitmapValue& value, uint8_t* out) {
                value.write_to(reinterpret_cast<char*>(out));
                return value.getSizeInBytes();
            });
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_HLL>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers) {
    return storage_layout_detail::serialize_batch<ColumnHLL>(
            column, row_pos, n, buffers,
            [](const HyperLogLog& value) { return value.max_serialized_size(); },
            [](const HyperLogLog& value, uint8_t* out) { return value.serialize(out); });
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers) {
    return storage_layout_detail::serialize_batch<ColumnQuantileState>(
            column, row_pos, n, buffers,
            [](const QuantileState& value) { return value.get_serialized_size(); },
            [](const QuantileState& value, uint8_t* out) { return value.serialize(out); });
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_AGG_STATE>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers) {
    const auto& objects = assert_cast<const ColumnFixedLengthObject&>(column);
    const size_t item_size = objects.item_size();
    const auto* data =
            reinterpret_cast<const char*>(objects.get_data().data()) + row_pos * item_size;
    auto& cells = buffers.cells;
    cells.resize(n);
    for (size_t i = 0; i < n; ++i) {
        cells[i] = Slice(data + i * item_size, item_size);
    }
    return reinterpret_cast<const uint8_t*>(cells.data());
}

const uint8_t* column_to_storage(FieldType type, const IColumn& column, size_t row_pos, size_t n,
                                 StorageBuffers& buffers) {
    switch (type) {
#define CONVERT(FT)     \
    case FieldType::FT: \
        return StorageLayout<FieldType::FT>::column_to_storage(column, row_pos, n, buffers);
        DORIS_APPLY_FOR_FIXED_WIDTH_STORAGE_LAYOUT_TYPES(CONVERT)
        CONVERT(OLAP_FIELD_TYPE_VARCHAR)
        CONVERT(OLAP_FIELD_TYPE_STRING)
        CONVERT(OLAP_FIELD_TYPE_JSONB)
        CONVERT(OLAP_FIELD_TYPE_VARIANT)
        CONVERT(OLAP_FIELD_TYPE_BITMAP)
        CONVERT(OLAP_FIELD_TYPE_HLL)
        CONVERT(OLAP_FIELD_TYPE_QUANTILE_STATE)
        CONVERT(OLAP_FIELD_TYPE_AGG_STATE)
#undef CONVERT
    default:
        // CHAR included: its caller decides whether to pad it.
        throw Exception(ErrorCode::INTERNAL_ERROR, "No storage layout for type {}",
                        static_cast<int>(type));
    }
}

Status admit_storage_rows(FieldType type, const IColumn& column, size_t row_pos, size_t n,
                          const uint8_t* null_map) {
    if (type != FieldType::OLAP_FIELD_TYPE_STRING && type != FieldType::OLAP_FIELD_TYPE_JSONB) {
        return Status::OK();
    }
    const auto& strings = assert_cast<const ColumnString&>(column);
    for (size_t i = 0; i < n; ++i) {
        if (null_map != nullptr && null_map[i] != 0) {
            continue;
        }
        const StringRef value = strings.get_data_at(row_pos + i);
        if (UNLIKELY(std::cmp_greater(value.size, config::string_type_length_soft_limit_bytes))) {
            return Status::NotSupported(
                    "Not support string len over than `string_type_length_soft_limit_bytes`"
                    " in vec engine.");
        }
        // Make sure that the json binary data written in is the correct jsonb value.
        if (type == FieldType::OLAP_FIELD_TYPE_JSONB) {
            const JsonbDocument* doc = nullptr;
            RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(value.data, value.size, &doc));
        }
    }
    return Status::OK();
}

} // namespace doris
