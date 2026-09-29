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

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type/storage_field_type.h"
#include "storage/index/index_writer.h"
#include "storage/storage_layout.h"
#include "util/slice.h"

namespace doris::segment_v2 {

// The column entries of an index writer, fed from the cells a test holds: a
// test keeps its rows as Slices or as storage cells, the writer takes the
// column they came from.

inline MutableColumnPtr column_of_slices(const Slice* values, size_t n) {
    auto column = ColumnString::create();
    for (size_t i = 0; i < n; ++i) {
        if (values[i].data == nullptr) {
            column->insert_default();
        } else {
            column->insert_data(values[i].data, values[i].size);
        }
    }
    return column;
}

template <FieldType FT>
MutableColumnPtr column_of_cells(const void* cells, size_t n) {
    constexpr PrimitiveType PT = storage_field_type_to_primitive_type(FT);
    using Column = typename PrimitiveTypeTraits<PT>::ColumnType;
    MutableColumnPtr column;
    if constexpr (is_decimal(PT)) {
        column = Column::create(0, 0);
    } else {
        column = Column::create();
    }
    if (n > 0) {
        read_to_column<FT>(static_cast<const uint8_t*>(cells), n, *column);
    }
    return column;
}

inline MutableColumnPtr column_of_cells(FieldType type, const void* cells, size_t n) {
    switch (type) {
#define CASE(FT)        \
    case FieldType::FT: \
        return column_of_cells<FieldType::FT>(cells, n);
        DORIS_APPLY_FOR_FIXED_WIDTH_STORAGE_LAYOUT_TYPES(CASE)
#undef CASE
    default:
        LOG(FATAL) << "column_of_cells: FieldType " << static_cast<int>(type)
                   << " has no fixed-width cell";
        return nullptr;
    }
}

// Wraps the item column in a Nullable when the test marks NULL elements.
inline MutableColumnPtr with_item_nulls(MutableColumnPtr items, const uint8_t* item_nulls,
                                        size_t n) {
    if (item_nulls == nullptr) {
        return items;
    }
    auto null_map = ColumnUInt8::create();
    null_map->get_data().assign(item_nulls, item_nulls + n);
    return ColumnNullable::create(std::move(items), std::move(null_map));
}

inline size_t elements_of(const uint64_t* offsets, size_t num_rows) {
    return num_rows == 0 ? 0 : offsets[num_rows] - offsets[0];
}

inline Status add_slices(IndexColumnWriter& writer, const Slice* values, size_t n) {
    return writer.add(*column_of_slices(values, n), 0, n);
}

inline Status add_cells(IndexColumnWriter& writer, FieldType type, const void* cells, size_t n) {
    return writer.add(*column_of_cells(type, cells, n), 0, n);
}

inline Status add_slice_arrays(IndexColumnWriter& writer, const Slice* items,
                               const uint8_t* item_nulls, const uint64_t* offsets,
                               size_t num_rows) {
    const size_t n = elements_of(offsets, num_rows);
    return writer.add_array(*with_item_nulls(column_of_slices(items, n), item_nulls, n), 0, offsets,
                            num_rows);
}

inline Status add_cell_arrays(IndexColumnWriter& writer, FieldType item_type, const void* items,
                              const uint8_t* item_nulls, const uint64_t* offsets, size_t num_rows) {
    const size_t n = elements_of(offsets, num_rows);
    return writer.add_array(*with_item_nulls(column_of_cells(item_type, items, n), item_nulls, n),
                            0, offsets, num_rows);
}

// The float vectors of an ANN writer: `values` holds every row's elements back
// to back.
inline Status add_float_arrays(IndexColumnWriter& writer, const float* values,
                               const uint64_t* offsets, size_t num_rows) {
    const size_t n = elements_of(offsets, num_rows);
    auto column = ColumnFloat32::create();
    if (n > 0) {
        column->insert_many_raw_data(reinterpret_cast<const char*>(values), n);
    }
    return writer.add_array(*column, 0, offsets, num_rows);
}

} // namespace doris::segment_v2
