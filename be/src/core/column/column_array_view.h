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

#include "core/column/column_array.h"
#include "core/column/column_execute_util.h"

namespace doris {

// ArrayDataView represents a read-only view of a single row's array data
// (a slice of ColumnArray's flat nested data).
// Used as the return type of ColumnArrayView::operator[].
template <PrimitiveType PType>
struct ArrayDataView {
    using ElementType = typename ColumnElementView<PType>::ElementType;

    const ColumnElementView<PType>& data;
    const NullMap& nested_null_map;
    const size_t offset;
    const size_t length;

    size_t size() const { return length; }

    const ElementType* get_data() const {
        const ElementType* raw_data = data.get_data();
        return raw_data + offset;
    }

    // ColumnArray's data column is always Nullable, no need to check nullptr
    bool is_null_at(size_t idx) const { return nested_null_map[offset + idx]; }

    ElementType value_at(size_t idx) const { return data.get_element(offset + idx); }
};

// ColumnArrayView provides a read-only view over a column of Array<scalar_type>,
// handling Const / Nullable wrapping automatically.
//
// Supports index-based access: operator[](row) returns ArrayDataView, uses offsets[row-1] (sentinel)
template <PrimitiveType PType>
struct ColumnArrayView {
    const ColumnElementView<PType> element_data;
    const ColumnArray::Offsets64& offsets;
    const NullMap* outer_null_map;
    const NullMap& nested_null_map;
    const bool is_const;
    const size_t count;

    static ColumnArrayView create(const ColumnPtr& column_ptr) {
        // Step 1: unpack const
        const auto& [unpacked, is_const] = unpack_if_const(column_ptr);

        // Step 2: unpack outer nullable
        const NullMap* outer_null_map = nullptr;
        const IColumn* array_raw = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(unpacked.get())) {
            outer_null_map = &nullable->get_null_map_data();
            array_raw = nullable->get_nested_column_ptr().get();
        } else {
            array_raw = unpacked.get();
        }

        // Step 3: get ColumnArray
        const auto& array_column = assert_cast<const ColumnArray&>(*array_raw);

        // Step 4: unpack inner nullable (data column is always Nullable)
        const auto& nested_nullable = assert_cast<const ColumnNullable&>(array_column.get_data());
        const NullMap& nested_null_map = nested_nullable.get_null_map_data();
        const IColumn* data_column = nested_nullable.get_nested_column_ptr().get();

        return ColumnArrayView {.element_data = ColumnElementView<PType>(*data_column),
                                .offsets = array_column.get_offsets(),
                                .outer_null_map = outer_null_map,
                                .nested_null_map = nested_null_map,
                                .is_const = is_const,
                                .count = column_ptr->size()};
    }

    size_t size() const { return count; }

    bool is_null_at(size_t idx) const {
        if (outer_null_map) {
            return (*outer_null_map)[is_const ? 0 : idx];
        }
        return false;
    }

    // Index-based access: uses offsets[actual - 1] (PaddedPODArray sentinel guarantees [-1] is valid)
    ArrayDataView<PType> operator[](size_t idx) const {
        size_t actual = is_const ? 0 : idx;
        size_t off = offsets[actual - 1];
        size_t len = offsets[actual] - off;
        return ArrayDataView<PType> {.data = element_data,
                                     .nested_null_map = nested_null_map,
                                     .offset = off,
                                     .length = len};
    }
};

} // namespace doris
