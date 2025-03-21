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

#include "vec/functions/array/function_array_utils.h"

#include <stddef.h>

#include <algorithm>
#include <utility>

#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

bool extract_column_array_info(const IColumn& src, ColumnArrayExecutionData& data) {
    const IColumn* array_col = &src;
    // extract array nullable info
    if (src.is_nullable()) {
        const auto& null_col = reinterpret_cast<const ColumnNullable&>(src);
        data.array_nullmap_data = null_col.get_null_map_data().data();
        array_col = null_col.get_nested_column_ptr().get();
    }

    // check and get array column
    data.array_col = check_and_get_column<ColumnArray>(array_col);
    if (!data.array_col) {
        return false;
    }

    // extract array offsets and nested column
    data.offsets_ptr = &data.array_col->get_offsets();
    data.nested_col = data.array_col->get_data_ptr();
    // extract nested column is nullable
    if (data.nested_col->is_nullable()) {
        const auto& nested_null_col = reinterpret_cast<const ColumnNullable&>(*data.nested_col);
        data.nested_nullmap_data = nested_null_col.get_null_map_data().data();
        data.nested_col = nested_null_col.get_nested_column_ptr();
    }
    if (data.output_as_variant &&
        !WhichDataType(remove_nullable(data.nested_type)).is_variant_type()) {
        // set variant root column/type to from column/type
        auto variant = ColumnObject::create(true /*always nullable*/);
        variant->create_root(data.nested_type, make_nullable(data.nested_col)->assume_mutable());
        data.nested_col = variant->get_ptr();
    }
    return true;
}

ColumnArrayMutableData create_mutable_data(const IColumn* nested_col, bool is_nullable) {
    ColumnArrayMutableData dst;
    if (is_nullable) {
        dst.array_nested_col =
                ColumnNullable::create(nested_col->clone_empty(), ColumnUInt8::create());
        auto* nullable_col = reinterpret_cast<ColumnNullable*>(dst.array_nested_col.get());
        dst.nested_nullmap_data = &nullable_col->get_null_map_data();
        dst.nested_col = nullable_col->get_nested_column_ptr().get();
    } else {
        dst.array_nested_col = nested_col->clone_empty();
        dst.nested_col = dst.array_nested_col.get();
    }
    dst.offsets_col = ColumnArray::ColumnOffsets::create();
    dst.offsets_ptr =
            &reinterpret_cast<ColumnArray::ColumnOffsets*>(dst.offsets_col.get())->get_data();
    return dst;
}

MutableColumnPtr assemble_column_array(ColumnArrayMutableData& data) {
    return ColumnArray::create(std::move(data.array_nested_col), std::move(data.offsets_col));
}

void slice_array(ColumnArrayMutableData& dst, ColumnArrayExecutionData& src,
                 const IColumn& offset_column, const IColumn* length_column) {
    size_t cur = 0;
    for (size_t row = 0; row < src.offsets_ptr->size(); ++row) {
        size_t off = (*src.offsets_ptr)[row - 1];
        size_t len = (*src.offsets_ptr)[row] - off;
        Int64 start = offset_column.get_int(row);
        if (len == 0 || start == 0) {
            dst.offsets_ptr->push_back(cur);
            continue;
        }
        if (start > 0 && start <= len) {
            start += off - 1;
        } else if (start < 0 && -start <= len) {
            start += off + len;
        } else {
            dst.offsets_ptr->push_back(cur);
            continue;
        }
        Int64 end;
        if (length_column) {
            Int64 size = length_column->get_int(row);
            end = std::max((Int64)off, std::min((Int64)(off + len), start + size));
        } else {
            end = off + len;
        }
        for (size_t pos = start; pos < end; ++pos) {
            if (src.nested_nullmap_data && src.nested_nullmap_data[pos]) {
                dst.nested_col->insert_default();
                dst.nested_nullmap_data->push_back(1);
            } else {
                dst.nested_col->insert_from(*src.nested_col, pos);
                if (dst.nested_nullmap_data) {
                    dst.nested_nullmap_data->push_back(0);
                }
            }
        }
        if (start < end) {
            cur += end - start;
        }
        dst.offsets_ptr->push_back(cur);
    }
}

} // namespace doris::vectorized
