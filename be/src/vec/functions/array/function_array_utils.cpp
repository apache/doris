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
    data.nested_col = &data.array_col->get_data();
    // extract nested column is nullable
    if (data.nested_col->is_nullable()) {
        const auto& nested_null_col = reinterpret_cast<const ColumnNullable&>(*data.nested_col);
        data.nested_nullmap_data = nested_null_col.get_null_map_data().data();
        data.nested_col = nested_null_col.get_nested_column_ptr().get();
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

} // namespace doris::vectorized
