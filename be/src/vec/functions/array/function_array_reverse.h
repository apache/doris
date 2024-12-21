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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayReverse.cpp
// and modified by Doris
#pragma once

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"

namespace doris::vectorized {

struct ArrayReverseImpl {
    static Status _execute(Block& block, const ColumnNumbers& arguments, uint32_t result,
                           size_t input_rows_count) {
        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        ColumnArrayExecutionData src;
        if (!extract_column_array_info(*src_column, src)) {
            return Status::RuntimeError(
                    fmt::format("execute failed, unsupported types for function {}({})", "reverse",
                                block.get_by_position(arguments[0]).type->get_name()));
        }

        bool is_nullable = src.nested_nullmap_data ? true : false;
        ColumnArrayMutableData dst = create_mutable_data(src.nested_col, is_nullable);
        dst.offsets_ptr->reserve(input_rows_count);

        auto res_val = _execute_internal(*src.nested_col, *src.offsets_ptr, *dst.nested_col,
                                         *dst.offsets_ptr, src.nested_nullmap_data,
                                         dst.nested_nullmap_data);
        if (!res_val) {
            return Status::RuntimeError(
                    fmt::format("execute failed or unsupported types for function {}({})",
                                "reverse", block.get_by_position(arguments[0]).type->get_name()));
        }

        ColumnPtr res_column = assemble_column_array(dst);
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

    static bool _execute_internal(const IColumn& src_column,
                                  const ColumnArray::Offsets64& src_offsets, IColumn& dest_column,
                                  ColumnArray::Offsets64& dest_offsets, const UInt8* src_null_map,
                                  ColumnUInt8::Container* dest_null_map) {
        size_t prev_src_offset = 0;

        for (auto curr_src_offset : src_offsets) {
            size_t array_size = curr_src_offset - prev_src_offset;
            for (size_t j = 0; j < array_size; ++j) {
                size_t j_reverse = curr_src_offset - j - 1;
                if (src_null_map && src_null_map[j_reverse]) {
                    DCHECK(dest_null_map != nullptr);
                    // Note: here we need to insert default value
                    dest_column.insert_default();
                    (*dest_null_map).push_back(true);
                    continue;
                }

                dest_column.insert_from(src_column, j_reverse);
                if (dest_null_map) {
                    (*dest_null_map).push_back(false);
                }
            }

            dest_offsets.push_back(curr_src_offset);
            prev_src_offset = curr_src_offset;
        }

        return true;
    }
};

} // namespace doris::vectorized
