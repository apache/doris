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

#include "util/simd/bits.h"
#include "vec/core/columns_with_type_and_name.h"

namespace doris::vectorized {
/*
    If skip_null_key is false
        Check if there are null values in the columns of key_data, if so, return an error
        If there are no null values, convert the columns in key_data to non-nullable columns
    If skip_null_key is true
        Skip the rows where the key contains null
        Finally, convert the columns in key_data to non-nullable columns
*/
Status inline check_dict_input_data(ColumnsWithTypeAndName& key_data,
                                    ColumnsWithTypeAndName& value_data, bool skip_null_key) {
    if (!skip_null_key) {
        for (auto& key : key_data) {
            if (key.column->has_null()) {
                return Status::InternalError("key column {} has null value", key.name);
            }
            key.column = remove_nullable(key.column);
        }
        return Status::OK();
    }
    IColumn::Filter filter(key_data.front().column->size(), 1);

    for (auto& key : key_data) {
        if (key.column->is_nullable()) {
            const auto& null_map = assert_cast<const ColumnNullable*>(key.column.get())
                                           ->get_null_map_data(); // Get the null_map in the key
            for (size_t i = 0; i < key.column->size(); ++i) {
                if (null_map[i] == 1) {
                    // If the value in the null_map of the key is 0, filter it out
                    filter[i] = 0;
                }
            }
        }
        key.column = remove_nullable(key.column);
    }
    const size_t count =
            filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    // Similar to the filter_block_internal function in block.cpp
    auto filter_column = [&](ColumnPtr& column) {
        if (column->is_exclusive()) {
            column->assume_mutable()->filter(filter);
        } else {
            column = column->filter(filter, count);
        }
    };

    for (auto& key : key_data) {
        filter_column(key.column);
    }
    for (auto& value : value_data) {
        filter_column(value.column);
    }
    return Status::OK();
}

} // namespace doris::vectorized
