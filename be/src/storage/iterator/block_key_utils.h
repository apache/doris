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

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <ranges>

#include "core/block/block.h"
#include "core/column/column.h"
#include "util/simd/bits.h"

namespace doris {

// Marks `(*same_key)[i] = 1` when row `i` shares all leading `num_key_columns` key columns with
// row `i - 1` (for `i > 0`), and sets `(*same_key)[0] = 0`.
inline void mark_same_keys_in_block(const Block& block, size_t num_key_columns,
                                    EqualFlags* same_key) {
    const size_t rows = block.rows();
    DCHECK_GT(rows, 0);
    same_key->assign(rows, 1);
    (*same_key)[0] = 0;
    if (rows <= 1) {
        return;
    }
    // Compare from the last key column, which changes most often: once no row matches the row
    // before, earlier key columns need no comparison.
    for (size_t col_idx : std::views::iota(size_t {0}, num_key_columns) | std::views::reverse) {
        block.get_by_position(col_idx).column->update_equal_flags(*same_key);
        if (!simd::contain_one(same_key->data() + 1, rows - 1)) {
            break;
        }
    }
}

// Returns true if row `prev_row` of `prev_columns` matches row 0 of `block` on all leading
// `num_key_columns` key columns.
inline bool row_matches_first_key(const MutableColumns& prev_columns, size_t prev_row,
                                  const Block& block, size_t num_key_columns) {
    return std::ranges::all_of(std::views::iota(size_t {0}, num_key_columns) | std::views::reverse,
                               [&](size_t col_idx) {
                                   return prev_columns[col_idx]->compare_at(
                                                  prev_row, 0,
                                                  *block.get_by_position(col_idx).column, -1) == 0;
                               });
}

} // namespace doris
