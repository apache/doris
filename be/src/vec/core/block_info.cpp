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

#include "vec/core/block_info.h"

#include "vec/common/exception.h"
#include "vec/core/types.h"

namespace doris::vectorized {

void BlockMissingValues::set_bit(size_t column_idx, size_t row_idx) {
    RowsBitMask& mask = rows_mask_by_column_id[column_idx];
    mask.resize(row_idx + 1);
    mask[row_idx] = true;
}

const BlockMissingValues::RowsBitMask& BlockMissingValues::get_defaults_bitmask(
        size_t column_idx) const {
    static RowsBitMask none;
    auto it = rows_mask_by_column_id.find(column_idx);
    if (it != rows_mask_by_column_id.end()) return it->second;
    return none;
}

} // namespace doris::vectorized
