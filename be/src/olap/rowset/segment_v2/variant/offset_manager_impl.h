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

#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"

namespace doris::segment_v2 {

template <typename NestedGroupT>
void OffsetManager::pad_group_to_row(NestedGroupT& group, size_t target_row) {
    group.ensure_offsets();
    auto* offsets_col = assert_cast<vectorized::ColumnOffset64*>(group.offsets.get());
    // If offsets.size() <= target_row, this group is missing entries
    // for some rows. Pad with the same offset (indicating empty arrays).
    while (offsets_col->size() <= target_row) {
        offsets_col->get_data().push_back(static_cast<uint64_t>(group.current_flat_size));
    }
}

template <typename NestedGroupsMapT>
void OffsetManager::pad_all_groups_to_row(NestedGroupsMapT& groups, size_t target_row) {
    for (auto& [path, group] : groups) {
        if (!group || group->is_disabled) {
            continue;
        }
        pad_group_to_row(*group, target_row);
    }
}

template <typename NestedGroupT>
void OffsetManager::append_offset(NestedGroupT& group, size_t array_size) {
    group.ensure_offsets();
    auto* offsets_col = assert_cast<vectorized::ColumnOffset64*>(group.offsets.get());
    const size_t new_total = group.current_flat_size + array_size;
    offsets_col->get_data().push_back(static_cast<uint64_t>(new_total));
    group.current_flat_size = new_total;
}

template <typename NestedGroupT>
void OffsetManager::backfill_to_element(NestedGroupT& group, size_t element_idx) {
    group.ensure_offsets();
    auto* offsets_col = assert_cast<vectorized::ColumnOffset64*>(group.offsets.get());
    // Back-fill missing rows with empty arrays before processing current row.
    // This handles the case when a NestedGroup is created mid-batch.
    while (offsets_col->size() < element_idx) {
        offsets_col->get_data().push_back(static_cast<uint64_t>(group.current_flat_size));
    }
}

} // namespace doris::segment_v2
