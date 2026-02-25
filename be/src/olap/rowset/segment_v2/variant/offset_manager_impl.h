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

#include "olap/rowset/segment_v2/column_reader.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"

namespace doris::segment_v2 {

template <typename NestedGroupT>
void OffsetManager::pad_group_to_row(NestedGroupT& group, size_t target_row) {
    pad_group(group, target_row, PadMode::RowInclusive);
}

template <typename NestedGroupT>
void OffsetManager::pad_group(NestedGroupT& group, size_t target, PadMode mode) {
    group.ensure_offsets();
    auto* offsets_col = assert_cast<vectorized::ColumnOffset64*>(group.offsets.get());
    if (mode == PadMode::RowInclusive) {
        // If offsets.size() <= target_row, pad with empty arrays until offsets.size() > target_row.
        while (offsets_col->size() <= target) {
            offsets_col->get_data().push_back(static_cast<uint64_t>(group.current_flat_size));
        }
    } else {
        // If offsets.size() < target_element, pad with empty arrays until offsets.size() >= target.
        while (offsets_col->size() < target) {
            offsets_col->get_data().push_back(static_cast<uint64_t>(group.current_flat_size));
        }
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
    // Back-fill missing rows with empty arrays before processing current row.
    // This handles the case when a NestedGroup is created mid-batch.
    pad_group(group, element_idx, PadMode::ElementExclusive);
}

inline Status OffsetManager::read_offsets_with_prev(ColumnIterator* iter, ordinal_t start,
                                                    size_t count, uint64_t* prev,
                                                    vectorized::MutableColumnPtr* out) {
    *prev = 0;
    if (!(*out)) {
        *out = vectorized::ColumnOffset64::create();
    } else {
        (*out)->clear();
    }
    if (count == 0) {
        return Status::OK();
    }

    if (start > 0) {
        RETURN_IF_ERROR(iter->seek_to_ordinal(start - 1));
        vectorized::MutableColumnPtr prev_col = vectorized::ColumnOffset64::create();
        size_t one = 1;
        bool has_null = false;
        RETURN_IF_ERROR(iter->next_batch(&one, prev_col, &has_null));
        auto* prev_data = assert_cast<vectorized::ColumnOffset64*>(prev_col.get());
        if (!prev_data->get_data().empty()) {
            *prev = prev_data->get_data()[0];
        }
    }

    RETURN_IF_ERROR(iter->seek_to_ordinal(start));
    bool has_null = false;
    size_t to_read = count;
    RETURN_IF_ERROR(iter->next_batch(&to_read, *out, &has_null));
    return Status::OK();
}

inline Status OffsetManager::read_offsets_with_prev(ColumnIterator* iter, ordinal_t start,
                                                    size_t count, uint64_t* prev,
                                                    std::vector<uint64_t>* out) {
    out->clear();
    vectorized::MutableColumnPtr col;
    RETURN_IF_ERROR(read_offsets_with_prev(iter, start, count, prev, &col));
    auto* data = assert_cast<vectorized::ColumnOffset64*>(col.get());
    out->assign(data->get_data().begin(), data->get_data().end());
    return Status::OK();
}

} // namespace doris::segment_v2
