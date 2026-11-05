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

#include <pdqsort.h>

#include <algorithm>
#include <iterator>

#include "core/block/block.h"
#include "core/custom_allocator.h"
#include "load/memtable/memtable.h"

namespace doris {

// Sorting shared by every memtable model. The row representation differs (compact positions for
// DUP_KEYS, RowInBlock for aggregating models), so the caller supplies how a tracked row maps to
// its position in the mutable block. Keeping this as a free template lets each model instantiate
// the comparator with its own row type and keeps the inner loops free of virtual calls.
namespace memtable_detail {

template <typename RowRef, typename Comparator>
void sort_one_column(DorisVector<RowRef>& rows, Tie& tie, Comparator&& cmp) {
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(rows.begin(), static_cast<int>(iter.left())),
                std::next(rows.begin(), static_cast<int>(iter.right())),
                [&](const RowRef& lhs, const RowRef& rhs) -> bool { return cmp(lhs, rhs) < 0; });
        tie[iter.left()] = 0;
        for (auto i = iter.left() + 1; i < iter.right(); i++) {
            tie[i] = (cmp(rows[i - 1], rows[i]) == 0);
        }
    }
}

// Sorts rows[last_sorted_pos, size) by key and merges them into the already sorted prefix.
// newer_first selects the deterministic order of equal keys: DUP_KEYS emits the newest row first,
// aggregating models keep increasing row positions before equal rows are merged.
template <typename RowRef, typename RowPosGetter>
size_t sort_rows(MutableBlock& block, const TabletSchema& tablet_schema, size_t& last_sorted_pos,
                 DorisVector<RowRef>& rows, RowPosGetter&& get_row_pos, bool newer_first) {
    size_t same_keys_num = 0;
    Tie tie(last_sorted_pos, rows.size());
    for (size_t i = 0; i < tablet_schema.num_key_columns(); i++) {
        auto cmp = [&](const RowRef& lhs, const RowRef& rhs) -> int {
            return block.compare_one_column(get_row_pos(lhs), get_row_pos(rhs), i, -1);
        };
        sort_one_column(rows, tie, cmp);
    }

    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(rows.begin(), iter.left()), std::next(rows.begin(), iter.right()),
                [&](const RowRef& lhs, const RowRef& rhs) -> bool {
                    return newer_first ? get_row_pos(lhs) > get_row_pos(rhs)
                                       : get_row_pos(lhs) < get_row_pos(rhs);
                });
        same_keys_num += iter.right() - iter.left();
    }

    auto cmp = [&](const RowRef& lhs, const RowRef& rhs) -> bool {
        const auto lhs_pos = get_row_pos(lhs);
        const auto rhs_pos = get_row_pos(rhs);
        const auto value =
                block.compare_at(lhs_pos, rhs_pos, tablet_schema.num_key_columns(), block, -1);
        if (value == 0) {
            same_keys_num++;
            return newer_first ? lhs_pos > rhs_pos : lhs_pos < rhs_pos;
        }
        return value < 0;
    };
    auto new_row_it = std::next(rows.begin(), last_sorted_pos);
    std::inplace_merge(rows.begin(), new_row_it, rows.end(), cmp);
    last_sorted_pos = rows.size();
    return same_keys_num;
}

} // namespace memtable_detail

} // namespace doris
