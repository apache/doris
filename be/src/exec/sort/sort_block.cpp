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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/sortBlock.cpp
// and modified by Doris

#include "exec/sort/sort_block.h"

#include <algorithm>

#include "core/block/column_with_type_and_name.h"
#include "core/column/variant_v2/column_variant_v2.h"

namespace doris {
void ColumnSorter::sort_column(const ColumnVariantV2& column, EqualFlags& flags,
                               IColumn::Permutation& perms, EqualRange& range,
                               bool last_column) const {
    _sort_by_default(column, flags, perms, range, last_column);
}

ColumnsWithSortDescriptions get_columns_with_sort_description(const Block& block,
                                                              const SortDescription& description) {
    size_t size = description.size();
    ColumnsWithSortDescriptions res;
    res.reserve(size);

    for (size_t i = 0; i < size; ++i) {
        const IColumn* column =
                block.safe_get_by_position(description[i].column_number).column.get();
        res.emplace_back(column, description[i]);
    }

    return res;
}

namespace {
// The order of the rows of `block` by `description`; when `limit` is not 0, only its first `limit`
// entries are sorted, and a `limit` that covers every row is reset to 0.
IColumn::Permutation get_block_permutation(const Block& block, const SortDescription& description,
                                           HybridSorter& hybrid_sorter, UInt64& limit) {
    /// If only one column to sort by
    if (description.size() == 1) {
        bool reverse = description[0].direction == -1;

        const IColumn* column =
                block.safe_get_by_position(description[0].column_number).column.get();

        IColumn::Permutation perm;
        column->get_permutation(reverse, limit, description[0].nulls_direction, hybrid_sorter,
                                perm);
        return perm;
    }
    size_t size = block.rows();
    IColumn::Permutation perm(size);
    for (size_t i = 0; i < size; ++i) {
        perm[i] = i;
    }

    if (limit >= size) {
        limit = 0;
    }

    ColumnsWithSortDescriptions columns_with_sort_desc =
            get_columns_with_sort_description(block, description);
    {
        EqualFlags flags(size, 1);
        EqualRange range {0, size};

        // TODO: ColumnSorter should be constructed only once.
        for (size_t i = 0; i < columns_with_sort_desc.size(); i++) {
            ColumnSorter sorter(columns_with_sort_desc[i], hybrid_sorter, limit);
            sorter.operator()(flags, perm, range, i == columns_with_sort_desc.size() - 1);
        }
    }
    return perm;
}

void permute_block(Block& src_block, Block& dest_block, const IColumn::Permutation& perm,
                   UInt64 limit) {
    size_t columns = src_block.columns();
    for (size_t i = 0; i < columns; ++i) {
        dest_block.replace_by_position(i,
                                       src_block.get_by_position(i).column->permute(perm, limit));
    }
}
} // namespace

void sort_block(Block& src_block, Block& dest_block, const SortDescription& description,
                HybridSorter& hybrid_sorter, UInt64 limit) {
    if (!src_block.columns()) {
        return;
    }
    const auto perm = get_block_permutation(src_block, description, hybrid_sorter, limit);
    permute_block(src_block, dest_block, perm, limit);
}

void sort_block_tail(Block& src_block, Block& dest_block, const SortDescription& description,
                     HybridSorter& hybrid_sorter, size_t rows) {
    if (!src_block.columns()) {
        return;
    }
    UInt64 limit = 0;
    const auto perm = get_block_permutation(src_block, description, hybrid_sorter, limit);
    const size_t kept = std::min(rows, perm.size());
    const IColumn::Permutation tail(perm.end() - kept, perm.end());
    permute_block(src_block, dest_block, tail, kept);
}

} // namespace doris
