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

#include "vec/core/sort_block.h"

#include "vec/core/column_with_type_and_name.h"

namespace doris::vectorized {

ColumnsWithSortDescriptions get_columns_with_sort_description(const Block& block,
                                                              const SortDescription& description) {
    size_t size = description.size();
    ColumnsWithSortDescriptions res;
    res.reserve(size);

    for (size_t i = 0; i < size; ++i) {
        const IColumn* column =
                !description[i].column_name.empty()
                        ? block.get_by_name(description[i].column_name).column.get()
                        : block.safe_get_by_position(description[i].column_number).column.get();

        res.emplace_back(column, description[i]);
    }

    return res;
}

void sort_block(Block& src_block, Block& dest_block, const SortDescription& description,
                UInt64 limit) {
    if (!src_block.columns()) {
        return;
    }

    /// If only one column to sort by
    if (description.size() == 1) {
        bool reverse = description[0].direction == -1;

        const IColumn* column =
                !description[0].column_name.empty()
                        ? src_block.get_by_name(description[0].column_name).column.get()
                        : src_block.safe_get_by_position(description[0].column_number).column.get();

        IColumn::Permutation perm;
        column->get_permutation(reverse, limit, description[0].nulls_direction, perm);

        size_t columns = src_block.columns();
        for (size_t i = 0; i < columns; ++i) {
            dest_block.replace_by_position(
                    i, src_block.get_by_position(i).column->permute(perm, limit));
        }
    } else {
        size_t size = src_block.rows();
        IColumn::Permutation perm(size);
        for (size_t i = 0; i < size; ++i) {
            perm[i] = i;
        }

        if (limit >= size) {
            limit = 0;
        }

        ColumnsWithSortDescriptions columns_with_sort_desc =
                get_columns_with_sort_description(src_block, description);
        {
            EqualFlags flags(size, 1);
            EqualRange range {0, size};

            // TODO: ColumnSorter should be constructed only once.
            for (size_t i = 0; i < columns_with_sort_desc.size(); i++) {
                ColumnSorter sorter(columns_with_sort_desc[i], limit);
                sorter.operator()(flags, perm, range, i == columns_with_sort_desc.size() - 1);
            }
        }

        size_t columns = src_block.columns();
        for (size_t i = 0; i < columns; ++i) {
            dest_block.replace_by_position(
                    i, src_block.get_by_position(i).column->permute(perm, limit));
        }
    }
}

} // namespace doris::vectorized
