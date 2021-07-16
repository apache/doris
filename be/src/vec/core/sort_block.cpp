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

#include "vec/core/sort_block.h"

#include <pdqsort.h>

#include "vec/columns/column_string.h"
#include "vec/common/typeid_cast.h"

namespace doris::vectorized {

static inline bool needCollation(const IColumn* column, const SortColumnDescription& description) {
    if (!description.collator) return false;

    if (!typeid_cast<const ColumnString*>(column)) { /// TODO Nullable(String)
        LOG(FATAL) << "Collations could be specified only for String columns.";
    }

    return true;
}

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

struct PartialSortingLess {
    const ColumnsWithSortDescriptions& columns;

    explicit PartialSortingLess(const ColumnsWithSortDescriptions& columns_) : columns(columns_) {}

    bool operator()(size_t a, size_t b) const {
        for (ColumnsWithSortDescriptions::const_iterator it = columns.begin(); it != columns.end();
             ++it) {
            int res = it->second.direction *
                      it->first->compare_at(a, b, *it->first, it->second.nulls_direction);
            if (res < 0)
                return true;
            else if (res > 0)
                return false;
        }
        return false;
    }
};

struct PartialSortingLessWithCollation {
    const ColumnsWithSortDescriptions& columns;

    explicit PartialSortingLessWithCollation(const ColumnsWithSortDescriptions& columns_)
            : columns(columns_) {}

    bool operator()(size_t a, size_t b) const {
        for (ColumnsWithSortDescriptions::const_iterator it = columns.begin(); it != columns.end();
             ++it) {
            int res;
            if (needCollation(it->first, it->second)) {
                const ColumnString& column_string = typeid_cast<const ColumnString&>(*it->first);
                res = column_string.compare_at_with_collation(a, b, *it->first,
                                                              *it->second.collator);
            } else
                res = it->first->compare_at(a, b, *it->first, it->second.nulls_direction);

            res *= it->second.direction;
            if (res < 0)
                return true;
            else if (res > 0)
                return false;
        }
        return false;
    }
};

void sort_block(Block& block, const SortDescription& description, UInt64 limit) {
    if (!block) return;

    /// If only one column to sort by
    if (description.size() == 1) {
        bool reverse = description[0].direction == -1;

        const IColumn* column =
                !description[0].column_name.empty()
                        ? block.get_by_name(description[0].column_name).column.get()
                        : block.safe_get_by_position(description[0].column_number).column.get();

        IColumn::Permutation perm;
        column->get_permutation(reverse, limit, description[0].nulls_direction, perm);

        size_t columns = block.columns();
        for (size_t i = 0; i < columns; ++i)
            block.get_by_position(i).column = block.get_by_position(i).column->permute(perm, limit);
    } else {
        size_t size = block.rows();
        IColumn::Permutation perm(size);
        for (size_t i = 0; i < size; ++i) perm[i] = i;

        if (limit >= size) limit = 0;

        ColumnsWithSortDescriptions columns_with_sort_desc =
                get_columns_with_sort_description(block, description);
        {
            PartialSortingLess less(columns_with_sort_desc);

            if (limit)
                std::partial_sort(perm.begin(), perm.begin() + limit, perm.end(), less);
            else
                pdqsort(perm.begin(), perm.end(), less);
        }

        size_t columns = block.columns();
        for (size_t i = 0; i < columns; ++i)
            block.get_by_position(i).column = block.get_by_position(i).column->permute(perm, limit);
    }
}

void stable_get_permutation(const Block& block, const SortDescription& description,
                            IColumn::Permutation& out_permutation) {
    if (!block) return;

    size_t size = block.rows();
    out_permutation.resize(size);
    for (size_t i = 0; i < size; ++i) out_permutation[i] = i;

    ColumnsWithSortDescriptions columns_with_sort_desc =
            get_columns_with_sort_description(block, description);

    std::stable_sort(out_permutation.begin(), out_permutation.end(),
                     PartialSortingLess(columns_with_sort_desc));
}

bool is_already_sorted(const Block& block, const SortDescription& description) {
    if (!block) return true;

    size_t rows = block.rows();

    ColumnsWithSortDescriptions columns_with_sort_desc =
            get_columns_with_sort_description(block, description);

    PartialSortingLess less(columns_with_sort_desc);

    /** If the rows are not too few, then let's make a quick attempt to verify that the block is not sorted.
     * Constants - at random.
     */
    static constexpr size_t num_rows_to_try = 10;
    if (rows > num_rows_to_try * 5) {
        for (size_t i = 1; i < num_rows_to_try; ++i) {
            size_t prev_position = rows * (i - 1) / num_rows_to_try;
            size_t curr_position = rows * i / num_rows_to_try;

            if (less(curr_position, prev_position)) return false;
        }
    }

    for (size_t i = 1; i < rows; ++i)
        if (less(i, i - 1)) return false;

    return true;
}

void stable_sort_block(Block& block, const SortDescription& description) {
    if (!block) return;

    IColumn::Permutation perm;
    stable_get_permutation(block, description, perm);

    size_t columns = block.columns();
    for (size_t i = 0; i < columns; ++i)
        block.safe_get_by_position(i).column =
                block.safe_get_by_position(i).column->permute(perm, 0);
}

} // namespace doris::vectorized
