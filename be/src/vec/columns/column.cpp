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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/IColumn.cpp
// and modified by Doris

#include "vec/columns/column.h"

#include "util/simd/bits.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

std::string IColumn::dump_structure() const {
    std::stringstream res;
    res << get_family_name() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr& subcolumn) {
        res << ", " << subcolumn->dump_structure();
    };

    const_cast<IColumn*>(this)->for_each_subcolumn(callback);

    res << ")";
    return res.str();
}

void IColumn::insert_from(const IColumn& src, size_t n) {
    insert(src[n]);
}

void IColumn::insert_from_multi_column(const std::vector<const IColumn*>& srcs,
                                       std::vector<size_t> positions) {
    for (size_t i = 0; i < srcs.size(); ++i) {
        insert_from(*srcs[i], positions[i]);
    }
}

void IColumn::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                          IColumn::Permutation& perms, EqualRange& range, bool last_column) const {
    sorter->sort_column(static_cast<const IColumn&>(*this), flags, perms, range, last_column);
}

void IColumn::compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                               int direction, std::vector<uint8>& cmp_res,
                               uint8* __restrict filter) const {
    auto sz = this->size();
    DCHECK(cmp_res.size() == sz);
    size_t begin = simd::find_zero(cmp_res, 0);
    while (begin < sz) {
        size_t end = simd::find_one(cmp_res, begin + 1);
        for (size_t row_id = begin; row_id < end; row_id++) {
            int res = this->compare_at(row_id, rhs_row_id, rhs, nan_direction_hint);
            if (res * direction < 0) {
                filter[row_id] = 1;
                cmp_res[row_id] = 1;
            } else if (res * direction > 0) {
                cmp_res[row_id] = 1;
            }
        }
        begin = simd::find_zero(cmp_res, end + 1);
    }
}

bool is_column_nullable(const IColumn& column) {
    return check_column<ColumnNullable>(column);
}

bool is_column_const(const IColumn& column) {
    return check_column<ColumnConst>(column);
}

ColumnPtr IColumn::create_with_offsets(const Offsets64& offsets, const Field& default_field,
                                       size_t total_rows, size_t shift) const {
    if (offsets.size() + shift != size()) {
        LOG(FATAL) << fmt::format(
                "Incompatible sizes of offsets ({}), shift ({}) and size of column {}",
                offsets.size(), shift, size());
    }
    auto res = clone_empty();
    res->reserve(total_rows);
    ssize_t current_offset = -1;
    for (size_t i = 0; i < offsets.size(); ++i) {
        ssize_t offsets_diff = static_cast<ssize_t>(offsets[i]) - current_offset;
        current_offset = offsets[i];
        if (offsets_diff > 1) {
            res->insert_many(default_field, offsets_diff - 1);
        }
        res->insert_from(*this, i + shift);
    }
    ssize_t offsets_diff = static_cast<ssize_t>(total_rows) - current_offset;
    if (offsets_diff > 1) {
        res->insert_many(default_field, offsets_diff - 1);
    }
    return res;
}

} // namespace doris::vectorized
