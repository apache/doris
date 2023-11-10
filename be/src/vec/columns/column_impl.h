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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/IColumnImpl.h
// and modified by Doris

/**
  * This file implements template methods of IColumn that depend on other types
  * we don't want to include.
  * Currently, this is only the scatter_impl method that depends on PODArray
  * implementation.
  */

#pragma once

#include "vec/columns/column.h"
#include "vec/common/pod_array.h"

namespace doris::vectorized {

template <typename Derived>
std::vector<IColumn::MutablePtr> IColumn::scatter_impl(ColumnIndex num_columns,
                                                       const Selector& selector) const {
    size_t num_rows = size();

    if (num_rows != selector.size()) {
        LOG(FATAL) << fmt::format("Size of selector: {}, doesn't match size of column:{}",
                                  selector.size(), num_rows);
    }

    std::vector<MutablePtr> columns(num_columns);
    for (auto& column : columns) column = clone_empty();

    {
        size_t reserve_size =
                num_rows * 1.1 / num_columns; /// 1.1 is just a guess. Better to use n-sigma rule.

        if (reserve_size > 1)
            for (auto& column : columns) column->reserve(reserve_size);
    }

    for (size_t i = 0; i < num_rows; ++i)
        static_cast<Derived&>(*columns[selector[i]]).insert_from(*this, i);

    return columns;
}

template <typename Derived>
void IColumn::append_data_by_selector_impl(MutablePtr& res, const Selector& selector) const {
    size_t num_rows = size();

    if (num_rows < selector.size()) {
        LOG(FATAL) << fmt::format("Size of selector: {}, is larger than size of column:{}",
                                  selector.size(), num_rows);
    }

    res->reserve(num_rows);

    for (size_t i = 0; i < selector.size(); ++i)
        static_cast<Derived&>(*res).insert_from(*this, selector[i]);
}

template <typename Derived>
void IColumn::get_indices_of_non_default_rows_impl(IColumn::Offsets64& indices, size_t from,
                                                   size_t limit) const {
    size_t to = limit && from + limit < size() ? from + limit : size();
    indices.reserve(indices.size() + to - from);
    for (size_t i = from; i < to; ++i) {
        if (!static_cast<const Derived&>(*this).is_default_at(i)) {
            indices.push_back(i);
        }
    }
}

template <typename Derived>
double IColumn::get_ratio_of_default_rows_impl(double sample_ratio) const {
    if (sample_ratio <= 0.0 || sample_ratio > 1.0) {
        LOG(FATAL) << "Value of 'sample_ratio' must be in interval (0.0; 1.0], but got: "
                   << sample_ratio;
    }
    static constexpr auto max_number_of_rows_for_full_search = 1000;
    size_t num_rows = size();
    size_t num_sampled_rows = std::min(static_cast<size_t>(num_rows * sample_ratio), num_rows);
    size_t num_checked_rows = 0;
    size_t res = 0;
    if (num_sampled_rows == num_rows || num_rows <= max_number_of_rows_for_full_search) {
        for (size_t i = 0; i < num_rows; ++i)
            res += static_cast<const Derived&>(*this).is_default_at(i);
        num_checked_rows = num_rows;
    } else if (num_sampled_rows != 0) {
        for (size_t i = 0; i < num_rows; ++i) {
            if (num_checked_rows * num_rows <= i * num_sampled_rows) {
                res += static_cast<const Derived&>(*this).is_default_at(i);
                ++num_checked_rows;
            }
        }
    }
    if (num_checked_rows == 0) {
        return 0.0;
    }
    return static_cast<double>(res) / num_checked_rows;
}

} // namespace doris::vectorized
