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

} // namespace doris::vectorized
