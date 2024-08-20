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
void IColumn::append_data_by_selector_impl(MutablePtr& res, const Selector& selector, size_t begin,
                                           size_t end) const {
    size_t num_rows = size();

    if (num_rows < selector.size()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Size of selector: {} is larger than size of column: {}",
                               selector.size(), num_rows);
    }

    res->reserve(num_rows);

    for (size_t i = begin; i < end; ++i) {
        static_cast<Derived&>(*res).insert_from(*this, selector[i]);
    }
}
template <typename Derived>
void IColumn::append_data_by_selector_impl(MutablePtr& res, const Selector& selector) const {
    append_data_by_selector_impl<Derived>(res, selector, 0, selector.size());
}

} // namespace doris::vectorized
