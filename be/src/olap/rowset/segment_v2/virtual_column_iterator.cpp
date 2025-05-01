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

#include "virtual_column_iterator.h"

#include <memory>

#include "vec/columns/column.h"
#include "vec/columns/column_nothing.h"

namespace doris::segment_v2 {

VirtualColumnIterator::VirtualColumnIterator()
        : _materialized_column_ptr(vectorized::ColumnNothing::create(1)) {}

// Init implementation
Status VirtualColumnIterator::init(const ColumnIteratorOptions& opts) {
    // Virtual column doesn't need special initialization
    return Status::OK();
}

// Next batch implementation
Status VirtualColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                         bool* has_null) {
    // if (_materialized_column_ptr == nullptr) {
    //     return Status::InternalError("No materialized column set in VirtualColumnIterator");
    // }

    // // Copy data from materialized column to destination
    // size_t rows_to_copy = std::min(*n, _materialized_column_ptr->size());

    // // Reset destination column first
    // dst->resize(0);

    // // Insert data from materialized column to destination
    // for (size_t i = 0; i < rows_to_copy; i++) {
    //     dst->insert_from(*_materialized_column_ptr, i);
    // }

    // *n = rows_to_copy;

    // // Determine if there are null values
    // if (has_null != nullptr) {
    //     *has_null = _materialized_column_ptr->has_nulls();
    // }

    return Status::OK();
}

Status VirtualColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                             vectorized::MutableColumnPtr& dst) {
    if (vectorized::check_and_get_column<vectorized::ColumnNothing>(*_materialized_column_ptr)) {
        return Status::OK();
    }

    doris::vectorized::IColumn::Filter filter(_materialized_column_ptr->size(), 0);

    // Convert rowids to filter
    for (size_t i = 0; i < count; ++i) {
        filter[rowids[i]] = 1;
    }

    // Apply filter to materialized column
    doris::vectorized::IColumn::Ptr res_col = _materialized_column_ptr->filter(filter, 0);
    // Update dst column
    dst->clear();
    dst->insert_range_from(*res_col, 0, res_col->size());

    return Status::OK();
}

} // namespace doris::segment_v2