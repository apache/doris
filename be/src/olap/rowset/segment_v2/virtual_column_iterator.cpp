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

#include <cstring>
#include <memory>

#include "vec/columns/column.h"
#include "vec/columns/column_nothing.h"

namespace doris::segment_v2 {

VirtualColumnIterator::VirtualColumnIterator()
        : _materialized_column_ptr(vectorized::ColumnNothing::create(0)) {}

// Init implementation
Status VirtualColumnIterator::init(const ColumnIteratorOptions& opts) {
    // Virtual column doesn't need special initialization
    return Status::OK();
}

void VirtualColumnIterator::prepare_materialization(vectorized::IColumn::Ptr column,
                                                    std::unique_ptr<std::vector<uint64_t>> labels) {
    _materialized_column_ptr = column;
    _row_id_to_idx.clear();
    DCHECK(labels->size() == _materialized_column_ptr->size())
            << "labels size: " << labels->size()
            << ", materialized column size: " << _materialized_column_ptr->size();
    _size = _materialized_column_ptr->size();
    for (size_t i = 0; i < _size; ++i) {
        _row_id_to_idx[(*labels)[i]] = i;
    }

    _filter = doris::vectorized::IColumn::Filter(_size, 0);
}

Status VirtualColumnIterator::seek_to_first() {
    if (_size < 0) {
        // _materialized_column is not set. do nothing.
        return Status::OK();
    }
    _current_ordinal = 0;

    return Status::OK();
}

Status VirtualColumnIterator::seek_to_ordinal(ordinal_t ord_idx) {
    if (_size < 0 ||
        vectorized::check_and_get_column<vectorized::ColumnNothing>(*_materialized_column_ptr)) {
        // _materialized_column is not set. do nothing.
        return Status::OK();
    }

    if (ord_idx >= _size) {
        return Status::InternalError("Seek to ordinal out of range: {} out of {}", ord_idx, _size);
    }

    _current_ordinal = ord_idx;

    return Status::OK();
}

// Next batch implementation
Status VirtualColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                         bool* has_null) {
    if (vectorized::check_and_get_column<vectorized::ColumnNothing>(*_materialized_column_ptr)) {
        return Status::OK();
    }
    size_t rows_num_to_read = *n;
    if (_row_id_to_idx.find(_current_ordinal) == _row_id_to_idx.end()) {
        return Status::InternalError("Current ordinal {} not found in row_id_to_idx map",
                                     _current_ordinal);
    }
    size_t start = _row_id_to_idx[_current_ordinal];
    // Update dst column
    dst = _materialized_column_ptr->clone_empty();
    dst->insert_range_from(*_materialized_column_ptr, start, rows_num_to_read);

    LOG_INFO("Virtual column iterators, next_batch, rows reads: {}, dst size: {}", rows_num_to_read,
             dst->size());

    _current_ordinal += rows_num_to_read;
    return Status::OK();
}

Status VirtualColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                             vectorized::MutableColumnPtr& dst) {
    if (vectorized::check_and_get_column<vectorized::ColumnNothing>(*_materialized_column_ptr)) {
        return Status::OK();
    }

    memset(_filter.data(), 0, _size);

    // Convert rowids to filter
    for (size_t i = 0; i < count; ++i) {
        _filter[_row_id_to_idx[rowids[i]]] = 1;
    }

    // Apply filter to materialized column
    doris::vectorized::IColumn::Ptr res_col = _materialized_column_ptr->filter(_filter, 0);
    // Update dst column
    dst = res_col->assume_mutable();

    LOG_INFO("Virtual column iterators, read_by_rowids, rowids size: {}, dst size: {}", count,
             dst->size());
    return Status::OK();
}

} // namespace doris::segment_v2