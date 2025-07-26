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

#include <cstddef>
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
    DCHECK(labels->size() == column->size()) << "labels size: " << labels->size()
                                             << ", materialized column size: " << column->size();
    // 1. do sort to labels
    // column: [100, 101, 102, 99, 50, 49]
    // lables: [5,   4,   1,   10, 7,  2]
    const std::vector<uint64_t>& labels_ref = *labels;
    const size_t n = labels_ref.size();
    VLOG_DEBUG << fmt::format("Input labels {}", fmt::join(labels_ref, ", "));
    if (n == 0) {
        _size = 0;
        _max_ordinal = 0;
        return;
    }
    std::vector<std::pair<size_t, size_t>> order(n);
    // {5:0, 4:1, 1:2, 10:3, 7:4, 2:5}
    for (size_t i = 0; i < n; ++i) {
        order[i] = {labels_ref[i], i};
    }
    // Sort by labels, so we can scatter the column by global row id.
    // After sort, order will be:
    // order: {1-2, 2-5, 4-1, 5-0, 7-4, 10-3}
    std::sort(order.begin(), order.end(),
              [&](const auto& a, const auto& b) { return a.first < b.first; });
    _max_ordinal = order[n - 1].first;
    // 2. scatter column
    auto scattered_column = column->clone_empty();
    // We need a mapping from global row id to local index in the materialized column.
    _row_id_to_idx.clear();
    for (size_t i = 0; i < n; ++i) {
        size_t global_idx = order[i].first;        // global row id
        size_t original_col_idx = order[i].second; // original index in the column
        _row_id_to_idx[global_idx] = i;
        scattered_column->insert_from(*column, original_col_idx);
    }

    // After scatter:
    // scattered_column: [102, 49, 101, 100, 50, 99]
    // _row_id_to_idx: {1:0, 2:1, 4:2, 5:3, 7:4, 10:5}
    _materialized_column_ptr = std::move(scattered_column);

    _size = n;

    std::string msg;
    for (const auto& pair : _row_id_to_idx) {
        msg += fmt::format("{}: {}, ", pair.first, pair.second);
    }

    VLOG_DEBUG << fmt::format("virtual column iterator, row_idx_to_idx:\n{}", msg);
    _filter = doris::vectorized::IColumn::Filter(_size, 0);
}

Status VirtualColumnIterator::seek_to_ordinal(ordinal_t ord_idx) {
    if (_size == 0 ||
        vectorized::check_and_get_column<vectorized::ColumnNothing>(*_materialized_column_ptr)) {
        // _materialized_column is not set. do nothing.
        return Status::OK();
    }

    if (ord_idx >= _max_ordinal) {
        return Status::InternalError("Seek to ordinal out of range: {} out of {}", ord_idx,
                                     _max_ordinal);
    }

    _current_ordinal = ord_idx;

    return Status::OK();
}

// Next batch implementation
Status VirtualColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                         bool* has_null) {
    size_t rows_num_to_read = *n;
    if (rows_num_to_read == 0 ||
        vectorized::check_and_get_column<vectorized::ColumnNothing>(*_materialized_column_ptr)) {
        return Status::OK();
    }

    if (_row_id_to_idx.find(_current_ordinal) == _row_id_to_idx.end()) {
        return Status::InternalError("Current ordinal {} not found in row_id_to_idx map",
                                     _current_ordinal);
    }

    // Update dst column
    if (vectorized::check_and_get_column<vectorized::ColumnNothing>(*dst)) {
        VLOG_DEBUG << fmt::format("Dst is nothing column, create new mutable column");
        dst = _materialized_column_ptr->clone_empty();
    }

    size_t start = _row_id_to_idx[_current_ordinal];
    dst->insert_range_from(*_materialized_column_ptr, start, rows_num_to_read);

    VLOG_DEBUG << fmt::format("Virtual column iterators, next_batch, rows reads: {}, dst size: {}",
                              rows_num_to_read, dst->size());

    _current_ordinal += rows_num_to_read;
    return Status::OK();
}

Status VirtualColumnIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                             vectorized::MutableColumnPtr& dst) {
    if (count == 0 ||
        vectorized::check_and_get_column<vectorized::ColumnNothing>(*_materialized_column_ptr)) {
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
    if (vectorized::check_and_get_column<vectorized::ColumnNothing>(*dst)) {
        VLOG_DEBUG << fmt::format("Dst is nothing column, create new mutable column");
        dst = res_col->assume_mutable();
    } else {
        dst->insert_range_from(*res_col, 0, res_col->size());
    }

    VLOG_DEBUG << fmt::format(
            "Virtual column iterators, read_by_rowids, rowids size: {}, dst size: {}", count,
            dst->size());
    return Status::OK();
}

} // namespace doris::segment_v2