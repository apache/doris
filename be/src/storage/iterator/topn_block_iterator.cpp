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

#include "storage/iterator/topn_block_iterator.h"

#include <algorithm>
#include <iterator>
#include <utility>

#include "common/cast_set.h"
#include "core/column/column.h"
#include "core/field.h"

namespace doris {

TopNBlockIterator::TopNBlockIterator(std::vector<RowsetOpener> rowsets, size_t limit,
                                     std::vector<uint32_t> sort_columns, SortDirection direction,
                                     std::map<uint32_t, DataTypePtr> virtual_column_types,
                                     FilterUpdater update_filter)
        : _rowsets(std::move(rowsets)),
          _limit(limit),
          _sort_columns(std::move(sort_columns)),
          _direction(direction),
          _virtual_column_types(std::move(virtual_column_types)),
          _update_filter(std::move(update_filter)) {
    DORIS_CHECK_GT(_limit, 0);
    DORIS_CHECK(!_sort_columns.empty());
}

bool TopNBlockIterator::RowOrder::operator()(size_t lhs, size_t rhs) const {
    // NULLs are the smallest (-1).
    const int r = topn->_rows.compare_at(lhs, rhs, &topn->_sort_columns, topn->_rows, -1);
    return topn->_direction == SortDirection::DESC ? r > 0 : r < 0;
}

bool TopNBlockIterator::_sorts_before(const Block& block, size_t row, size_t top_row) const {
    for (uint32_t position : _sort_columns) {
        const int r = block.get_by_position(position).column->compare_at(
                row, top_row, *_rows.get_column_by_position(position), -1);
        if (r != 0) {
            return _direction == SortDirection::DESC ? r > 0 : r < 0;
        }
    }
    return false;
}

Status TopNBlockIterator::next_block(Block* block, bool* eof) {
    if (_done) {
        *eof = true;
        return Status::OK();
    }
    _done = true;
    for (uint32_t position : _sort_columns) {
        DORIS_CHECK_LT(position, block->columns());
    }
    // A virtual column holds an empty placeholder in the read Blocks until a segment fills it; the
    // rows kept need a column of its type.
    Block empty_rows = block->clone_empty();
    for (const auto& [position, type] : _virtual_column_types) {
        DORIS_CHECK_LT(position, empty_rows.columns());
        empty_rows.get_by_position(position).column = type->create_column();
    }
    _rows = MutableBlock::build_mutable_block(std::move(empty_rows));

    for (const RowsetOpener& open_rowset : _rowsets) {
        std::vector<RowwiseIteratorUPtr> streams;
        RETURN_IF_ERROR(open_rowset(&streams));
        for (RowwiseIteratorUPtr& stream : streams) {
            RETURN_IF_ERROR(_read_stream(stream.get(), block));
            // Release a segment once it is read, not at the end of its rowset.
            stream.reset();
        }
    }
    _remove_dropped_rows();
    *block = _rows.to_block();
    *eof = block->rows() == 0;
    return Status::OK();
}

Status TopNBlockIterator::_read_stream(RowwiseIterator* stream, Block* block) {
    size_t read_rows = 0;
    while (read_rows < _limit) {
        block->clear_column_data();
        Status st = stream->next_batch(block);
        if (st.is<ErrorCode::END_OF_FILE>()) {
            break;
        }
        RETURN_IF_ERROR(st);
        read_rows += block->rows();
        RETURN_IF_ERROR(_keep_top_rows(*block));
    }
    return Status::OK();
}

Status TopNBlockIterator::_keep_top_rows(const Block& block) {
    const size_t rows = _count_top_rows(block);
    if (rows == 0) {
        return Status::OK();
    }
    const size_t first = _rows.rows();
    RETURN_IF_ERROR(_rows.add_rows(&block, 0, rows));
    for (size_t row = first; row < first + rows; ++row) {
        _top_rows.insert(row);
    }
    if (_top_rows.size() > _limit) {
        auto first_dropped = _top_rows.begin();
        std::advance(first_dropped, _limit);
        _top_rows.erase(first_dropped, _top_rows.end());
        // Dropped rows are removed in bulk, once they outnumber the rows kept.
        if (_rows.rows() > 2 * _limit) {
            _remove_dropped_rows();
        }
    }
    if (_update_filter && _top_rows.size() == _limit) {
        Field value;
        _rows.get_column_by_position(_sort_columns[0])->get(*_top_rows.rbegin(), value);
        RETURN_IF_ERROR(_update_filter(value));
    }
    return Status::OK();
}

size_t TopNBlockIterator::_count_top_rows(const Block& block) const {
    if (_top_rows.empty()) {
        return std::min(block.rows(), _limit);
    }
    // Once a row does not sort before the last top row, neither do the rows after it.
    const size_t last_top_row = *_top_rows.rbegin();
    size_t rows = 0;
    while (rows < block.rows() &&
           (_top_rows.size() + rows < _limit || _sorts_before(block, rows, last_top_row))) {
        ++rows;
    }
    return rows;
}

void TopNBlockIterator::_remove_dropped_rows() {
    Block rows = _rows.to_block();
    _rows = MutableBlock::build_mutable_block(rows.clone_empty());
    for (size_t row : _top_rows) {
        _rows.add_row(&rows, cast_set<int>(row));
    }
    const size_t top_rows = _top_rows.size();
    _top_rows.clear();
    for (size_t row = 0; row < top_rows; ++row) {
        _top_rows.insert(_top_rows.end(), row);
    }
}

} // namespace doris
