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

#include "storage/iterator/heap_merge_iterator.h"

#include <algorithm>
#include <utility>

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
using namespace ErrorCode;

VHeapMergeIterator::VHeapMergeIterator(std::vector<RowwiseIteratorUPtr> children,
                                       ReadSchemaSPtr read_schema, MergeDecision decision)
        : _read_schema(std::move(read_schema)), _decision(std::move(decision)) {
    _children.reserve(children.size());
    for (auto& child : children) {
        _children.emplace_back();
        _children.back().iter = std::move(child);
    }
}

Status VHeapMergeIterator::init(const StorageReadOptions& opts) {
    DORIS_CHECK_GT(opts.block_row_max, 0);
    _block_row_max = opts.block_row_max;
    _preferred_block_size_bytes = opts.preferred_block_size_bytes;
    _runtime_state = opts.runtime_state;
    _stats = opts.stats;

    for (size_t index = 0; index < _children.size(); ++index) {
        // An empty Block is used up: the child reads its first Block.
        _children[index].block = _read_schema->create_read_block();
        RETURN_IF_ERROR(_push_back_to_heap(cast_set<uint32_t>(index)));
    }
    return Status::OK();
}

Status VHeapMergeIterator::_push_back_to_heap(uint32_t child) {
    Child& current = _children[child];
    if (current.pos == current.block.rows()) {
        bool eof = false;
        RETURN_IF_ERROR(_read_next_block(current, &eof));
        if (eof) {
            // The child is done: release its segment and its Block now, not at the end of the
            // merge.
            current.iter.reset();
            current.block.clear();
            return Status::OK();
        }
    }
    _heap.push(child);
    return Status::OK();
}

Status VHeapMergeIterator::_read_next_block(Child& child, bool* eof) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    // A segment returns an empty batch when its predicates filter out every row it read.
    do {
        child.block.clear_column_data();
        Status st = child.iter->next_batch(&child.block);
        if (st.is<END_OF_FILE>()) {
            DCHECK_EQ(child.block.rows(), 0);
            *eof = true;
            child.pos = 0;
            return Status::OK();
        }
        RETURN_IF_ERROR(st);
        if (_runtime_state != nullptr && _runtime_state->is_cancelled()) [[unlikely]] {
            return _runtime_state->cancel_reason();
        }
    } while (child.block.rows() == 0);
    DCHECK_EQ(child.block.columns(), _read_schema->num_block_columns());
    child.pos = 0;
    *eof = false;
    return Status::OK();
}

bool VHeapMergeIterator::_less_than_current_row(uint32_t child, size_t row, uint32_t other) const {
    const Block& block = _children[child].block;
    const Child& other_child = _children[other];
    // `row` against the current row of `other` on `column`, in the direction of `column`: negative
    // when `row` goes first.
    auto compare = [&](const SortColumnDescription& column) {
        const int ordinal = column.column_number;
        return block.get_by_position(ordinal).column->compare_at(
                       row, other_child.pos, *other_child.block.get_by_position(ordinal).column,
                       column.nulls_direction) *
               column.direction;
    };
    for (const SortColumnDescription& key : _decision.keys) {
        if (const int r = compare(key); r != 0) {
            return r < 0;
        }
    }
    if (_decision.sequence.has_value()) {
        if (const int r = compare(*_decision.sequence); r != 0) {
            return r < 0;
        }
    }
    // The same key and sequence value: the newer child first.
    return child > other;
}

bool VHeapMergeIterator::_can_take_whole_block(uint32_t child, size_t max_rows) const {
    const Child& current = _children[child];
    const size_t block_rows = current.block.rows();
    return current.pos == 0 && block_rows <= max_rows &&
           (_heap.empty() || _less_than_current_row(child, block_rows - 1, _heap.top()));
}

size_t VHeapMergeIterator::_rows_before_next_child(uint32_t child, size_t limit) const {
    const Child& current = _children[child];
    limit = std::min(limit, current.block.rows() - current.pos);
    if (_heap.empty()) {
        return limit;
    }
    size_t rows = 1;
    while (rows < limit && _less_than_current_row(child, current.pos + rows, _heap.top())) {
        ++rows;
    }
    return rows;
}

size_t VHeapMergeIterator::_output_row_limit() const {
    if (_preferred_block_size_bytes == 0) {
        return _block_row_max;
    }
    // A child that is done holds an empty Block.
    size_t bytes = 0;
    size_t rows = 0;
    for (const Child& child : _children) {
        bytes += child.block.bytes();
        rows += child.block.rows();
    }
    const size_t row_bytes = std::max<size_t>(1, bytes / rows);
    return std::clamp<size_t>(_preferred_block_size_bytes / row_bytes, 1, _block_row_max);
}

void VHeapMergeIterator::_copy_rows(MutableColumns& columns, const Child& child, size_t rows) {
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        const IColumn& source = *child.block.get_by_position(ordinal).column;
        if (rows == 1) {
            columns[ordinal]->insert_from(source, child.pos);
        } else {
            columns[ordinal]->insert_range_from(source, child.pos, rows);
        }
    }
}

void VHeapMergeIterator::_move_block_to_output(MutableColumns& columns, Child& child) {
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        ColumnPtr& child_column = child.block.get_by_position(ordinal).column;
        MutableColumnPtr rows = IColumn::mutate(std::move(child_column));
        child_column = std::move(columns[ordinal]);
        columns[ordinal] = std::move(rows);
    }
}

Status VHeapMergeIterator::next_batch(Block* block) {
    DCHECK_EQ(block->columns(), _read_schema->num_block_columns());
    DCHECK_EQ(block->rows(), 0);
    if (_heap.empty()) {
        return Status::EndOfFile("no more rows to merge");
    }
    const size_t max_rows = _output_row_limit();
    size_t output_rows = 0;
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    while (!_heap.empty() && output_rows < max_rows) {
        // The child whose current row is the smallest.
        const uint32_t child_index = _heap.top();
        _heap.pop();
        Child& child = _children[child_index];
        const size_t block_rows = child.block.rows();
        const bool take_whole_block = _can_take_whole_block(child_index, max_rows);
        if (take_whole_block && output_rows == 0) {
            // The output is empty: it takes the Block's columns instead of copying its rows.
            RETURN_IF_CATCH_EXCEPTION(_move_block_to_output(columns, child));
            output_rows = block_rows;
        } else if (take_whole_block && output_rows + block_rows > max_rows) {
            // The Block does not fit: the output ends in front of it, so that the next output
            // takes it whole.
            _heap.push(child_index);
            break;
        } else {
            const size_t rows = _rows_before_next_child(child_index, max_rows - output_rows);
            RETURN_IF_CATCH_EXCEPTION(_copy_rows(columns, child, rows));
            child.pos += rows;
            output_rows += rows;
        }
        RETURN_IF_ERROR(_push_back_to_heap(child_index));
    }
    return Status::OK();
}

void VHeapMergeIterator::update_profile(RuntimeProfile* profile) {
    for (auto& child : _children) {
        if (child.iter != nullptr) {
            child.iter->update_profile(profile);
        }
    }
}

} // namespace doris
