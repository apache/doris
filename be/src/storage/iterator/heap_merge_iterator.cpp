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

HeapMergeOrder HeapMergeOrder::by_all_keys(SortDirection child_iterator_direction) {
    HeapMergeOrder order;
    order._child_iterator_direction = child_iterator_direction;
    return order;
}

HeapMergeOrder HeapMergeOrder::by_all_keys_and_same_key_col(
        int32_t same_key_sort_col_idx, SortDirection same_key_sort_direction,
        SortDirection child_iterator_direction) {
    DORIS_CHECK_GE(same_key_sort_col_idx, 0);
    HeapMergeOrder order;
    order._same_key_sort_col_idx = same_key_sort_col_idx;
    order._same_key_sort_direction = same_key_sort_direction;
    order._child_iterator_direction = child_iterator_direction;
    return order;
}

std::vector<HeapSortCol> HeapMergeOrder::resolve_sort_cols(size_t num_key_columns) const {
    std::vector<HeapSortCol> cols;
    cols.reserve(num_key_columns + (_same_key_sort_col_idx >= 0 ? 1 : 0));
    for (size_t col_idx = 0; col_idx < num_key_columns; ++col_idx) {
        cols.push_back({cast_set<uint32_t>(col_idx), SortDirection::ASC});
    }
    if (_same_key_sort_col_idx >= 0) {
        cols.push_back({static_cast<uint32_t>(_same_key_sort_col_idx), _same_key_sort_direction});
    }
    return cols;
}

VHeapMergeIterator::VHeapMergeIterator(std::vector<RowwiseIteratorUPtr> children,
                                       ReadSchemaSPtr read_schema, HeapMergeOrder order)
        : _read_schema(std::move(read_schema)),
          _sort_cols(order.resolve_sort_cols(_read_schema->num_key_columns())),
          _child_iterator_direction(order.child_iterator_direction()) {
    _children.reserve(children.size());
    for (auto& child : children) {
        _children.emplace_back();
        _children.back().iter = std::move(child);
    }
}

Status VHeapMergeIterator::init(const StorageReadOptions& opts) {
    _batch_limit = BlockBatchLimit(opts.block_row_max, opts.preferred_block_size_bytes);
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
        RETURN_IF_ERROR(_read_next_block(&current, &eof));
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

Status VHeapMergeIterator::_read_next_block(Child* child, bool* eof) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    // A segment returns an empty batch when its predicates filter out every row it read.
    do {
        child->block.clear_column_data();
        Status st = child->iter->next_batch(&child->block);
        if (st.is<END_OF_FILE>()) {
            DCHECK_EQ(child->block.rows(), 0);
            *eof = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(st);
        if (_runtime_state != nullptr && _runtime_state->is_cancelled()) [[unlikely]] {
            return _runtime_state->cancel_reason();
        }
    } while (child->block.rows() == 0);
    DCHECK_EQ(child->block.columns(), _read_schema->num_block_columns());
    child->pos = 0;
    *eof = false;
    return Status::OK();
}

bool VHeapMergeIterator::_less_than_current_row(uint32_t child, size_t row, uint32_t other) const {
    const Block& block = _children[child].block;
    const Child& other_child = _children[other];
    for (const auto& [col_idx, direction] : _sort_cols) {
        // NULLs are smallest (-1).
        const int r = block.get_by_position(col_idx).column->compare_at(
                row, other_child.pos, *other_child.block.get_by_position(col_idx).column, -1);
        if (r != 0) {
            return direction == SortDirection::ASC ? r < 0 : r > 0;
        }
    }
    return _child_iterator_direction == SortDirection::ASC ? child < other : child > other;
}

size_t VHeapMergeIterator::_rows_before_next_child(uint32_t child, size_t limit) const {
    DCHECK(!_heap.empty());
    const Child& current = _children[child];
    limit = std::min(limit, current.block.rows() - current.pos);
    // The current row goes first, as the child was on top of the heap.
    const uint32_t next = _heap.top();
    size_t rows = 1;
    while (rows < limit && _less_than_current_row(child, current.pos + rows, next)) {
        ++rows;
    }
    return rows;
}

size_t VHeapMergeIterator::_output_row_limit() const {
    if (_batch_limit.preferred_bytes() == 0) {
        return _batch_limit.max_rows();
    }
    // A child that is done holds an empty Block.
    size_t bytes = 0;
    size_t rows = 0;
    for (const Child& child : _children) {
        bytes += child.block.bytes();
        rows += child.block.rows();
    }
    return _batch_limit.row_limit(bytes, rows);
}

void VHeapMergeIterator::_copy_rows(MutableColumns* columns, const Child& child, size_t rows) {
    for (size_t col_idx = 0; col_idx < columns->size(); ++col_idx) {
        const IColumn& source = *child.block.get_by_position(col_idx).column;
        if (rows == 1) {
            (*columns)[col_idx]->insert_from(source, child.pos);
        } else {
            (*columns)[col_idx]->insert_range_from(source, child.pos, rows);
        }
    }
}

void VHeapMergeIterator::_move_block_to_output(MutableColumns* columns, Child* child) {
    for (size_t col_idx = 0; col_idx < columns->size(); ++col_idx) {
        ColumnPtr& child_column = child->block.get_by_position(col_idx).column;
        MutableColumnPtr rows = IColumn::mutate(std::move(child_column));
        child_column = std::move((*columns)[col_idx]);
        (*columns)[col_idx] = std::move(rows);
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
        const uint32_t child_index = _heap.top();
        _heap.pop();
        Child& child = _children[child_index];
        const size_t block_rows = child.block.rows();
        // Whether every row left in the Block sorts before the next child.
        const bool less_than_all =
                _heap.empty() || _less_than_current_row(child_index, block_rows - 1, _heap.top());
        size_t rows = 0;
        if (less_than_all) {
            if (child.pos == 0 && block_rows <= max_rows) {
                if (output_rows > 0) {
                    // The output ends in front of the Block, so that the next one takes its
                    // columns instead of copying its rows.
                    _heap.push(child_index);
                    break;
                }
                RETURN_IF_CATCH_EXCEPTION(_move_block_to_output(&columns, &child));
                // The output is that Block alone: rows appended to its columns could reallocate
                // them.
                return _push_back_to_heap(child_index);
            }
            rows = std::min(block_rows - child.pos, max_rows - output_rows);
        } else {
            rows = _rows_before_next_child(child_index, max_rows - output_rows);
        }
        RETURN_IF_CATCH_EXCEPTION(_copy_rows(&columns, child, rows));
        child.pos += rows;
        output_rows += rows;
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
