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

#include "vec/common/sort/heap_sorter.h"

#include <glog/logging.h>

#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/sort_block.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

HeapSorter::HeapSorter(VSortExecExprs& vsort_exec_exprs, RuntimeState* state, int64_t limit,
                       int64_t offset, ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                       bool have_runtime_predicate)
        : Sorter(vsort_exec_exprs, state, limit, offset, pool, is_asc_order, nulls_first),
          _heap_size(limit + offset),
          _state(MergeSorterState::create_unique(row_desc, offset)),
          _have_runtime_predicate(have_runtime_predicate) {}

Status HeapSorter::append_block(Block* block) {
    auto tmp_block = std::make_shared<Block>(block->clone_empty());
    if (!_have_runtime_predicate && _queue.is_valid() && _queue_row_num >= _heap_size) {
        RETURN_IF_ERROR(_prepare_sort_columns(*block, *tmp_block, false));
        if (_materialize_sort_exprs) {
            block->clear_column_data();
        } else {
            tmp_block->swap(*block);
        }
        auto tmp_cursor_impl = MergeSortCursorImpl::create_shared(tmp_block, _sort_description);
        size_t num_rows = tmp_block->rows();
        _do_filter(*tmp_cursor_impl, num_rows);
        size_t remain_rows = tmp_block->rows();
        COUNTER_UPDATE(_topn_filter_rows_counter, (num_rows - remain_rows));
        if (remain_rows == 0) {
            return Status::OK();
        }
        // After filtering, sort the remaining rows with reversed description and push that block.
        auto sorted_block = std::make_shared<Block>(tmp_block->clone_empty());
        SortDescription rev_desc = _sort_description;
        for (auto& d : rev_desc) {
            d.direction *= -1;
        }
        sort_block(*tmp_block, *sorted_block, rev_desc, _hybrid_sorter, 0 /*limit*/);
        _queue_row_num += sorted_block->rows();
        _data_size += sorted_block->allocated_bytes();
        _queue.push(MergeSortCursor(MergeSortCursorImpl::create_shared(sorted_block, rev_desc)));
    } else {
        RETURN_IF_ERROR(partial_sort(*block, *tmp_block, true));
        _queue_row_num += tmp_block->rows();
        _data_size += tmp_block->allocated_bytes();
        _queue.push(
                MergeSortCursor(MergeSortCursorImpl::create_shared(tmp_block, _sort_description)));
    }

    while (_queue.is_valid() && _queue_row_num > _heap_size) {
        auto [current, current_rows] = _queue.current();
        current_rows = std::min(current_rows, _queue_row_num - _heap_size);

        if (!current->impl->is_last(current_rows)) {
            _queue.next(current_rows);
        } else {
            _data_size -= current->impl->block->allocated_bytes();
            _queue.remove_top();
        }
        _queue_row_num -= current_rows;
    }

    return Status::OK();
}

Status HeapSorter::prepare_for_read(bool is_spill) {
    if (is_spill) {
        return Status::InternalError("HeapSorter does not support spill");
    }
    while (_queue.is_valid()) {
        auto [current, current_rows] = _queue.current();
        if (current_rows) {
            current->impl->reverse(_reverse_buffer);
            _state->get_queue().push(MergeSortCursor(current->impl));
        }
        _queue.remove_top();
    }
    return Status::OK();
}

Status HeapSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    return _state->merge_sort_read(block, state->batch_size(), eos);
}

Field HeapSorter::get_top_value() {
    Field field {PrimitiveType::TYPE_NULL};
    // get field from first sort column of top row
    if (_queue_row_num >= _heap_size) {
        auto [current, current_rows] = _queue.current();
        field = current->get_top_value();
    }

    return field;
}

size_t HeapSorter::data_size() const {
    return _data_size;
}

void HeapSorter::_do_filter(MergeSortCursorImpl& block_cursor, size_t num_rows) {
    SCOPED_TIMER(_topn_filter_timer);
    auto [top_cursor, current_rows] = _queue.current();
    const auto cursor_rid = top_cursor->impl->pos;
    IColumn::Filter filter(num_rows, 0);
    std::vector<uint8_t> cmp_res(num_rows, 0);

    for (size_t col_id = 0; col_id < _sort_description.size(); ++col_id) {
        block_cursor.sort_columns[col_id]->compare_internal(
                cursor_rid, *top_cursor->impl->sort_columns[col_id],
                _sort_description[col_id].nulls_direction, _sort_description[col_id].direction,
                cmp_res, filter.data());
    }
    block_cursor.filter_block(filter);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
