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

#include "common/status.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

Status HeapSorterWithRuntimePredicate::append_block(Block* block) {
    auto tmp_block = std::make_shared<Block>(block->clone_empty());
    RETURN_IF_ERROR(partial_sort(*block, *tmp_block, true));
    _queue.push(
            MergeSortCursor(std::make_shared<MergeSortCursorImpl>(tmp_block, _sort_description)));
    _queue_row_num += tmp_block->rows();
    _data_size += tmp_block->allocated_bytes();

    while (_queue.is_valid() && _queue_row_num > _heap_size) {
        auto [current, current_rows] = _queue.current();
        current_rows = std::min(current_rows, _queue_row_num - _heap_size);

        if (!current->impl->is_last(current_rows)) {
            _queue.next(current_rows);
        } else {
            _queue.remove_top();
            _data_size -= current->impl->block->allocated_bytes();
        }
        _queue_row_num -= current_rows;
    }
    return Status::OK();
}

Status HeapSorterWithRuntimePredicate::prepare_for_read(bool is_spill) {
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

Status HeapSorterWithRuntimePredicate::get_next(RuntimeState* state, Block* block, bool* eos) {
    return _state->merge_sort_read(block, state->batch_size(), eos);
}

size_t HeapSorterWithRuntimePredicate::data_size() const {
    return _data_size;
}

Field HeapSorterWithRuntimePredicate::get_top_value() {
    Field field {PrimitiveType::TYPE_NULL};
    // get field from first sort column of top row

    if (_queue_row_num >= _heap_size) {
        auto [current, current_rows] = _queue.current();
        field = current->get_top_value();
    }

    return field;
}

Status HeapSorterWithoutRuntimePredicate::append_block(Block* block) {
    DCHECK(block->rows() > 0);
    auto tmp_block = block->clone_empty();
    RETURN_IF_ERROR(_prepare_sort_columns(*block, tmp_block, false));
    if (_materialize_sort_exprs) {
        block->clear_column_data();
    } else {
        tmp_block.swap(*block);
    }
    size_t num_rows = tmp_block.rows();
    auto block_view =
            std::make_shared<HeapSortCursorBlockView>(std::move(tmp_block), _sort_description);
    bool filtered = false;
    if (_heap_size == _heap->size()) {
        {
            SCOPED_TIMER(_topn_filter_timer);
            _do_filter(*block_view, num_rows);
        }
        size_t remain_rows = block_view->block.rows();
        _topn_filter_rows += (num_rows - remain_rows);
        COUNTER_SET(_topn_filter_rows_counter, _topn_filter_rows);
        filtered = remain_rows == 0;
        for (size_t i = 0; i < remain_rows; ++i) {
            HeapSortCursorImpl cursor(i, block_view);
            _heap->replace_top_if_less(std::move(cursor));
        }
    } else {
        size_t free_slots = std::min<size_t>(_heap_size - _heap->size(), num_rows);
        size_t i = 0;
        for (; i < free_slots; ++i) {
            HeapSortCursorImpl cursor(i, block_view);
            _heap->push(std::move(cursor));
        }

        for (; i < num_rows; ++i) {
            HeapSortCursorImpl cursor(i, block_view);
            _heap->replace_top_if_less(std::move(cursor));
        }
    }
    if (!filtered) {
        _data_size += block_view->block.allocated_bytes();
    }
    return Status::OK();
}

Status HeapSorterWithoutRuntimePredicate::prepare_for_read(bool is_spill) {
    if (is_spill) {
        return Status::InternalError("HeapSorter does not support spill");
    }
    if (!_heap->empty() && _heap->size() > _offset) {
        const auto& top = _heap->top();
        size_t num_columns = top.block()->columns();
        MutableColumns result_columns = top.block()->clone_empty_columns();

        size_t init_size = std::min((size_t)_limit, _heap->size());
        result_columns.reserve(init_size);

        DCHECK(_heap->size() <= _heap_size);
        // Use a vector to reverse elements in heap
        std::vector<HeapSortCursorImpl> vector_to_reverse;
        vector_to_reverse.reserve(init_size);
        size_t capacity = 0;
        while (!_heap->empty()) {
            auto current = _heap->top();
            _heap->pop();
            vector_to_reverse.emplace_back(std::move(current));
            capacity++;
            if (_offset != 0 && _heap->size() == _offset) {
                break;
            }
        }
        for (int64_t i = capacity - 1; i >= 0; i--) {
            auto rid = vector_to_reverse[i].row_id();
            const auto* cur_block = vector_to_reverse[i].block();
            Columns columns = cur_block->get_columns();
            for (size_t j = 0; j < num_columns; ++j) {
                result_columns[j]->insert_from(*(columns[j]), rid);
            }
        }
        _return_block = vector_to_reverse[0].block()->clone_with_columns(std::move(result_columns));
    }
    return Status::OK();
}

Status HeapSorterWithoutRuntimePredicate::get_next(RuntimeState* state, Block* block, bool* eos) {
    _return_block.swap(*block);
    *eos = true;
    return Status::OK();
}

size_t HeapSorterWithoutRuntimePredicate::data_size() const {
    return _data_size;
}

Field HeapSorterWithoutRuntimePredicate::get_top_value() {
    Field field {PrimitiveType::TYPE_NULL};
    // get field from first sort column of top row
    if (_heap->size() >= _heap_size) {
        const auto& top = _heap->top();
        top.sort_columns()[0]->get(top.row_id(), field);
    }
    return field;
}

void HeapSorterWithoutRuntimePredicate::_do_filter(HeapSortCursorBlockView& block_view,
                                                   size_t num_rows) {
    const auto& top_cursor = _heap->top();
    const auto cursor_rid = top_cursor.row_id();
    IColumn::Filter filter(num_rows, 0);
    std::vector<uint8_t> cmp_res(num_rows, 0);

    for (size_t col_id = 0; col_id < _sort_description.size(); ++col_id) {
        block_view.sort_columns[col_id]->compare_internal(
                cursor_rid, *top_cursor.sort_columns()[col_id],
                _sort_description[col_id].nulls_direction, _sort_description[col_id].direction,
                cmp_res, filter.data());
    }
    block_view.filter_block(filter);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
