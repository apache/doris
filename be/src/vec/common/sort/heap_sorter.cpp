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

namespace doris::vectorized {
#include "common/compile_check_begin.h"

HeapSorter::HeapSorter(VSortExecExprs& vsort_exec_exprs, int64_t limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _heap_size(limit + offset),
          _state(MergeSorterState::create_unique(row_desc, offset)) {}

Status HeapSorter::append_block(Block* block) {
    //_do_filter(block);
    if (block->empty()) {
        return Status::OK();
    }
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

void HeapSorter::_do_filter(Block* block) {
    if (_queue_row_num < _heap_size) {
        return;
    }

    IColumn::Filter filter(block->rows());
    for (size_t i = 0; i < block->rows(); ++i) {
        filter[i] = 0;
    }

    auto [current, current_rows] = _queue.current();

    std::vector<uint8_t> cmp_res(block->rows(), 0);

    for (size_t col_id = 0; col_id < _sort_description.size(); ++col_id) {
        block->get_by_position(_sort_description[col_id].column_number)
                .column->compare_internal(current->impl->pos, *current->impl->sort_columns[col_id],
                                          _sort_description[col_id].nulls_direction,
                                          _sort_description[col_id].direction, cmp_res,
                                          filter.data());
    }
    Block::filter_block_internal(block, filter);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
