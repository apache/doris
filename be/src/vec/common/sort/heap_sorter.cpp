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
    if (!_init_sort_descs) {
        RETURN_IF_ERROR(_prepare_sort_descs(block));
    }
    auto tmp_block = std::make_shared<Block>(block->clone_empty());
    RETURN_IF_ERROR(partial_sort(*block, *tmp_block));
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
// [1, 3) :  2, [1, 0]
// [0, 2) : [0, 1], 2
// pos,rows -> 0,rows-pos-1

Status HeapSorter::prepare_for_read() {
    while (_queue.is_valid()) {
        auto [current, current_rows] = _queue.current();
        if (current_rows) {
            current->impl->reverse();
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

Status HeapSorter::_prepare_sort_descs(Block* block) {
    _sort_description.resize(_vsort_exec_exprs.ordering_expr_ctxs().size());
    for (int i = 0; i < _sort_description.size(); i++) {
        const auto& ordering_expr = _vsort_exec_exprs.ordering_expr_ctxs()[i];
        RETURN_IF_ERROR(ordering_expr->execute(block, &_sort_description[i].column_number));

        _sort_description[i].direction = _is_asc_order[i] ? -1 : 1; // reversed
        _sort_description[i].nulls_direction =
                _nulls_first[i] ? -_sort_description[i].direction : _sort_description[i].direction;
    }

    _init_sort_descs = true;
    return Status::OK();
}

size_t HeapSorter::data_size() const {
    return _data_size;
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
