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

#include "vec/common/sort/topn_sorter.h"

namespace doris::vectorized {

TopNSorter::TopNSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _heap_size(limit + offset),
          _heap(std::make_unique<SortingHeap>()),
          _topn_filter_rows(0) {}

Status TopNSorter::append_block(Block* block, bool* mem_reuse) {
    DCHECK(block->rows() > 0);
    {
        if (_vsort_exec_exprs.need_materialize_tuple()) {
            auto output_tuple_expr_ctxs = _vsort_exec_exprs.sort_tuple_slot_expr_ctxs();
            std::vector<int> valid_column_ids(output_tuple_expr_ctxs.size());
            for (int i = 0; i < output_tuple_expr_ctxs.size(); ++i) {
                RETURN_IF_ERROR(output_tuple_expr_ctxs[i]->execute(block, &valid_column_ids[i]));
            }

            Block new_block;
            for (auto column_id : valid_column_ids) {
                new_block.insert(block->get_by_position(column_id));
            }
            block->swap(new_block);
        }

        // TODO: could we init `_sort_description` only once?
        _sort_description.resize(_vsort_exec_exprs.lhs_ordering_expr_ctxs().size());
        for (int i = 0; i < _sort_description.size(); i++) {
            const auto& ordering_expr = _vsort_exec_exprs.lhs_ordering_expr_ctxs()[i];
            RETURN_IF_ERROR(ordering_expr->execute(block, &_sort_description[i].column_number));

            _sort_description[i].direction = _is_asc_order[i] ? 1 : -1;
            _sort_description[i].nulls_direction = _nulls_first[i] ? -_sort_description[i].direction
                                                                   : _sort_description[i].direction;
        }
    }
    Block tmp_block = block->clone_empty();
    tmp_block.swap(*block);
    std::shared_ptr<HeapSortCursorBlockView> block_view(
            new HeapSortCursorBlockView(std::move(tmp_block), _sort_description));
    size_t num_rows = tmp_block.rows();
    if (_heap_size == _heap->size()) {
        {
            SCOPED_TIMER(_topn_filter_timer);
            _do_filter(block_view.get(), num_rows);
        }
        size_t remain_rows = block_view->block.rows();
        _topn_filter_rows += (num_rows - remain_rows);
        COUNTER_SET(_topn_filter_rows_counter, _topn_filter_rows);
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
    return Status::OK();
}

Status TopNSorter::prepare_for_read() {
    if (!_heap->empty() && _heap->size() > _offset) {
        auto top = _heap->top();
        size_t num_columns = top.block()->columns();
        MutableColumns result_columns = top.block()->clone_empty_columns();

        size_t init_size = std::min((size_t)_limit, _heap->size());
        result_columns.reserve(init_size);

        DCHECK(_heap->size() <= _heap_size);
        // Use a vector to reverse elements in heap
        std::vector<HeapSortCursorImpl> vector_to_reverse(init_size);
        size_t capacity = 0;
        while (!_heap->empty()) {
            auto current = _heap->top();
            _heap->pop();
            vector_to_reverse[capacity++] = std::move(current);
            if (_offset != 0 && _heap->size() == _offset) {
                break;
            }
        }
        for (size_t i = capacity - 1; i > 0; i--) {
            auto rid = vector_to_reverse[i].row_id();
            const auto cur_block = vector_to_reverse[i].block();
            for (size_t j = 0; j < num_columns; ++j) {
                result_columns[j]->insert_from(*(cur_block->get_columns()[j]), rid);
            }
        }
        auto rid = vector_to_reverse[0].row_id();
        const auto cur_block = vector_to_reverse[0].block();
        for (size_t j = 0; j < num_columns; ++j) {
            result_columns[j]->insert_from(*(cur_block->get_columns()[j]), rid);
        }
        _return_block = top.block()->clone_with_columns(std::move(result_columns));
    }
    return Status::OK();
}

Status TopNSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    _return_block.swap(*block);
    *eos = true;
    return Status::OK();
}

void TopNSorter::_do_filter(HeapSortCursorBlockView* block_view, size_t num_rows) {
    const auto& top_cursor = _heap->top();
    const int cursor_rid = top_cursor.row_id();

    IColumn::Filter filter(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        filter[i] = 0;
    }

    std::vector<uint8_t> cmp_res(num_rows, 0);

    for (size_t col_id = 0; col_id < _sort_description.size(); ++col_id) {
        block_view->sort_columns[col_id]->compare_internal(
                cursor_rid, *top_cursor.sort_columns()[col_id],
                _sort_description[col_id].nulls_direction, _sort_description[col_id].direction,
                cmp_res, filter.data());
    }
    block_view->filter_block(filter);
}

} // namespace doris::vectorized
