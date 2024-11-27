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

#include <algorithm>

#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/sort_description.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {
HeapSorter::HeapSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _data_size(0),
          _heap_size(limit + offset),
          _heap(SortingHeap::create_unique()),
          _topn_filter_rows(0),
          _init_sort_descs(false) {}

Status HeapSorter::append_block(Block* block) {
    DCHECK(block->rows() > 0);
    {
        SCOPED_TIMER(_materialize_timer);
        if (_vsort_exec_exprs.need_materialize_tuple()) {
            auto output_tuple_expr_ctxs = _vsort_exec_exprs.sort_tuple_slot_expr_ctxs();
            std::vector<int> valid_column_ids(output_tuple_expr_ctxs.size());
            for (int i = 0; i < output_tuple_expr_ctxs.size(); ++i) {
                RETURN_IF_ERROR(output_tuple_expr_ctxs[i]->execute(block, &valid_column_ids[i]));
            }

            Block new_block;
            int i = 0;
            const auto& convert_nullable_flags = _vsort_exec_exprs.get_convert_nullable_flags();
            for (auto column_id : valid_column_ids) {
                if (column_id < 0) {
                    continue;
                }
                if (i < convert_nullable_flags.size() && convert_nullable_flags[i]) {
                    auto column_ptr = make_nullable(block->get_by_position(column_id).column);
                    new_block.insert({column_ptr,
                                      make_nullable(block->get_by_position(column_id).type), ""});
                } else {
                    new_block.insert(block->get_by_position(column_id));
                }
                i++;
            }
            block->swap(new_block);
        }
    }
    if (!_init_sort_descs) {
        RETURN_IF_ERROR(_prepare_sort_descs(block));
    }
    Block tmp_block = block->clone_empty();
    tmp_block.swap(*block);
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

Status HeapSorter::prepare_for_read() {
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
        for (int i = capacity - 1; i >= 0; i--) {
            auto rid = vector_to_reverse[i].row_id();
            const auto cur_block = vector_to_reverse[i].block();
            Columns columns = cur_block->get_columns();
            for (size_t j = 0; j < num_columns; ++j) {
                result_columns[j]->insert_from(*(columns[j]), rid);
            }
        }
        _return_block = vector_to_reverse[0].block()->clone_with_columns(std::move(result_columns));
    }
    return Status::OK();
}

Status HeapSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    _return_block.swap(*block);
    *eos = true;
    return Status::OK();
}

Field HeapSorter::get_top_value() {
    Field field {Field::Types::Null};
    // get field from first sort column of top row
    if (_heap->size() >= _heap_size) {
        auto& top = _heap->top();
        top.sort_columns()[0]->get(top.row_id(), field);
    }

    return field;
}

// need exception safety
void HeapSorter::_do_filter(HeapSortCursorBlockView& block_view, size_t num_rows) {
    const auto& top_cursor = _heap->top();
    const int cursor_rid = top_cursor.row_id();

    IColumn::Filter filter(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        filter[i] = 0;
    }

    std::vector<uint8_t> cmp_res(num_rows, 0);

    for (size_t col_id = 0; col_id < _sort_description.size(); ++col_id) {
        block_view.sort_columns[col_id]->compare_internal(
                cursor_rid, *top_cursor.sort_columns()[col_id],
                _sort_description[col_id].nulls_direction, _sort_description[col_id].direction,
                cmp_res, filter.data());
    }
    block_view.filter_block(filter);
}

Status HeapSorter::_prepare_sort_descs(Block* block) {
    _sort_description.resize(_vsort_exec_exprs.lhs_ordering_expr_ctxs().size());
    for (int i = 0; i < _sort_description.size(); i++) {
        const auto& ordering_expr = _vsort_exec_exprs.lhs_ordering_expr_ctxs()[i];
        RETURN_IF_ERROR(ordering_expr->execute(block, &_sort_description[i].column_number));

        _sort_description[i].direction = _is_asc_order[i] ? 1 : -1;
        _sort_description[i].nulls_direction =
                _nulls_first[i] ? -_sort_description[i].direction : _sort_description[i].direction;
    }
    _init_sort_descs = true;
    return Status::OK();
}

size_t HeapSorter::data_size() const {
    return _data_size;
}

} // namespace doris::vectorized
