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

#include "vec/common/sort/sorter.h"

#include "runtime/thread_context.h"

namespace doris::vectorized {

void MergeSorterState::build_merge_tree(SortDescription& sort_description) {
    for (const auto& block : sorted_blocks) {
        cursors.emplace_back(block, sort_description);
    }

    if (sorted_blocks.size() > 1) {
        for (auto& cursor : cursors) priority_queue.push(MergeSortCursor(&cursor));
    }
}

Status MergeSorterState::merge_sort_read(doris::RuntimeState* state,
                                         doris::vectorized::Block* block, bool* eos) {
    size_t num_columns = sorted_blocks[0].columns();

    bool mem_reuse = block->mem_reuse();
    MutableColumns merged_columns =
            mem_reuse ? block->mutate_columns() : sorted_blocks[0].clone_empty_columns();

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (!priority_queue.empty()) {
        auto current = priority_queue.top();
        priority_queue.pop();

        if (_offset == 0) {
            for (size_t i = 0; i < num_columns; ++i)
                merged_columns[i]->insert_from(*current->all_columns[i], current->pos);
            ++merged_rows;
        } else {
            _offset--;
        }

        if (!current->isLast()) {
            current->next();
            priority_queue.push(current);
        }

        if (merged_rows == state->batch_size()) break;
    }

    if (merged_rows == 0) {
        *eos = true;
        return Status::OK();
    }

    if (!mem_reuse) {
        Block merge_block = sorted_blocks[0].clone_with_columns(std::move(merged_columns));
        merge_block.swap(*block);
    }

    return Status::OK();
}

Status Sorter::partial_sort(Block& src_block, Block& dest_block) {
    size_t num_cols = src_block.columns();
    if (_materialize_sort_exprs) {
        auto output_tuple_expr_ctxs = _vsort_exec_exprs.sort_tuple_slot_expr_ctxs();
        std::vector<int> valid_column_ids(output_tuple_expr_ctxs.size());
        for (int i = 0; i < output_tuple_expr_ctxs.size(); ++i) {
            RETURN_IF_ERROR(output_tuple_expr_ctxs[i]->execute(&src_block, &valid_column_ids[i]));
        }

        Block new_block;
        int i = 0;
        const auto& convert_nullable_flags = _vsort_exec_exprs.get_convert_nullable_flags();
        for (auto column_id : valid_column_ids) {
            if (convert_nullable_flags[i]) {
                auto column_ptr = make_nullable(src_block.get_by_position(column_id).column);
                new_block.insert(
                        {column_ptr, make_nullable(src_block.get_by_position(column_id).type), ""});
            } else {
                new_block.insert(src_block.get_by_position(column_id));
            }
            i++;
        }
        dest_block.swap(new_block);
    }

    _sort_description.resize(_vsort_exec_exprs.lhs_ordering_expr_ctxs().size());
    Block* result_block = _materialize_sort_exprs ? &dest_block : &src_block;
    for (int i = 0; i < _sort_description.size(); i++) {
        const auto& ordering_expr = _vsort_exec_exprs.lhs_ordering_expr_ctxs()[i];
        RETURN_IF_ERROR(ordering_expr->execute(result_block, &_sort_description[i].column_number));

        _sort_description[i].direction = _is_asc_order[i] ? 1 : -1;
        _sort_description[i].nulls_direction =
                _nulls_first[i] ? -_sort_description[i].direction : _sort_description[i].direction;
    }

    {
        SCOPED_TIMER(_partial_sort_timer);
        if (_materialize_sort_exprs) {
            sort_block(dest_block, dest_block, _sort_description, _offset + _limit);
        } else {
            sort_block(src_block, dest_block, _sort_description, _offset + _limit);
        }
        src_block.clear_column_data(num_cols);
    }

    return Status::OK();
}

FullSorter::FullSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _state(std::unique_ptr<MergeSorterState>(new MergeSorterState(row_desc, offset))) {}

Status FullSorter::append_block(Block* block) {
    DCHECK(block->rows() > 0);
    {
        SCOPED_TIMER(_merge_block_timer);
        auto& data = _state->unsorted_block->get_columns_with_type_and_name();
        const auto& arrival_data = block->get_columns_with_type_and_name();
        auto sz = block->rows();
        for (int i = 0; i < data.size(); ++i) {
            DCHECK(data[i].type->equals(*(arrival_data[i].type)));
            RETURN_IF_CATCH_BAD_ALLOC(data[i].column->assume_mutable()->insert_range_from(
                    *arrival_data[i].column->convert_to_full_column_if_const().get(), 0, sz));
        }
        block->clear_column_data();
    }
    if (_reach_limit()) {
        RETURN_IF_ERROR(_do_sort());
    }
    return Status::OK();
}

Status FullSorter::prepare_for_read() {
    if (_state->unsorted_block->rows() > 0) {
        RETURN_IF_ERROR(_do_sort());
    }
    _state->build_merge_tree(_sort_description);
    return Status::OK();
}

Status FullSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    if (_state->sorted_blocks.empty()) {
        *eos = true;
    } else if (_state->sorted_blocks.size() == 1) {
        if (_offset != 0) {
            _state->sorted_blocks[0].skip_num_rows(_offset);
        }
        block->swap(_state->sorted_blocks[0]);
        *eos = true;
    } else {
        RETURN_IF_ERROR(_state->merge_sort_read(state, block, eos));
    }
    return Status::OK();
}

Status FullSorter::_do_sort() {
    Block* src_block = _state->unsorted_block.get();
    Block desc_block = src_block->clone_without_columns();
    RETURN_IF_ERROR(partial_sort(*src_block, desc_block));

    // dispose TOP-N logic
    if (_limit != -1) {
        // Here is a little opt to reduce the mem usage, we build a max heap
        // to order the block in _block_priority_queue.
        // if one block totally greater the heap top of _block_priority_queue
        // we can throw the block data directly.
        if (_state->num_rows < _limit) {
            _state->num_rows += desc_block.rows();
            _state->sorted_blocks.emplace_back(std::move(desc_block));
            _block_priority_queue.emplace(_pool->add(
                    new MergeSortCursorImpl(_state->sorted_blocks.back(), _sort_description)));
        } else {
            MergeSortBlockCursor block_cursor(
                    _pool->add(new MergeSortCursorImpl(desc_block, _sort_description)));
            if (!block_cursor.totally_greater(_block_priority_queue.top())) {
                _state->sorted_blocks.emplace_back(std::move(desc_block));
                _block_priority_queue.push(block_cursor);
            }
        }
    } else {
        // dispose normal sort logic
        _state->sorted_blocks.emplace_back(std::move(desc_block));
    }
    return Status::OK();
}

} // namespace doris::vectorized
