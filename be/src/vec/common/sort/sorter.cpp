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

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <ostream>
#include <string>
#include <utility>

#include "common/object_pool.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
} // namespace doris

namespace doris::vectorized {

// When doing spillable sorting, each sorted block is spilled into a single file.
//
// In order to decrease memory pressure when merging
// multiple spilled blocks into one bigger sorted block, only part
// of each spilled blocks are read back into memory at a time.
//
// Currently the spilled blocks are splitted into small sub blocks,
// each sub block is serialized in PBlock format and appended
// to the spill file.
//

void MergeSorterState::reset() {
    std::vector<std::shared_ptr<MergeSortCursorImpl>> empty_cursors(0);
    std::vector<std::shared_ptr<Block>> empty_blocks(0);
    _sorted_blocks.swap(empty_blocks);
    unsorted_block() = Block::create_unique(unsorted_block()->clone_empty());
    _in_mem_sorted_bocks_size = 0;
}

void MergeSorterState::add_sorted_block(std::shared_ptr<Block> block) {
    auto rows = block->rows();
    if (0 == rows) {
        return;
    }
    _in_mem_sorted_bocks_size += block->bytes();
    _sorted_blocks.emplace_back(block);
    _num_rows += rows;
}

Status MergeSorterState::build_merge_tree(const SortDescription& sort_description) {
    std::vector<MergeSortCursor> cursors;
    for (auto& block : _sorted_blocks) {
        cursors.emplace_back(
                MergeSortCursorImpl::create_shared(std::move(block), sort_description));
    }
    _queue = MergeSorterQueue(cursors);

    _sorted_blocks.clear();
    return Status::OK();
}

Status MergeSorterState::merge_sort_read(doris::vectorized::Block* block, int batch_size,
                                         bool* eos) {
    DCHECK(_sorted_blocks.empty());
    DCHECK(unsorted_block()->empty());
    _merge_sort_read_impl(batch_size, block, eos);
    return Status::OK();
}

void MergeSorterState::_merge_sort_read_impl(int batch_size, doris::vectorized::Block* block,
                                             bool* eos) {
    size_t num_columns = unsorted_block()->columns();

    MutableBlock m_block = VectorizedUtils::build_mutable_mem_reuse_block(block, *unsorted_block());
    MutableColumns& merged_columns = m_block.mutable_columns();

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    // process single element queue on merge_sort_read()
    while (_queue.is_valid() && merged_rows < batch_size) {
        auto [current, current_rows] = _queue.current();
        current_rows = std::min(current_rows, batch_size - merged_rows);

        size_t step = std::min(_offset, current_rows);
        _offset -= step;
        current_rows -= step;

        if (current_rows) {
            for (size_t i = 0; i < num_columns; ++i) {
                merged_columns[i]->insert_range_from(*current->impl->columns[i],
                                                     current->impl->pos + step, current_rows);
            }
            merged_rows += current_rows;
        }

        if (!current->impl->is_last(current_rows + step)) {
            _queue.next(current_rows + step);
        } else {
            _queue.remove_top();
        }
    }

    block->set_columns(std::move(merged_columns));
    *eos = merged_rows == 0;
}

Status Sorter::merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                         int batch_size, bool* eos) {
    return get_next(state, block, eos);
}

Status Sorter::partial_sort(Block& src_block, Block& dest_block, bool reversed) {
    size_t num_cols = src_block.columns();
    if (_materialize_sort_exprs) {
        auto output_tuple_expr_ctxs = _vsort_exec_exprs.sort_tuple_slot_expr_ctxs();
        std::vector<int> valid_column_ids(output_tuple_expr_ctxs.size());
        for (int i = 0; i < output_tuple_expr_ctxs.size(); ++i) {
            RETURN_IF_ERROR(output_tuple_expr_ctxs[i]->execute(&src_block, &valid_column_ids[i]));
        }

        Block new_block;
        for (auto column_id : valid_column_ids) {
            if (column_id < 0) {
                continue;
            }
            new_block.insert(src_block.get_by_position(column_id));
        }
        dest_block.swap(new_block);
    }

    _sort_description.resize(_vsort_exec_exprs.ordering_expr_ctxs().size());
    Block* result_block = _materialize_sort_exprs ? &dest_block : &src_block;
    for (int i = 0; i < _sort_description.size(); i++) {
        const auto& ordering_expr = _vsort_exec_exprs.ordering_expr_ctxs()[i];
        RETURN_IF_ERROR(ordering_expr->execute(result_block, &_sort_description[i].column_number));

        _sort_description[i].direction = _is_asc_order[i] ? 1 : -1;
        _sort_description[i].nulls_direction =
                _nulls_first[i] ? -_sort_description[i].direction : _sort_description[i].direction;
        if (reversed) {
            _sort_description[i].direction *= -1;
        }
    }

    {
        SCOPED_TIMER(_partial_sort_timer);
        uint64_t limit = reversed ? 0 : (_offset + _limit);
        sort_block(*result_block, dest_block, _sort_description, limit);
    }

    src_block.clear_column_data(num_cols);
    return Status::OK();
}

FullSorter::FullSorter(VSortExecExprs& vsort_exec_exprs, int64_t limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                       RuntimeState* state, RuntimeProfile* profile)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _state(MergeSorterState::create_unique(row_desc, offset)) {}

// check whether the unsorted block can hold more data from input block and no need to alloc new memory
bool FullSorter::has_enough_capacity(Block* input_block, Block* unsorted_block) const {
    DCHECK_EQ(input_block->columns(), unsorted_block->columns());
    for (auto i = 0; i < input_block->columns(); ++i) {
        if (!unsorted_block->get_by_position(i).column->has_enough_capacity(
                    *input_block->get_by_position(i).column)) {
            return false;
        }
    }
    return true;
}

size_t FullSorter::get_reserve_mem_size(RuntimeState* state, bool eos) const {
    size_t size_to_reserve = 0;
    const auto rows = _state->unsorted_block()->rows();
    if (rows != 0) {
        const auto bytes = _state->unsorted_block()->bytes();
        const auto allocated_bytes = _state->unsorted_block()->allocated_bytes();
        const auto bytes_per_row = bytes / rows;
        const auto estimated_size_of_next_block = bytes_per_row * state->batch_size();
        auto new_block_bytes = estimated_size_of_next_block + bytes;
        auto new_rows = rows + state->batch_size();
        // If the new size is greater than 85% of allocalted bytes, it maybe need to realloc.
        if ((new_block_bytes * 100 / allocated_bytes) >= 85) {
            size_to_reserve += (size_t)(allocated_bytes * 1.15);
        }
        auto sort = new_rows > _buffered_block_size || new_block_bytes > _buffered_block_bytes;
        if (sort) {
            // new column is created when doing sort, reserve average size of one column
            // for estimation
            size_to_reserve += new_block_bytes / _state->unsorted_block()->columns();

            // helping data structures used during sorting
            size_to_reserve += new_rows * sizeof(IColumn::Permutation::value_type);

            auto sort_columns_count = _vsort_exec_exprs.ordering_expr_ctxs().size();
            if (1 != sort_columns_count) {
                size_to_reserve += new_rows * sizeof(EqualRangeIterator);
            }
        }
    }
    return size_to_reserve;
}

Status FullSorter::append_block(Block* block) {
    DCHECK(block->rows() > 0);

    // iff have reach limit and the unsorted block capacity can't hold the block data size
    if (_reach_limit() && !has_enough_capacity(block, _state->unsorted_block().get())) {
        RETURN_IF_ERROR(_do_sort());
    }

    {
        SCOPED_TIMER(_merge_block_timer);
        const auto& data = _state->unsorted_block()->get_columns_with_type_and_name();
        const auto& arrival_data = block->get_columns_with_type_and_name();
        auto sz = block->rows();
        for (int i = 0; i < data.size(); ++i) {
            DCHECK(data[i].type->equals(*(arrival_data[i].type)))
                    << " type1: " << data[i].type->get_name()
                    << " type2: " << arrival_data[i].type->get_name() << " i: " << i;
            if (is_column_const(*arrival_data[i].column)) {
                data[i].column->assume_mutable()->insert_many_from(
                        assert_cast<const ColumnConst*>(arrival_data[i].column.get())
                                ->get_data_column(),
                        0, sz);
            } else {
                data[i].column->assume_mutable()->insert_range_from(*arrival_data[i].column, 0, sz);
            }
        }
        block->clear_column_data();
    }
    return Status::OK();
}

Status FullSorter::prepare_for_read(bool is_spill) {
    if (is_spill) {
        _limit += _offset;
        _offset = 0;
        _state->ignore_offset();
    }
    if (_state->unsorted_block()->rows() > 0) {
        RETURN_IF_ERROR(_do_sort());
    }
    return _state->build_merge_tree(_sort_description);
}

Status FullSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    return _state->merge_sort_read(block, state->batch_size(), eos);
}

Status FullSorter::merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                             int batch_size, bool* eos) {
    return _state->merge_sort_read(block, batch_size, eos);
}

Status FullSorter::_do_sort() {
    Block* src_block = _state->unsorted_block().get();
    Block desc_block = src_block->clone_without_columns();
    COUNTER_UPDATE(_partial_sort_counter, 1);
    RETURN_IF_ERROR(partial_sort(*src_block, desc_block));

    // dispose TOP-N logic
    if (_limit != -1 && !_enable_spill) {
        // Here is a little opt to reduce the mem usage, we build a max heap
        // to order the block in _block_priority_queue.
        // if one block totally greater the heap top of _block_priority_queue
        // we can throw the block data directly.
        if (_state->num_rows() < _offset + _limit) {
            _state->add_sorted_block(Block::create_shared(std::move(desc_block)));
            _block_priority_queue.emplace(MergeSortCursorImpl::create_shared(
                    _state->last_sorted_block(), _sort_description));
        } else {
            auto tmp_cursor_impl = MergeSortCursorImpl::create_shared(
                    Block::create_shared(std::move(desc_block)), _sort_description);
            MergeSortBlockCursor block_cursor(tmp_cursor_impl);
            if (!block_cursor.totally_greater(_block_priority_queue.top())) {
                _state->add_sorted_block(tmp_cursor_impl->block);
                _block_priority_queue.emplace(MergeSortCursorImpl::create_shared(
                        _state->last_sorted_block(), _sort_description));
            }
        }
    } else {
        // dispose normal sort logic
        _state->add_sorted_block(Block::create_shared(std::move(desc_block)));
    }
    return Status::OK();
}

size_t FullSorter::data_size() const {
    return _state->data_size();
}

void FullSorter::reset() {
    _state->reset();
}

} // namespace doris::vectorized
