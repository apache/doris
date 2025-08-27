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

#include "vec/common/sort/partition_sorter.h"

#include <glog/logging.h>

#include <algorithm>
#include <queue>

#include "common/object_pool.h"
#include "vec/core/block.h"
#include "vec/core/sort_cursor.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class VSortExecExprs;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

PartitionSorter::PartitionSorter(VSortExecExprs& vsort_exec_exprs, int64_t limit, int64_t offset,
                                 ObjectPool* pool, std::vector<bool>& is_asc_order,
                                 std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                                 RuntimeState* state, RuntimeProfile* profile,
                                 bool has_global_limit, int64_t partition_inner_limit,
                                 TopNAlgorithm::type top_n_algorithm, SortCursorCmp* previous_row)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _state(MergeSorterState::create_unique(row_desc, offset)),
          _row_desc(row_desc),
          _partition_inner_limit(partition_inner_limit),
          _top_n_algorithm(
                  has_global_limit
                          ? TopNAlgorithm::ROW_NUMBER
                          : top_n_algorithm), // FE will make this modification, but still maintain this code for compatibility
          _previous_row(previous_row) {}

Status PartitionSorter::append_block(Block* input_block) {
    Block sorted_block = VectorizedUtils::create_empty_columnswithtypename(_row_desc);
    DCHECK(input_block->columns() == sorted_block.columns());
    RETURN_IF_ERROR(partial_sort(*input_block, sorted_block));
    _state->add_sorted_block(Block::create_shared(std::move(sorted_block)));
    return Status::OK();
}

Status PartitionSorter::prepare_for_read(bool is_spill) {
    if (is_spill) {
        return Status::InternalError("PartitionSorter does not support spill");
    }
    auto& blocks = _state->get_sorted_block();
    auto& queue = _state->get_queue();
    std::vector<MergeSortCursor> cursors;
    for (auto& block : blocks) {
        cursors.emplace_back(
                MergeSortCursorImpl::create_shared(std::move(block), _sort_description));
    }
    queue = MergeSorterQueue(cursors);
    blocks.clear();
    return Status::OK();
}

// have done sorter and get topn records, so could reset those state to init
void PartitionSorter::reset_sorter_state(RuntimeState* runtime_state) {
    std::priority_queue<MergeSortBlockCursor> empty_queue;
    std::swap(_block_priority_queue, empty_queue);
    _state = MergeSorterState::create_unique(_row_desc, _offset);
    // _previous_row->impl inited at partition_sort_read function,
    // but maybe call get_next after do_partition_topn_sort() function, and running into else if branch at line 92L
    // so _previous_row->impl == nullptr and no need reset.
    if (_previous_row->impl) {
        _previous_row->reset();
    }
    _output_total_rows = 0;
    _output_distinct_rows = 0;
    _prepared_finish = false;
}

Status PartitionSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    if (_top_n_algorithm == TopNAlgorithm::ROW_NUMBER) {
        return _read_row_num(block, eos, state->batch_size());
    } else {
        return _read_row_rank(block, eos, state->batch_size());
    }
}

Status PartitionSorter::_read_row_num(Block* output_block, bool* eos, int batch_size) {
    auto& queue = _state->get_queue();
    size_t num_columns = _state->unsorted_block()->columns();

    MutableBlock m_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, *_state->unsorted_block());
    MutableColumns& merged_columns = m_block.mutable_columns();
    size_t merged_rows = 0;

    Defer defer {[&]() {
        if (merged_rows == 0 || _get_enough_data()) {
            *eos = true;
        }
    }};

    while (queue.is_valid() && merged_rows < batch_size && !_get_enough_data()) {
        auto [current, current_rows] = queue.current();

        // row_number no need to check distinct, just output partition_inner_limit row
        size_t needed_rows = _partition_inner_limit - _output_total_rows;
        size_t step = std::min(needed_rows, std::min(current_rows, batch_size - merged_rows));

        if (current->impl->is_last(step) && current->impl->pos == 0) {
            if (merged_rows != 0) {
                // return directly for next time's read swap whole block
                return Status::OK();
            }
            // swap and return block directly when we should get all data from cursor
            output_block->swap(*current->impl->block);
            merged_rows += step;
            _output_total_rows += step;
            queue.remove_top();
            return Status::OK();
        }

        if (step) {
            merged_rows += step;
            _output_total_rows += step;
            for (size_t i = 0; i < num_columns; ++i) {
                merged_columns[i]->insert_range_from(*current->impl->columns[i], current->impl->pos,
                                                     step);
            }
        }

        if (!current->impl->is_last(step)) {
            queue.next(step);
        } else {
            queue.remove_top();
        }
    }

    return Status::OK();
}

Status PartitionSorter::_read_row_rank(Block* output_block, bool* eos, int batch_size) {
    auto& queue = _state->get_queue();
    size_t num_columns = _state->unsorted_block()->columns();

    MutableBlock m_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, *_state->unsorted_block());
    MutableColumns& merged_columns = m_block.mutable_columns();
    size_t merged_rows = 0;

    Defer defer {[&]() {
        if (merged_rows == 0 || _get_enough_data()) {
            *eos = true;
        }
    }};

    while (queue.is_valid() && merged_rows < batch_size) {
        auto [current, current_rows] = queue.current();

        for (size_t offset = 0; offset < current_rows && merged_rows < batch_size; offset++) {
            bool cmp_res = _previous_row->impl && _previous_row->compare_two_rows(current->impl);
            if (!cmp_res) {
                // 1. dense_rank(): 1,1,1,2,2,2,2,.......,2,3,3,3, if SQL: where rk < 3, need output all 1 and 2
                // dense_rank() maybe need distinct rows of partition_inner_limit
                // so check have output distinct rows, not _output_total_rows
                // 2. rank(): 1,1,1,4,5,6,6,6.....,6,100,101. if SQL where rk < 7, need output all 1,1,1,4,5,6,6,....6
                // rank() maybe need check when have get a distinct row
                // so when the cmp_res is get a distinct row, need check have output all rows num
                if (_get_enough_data()) {
                    return Status::OK();
                }
                *_previous_row = *current;
                _output_distinct_rows++;
            }
            for (size_t i = 0; i < num_columns; ++i) {
                merged_columns[i]->insert_from(*current->impl->block->get_columns()[i],
                                               current->impl->pos);
            }
            merged_rows++;
            _output_total_rows++;
            if (!current->impl->is_last(1)) {
                queue.next(1);
            } else {
                queue.remove_top();
            }
        }
    }

    return Status::OK();
}

} // namespace doris::vectorized
