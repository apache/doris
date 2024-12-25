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
#include "vec/functions/function_binary_arithmetic.h"
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

PartitionSorter::PartitionSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset,
                                 ObjectPool* pool, std::vector<bool>& is_asc_order,
                                 std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                                 RuntimeState* state, RuntimeProfile* profile,
                                 bool has_global_limit, int partition_inner_limit,
                                 TopNAlgorithm::type top_n_algorithm, SortCursorCmp* previous_row)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _state(MergeSorterState::create_unique(row_desc, offset, limit, state, profile)),
          _row_desc(row_desc),
          _has_global_limit(has_global_limit),
          _partition_inner_limit(partition_inner_limit),
          _top_n_algorithm(top_n_algorithm),
          _previous_row(previous_row) {
    if (_has_global_limit) {
        // only need row number if has global limit, so we change algorithm directly
        _top_n_algorithm = TopNAlgorithm::ROW_NUMBER;
    }
}

Status PartitionSorter::append_block(Block* input_block) {
    Block sorted_block = VectorizedUtils::create_empty_columnswithtypename(_row_desc);
    DCHECK(input_block->columns() == sorted_block.columns());
    RETURN_IF_ERROR(partial_sort(*input_block, sorted_block));
    _state->add_sorted_block(Block::create_shared(std::move(sorted_block)));
    return Status::OK();
}

Status PartitionSorter::prepare_for_read() {
    auto& blocks = _state->get_sorted_block();
    auto& priority_queue = _state->get_priority_queue();
    std::vector<MergeSortCursor> cursors;
    for (auto& block : blocks) {
        cursors.emplace_back(
                MergeSortCursorImpl::create_shared(std::move(block), _sort_description));
    }
    priority_queue = SortingQueueBatch<MergeSortCursor>(cursors);
    blocks.clear();
    return Status::OK();
}

// have done sorter and get topn records, so could reset those state to init
void PartitionSorter::reset_sorter_state(RuntimeState* runtime_state) {
    std::priority_queue<MergeSortBlockCursor> empty_queue;
    std::swap(_block_priority_queue, empty_queue);
    _state = MergeSorterState::create_unique(_row_desc, _offset, _limit, runtime_state, nullptr);
    // _previous_row->impl inited at partition_sort_read function,
    // but maybe call get_next after do_partition_topn_sort() function, and running into else if branch at line 92L
    // so _previous_row->impl == nullptr and no need reset.
    if (_previous_row->impl) {
        _previous_row->reset();
    }
    _output_total_rows = 0;
    _output_distinct_rows = 0;
}

Status PartitionSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    RETURN_IF_ERROR(partition_sort_read(block, eos, state->batch_size()));
    return Status::OK();
}

Status PartitionSorter::partition_sort_read(Block* output_block, bool* eos, int batch_size) {
    auto& priority_queue = _state->get_priority_queue();
    size_t num_columns = _state->unsorted_block_->columns();

    MutableBlock m_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, *_state->unsorted_block_);
    MutableColumns& merged_columns = m_block.mutable_columns();
    size_t current_output_rows = 0;

    bool get_enough_data = false;
    while (priority_queue.is_valid()) {
        auto [current, current_rows] = priority_queue.current();
        current_rows = std::min(current_rows, batch_size - current_output_rows);
        if (UNLIKELY(_previous_row->impl == nullptr)) {
            *_previous_row = *current;
        }

        if (_top_n_algorithm == TopNAlgorithm::ROW_NUMBER) {
            // row_number no need to check distinct, just output partition_inner_limit row
            size_t needed_rows = _partition_inner_limit - _output_total_rows - current_output_rows;
            size_t step = std::min(needed_rows, current_rows);
            if (step) {
                for (size_t i = 0; i < num_columns; ++i) {
                    merged_columns[i]->insert_range_from(*current->impl->block->get_columns()[i],
                                                         current->impl->pos, step);
                }
            }

            current_rows -= step;
            current_output_rows += step;
            if ((current_output_rows + _output_total_rows) >= _partition_inner_limit) {
                //rows has get enough
                get_enough_data = true;
            }
            if (!current->impl->is_last(current_rows)) {
                priority_queue.next(current_rows);
            } else {
                priority_queue.remove_top();
            }
        } else {
            for (size_t offset = 0; offset < current_rows; offset++) {
                current_output_rows++;
                for (size_t i = 0; i < num_columns; ++i) {
                    merged_columns[i]->insert_from(*current->impl->block->get_columns()[i],
                                                   current->impl->pos);
                }

                bool cmp_res = _previous_row->compare_two_rows(current->impl);
                if (!cmp_res) {
                    _output_distinct_rows++;

                    if (_top_n_algorithm == TopNAlgorithm::DENSE_RANK) {
                        // dense_rank(): 1,1,1,2,2,2,2,.......,2,3,3,3, if SQL: where rk < 3, need output all 1 and 2
                        // dense_rank() maybe need distinct rows of partition_inner_limit
                        // so check have output distinct rows, not _output_total_rows
                        if (_output_distinct_rows >= _partition_inner_limit) {
                            get_enough_data = true;
                            break;
                        }
                    } else {
                        // rank(): 1,1,1,4,5,6,6,6.....,6,100,101. if SQL where rk < 7, need output all 1,1,1,4,5,6,6,....6
                        // rank() maybe need check when have get a distinct row
                        // so when the cmp_res is get a distinct row, need check have output all rows num
                        if ((current_output_rows + _output_total_rows) >= _partition_inner_limit) {
                            get_enough_data = true;
                            break;
                        }
                    }
                    *_previous_row = *current;
                }
                if (!current->impl->is_last(1)) {
                    priority_queue.next(1);
                } else {
                    priority_queue.remove_top();
                }
            }
        }

        if (current_output_rows == batch_size || get_enough_data) {
            break;
        }
    }

    _output_total_rows += output_block->rows();
    if (current_output_rows == 0 || get_enough_data) {
        *eos = true;
    }
    return Status::OK();
}

} // namespace doris::vectorized
