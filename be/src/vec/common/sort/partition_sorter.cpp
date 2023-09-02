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
          _previous_row(previous_row) {}

Status PartitionSorter::append_block(Block* input_block) {
    Block sorted_block = VectorizedUtils::create_empty_columnswithtypename(_row_desc);
    DCHECK(input_block->columns() == sorted_block.columns());
    RETURN_IF_ERROR(partial_sort(*input_block, sorted_block));
    RETURN_IF_ERROR(_state->add_sorted_block(sorted_block));
    return Status::OK();
}

Status PartitionSorter::prepare_for_read() {
    auto& cursors = _state->get_cursors();
    auto& blocks = _state->get_sorted_block();
    auto& priority_queue = _state->get_priority_queue();
    for (const auto& block : blocks) {
        cursors.emplace_back(block, _sort_description);
    }
    for (auto& cursor : cursors) {
        priority_queue.push(MergeSortCursor(&cursor));
    }
    return Status::OK();
}

Status PartitionSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    if (_state->get_sorted_block().empty()) {
        *eos = true;
    } else {
        if (_state->get_sorted_block().size() == 1 && _has_global_limit) {
            auto& sorted_block = _state->get_sorted_block()[0];
            block->swap(sorted_block);
            block->set_num_rows(_partition_inner_limit);
            *eos = true;
        } else {
            RETURN_IF_ERROR(partition_sort_read(block, eos, state->batch_size()));
        }
    }
    return Status::OK();
}

Status PartitionSorter::partition_sort_read(Block* output_block, bool* eos, int batch_size) {
    const auto& sorted_block = _state->get_sorted_block()[0];
    size_t num_columns = sorted_block.columns();
    MutableBlock m_block =
            VectorizedUtils::build_mutable_mem_reuse_block(output_block, sorted_block);
    MutableColumns& merged_columns = m_block.mutable_columns();
    size_t current_output_rows = 0;
    auto& priority_queue = _state->get_priority_queue();

    bool get_enough_data = false;
    bool first_compare_row = false;
    while (!priority_queue.empty()) {
        auto current = priority_queue.top();
        priority_queue.pop();
        if (UNLIKELY(_previous_row->impl == nullptr)) {
            first_compare_row = true;
            *_previous_row = current;
        }

        switch (_top_n_algorithm) {
        case TopNAlgorithm::ROW_NUMBER: {
            //1 row_number no need to check distinct, just output partition_inner_limit row
            if ((current_output_rows + _output_total_rows) < _partition_inner_limit) {
                for (size_t i = 0; i < num_columns; ++i) {
                    merged_columns[i]->insert_from(*current->all_columns[i], current->pos);
                }
            } else {
                //rows has get enough
                get_enough_data = true;
            }
            current_output_rows++;
            break;
        }
        case TopNAlgorithm::DENSE_RANK: {
            //3 dense_rank() maybe need distinct rows of partition_inner_limit
            if ((current_output_rows + _output_total_rows) < _partition_inner_limit) {
                for (size_t i = 0; i < num_columns; ++i) {
                    merged_columns[i]->insert_from(*current->all_columns[i], current->pos);
                }
            } else {
                get_enough_data = true;
            }
            if (_has_global_limit) {
                current_output_rows++;
            } else {
                //when it's first comes, the rows are same no need compare
                if (first_compare_row) {
                    current_output_rows++;
                    first_compare_row = false;
                } else {
                    // not the first comes, so need compare those, when is distinct row
                    // so could current_output_rows++
                    bool cmp_res = _previous_row->compare_two_rows(current);
                    if (cmp_res == false) { // distinct row
                        current_output_rows++;
                        *_previous_row = current;
                    }
                }
            }
            break;
        }
        case TopNAlgorithm::RANK: {
            if (_has_global_limit &&
                (current_output_rows + _output_total_rows) >= _partition_inner_limit) {
                get_enough_data = true;
                break;
            }
            bool cmp_res = _previous_row->compare_two_rows(current);
            //get a distinct row
            if (cmp_res == false) {
                //here must be check distinct of two rows, and then check nums of row
                if ((current_output_rows + _output_total_rows) >= _partition_inner_limit) {
                    get_enough_data = true;
                    break;
                }
                *_previous_row = current;
            }
            for (size_t i = 0; i < num_columns; ++i) {
                merged_columns[i]->insert_from(*current->all_columns[i], current->pos);
            }
            current_output_rows++;
            break;
        }
        default:
            break;
        }

        if (!current->isLast()) {
            current->next();
            priority_queue.push(current);
        }

        if (current_output_rows == batch_size || get_enough_data == true) {
            break;
        }
    }

    _output_total_rows += output_block->rows();
    if (current_output_rows == 0 || get_enough_data == true) {
        *eos = true;
    }
    return Status::OK();
}

} // namespace doris::vectorized
