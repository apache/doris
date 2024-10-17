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

#include "pipeline/common/partition_sort_utils.h"

namespace doris {

Status PartitionBlocks::append_block_by_selector(const vectorized::Block* input_block, bool eos) {
    if (_blocks.empty() || reach_limit()) {
        _init_rows = _partition_sort_info->_runtime_state->batch_size();
        _blocks.push_back(vectorized::Block::create_unique(
                vectorized::VectorizedUtils::create_empty_block(_partition_sort_info->_row_desc)));
    }
    auto columns = input_block->get_columns();
    auto mutable_columns = _blocks.back()->mutate_columns();
    DCHECK(columns.size() == mutable_columns.size());
    for (int i = 0; i < mutable_columns.size(); ++i) {
        columns[i]->append_data_by_selector(mutable_columns[i], _selector);
    }
    _blocks.back()->set_columns(std::move(mutable_columns));
    auto selector_rows = _selector.size();
    _init_rows = _init_rows - selector_rows;
    _total_rows = _total_rows + selector_rows;
    _current_input_rows = _current_input_rows + selector_rows;
    _selector.clear();
    // maybe better could change by user PARTITION_SORT_ROWS_THRESHOLD
    if (!eos && _partition_sort_info->_partition_inner_limit != -1 &&
        _current_input_rows >= PARTITION_SORT_ROWS_THRESHOLD &&
        _partition_sort_info->_topn_phase != TPartTopNPhase::TWO_PHASE_GLOBAL) {
        create_or_reset_sorter_state();
        RETURN_IF_ERROR(do_partition_topn_sort());
        _current_input_rows = 0; // reset record
        _do_partition_topn_count++;
    }
    return Status::OK();
}

void PartitionBlocks::create_or_reset_sorter_state() {
    if (_partition_topn_sorter == nullptr) {
        _previous_row = std::make_unique<vectorized::SortCursorCmp>();
        _partition_topn_sorter = vectorized::PartitionSorter::create_unique(
                *_partition_sort_info->_vsort_exec_exprs, _partition_sort_info->_limit,
                _partition_sort_info->_offset, _partition_sort_info->_pool,
                _partition_sort_info->_is_asc_order, _partition_sort_info->_nulls_first,
                _partition_sort_info->_row_desc, _partition_sort_info->_runtime_state,
                _is_first_sorter ? _partition_sort_info->_runtime_profile : nullptr,
                _partition_sort_info->_has_global_limit,
                _partition_sort_info->_partition_inner_limit,
                _partition_sort_info->_top_n_algorithm, _previous_row.get());
        _partition_topn_sorter->init_profile(_partition_sort_info->_runtime_profile);
    } else {
        _partition_topn_sorter->reset_sorter_state(_partition_sort_info->_runtime_state);
    }
}

Status PartitionBlocks::do_partition_topn_sort() {
    for (const auto& block : _blocks) {
        RETURN_IF_ERROR(_partition_topn_sorter->append_block(block.get()));
    }
    _blocks.clear();
    RETURN_IF_ERROR(_partition_topn_sorter->prepare_for_read());
    bool current_eos = false;
    size_t current_output_rows = 0;
    while (!current_eos) {
        // output_block maybe need better way
        auto output_block = vectorized::Block::create_unique(
                vectorized::VectorizedUtils::create_empty_block(_partition_sort_info->_row_desc));
        RETURN_IF_ERROR(_partition_topn_sorter->get_next(_partition_sort_info->_runtime_state,
                                                         output_block.get(), &current_eos));
        auto rows = output_block->rows();
        if (rows > 0) {
            current_output_rows += rows;
            _blocks.emplace_back(std::move(output_block));
        }
    }

    _topn_filter_rows += (_current_input_rows - current_output_rows);
    return Status::OK();
}

} // namespace doris
