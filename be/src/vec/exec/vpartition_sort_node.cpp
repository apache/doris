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

#include "vec/exec/vpartition_sort_node.h"

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_map_context_creator.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/hash_table/partitioned_hash_map.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
Status PartitionBlocks::append_block_by_selector(const vectorized::Block* input_block, bool eos) {
    if (_blocks.empty() || reach_limit()) {
        _init_rows = _partition_sort_info->_runtime_state->batch_size();
        _blocks.push_back(Block::create_unique(
                VectorizedUtils::create_empty_block(_partition_sort_info->_row_desc)));
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
        _previous_row = std::make_unique<SortCursorCmp>();
        _partition_topn_sorter = PartitionSorter::create_unique(
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
        auto output_block = Block::create_unique(
                VectorizedUtils::create_empty_block(_partition_sort_info->_row_desc));
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

VPartitionSortNode::VPartitionSortNode(ObjectPool* pool, const TPlanNode& tnode,
                                       const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _hash_table_size_counter(nullptr) {
    _partitioned_data = std::make_unique<PartitionedHashMapVariants>();
    _agg_arena_pool = std::make_unique<Arena>();
}

Status VPartitionSortNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    //order by key
    if (tnode.partition_sort_node.__isset.sort_info) {
        RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.partition_sort_node.sort_info, _pool));
        _is_asc_order = tnode.partition_sort_node.sort_info.is_asc_order;
        _nulls_first = tnode.partition_sort_node.sort_info.nulls_first;
    }
    //partition by key
    if (tnode.partition_sort_node.__isset.partition_exprs) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.partition_sort_node.partition_exprs,
                                                 _partition_expr_ctxs));
        _partition_exprs_num = _partition_expr_ctxs.size();
        _partition_columns.resize(_partition_exprs_num);
    }

    _has_global_limit = tnode.partition_sort_node.has_global_limit;
    _top_n_algorithm = tnode.partition_sort_node.top_n_algorithm;
    _partition_inner_limit = tnode.partition_sort_node.partition_inner_limit;
    _topn_phase = tnode.partition_sort_node.ptopn_phase;
    return Status::OK();
}

Status VPartitionSortNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VPartitionSortNode::prepare";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _hash_table_size_counter = ADD_COUNTER(_runtime_profile, "HashTableSize", TUnit::UNIT);
    _build_timer = ADD_TIMER(runtime_profile(), "HashTableBuildTime");
    _partition_sort_timer = ADD_TIMER(runtime_profile(), "PartitionSortTime");
    _get_sorted_timer = ADD_TIMER(runtime_profile(), "GetSortedTime");
    _selector_block_timer = ADD_TIMER(runtime_profile(), "SelectorBlockTime");
    _emplace_key_timer = ADD_TIMER(runtime_profile(), "EmplaceKeyTime");
    runtime_profile()->add_info_string("CurrentTopNPhase", std::to_string(_topn_phase));

    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_TIMER(_exec_timer);
    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, child(0)->row_desc(), _row_descriptor));
    RETURN_IF_ERROR(VExpr::prepare(_partition_expr_ctxs, state, child(0)->row_desc()));
    _init_hash_method();

    _partition_sort_info = std::make_shared<PartitionSortInfo>(
            &_vsort_exec_exprs, _limit, 0, _pool, _is_asc_order, _nulls_first, child(0)->row_desc(),
            state, _runtime_profile.get(), _has_global_limit, _partition_inner_limit,
            _top_n_algorithm, _topn_phase);
    return Status::OK();
}

Status VPartitionSortNode::_split_block_by_partition(vectorized::Block* input_block, bool eos) {
    for (int i = 0; i < _partition_exprs_num; ++i) {
        int result_column_id = -1;
        RETURN_IF_ERROR(_partition_expr_ctxs[i]->execute(input_block, &result_column_id));
        DCHECK(result_column_id != -1);
        _partition_columns[i] = input_block->get_by_position(result_column_id).column.get();
    }
    RETURN_IF_ERROR(_emplace_into_hash_table(_partition_columns, input_block, eos));
    return Status::OK();
}

Status VPartitionSortNode::_emplace_into_hash_table(const ColumnRawPtrs& key_columns,
                                                    const vectorized::Block* input_block,
                                                    bool eos) {
    return std::visit(
            [&](auto&& agg_method) -> Status {
                SCOPED_TIMER(_build_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using AggState = typename HashMethodType::State;

                AggState state(key_columns);
                size_t num_rows = input_block->rows();
                agg_method.init_serialized_keys(key_columns, num_rows);

                auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                    HashMethodType::try_presis_key(key, origin, *_agg_arena_pool);
                    auto* aggregate_data = _pool->add(
                            new PartitionBlocks(_partition_sort_info, _value_places.empty()));
                    _value_places.push_back(aggregate_data);
                    ctor(key, aggregate_data);
                    _num_partition++;
                };
                auto creator_for_null_key = [&](auto& mapped) {
                    mapped = _pool->add(
                            new PartitionBlocks(_partition_sort_info, _value_places.empty()));
                    _value_places.push_back(mapped);
                    _num_partition++;
                };
                {
                    SCOPED_TIMER(_emplace_key_timer);
                    for (size_t row = 0; row < num_rows; ++row) {
                        auto& mapped =
                                agg_method.lazy_emplace(state, row, creator, creator_for_null_key);
                        mapped->add_row_idx(row);
                    }
                }
                {
                    SCOPED_TIMER(_selector_block_timer);
                    for (auto* place : _value_places) {
                        RETURN_IF_ERROR(place->append_block_by_selector(input_block, eos));
                    }
                }
                return Status::OK();
            },
            _partitioned_data->method_variant);
}

Status VPartitionSortNode::sink(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    SCOPED_TIMER(_exec_timer);
    auto current_rows = input_block->rows();
    if (current_rows > 0) {
        child_input_rows = child_input_rows + current_rows;
        if (UNLIKELY(_partition_exprs_num == 0)) {
            if (UNLIKELY(_value_places.empty())) {
                _value_places.push_back(_pool->add(
                        new PartitionBlocks(_partition_sort_info, _value_places.empty())));
            }
            //no partition key
            _value_places[0]->append_whole_block(input_block, child(0)->row_desc());
        } else {
            //just simply use partition num to check
            //if is TWO_PHASE_GLOBAL, must be sort all data thought partition num threshold have been exceeded.
            if (_topn_phase != TPartTopNPhase::TWO_PHASE_GLOBAL &&
                _num_partition > config::partition_topn_partition_threshold &&
                child_input_rows < 10000 * _num_partition) {
                {
                    std::lock_guard<std::mutex> lock(_buffer_mutex);
                    _blocks_buffer.push(std::move(*input_block));
                }
            } else {
                RETURN_IF_ERROR(_split_block_by_partition(input_block, eos));
                RETURN_IF_CANCELLED(state);
                input_block->clear_column_data();
            }
        }
    }

    if (eos) {
        //seems could free for hashtable
        _agg_arena_pool.reset(nullptr);
        _partitioned_data.reset(nullptr);
        SCOPED_TIMER(_partition_sort_timer);
        for (int i = 0; i < _value_places.size(); ++i) {
            _value_places[i]->create_or_reset_sorter_state();
            auto sorter = std::ref(_value_places[i]->_partition_topn_sorter);

            //get blocks from every partition, and sorter get those data.
            for (const auto& block : _value_places[i]->_blocks) {
                RETURN_IF_ERROR(sorter.get()->append_block(block.get()));
            }
            _value_places[i]->_blocks.clear();
            RETURN_IF_ERROR(sorter.get()->prepare_for_read());
            _partition_sorts.push_back(std::move(sorter.get()));
        }

        COUNTER_SET(_hash_table_size_counter, int64_t(_num_partition));
        //so all data from child have sink completed
        _can_read = true;
    }
    return Status::OK();
}

Status VPartitionSortNode::open(RuntimeState* state) {
    VLOG_CRITICAL << "VPartitionSortNode::open";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(child(0)->open(state));

    bool eos = false;
    std::unique_ptr<Block> input_block = Block::create_unique();
    do {
        RETURN_IF_ERROR(child(0)->get_next_after_projects(
                state, input_block.get(), &eos,
                std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                  ExecNode::get_next,
                          _children[0], std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));
        RETURN_IF_ERROR(sink(state, input_block.get(), eos));
    } while (!eos);

    RETURN_IF_ERROR(child(0)->close(state));

    return Status::OK();
}

Status VPartitionSortNode::alloc_resource(RuntimeState* state) {
    SCOPED_TIMER(_exec_timer);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));
    RETURN_IF_ERROR(VExpr::open(_partition_expr_ctxs, state));
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    RETURN_IF_CANCELLED(state);
    return Status::OK();
}

bool VPartitionSortNode::can_read() {
    std::lock_guard<std::mutex> lock(_buffer_mutex);
    return !_blocks_buffer.empty() || _can_read;
}

Status VPartitionSortNode::pull(doris::RuntimeState* state, vectorized::Block* output_block,
                                bool* eos) {
    SCOPED_TIMER(_exec_timer);
    RETURN_IF_CANCELLED(state);
    output_block->clear_column_data();
    {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        if (_blocks_buffer.empty() == false) {
            _blocks_buffer.front().swap(*output_block);
            _blocks_buffer.pop();
            RETURN_IF_ERROR(
                    VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
            return Status::OK();
        }
    }
    // notice: must output block from _blocks_buffer firstly, and then get_sorted_block.
    // as when the child is eos, then set _can_read = true, and _partition_sorts have push_back sorter.
    // if we move the _blocks_buffer output at last(behind 286 line),
    // it's maybe eos but not output all data: when _blocks_buffer.empty() and _can_read = false (this: _sort_idx && _partition_sorts.size() are 0)
    if (_can_read) {
        bool current_eos = false;
        RETURN_IF_ERROR(get_sorted_block(state, output_block, &current_eos));
    }
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, output_block, output_block->columns()));
    {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        if (_blocks_buffer.empty() && _sort_idx >= _partition_sorts.size()) {
            _can_read = false;
            *eos = true;
        }
    }
    return Status::OK();
}

Status VPartitionSortNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    if (state == nullptr || output_block == nullptr || eos == nullptr) {
        return Status::InternalError("input is nullptr");
    }
    VLOG_CRITICAL << "VPartitionSortNode::get_next";
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return pull(state, output_block, eos);
}

Status VPartitionSortNode::get_sorted_block(RuntimeState* state, Block* output_block,
                                            bool* current_eos) {
    SCOPED_TIMER(_get_sorted_timer);
    //sorter output data one by one
    if (_sort_idx < _partition_sorts.size()) {
        RETURN_IF_ERROR(_partition_sorts[_sort_idx]->get_next(state, output_block, current_eos));
    }
    if (*current_eos) {
        //current sort have eos, so get next idx
        auto rows = _partition_sorts[_sort_idx]->get_output_rows();
        partition_profile_output_rows.push_back(rows);
        _num_rows_returned += rows;
        _partition_sorts[_sort_idx].reset(nullptr);
        _sort_idx++;
    }

    return Status::OK();
}

Status VPartitionSortNode::close(RuntimeState* state) {
    VLOG_CRITICAL << "VPartitionSortNode::close";
    if (is_closed()) {
        return Status::OK();
    }

    return ExecNode::close(state);
}

void VPartitionSortNode::release_resource(RuntimeState* state) {
    _vsort_exec_exprs.close(state);
    if (state->enable_profile()) {
        debug_profile();
    }
    ExecNode::release_resource(state);
}

void VPartitionSortNode::_init_hash_method() {
    init_partition_hash_method(_partitioned_data.get(), _partition_expr_ctxs, true);
}

void VPartitionSortNode::debug_profile() {
    fmt::memory_buffer partition_rows_read, partition_topn_count, partition_filter_rows;
    fmt::format_to(partition_rows_read, "[");
    fmt::format_to(partition_topn_count, "[");
    fmt::format_to(partition_filter_rows, "[");

    for (auto* place : _value_places) {
        fmt::format_to(partition_rows_read, "{}, ", place->get_total_rows());
        fmt::format_to(partition_filter_rows, "{}, ", place->get_topn_filter_rows());
        fmt::format_to(partition_topn_count, "{}, ", place->get_do_topn_count());
    }
    fmt::format_to(partition_rows_read, "]");
    fmt::format_to(partition_topn_count, "]");
    fmt::format_to(partition_filter_rows, "]");

    runtime_profile()->add_info_string("PerPartitionDoTopNCount",
                                       fmt::to_string(partition_topn_count));
    runtime_profile()->add_info_string("PerPartitionRowsRead", fmt::to_string(partition_rows_read));
    runtime_profile()->add_info_string("PerPartitionFilterRows",
                                       fmt::to_string(partition_filter_rows));
    fmt::memory_buffer partition_output_rows;
    fmt::format_to(partition_output_rows, "[");
    for (auto row : partition_profile_output_rows) {
        fmt::format_to(partition_output_rows, "{}, ", row);
    }
    fmt::format_to(partition_output_rows, "]");
    runtime_profile()->add_info_string("PerPartitionOutputRows",
                                       fmt::to_string(partition_output_rows));
}

} // namespace doris::vectorized
