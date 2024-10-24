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

#include "partition_sort_sink_operator.h"

#include <cstdint>

#include "common/status.h"
#include "partition_sort_source_operator.h"
#include "vec/common/hash_table/hash.h"

namespace doris::pipeline {

Status PartitionSortSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<PartitionSortNodeSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<PartitionSortSinkOperatorX>();
    RETURN_IF_ERROR(p._vsort_exec_exprs.clone(state, _vsort_exec_exprs));
    _partition_expr_ctxs.resize(p._partition_expr_ctxs.size());
    _partition_columns.resize(p._partition_expr_ctxs.size());
    for (size_t i = 0; i < p._partition_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._partition_expr_ctxs[i]->clone(state, _partition_expr_ctxs[i]));
    }
    _topn_phase = p._topn_phase;
    _partition_exprs_num = p._partition_exprs_num;
    _hash_table_size_counter = ADD_COUNTER(_profile, "HashTableSize", TUnit::UNIT);
    _serialize_key_arena_memory_usage =
            _profile->AddHighWaterMarkCounter("MemoryUsageSerializeKeyArena", TUnit::BYTES, "", 1);
    _hash_table_memory_usage =
            ADD_COUNTER_WITH_LEVEL(_profile, "MemoryUsageHashTable", TUnit::BYTES, 1);
    _build_timer = ADD_TIMER(_profile, "HashTableBuildTime");
    _selector_block_timer = ADD_TIMER(_profile, "SelectorBlockTime");
    _emplace_key_timer = ADD_TIMER(_profile, "EmplaceKeyTime");
    _passthrough_rows_counter = ADD_COUNTER(_profile, "PassThroughRowsCounter", TUnit::UNIT);
    _sorted_partition_input_rows_counter =
            ADD_COUNTER(_profile, "SortedPartitionInputRows", TUnit::UNIT);
    _partition_sort_info = std::make_shared<PartitionSortInfo>(
            &_vsort_exec_exprs, p._limit, 0, p._pool, p._is_asc_order, p._nulls_first,
            p._child->row_desc(), state, _profile, p._has_global_limit, p._partition_inner_limit,
            p._top_n_algorithm, p._topn_phase);
    _profile->add_info_string("PartitionTopNPhase", to_string(p._topn_phase));
    _profile->add_info_string("PartitionTopNLimit", std::to_string(p._partition_inner_limit));
    RETURN_IF_ERROR(_init_hash_method());
    return Status::OK();
}

PartitionSortSinkOperatorX::PartitionSortSinkOperatorX(ObjectPool* pool, int operator_id,
                                                       const TPlanNode& tnode,
                                                       const DescriptorTbl& descs)
        : DataSinkOperatorX(operator_id, tnode.node_id),
          _pool(pool),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
          _limit(tnode.limit),
          _partition_exprs_num(tnode.partition_sort_node.partition_exprs.size()),
          _topn_phase(tnode.partition_sort_node.ptopn_phase),
          _has_global_limit(tnode.partition_sort_node.has_global_limit),
          _top_n_algorithm(tnode.partition_sort_node.top_n_algorithm),
          _partition_inner_limit(tnode.partition_sort_node.partition_inner_limit) {}

Status PartitionSortSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));

    //order by key
    if (tnode.partition_sort_node.__isset.sort_info) {
        RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.partition_sort_node.sort_info, _pool));
        _is_asc_order = tnode.partition_sort_node.sort_info.is_asc_order;
        _nulls_first = tnode.partition_sort_node.sort_info.nulls_first;
    }
    //partition by key
    if (tnode.partition_sort_node.__isset.partition_exprs) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.partition_sort_node.partition_exprs, _partition_expr_ctxs));
    }

    return Status::OK();
}

Status PartitionSortSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<PartitionSortSinkLocalState>::open(state));
    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, _child->row_desc(), _row_descriptor));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_partition_expr_ctxs, state, _child->row_desc()));
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_partition_expr_ctxs, state));
    return Status::OK();
}

Status PartitionSortSinkOperatorX::sink(RuntimeState* state, vectorized::Block* input_block,
                                        bool eos) {
    auto& local_state = get_local_state(state);
    auto current_rows = input_block->rows();
    SCOPED_TIMER(local_state.exec_time_counter());
    if (current_rows > 0) {
        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)input_block->rows());
        if (UNLIKELY(_partition_exprs_num == 0)) {
            if (UNLIKELY(local_state._value_places.empty())) {
                local_state._value_places.push_back(_pool->add(new PartitionBlocks(
                        local_state._partition_sort_info, local_state._value_places.empty())));
            }
            local_state._value_places[0]->append_whole_block(input_block, _child->row_desc());
        } else {
            if (local_state._is_need_passthrough) {
                {
                    COUNTER_UPDATE(local_state._passthrough_rows_counter, (int64_t)current_rows);
                    std::lock_guard<std::mutex> lock(local_state._shared_state->buffer_mutex);
                    local_state._shared_state->blocks_buffer.push(std::move(*input_block));
                    // buffer have data, source could read this.
                    local_state._dependency->set_ready_to_read();
                }
            } else {
                RETURN_IF_ERROR(_split_block_by_partition(input_block, local_state, eos));
                RETURN_IF_CANCELLED(state);
                input_block->clear_column_data();
            }
        }
    }

    if (eos) {
        //seems could free for hashtable
        local_state._agg_arena_pool.reset(nullptr);
        local_state._partitioned_data.reset(nullptr);
        for (int i = 0; i < local_state._value_places.size(); ++i) {
            local_state._value_places[i]->create_or_reset_sorter_state();
            auto sorter = std::move(local_state._value_places[i]->_partition_topn_sorter);

            //get blocks from every partition, and sorter get those data.
            for (const auto& block : local_state._value_places[i]->_blocks) {
                RETURN_IF_ERROR(sorter->append_block(block.get()));
            }
            local_state._value_places[i]->_blocks.clear();
            RETURN_IF_ERROR(sorter->prepare_for_read());
            local_state._shared_state->partition_sorts.push_back(std::move(sorter));
        }

        COUNTER_SET(local_state._hash_table_size_counter, int64_t(local_state._num_partition));
        COUNTER_SET(local_state._sorted_partition_input_rows_counter,
                    local_state._sorted_partition_input_rows);
        //so all data from child have sink completed
        {
            std::unique_lock<std::mutex> lc(local_state._shared_state->sink_eos_lock);
            local_state._shared_state->sink_eos = true;
            local_state._dependency->set_ready_to_read();
        }
        local_state._profile->add_info_string("HasPassThrough",
                                              local_state._is_need_passthrough ? "Yes" : "No");
    }

    return Status::OK();
}

Status PartitionSortSinkOperatorX::_split_block_by_partition(
        vectorized::Block* input_block, PartitionSortSinkLocalState& local_state, bool eos) {
    for (int i = 0; i < _partition_exprs_num; ++i) {
        int result_column_id = -1;
        RETURN_IF_ERROR(_partition_expr_ctxs[i]->execute(input_block, &result_column_id));
        DCHECK(result_column_id != -1);
        local_state._partition_columns[i] =
                input_block->get_by_position(result_column_id).column.get();
    }
    RETURN_IF_ERROR(_emplace_into_hash_table(local_state._partition_columns, input_block,
                                             local_state, eos));
    return Status::OK();
}

Status PartitionSortSinkOperatorX::_emplace_into_hash_table(
        const vectorized::ColumnRawPtrs& key_columns, vectorized::Block* input_block,
        PartitionSortSinkLocalState& local_state, bool eos) {
    return std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> Status {
                        return Status::InternalError("Unit hash table");
                    },
                    [&](auto& agg_method) -> Status {
                        SCOPED_TIMER(local_state._build_timer);
                        using HashMethodType = std::decay_t<decltype(agg_method)>;
                        using AggState = typename HashMethodType::State;

                        AggState state(key_columns);
                        size_t num_rows = input_block->rows();
                        agg_method.init_serialized_keys(key_columns, num_rows);

                        auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                            HashMethodType::try_presis_key(key, origin,
                                                           *local_state._agg_arena_pool);
                            auto* aggregate_data = _pool->add(
                                    new PartitionBlocks(local_state._partition_sort_info,
                                                        local_state._value_places.empty()));
                            local_state._value_places.push_back(aggregate_data);
                            ctor(key, aggregate_data);
                            local_state._num_partition++;
                        };
                        auto creator_for_null_key = [&](auto& mapped) {
                            mapped = _pool->add(
                                    new PartitionBlocks(local_state._partition_sort_info,
                                                        local_state._value_places.empty()));
                            local_state._value_places.push_back(mapped);
                            local_state._num_partition++;
                        };

                        SCOPED_TIMER(local_state._emplace_key_timer);
                        int row = num_rows;
                        for (row = row - 1; row >= 0 && !local_state._is_need_passthrough; --row) {
                            auto& mapped = *agg_method.lazy_emplace(state, row, creator,
                                                                    creator_for_null_key);
                            mapped->add_row_idx(row);
                            local_state._sorted_partition_input_rows++;
                            local_state._is_need_passthrough =
                                    local_state.check_whether_need_passthrough();
                        }
                        for (auto* place : local_state._value_places) {
                            SCOPED_TIMER(local_state._selector_block_timer);
                            RETURN_IF_ERROR(place->append_block_by_selector(input_block, eos));
                        }
                        //Perform passthrough for the range [0, row] of input_block
                        if (local_state._is_need_passthrough && row >= 0) {
                            {
                                COUNTER_UPDATE(local_state._passthrough_rows_counter,
                                               (int64_t)(row + 1));
                                std::lock_guard<std::mutex> lock(
                                        local_state._shared_state->buffer_mutex);
                                // have emplace (num_rows - row) to hashtable, and now have row remaining needed in block;
                                // set_num_rows(x) retains the range [0, x - 1], so row + 1 is needed here.
                                input_block->set_num_rows(row + 1);
                                local_state._shared_state->blocks_buffer.push(
                                        std::move(*input_block));
                                // buffer have data, source could read this.
                                local_state._dependency->set_ready_to_read();
                            }
                        }
                        local_state._serialize_key_arena_memory_usage->set(
                                (int64_t)local_state._agg_arena_pool->size());
                        COUNTER_SET(local_state._hash_table_memory_usage,
                                    (int64_t)agg_method.hash_table->get_buffer_size_in_bytes());
                        return Status::OK();
                    }},
            local_state._partitioned_data->method_variant);
}

constexpr auto init_partition_hash_method = init_hash_method<PartitionedHashMapVariants>;

Status PartitionSortSinkLocalState::_init_hash_method() {
    RETURN_IF_ERROR(init_partition_hash_method(_partitioned_data.get(),
                                               get_data_types(_partition_expr_ctxs), true));
    return Status::OK();
}

// NOLINTBEGIN(readability-simplify-boolean-expr)
// just simply use partition num to check
// but if is TWO_PHASE_GLOBAL, must be sort all data thought partition num threshold have been exceeded.
// partition_topn_max_partitions     default is : 1024
// partition_topn_per_partition_rows default is : 1000
bool PartitionSortSinkLocalState::check_whether_need_passthrough() {
    if (_topn_phase != TPartTopNPhase::TWO_PHASE_GLOBAL &&
        _num_partition > _state->partition_topn_max_partitions() &&
        _sorted_partition_input_rows <
                _state->partition_topn_per_partition_rows() * _num_partition) {
        return true;
    }
    return false;
}
// NOLINTEND(readability-simplify-boolean-expr)

} // namespace doris::pipeline
