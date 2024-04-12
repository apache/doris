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

#include "aggregation_source_operator.h"

#include <memory>
#include <string>

#include "common/exception.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/streaming_aggregation_source_operator.h"
#include "vec//utils/util.hpp"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(AggSourceOperator, SourceOperator)

AggLocalState::AggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent), _agg_helper(this) {}

Status AggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    _get_results_timer = ADD_TIMER(profile(), "GetResultsTime");
    _serialize_result_timer = ADD_TIMER(profile(), "SerializeResultTime");
    _hash_table_iterate_timer = ADD_TIMER(profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(profile(), "InsertKeysToColumnTime");
    _serialize_data_timer = ADD_TIMER(profile(), "SerializeDataTime");
    _hash_table_size_counter = ADD_COUNTER(profile(), "HashTableSize", TUnit::UNIT);

    _merge_timer = ADD_TIMER(Base::profile(), "MergeTime");
    _deserialize_data_timer = ADD_TIMER(Base::profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(Base::profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::profile(), "HashTableEmplaceTime");
    _hash_table_input_counter = ADD_COUNTER(Base::profile(), "HashTableInputCount", TUnit::UNIT);

    auto& p = _parent->template cast<AggSourceOperatorX>();
    if (p._without_key) {
        if (p._needs_finalize) {
            _executor.get_result = [&](RuntimeState* state, vectorized::Block* block, bool* eos) {
                return _agg_helper._get_without_key_result(state, block, eos);
            };
        } else {
            _executor.get_result = [&](RuntimeState* state, vectorized::Block* block, bool* eos) {
                return _agg_helper._serialize_without_key(state, block, eos);
            };
        }
    } else {
        if (p._needs_finalize) {
            _executor.get_result = [&](RuntimeState* state, vectorized::Block* block, bool* eos) {
                return _agg_helper._get_with_serialized_key_result(state, block, eos);
            };
        } else {
            _executor.get_result = _executor.get_result = [&](RuntimeState* state,
                                                              vectorized::Block* block, bool* eos) {
                return _agg_helper._serialize_with_serialized_key_result(state, block, eos);
            };
        }
    }

    _shared_state->agg_data_created_without_key = p._without_key;
    return Status::OK();
}

AggSourceOperatorX::AggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                       const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs),
          _needs_finalize(tnode.agg_node.need_finalize),
          _without_key(tnode.agg_node.grouping_exprs.empty()) {}

Status AggSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    RETURN_IF_ERROR(local_state._executor.get_result(state, block, eos));
    local_state.make_nullable_output_key(block);
    // dispose the having clause, should not be execute in prestreaming agg
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, block, block->columns()));
    local_state.reached_limit(block, eos);
    return Status::OK();
}

void AggLocalState::make_nullable_output_key(vectorized::Block* block) {
    if (block->rows() != 0) {
        for (auto cid : _shared_state->make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

template <bool limit>
Status AggLocalState::merge_with_serialized_key_helper(vectorized::Block* block) {
    SCOPED_TIMER(_merge_timer);

    size_t key_size = Base::_shared_state->probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);

    for (size_t i = 0; i < key_size; ++i) {
        key_columns[i] = block->get_by_position(i).column.get();
    }

    int rows = block->rows();
    if (_places.size() < rows) {
        _places.resize(rows);
    }

    if constexpr (limit) {
        _agg_helper._find_in_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
            if (Base::_shared_state->aggregate_evaluators[i]->is_merge()) {
                int col_id = AggSharedState::get_slot_column_id(
                        Base::_shared_state->aggregate_evaluators[i]);
                auto column = block->get_by_position(col_id).column;
                if (column->is_nullable()) {
                    column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
                }

                size_t buffer_size =
                        Base::_shared_state->aggregate_evaluators[i]->function()->size_of_data() *
                        rows;
                if (_deserialize_buffer.size() < buffer_size) {
                    _deserialize_buffer.resize(buffer_size);
                }

                {
                    SCOPED_TIMER(_deserialize_data_timer);
                    Base::_shared_state->aggregate_evaluators[i]
                            ->function()
                            ->deserialize_and_merge_vec_selected(
                                    _places.data(), _shared_state->offsets_of_aggregate_states[i],
                                    _deserialize_buffer.data(),
                                    (vectorized::ColumnString*)(column.get()),
                                    _shared_state->agg_arena_pool.get(), rows);
                }
            } else {
                RETURN_IF_ERROR(
                        Base::_shared_state->aggregate_evaluators[i]->execute_batch_add_selected(
                                block, _shared_state->offsets_of_aggregate_states[i],
                                _places.data(), _shared_state->agg_arena_pool.get()));
            }
        }
    } else {
        _agg_helper._emplace_into_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
            int col_id = 0;
            col_id = Base::_shared_state->probe_expr_ctxs.size() + i;
            auto column = block->get_by_position(col_id).column;
            if (column->is_nullable()) {
                column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
            }

            size_t buffer_size =
                    Base::_shared_state->aggregate_evaluators[i]->function()->size_of_data() * rows;
            if (_deserialize_buffer.size() < buffer_size) {
                _deserialize_buffer.resize(buffer_size);
            }

            {
                SCOPED_TIMER(_deserialize_data_timer);
                Base::_shared_state->aggregate_evaluators[i]->function()->deserialize_and_merge_vec(
                        _places.data(), _shared_state->offsets_of_aggregate_states[i],
                        _deserialize_buffer.data(), (vectorized::ColumnString*)(column.get()),
                        _shared_state->agg_arena_pool.get(), rows);
            }
        }

        if (_should_limit_output) {
            _reach_limit = _agg_helper._get_hash_table_size() >=
                           Base::_parent->template cast<AggSourceOperatorX>()._limit;
        }
    }

    return Status::OK();
}
template <bool limit>
Status AggSourceOperatorX::merge_with_serialized_key_helper(RuntimeState* state,
                                                            vectorized::Block* block) {
    auto& local_state = get_local_state(state);
    return local_state.merge_with_serialized_key_helper<limit>(block);
}
template Status AggSourceOperatorX::merge_with_serialized_key_helper<true>(
        RuntimeState* state, vectorized::Block* block);
template Status AggSourceOperatorX::merge_with_serialized_key_helper<false>(
        RuntimeState* state, vectorized::Block* block);

Status AggLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }

    /// _hash_table_size_counter may be null if prepare failed.
    if (_hash_table_size_counter) {
        std::visit(
                [&](auto&& agg_method) {
                    COUNTER_SET(_hash_table_size_counter, int64_t(agg_method.hash_table->size()));
                },
                _shared_state->agg_data->method_variant);
    }

    vectorized::PODArray<vectorized::AggregateDataPtr> tmp_places;
    _places.swap(tmp_places);

    std::vector<char> tmp_deserialize_buffer;
    _deserialize_buffer.swap(tmp_deserialize_buffer);
    return Base::close(state);
}

} // namespace doris::pipeline
