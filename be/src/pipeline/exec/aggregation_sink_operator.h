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

#pragma once

#include <stdint.h>

#include "pipeline/exec/operator.h"
#include "runtime/exec_env.h"

namespace doris::pipeline {

class AggSinkOperatorX;

class AggSinkLocalState : public PipelineXSinkLocalState<AggSharedState> {
public:
    ENABLE_FACTORY_CREATOR(AggSinkLocalState);
    using Base = PipelineXSinkLocalState<AggSharedState>;
    AggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~AggSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

protected:
    friend class AggSinkOperatorX;

    struct ExecutorBase {
        virtual Status execute(AggSinkLocalState* local_state, vectorized::Block* block) = 0;
        virtual void update_memusage(AggSinkLocalState* local_state) = 0;
        virtual ~ExecutorBase() = default;
    };
    template <bool WithoutKey, bool NeedToMerge>
    struct Executor final : public ExecutorBase {
        Status execute(AggSinkLocalState* local_state, vectorized::Block* block) override {
            if constexpr (WithoutKey) {
                if constexpr (NeedToMerge) {
                    return local_state->_merge_without_key(block);
                } else {
                    return local_state->_execute_without_key(block);
                }
            } else {
                if constexpr (NeedToMerge) {
                    return local_state->_merge_with_serialized_key(block);
                } else {
                    return local_state->_execute_with_serialized_key(block);
                }
            }
        }
        void update_memusage(AggSinkLocalState* local_state) override {
            if constexpr (WithoutKey) {
                local_state->_update_memusage_without_key();
            } else {
                local_state->_update_memusage_with_serialized_key();
            }
        }
    };

    Status _execute_without_key(vectorized::Block* block);
    Status _merge_without_key(vectorized::Block* block);
    void _update_memusage_without_key();
    Status _init_hash_method(const vectorized::VExprContextSPtrs& probe_exprs);
    Status _execute_with_serialized_key(vectorized::Block* block);
    Status _merge_with_serialized_key(vectorized::Block* block);
    void _update_memusage_with_serialized_key();
    template <bool limit>
    Status _execute_with_serialized_key_helper(vectorized::Block* block);
    void _find_in_hash_table(vectorized::AggregateDataPtr* places,
                             vectorized::ColumnRawPtrs& key_columns, size_t num_rows);
    void _emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                  vectorized::ColumnRawPtrs& key_columns, size_t num_rows);
    bool _emplace_into_hash_table_limit(vectorized::AggregateDataPtr* places,
                                        vectorized::Block* block, const std::vector<int>& key_locs,
                                        vectorized::ColumnRawPtrs& key_columns, size_t num_rows);
    size_t _get_hash_table_size() const;

    template <bool limit, bool for_spill = false>
    Status _merge_with_serialized_key_helper(vectorized::Block* block);

    Status _destroy_agg_status(vectorized::AggregateDataPtr data);
    Status _create_agg_status(vectorized::AggregateDataPtr data);
    size_t _memory_usage() const;

    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_limit_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;
    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _expr_timer = nullptr;
    RuntimeProfile::Counter* _serialize_key_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _serialize_data_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _max_row_size_counter = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _serialize_key_arena_memory_usage = nullptr;

    bool _should_limit_output = false;

    vectorized::PODArray<vectorized::AggregateDataPtr> _places;
    std::vector<char> _deserialize_buffer;

    vectorized::Block _preagg_block = vectorized::Block();

    AggregatedDataVariants* _agg_data = nullptr;
    vectorized::Arena* _agg_arena_pool = nullptr;
    std::unique_ptr<vectorized::Arena> _agg_profile_arena;

    std::unique_ptr<ExecutorBase> _executor = nullptr;
};

class AggSinkOperatorX final : public DataSinkOperatorX<AggSinkLocalState> {
public:
    AggSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                     const DescriptorTbl& descs, bool require_bucket_distribution);
    ~AggSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<AggSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    DataDistribution required_data_distribution() const override {
        if (_probe_expr_ctxs.empty()) {
            return _needs_finalize || DataSinkOperatorX<AggSinkLocalState>::_child
                                              ->ignore_data_distribution()
                           ? DataDistribution(ExchangeType::PASSTHROUGH)
                           : DataSinkOperatorX<AggSinkLocalState>::required_data_distribution();
        }
        return _is_colocate && _require_bucket_distribution && !_followed_by_shuffled_join
                       ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                       : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
    }
    bool require_data_distribution() const override { return _is_colocate; }
    bool require_shuffled_data_distribution() const override { return !_probe_expr_ctxs.empty(); }
    size_t get_revocable_mem_size(RuntimeState* state) const;

    AggregatedDataVariants* get_agg_data(RuntimeState* state) {
        auto& local_state = get_local_state(state);
        return local_state._agg_data;
    }

    Status reset_hash_table(RuntimeState* state);

    using DataSinkOperatorX<AggSinkLocalState>::node_id;
    using DataSinkOperatorX<AggSinkLocalState>::operator_id;
    using DataSinkOperatorX<AggSinkLocalState>::get_local_state;

protected:
    using LocalState = AggSinkLocalState;
    friend class AggSinkLocalState;
    std::vector<vectorized::AggFnEvaluator*> _aggregate_evaluators;
    bool _can_short_circuit = false;

    // may be we don't have to know the tuple id
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc = nullptr;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;

    bool _needs_finalize;
    bool _is_merge;
    const bool _is_first_phase;

    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    vectorized::Sizes _offsets_of_aggregate_states;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;

    // group by k1,k2
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    ObjectPool* _pool = nullptr;
    std::vector<size_t> _make_nullable_keys;
    int64_t _limit; // -1: no limit
    // do sort limit and directions
    bool _do_sort_limit = false;
    std::vector<int> _order_directions;
    std::vector<int> _null_directions;

    bool _have_conjuncts;
    const std::vector<TExpr> _partition_exprs;
    const bool _is_colocate;
    const bool _require_bucket_distribution;

    RowDescriptor _agg_fn_output_row_descriptor;
};

} // namespace doris::pipeline
