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

#include "operator.h"
#include "pipeline/exec/aggregation_sink_operator_helper.h"
#include "pipeline/pipeline_x/operator.h"
#include "runtime/block_spill_manager.h"
#include "runtime/exec_env.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;

namespace pipeline {

class AggSinkOperatorBuilder final : public OperatorBuilder<vectorized::AggregationNode> {
public:
    AggSinkOperatorBuilder(int32_t, ExecNode*);

    OperatorPtr build_operator() override;
    bool is_sink() const override { return true; }
};

class AggSinkOperator final : public StreamingOperator<vectorized::AggregationNode> {
public:
    AggSinkOperator(OperatorBuilderBase* operator_builder, ExecNode* node);
    bool can_write() override { return true; }
};

class AggSinkOperatorX;

class AggSinkLocalState : public PipelineXSinkLocalState<AggSharedState> {
public:
    ENABLE_FACTORY_CREATOR(AggSinkLocalState);
    using Base = PipelineXSinkLocalState<AggSharedState>;
    using Base::_shared_state;
    using Base::_dependency;
    AggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~AggSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

protected:
    friend class AggSinkOperatorX;
    friend class AggSinkLocalStateHelper<AggSinkLocalState, AggSinkOperatorX>;
    friend class AggLocalStateHelper<AggSinkLocalState, AggSinkOperatorX>;
    AggSinkLocalStateHelper<AggSinkLocalState, AggSinkOperatorX> _agg_helper;
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
                    return local_state->_agg_helper._merge_without_key(block);
                } else {
                    return local_state->_agg_helper._execute_without_key(block);
                }
            } else {
                if constexpr (NeedToMerge) {
                    return local_state->_agg_helper._merge_with_serialized_key(block);
                } else {
                    return local_state->_agg_helper._execute_with_serialized_key(block);
                }
            }
        }
        void update_memusage(AggSinkLocalState* local_state) override {
            if constexpr (WithoutKey) {
                local_state->_agg_helper._update_memusage_without_key();
            } else {
                local_state->_agg_helper._update_memusage_with_serialized_key();
            }
        }
    };

    size_t _memory_usage() const;

    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;
    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _expr_timer = nullptr;
    RuntimeProfile::Counter* _serialize_key_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
    RuntimeProfile::Counter* _serialize_data_timer = nullptr;
    RuntimeProfile::Counter* _deserialize_data_timer = nullptr;
    RuntimeProfile::Counter* _max_row_size_counter = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _serialize_key_arena_memory_usage = nullptr;

    bool _should_limit_output = false;
    bool _reach_limit = false;

    vectorized::PODArray<vectorized::AggregateDataPtr> _places;
    std::vector<char> _deserialize_buffer;

    vectorized::Block _preagg_block = vectorized::Block();

    vectorized::AggregatedDataVariants* _agg_data = nullptr;
    vectorized::Arena* _agg_arena_pool = nullptr;

    std::unique_ptr<ExecutorBase> _executor = nullptr;
    struct MemoryRecord {
        MemoryRecord() : used_in_arena(0), used_in_state(0) {}
        int64_t used_in_arena;
        int64_t used_in_state;
    };
    MemoryRecord _mem_usage_record;
};

class AggSinkOperatorX final : public DataSinkOperatorX<AggSinkLocalState> {
public:
    AggSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                     const DescriptorTbl& descs);
    ~AggSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<AggSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    DataDistribution required_data_distribution() const override {
        if (_probe_expr_ctxs.empty()) {
            return _needs_finalize || DataSinkOperatorX<AggSinkLocalState>::_child_x
                                              ->ignore_data_distribution()
                           ? DataDistribution(ExchangeType::PASSTHROUGH)
                           : DataSinkOperatorX<AggSinkLocalState>::required_data_distribution();
        }
        return _is_colocate ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                            : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
    }
    size_t get_revocable_mem_size(RuntimeState* state) const;

    vectorized::AggregatedDataVariants* get_agg_data(RuntimeState* state) {
        auto& local_state = get_local_state(state);
        return local_state._agg_data;
    }

    Status reset_hash_table(RuntimeState* state);

    using DataSinkOperatorX<AggSinkLocalState>::id;
    using DataSinkOperatorX<AggSinkLocalState>::operator_id;
    using DataSinkOperatorX<AggSinkLocalState>::get_local_state;

protected:
    using LocalState = AggSinkLocalState;
    friend class AggSinkLocalState;
    friend class AggSinkLocalStateHelper<AggSinkLocalState, AggSinkOperatorX>;
    friend class AggLocalStateHelper<AggSinkLocalState, AggSinkOperatorX>;
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
    vectorized::Sizes offsets_of_aggregate_states;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;

    size_t _external_agg_bytes_threshold;
    // group by k1,k2
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    ObjectPool* _pool = nullptr;
    std::vector<size_t> _make_nullable_keys;
    int64_t _limit; // -1: no limit
    bool _have_conjuncts;

    const std::vector<TExpr> _partition_exprs;
    const bool _is_colocate;
};

} // namespace pipeline
} // namespace doris
