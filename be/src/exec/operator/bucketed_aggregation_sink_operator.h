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

#include "exec/operator/operator.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"

namespace doris {

class BucketedAggSinkOperatorX;

/// Sink-side local state for bucketed hash aggregation.
/// Each pipeline instance builds 256 per-bucket hash tables (two-level hash table).
/// No locking: each instance writes to per_instance_data[_instance_idx].
class BucketedAggSinkLocalState : public PipelineXSinkLocalState<BucketedAggSharedState> {
public:
    ENABLE_FACTORY_CREATOR(BucketedAggSinkLocalState);
    using Base = PipelineXSinkLocalState<BucketedAggSharedState>;
    BucketedAggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~BucketedAggSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    size_t get_reserve_mem_size(RuntimeState* state, bool eos) const;

private:
    friend class BucketedAggSinkOperatorX;

    Status _execute_with_serialized_key(Block* block);
    void _emplace_into_hash_table(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                                  uint32_t num_rows);
    Status _create_agg_status(AggregateDataPtr data);
    Status _destroy_agg_status(AggregateDataPtr data);
    Status _init_hash_method(const VExprContextSPtrs& probe_exprs);
    size_t _get_hash_table_size() const;
    void _update_memusage();

    int _instance_idx = 0;
    /// Pointers into shared_state->per_instance_data[_instance_idx].
    std::vector<BucketedAggDataVariants*> _bucket_agg_data; // [256]
    Arena* _arena = nullptr;

    /// Per-instance clones of probe expression contexts. Required because
    /// VExprContext::execute() mutates _last_result_column_id and FunctionContext
    /// internal state, causing data races when multiple sink instances share the
    /// same VExprContext and call execute() concurrently.
    VExprContextSPtrs _probe_expr_ctxs;

    /// Per-instance clones of aggregate evaluators. Required because
    /// AggFnEvaluator::_calc_argument_columns() mutates internal state
    /// (_agg_columns), which causes data races when multiple sink instances
    /// share the same evaluator and call execute_batch_add() concurrently.
    std::vector<AggFnEvaluator*> _aggregate_evaluators;

    PODArray<AggregateDataPtr> _places;

    /// Pre-grouped row indices by bucket, reused across blocks.
    /// _bucket_row_indices[b] holds row indices that map to bucket b.
    DorisVector<uint32_t> _bucket_row_indices[BUCKETED_AGG_NUM_BUCKETS];

    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;
    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _expr_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;
    RuntimeProfile::Counter* _memory_usage_arena = nullptr;

    /// Peak memory consumed during the last _execute_with_serialized_key call.
    /// Used by get_reserve_mem_size() so the pipeline scheduler can apply
    /// back-pressure before OOM.
    int64_t _memory_usage_last_executing = 0;
};

/// Bucketed hash aggregation sink operator.
/// Fuses local + global aggregation for single-BE deployments.
/// Each pipeline instance builds 256 per-bucket hash tables from raw input.
/// The source operator then merges across instances per-bucket (second-phase agg).
class BucketedAggSinkOperatorX final : public DataSinkOperatorX<BucketedAggSinkLocalState> {
public:
    BucketedAggSinkOperatorX(ObjectPool* pool, int operator_id, int dest_id, const TPlanNode& tnode,
                             const DescriptorTbl& descs);
    ~BucketedAggSinkOperatorX() override = default;

    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink",
                                     DataSinkOperatorX<BucketedAggSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status sink(RuntimeState* state, Block* in_block, bool eos) override;

    // No local exchange needed — each instance builds its own hash tables independently.
    DataDistribution required_data_distribution(RuntimeState* state) const override {
        return DataDistribution(ExchangeType::NOOP);
    }

    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override;

    using DataSinkOperatorX<BucketedAggSinkLocalState>::node_id;
    using DataSinkOperatorX<BucketedAggSinkLocalState>::operator_id;
    using DataSinkOperatorX<BucketedAggSinkLocalState>::get_local_state;

private:
    friend class BucketedAggSinkLocalState;

    std::vector<AggFnEvaluator*> _aggregate_evaluators;

    // Bucketed agg is one-phase: intermediate and output tuples are identical.
    TupleId _tuple_id;
    TupleDescriptor* _intermediate_tuple_desc = nullptr;
    TupleDescriptor* _output_tuple_desc = nullptr;

    size_t _align_aggregate_states = 1;
    Sizes _offsets_of_aggregate_states;
    size_t _total_size_of_aggregate_states = 0;

    VExprContextSPtrs _probe_expr_ctxs;
    ObjectPool* _pool = nullptr;
    std::vector<size_t> _make_nullable_keys;
};

} // namespace doris
