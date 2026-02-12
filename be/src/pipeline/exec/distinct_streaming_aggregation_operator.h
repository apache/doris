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

#include <cstdint>
#include <memory>

#include "common/status.h"
#include "pipeline/common/distinct_agg_utils.h"
#include "pipeline/exec/operator.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {
#include "common/compile_check_begin.h"
class DistinctStreamingAggOperatorX;

class DistinctStreamingAggLocalState final : public PipelineXLocalState<FakeSharedState> {
public:
    using Parent = DistinctStreamingAggOperatorX;
    using Base = PipelineXLocalState<FakeSharedState>;
    ENABLE_FACTORY_CREATOR(DistinctStreamingAggLocalState);
    DistinctStreamingAggLocalState(RuntimeState* state, OperatorXBase* parent);

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

private:
    friend class DistinctStreamingAggOperatorX;
    template <typename LocalStateType>
    friend class StatefulOperatorX;
    Status _distinct_pre_agg_with_serialized_key(vectorized::Block* in_block,
                                                 vectorized::Block* out_block);
    Status _init_hash_method(const vectorized::VExprContextSPtrs& probe_exprs);
    void _emplace_into_hash_table_to_distinct(vectorized::IColumn::Selector& distinct_row,
                                              vectorized::ColumnRawPtrs& key_columns,
                                              const uint32_t num_rows);
    void _make_nullable_output_key(vectorized::Block* block);
    bool _should_expand_preagg_hash_tables();

    void _swap_cache_block(vectorized::Block* block) {
        DCHECK(!_cache_block.is_empty_column());
        block->swap(_cache_block);
        _cache_block = block->clone_empty();
    }

    vectorized::IColumn::Selector _distinct_row;
    vectorized::Arena _arena;
    size_t _input_num_rows = 0;
    bool _should_expand_hash_table = true;
    bool _stop_emplace_flag = false;
    const int batch_size;
    std::unique_ptr<DistinctDataVariants> _agg_data = nullptr;
    // group by k1,k2
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    std::unique_ptr<vectorized::Block> _child_block = nullptr;
    bool _child_eos = false;
    bool _reach_limit = false;
    std::unique_ptr<vectorized::Block> _aggregated_block = nullptr;
    vectorized::Block _cache_block;
    RuntimeProfile::Counter* _build_timer = nullptr;
    RuntimeProfile::Counter* _expr_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;
    RuntimeProfile::Counter* _hash_table_size_counter = nullptr;
    RuntimeProfile::Counter* _insert_keys_to_column_timer = nullptr;
};

class DistinctStreamingAggOperatorX final
        : public StatefulOperatorX<DistinctStreamingAggLocalState> {
public:
    DistinctStreamingAggOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                  const DescriptorTbl& descs);
#ifdef BE_TEST
    DistinctStreamingAggOperatorX()
            : _needs_finalize(false),
              _is_first_phase(true),
              _partition_exprs({}),
              _is_colocate(false) {}
#endif

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    void update_operator(const TPlanNode& tnode, bool followed_by_shuffled_operator,
                         bool require_bucket_distribution) override {
        _followed_by_shuffled_operator = followed_by_shuffled_operator;
        _require_bucket_distribution = require_bucket_distribution;
        _partition_exprs = tnode.__isset.distribute_expr_lists && _followed_by_shuffled_operator
                                   ? tnode.distribute_expr_lists[0]
                                   : tnode.agg_node.grouping_exprs;
    }
    Status prepare(RuntimeState* state) override;
    Status pull(RuntimeState* state, vectorized::Block* block, bool* eos) const override;
    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) const override;
    bool need_more_input_data(RuntimeState* state) const override;

    DataDistribution required_data_distribution(RuntimeState* state) const override {
        if (_needs_finalize && _probe_expr_ctxs.empty()) {
            return {ExchangeType::NOOP};
        }
        if (_needs_finalize || (!_probe_expr_ctxs.empty() && !_is_streaming_preagg)) {
            return _is_colocate && _require_bucket_distribution
                           ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                           : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
        }
        if (state->enable_distinct_streaming_agg_force_passthrough()) {
            return {ExchangeType::PASSTHROUGH};
        } else {
            return StatefulOperatorX<DistinctStreamingAggLocalState>::required_data_distribution(
                    state);
        }
    }

    bool is_colocated_operator() const override { return _is_colocate; }

private:
    friend class DistinctStreamingAggLocalState;
    void init_make_nullable(RuntimeState* state);
    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;
    const bool _needs_finalize;
    const bool _is_first_phase;
    std::vector<TExpr> _partition_exprs;
    const bool _is_colocate;
    // group by k1,k2
    vectorized::VExprContextSPtrs _probe_expr_ctxs;
    std::vector<size_t> _make_nullable_keys;

    // If _is_streaming_preagg = true, deduplication will be abandoned in cases where the deduplication rate is low.
    bool _is_streaming_preagg = false;
};

} // namespace pipeline
} // namespace doris
#include "common/compile_check_end.h"