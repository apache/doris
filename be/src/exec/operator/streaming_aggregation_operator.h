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

#include <memory>

#include "common/status.h"
#include "core/block/block.h"
#include "exec/operator/operator.h"
#include "runtime/runtime_profile.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

class StreamingAggOperatorX;
class GroupByAggContext;

class StreamingAggLocalState MOCK_REMOVE(final) : public PipelineXLocalState<FakeSharedState> {
public:
    using Parent = StreamingAggOperatorX;
    using Base = PipelineXLocalState<FakeSharedState>;
    ENABLE_FACTORY_CREATOR(StreamingAggLocalState);
    StreamingAggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~StreamingAggLocalState() override;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status do_pre_agg(RuntimeState* state, Block* input_block, Block* output_block);
    void make_nullable_output_key(Block* block);

private:
    friend class StreamingAggOperatorX;
    template <typename LocalStateType>
    friend class StatefulOperatorX;

    Status _pre_agg_with_serialized_key(doris::Block* in_block, doris::Block* out_block);

    RuntimeProfile::Counter* _streaming_agg_timer = nullptr;

    int64_t _cur_num_rows_returned = 0;
    size_t _input_num_rows = 0;

    std::unique_ptr<GroupByAggContext> _groupby_agg_ctx;

    // Sort limit: tracks whether sort limit filtering has been activated.
    // -1 = not yet determined, 0 = no, 1 = yes
    int _need_do_sort_limit = -1;

    std::unique_ptr<Block> _child_block = nullptr;
    bool _child_eos = false;
    std::unique_ptr<Block> _pre_aggregated_block = nullptr;

    bool _is_single_backend = false;
};

class StreamingAggOperatorX MOCK_REMOVE(final) : public StatefulOperatorX<StreamingAggLocalState> {
public:
    StreamingAggOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                          const DescriptorTbl& descs);
#ifdef BE_TEST
    StreamingAggOperatorX() : _is_first_phase {false} {}
#endif

    ~StreamingAggOperatorX() override = default;
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    void update_operator(const TPlanNode& tnode, bool followed_by_shuffled_operator,
                         bool require_bucket_distribution) override;
    Status prepare(RuntimeState* state) override;
    Status pull(RuntimeState* state, Block* block, bool* eos) const override;
    Status push(RuntimeState* state, Block* input_block, bool eos) const override;
    bool need_more_input_data(RuntimeState* state) const override;
    void set_low_memory_mode(RuntimeState* state) override {
        _spill_streaming_agg_mem_limit = 1024 * 1024;
    }
    DataDistribution required_data_distribution(RuntimeState* state) const override {
        if (_child && _child->is_hash_join_probe() &&
            state->enable_streaming_agg_hash_join_force_passthrough()) {
            return DataDistribution(ExchangeType::PASSTHROUGH);
        }
        if (!state->get_query_ctx()->should_be_shuffled_agg(
                    StatefulOperatorX<StreamingAggLocalState>::node_id())) {
            return StatefulOperatorX<StreamingAggLocalState>::required_data_distribution(state);
        }
        if (_partition_exprs.empty()) {
            return _needs_finalize
                           ? DataDistribution(ExchangeType::NOOP)
                           : StatefulOperatorX<StreamingAggLocalState>::required_data_distribution(
                                     state);
        }
        return DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
    }

private:
    friend class StreamingAggLocalState;

    MOCK_FUNCTION Status _init_probe_expr_ctx(RuntimeState* state);

    MOCK_FUNCTION Status _init_aggregate_evaluators(RuntimeState* state);

    MOCK_FUNCTION Status _calc_aggregate_evaluators();

    // may be we don't have to know the tuple id
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc = nullptr;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;
    bool _needs_finalize;
    const bool _is_first_phase;
    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    Sizes _offsets_of_aggregate_states;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;

    /// When spilling is enabled, the streaming agg should not occupy too much memory.
    size_t _spill_streaming_agg_mem_limit;
    // group by k1,k2
    VExprContextSPtrs _probe_expr_ctxs;
    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    bool _can_short_circuit = false;
    std::vector<size_t> _make_nullable_keys;
    RowDescriptor _agg_fn_output_row_descriptor;

    // For sort limit
    bool _do_sort_limit = false;
    int64_t _sort_limit = -1;
    std::vector<int> _order_directions;
    std::vector<int> _null_directions;

    std::vector<TExpr> _partition_exprs;
};

#include "common/compile_check_end.h"
} // namespace doris
