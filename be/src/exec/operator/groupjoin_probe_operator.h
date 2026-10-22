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

#include "core/arena.h"
#include "core/custom_allocator.h"
#include "exec/operator/groupjoin_shared_state.h"
#include "exec/operator/operator.h"

namespace doris {
class AggFnEvaluator;
class GroupJoinProbeOperatorX;

class GroupJoinProbeLocalState final : public PipelineXLocalState<GroupJoinSharedState> {
public:
    ENABLE_FACTORY_CREATOR(GroupJoinProbeLocalState);
    using Base = PipelineXLocalState<GroupJoinSharedState>;
    using Parent = GroupJoinProbeOperatorX;
    GroupJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent);
    ~GroupJoinProbeLocalState() override;
    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

private:
    friend class GroupJoinProbeOperatorX;
    template <typename LocalStateType>
    friend class StatefulOperatorX;
    void _release_probe_resources();
    void _update_memory_usage();

    std::unique_ptr<Block> _child_block = Block::create_unique();
    bool _child_eos = false;
    bool _probe_eos = false;
    VExprContextSPtrs _key_exprs;
    std::vector<AggFnEvaluator*> _evaluators;
    // Build arguments live until probe EOS; probe arguments live for one input batch.
    std::vector<std::vector<ColumnPtr>> _build_arguments;
    std::vector<std::vector<ColumnPtr>> _probe_arguments;
    // Borrowed from the argument holders above; build entries persist across input batches.
    std::vector<ColumnRawPtrs> _argument_columns;
    std::unique_ptr<JoinDataVariants> _probe_context;
    DorisVector<AggregateDataPtr> _match_places;
    DorisVector<uint32_t> _match_build_rows;
    DorisVector<uint32_t> _match_probe_rows;
    // Maps a representative build row directly to its aggregate state. A null entry means that
    // this build row has not been selected as a matched group representative.
    DorisVector<AggregateDataPtr> _state_by_build_row;
    // Parallel arrays: each output group has a representative build row and an aggregate state.
    DorisVector<uint32_t> _group_build_rows;
    DorisVector<AggregateDataPtr> _group_states;
    std::vector<AggregateDataPtr> _result_places;
    bool _initialized = false;
    Arena _arena;
    size_t _drain_position = 0;
    RuntimeProfile::Counter* _match_timer = nullptr;
    RuntimeProfile::Counter* _update_timer = nullptr;
    RuntimeProfile::Counter* _drain_timer = nullptr;
    RuntimeProfile::Counter* _matched_rows = nullptr;
    RuntimeProfile::Counter* _group_count = nullptr;
};

class GroupJoinProbeOperatorX final : public StatefulOperatorX<GroupJoinProbeLocalState> {
public:
    using Base = StatefulOperatorX<GroupJoinProbeLocalState>;
    GroupJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                            const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status push(RuntimeState* state, Block* block, bool eos) const override;
    Status pull(RuntimeState* state, Block* block, bool* eos) const override;
    bool need_more_input_data(RuntimeState* state) const override;
    Status set_child(OperatorPtr child) override;
    size_t get_reserve_mem_size(RuntimeState* state) override;
    DataDistribution required_data_distribution(RuntimeState* state) const override;
    bool is_shuffled_operator() const override;
    bool is_colocated_operator() const override;
    bool followed_by_shuffled_operator() const override;

private:
    friend class GroupJoinProbeLocalState;
    AggregateDataPtr _create_group_state(GroupJoinProbeLocalState& local, uint32_t head) const;
    Status _prepare_evaluator(RuntimeState* state, size_t index);
    Status _initialize_probe(GroupJoinProbeLocalState& local) const;
    Status _prepare_probe(Block& block, GroupJoinProbeLocalState& local) const;
    Block _make_joined_block(const Block& probe, const GroupJoinProbeLocalState& local,
                             size_t matches) const;
    Status _aggregate_batch(Block& probe, GroupJoinProbeLocalState& local, size_t matches) const;
    void _assign_group_states(GroupJoinProbeLocalState& local, const uint32_t* representatives,
                              size_t matches, uint32_t& previous_head,
                              AggregateDataPtr& previous_place) const;
    Status _aggregate_matches(RuntimeState* state, Block* block,
                              GroupJoinProbeLocalState& local) const;
    VExprContextSPtrs _key_exprs;
    RowDescriptor _joined_row_desc;
    bool _has_joined_arguments = false;
    std::vector<uint8_t> _joined_columns;
    std::vector<AggFnEvaluator*> _evaluators;
    std::vector<TGroupJoinAggSide::type> _sides;
    std::vector<size_t> _offsets;
    size_t _states_size = 0;
    size_t _states_alignment = 1;
    TupleId _output_tuple_id;
    OperatorPtr _build_side_child;
    TJoinDistributionType::type _distribution;
    std::vector<TExpr> _partition_exprs;
};

extern template class StatefulOperatorX<GroupJoinProbeLocalState>;
extern template class PipelineXLocalState<GroupJoinSharedState>;

} // namespace doris
