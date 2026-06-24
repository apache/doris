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

#include <gen_cpp/PlanNodes_types.h>

#include <memory>
#include <vector>

#include "core/arena.h"
#include "core/column/column_vector.h"
#include "exec/common/groupjoin_utils.h"
#include "exec/operator/operator.h"
#include "exec/pipeline/dependency.h"

namespace doris {

class AggFnEvaluator;
class GroupJoinProbeLocalState;
class GroupJoinProbeOperatorX;
namespace groupjoin {
Status drain_groupjoin_result(GroupJoinSharedState* shared_state, size_t batch_size,
                              GroupJoinProbeLocalState& local_state, MutableColumns& key_columns,
                              MutableColumns& value_columns, bool& output_eos);
}

class GroupJoinProbeLocalState final : public PipelineXLocalState<GroupJoinSharedState> {
public:
    ENABLE_FACTORY_CREATOR(GroupJoinProbeLocalState);
    using Base = PipelineXLocalState<GroupJoinSharedState>;
    using Parent = GroupJoinProbeOperatorX;

    GroupJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {}
    ~GroupJoinProbeLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;

private:
    friend class GroupJoinProbeOperatorX;
    friend Status groupjoin::drain_groupjoin_result(GroupJoinSharedState*, size_t,
                                                    GroupJoinProbeLocalState&, MutableColumns&,
                                                    MutableColumns&, bool&);
    template <typename LocalStateType>
    friend class StatefulOperatorX;

    std::unique_ptr<Block> _child_block = Block::create_unique();
    bool _child_eos = false;
    VExprContextSPtrs _probe_expr_ctxs;
    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    ColumnRawPtrs _probe_key_not_nullable_columns;
    std::vector<ColumnPtr> _key_columns_holder;
    std::vector<AggregateDataPtr> _places;
    std::vector<AggregateDataPtr> _values;
    std::vector<GroupJoinEntry*> _entries;
    std::vector<uint64_t> _repeats;
    Arena _output_arena;
    ColumnUInt8::MutablePtr _null_map_column;
};

class GroupJoinProbeOperatorX final : public StatefulOperatorX<GroupJoinProbeLocalState> {
public:
    using Base = StatefulOperatorX<GroupJoinProbeLocalState>;

    GroupJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                            const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status push(RuntimeState* state, Block* input_block, bool eos) const override;
    Status pull(RuntimeState* state, Block* output_block, bool* eos) const override;
    bool need_more_input_data(RuntimeState* state) const override;
    [[nodiscard]] const RowDescriptor& row_desc() const override {
        DORIS_CHECK(_output_row_desc != nullptr);
        return *_output_row_desc;
    }
    Status set_child(OperatorPtr child) override {
        if (Base::_child && _build_side_child == nullptr) {
            // The second child of a binary GroupJoin plan is the build side. Keep the first child
            // as the probe input in this pipeline, same as HashJoinProbeOperatorX.
            _build_side_child = child;
        } else {
            RETURN_IF_ERROR(Base::set_child(child));
        }
        return Status::OK();
    }

    DataDistribution required_data_distribution(RuntimeState* state) const override;
    bool is_shuffled_operator() const override;
    bool is_colocated_operator() const override;
    bool followed_by_shuffled_operator() const override;

private:
    friend class GroupJoinProbeLocalState;

    const TJoinDistributionType::type _join_distribution;
    std::vector<TExpr> _partition_exprs;
    VExprContextSPtrs _probe_expr_ctxs;
    std::vector<AggFnEvaluator*> _aggregate_evaluators;
    std::vector<int> _aggregate_indices;
    std::vector<TGroupJoinAggSide::type> _aggregate_sides;
    Sizes _sizes_of_aggregate_states;
    Sizes _aligns_of_aggregate_states;
    std::vector<size_t> _make_nullable_keys;
    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;
    std::unique_ptr<RowDescriptor> _output_row_desc;
    OperatorPtr _build_side_child = nullptr;
};

} // namespace doris
