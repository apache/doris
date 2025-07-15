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

#include "operator.h"
#include "runtime_filter/runtime_filter_producer_helper_set.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace vectorized {
template <class HashTableContext, bool is_intersected>
struct HashTableBuild;
}

namespace pipeline {

template <bool is_intersect>
class SetSinkOperatorX;

template <bool is_intersect>
class SetSinkLocalState final : public PipelineXSinkLocalState<SetSharedState> {
public:
    ENABLE_FACTORY_CREATOR(SetSinkLocalState);
    using Base = PipelineXSinkLocalState<SetSharedState>;
    using Parent = SetSinkOperatorX<is_intersect>;

    SetSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state) : Base(parent, state) {
        _finish_dependency = std::make_shared<CountedFinishDependency>(
                parent->operator_id(), parent->node_id(),
                parent->get_name() + "_FINISH_DEPENDENCY");
    }

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status terminate(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

private:
    friend class SetSinkOperatorX<is_intersect>;

    vectorized::MutableBlock _mutable_block;
    // every child has its result expr list
    vectorized::VExprContextSPtrs _child_exprs;
    vectorized::Arena _arena;

    RuntimeProfile::Counter* _merge_block_timer = nullptr;
    RuntimeProfile::Counter* _build_timer = nullptr;

    std::shared_ptr<RuntimeFilterProducerHelperSet> _runtime_filter_producer_helper;
    std::shared_ptr<CountedFinishDependency> _finish_dependency;
};

template <bool is_intersect>
class SetSinkOperatorX final : public DataSinkOperatorX<SetSinkLocalState<is_intersect>> {
public:
    using Base = DataSinkOperatorX<SetSinkLocalState<is_intersect>>;
    using DataSinkOperatorXBase::operator_id;
    using Base::get_local_state;
    using typename Base::LocalState;

    friend class SetSinkLocalState<is_intersect>;
    SetSinkOperatorX(int child_id, int sink_id, int dest_id, ObjectPool* pool,
                     const TPlanNode& tnode, const DescriptorTbl& descs)
            : Base(sink_id, tnode.node_id, dest_id),
              _child_quantity(tnode.node_type == TPlanNodeType::type::INTERSECT_NODE
                                      ? tnode.intersect_node.result_expr_lists.size()
                                      : tnode.except_node.result_expr_lists.size()),
              _is_colocate(is_intersect ? tnode.intersect_node.is_colocate
                                        : tnode.except_node.is_colocate),
              _partition_exprs(is_intersect ? tnode.intersect_node.result_expr_lists[child_id]
                                            : tnode.except_node.result_expr_lists[child_id]),
              _runtime_filter_descs(tnode.runtime_filters) {
        DCHECK_EQ(child_id, _cur_child_id);
        DCHECK_GT(_child_quantity, 1);
    }

#ifdef BE_TEST
    SetSinkOperatorX(int _child_quantity)
            : _cur_child_id(0),
              _child_quantity(_child_quantity),
              _is_colocate(false),
              _partition_exprs() {}
#endif

    ~SetSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink",
                                     DataSinkOperatorX<SetSinkLocalState<is_intersect>>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;
    DataDistribution required_data_distribution() const override {
        return _is_colocate ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                            : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
    }

    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override;

private:
    template <class HashTableContext, bool is_intersected>
    friend struct HashTableBuild;

    Status _process_build_block(SetSinkLocalState<is_intersect>& local_state,
                                vectorized::Block& block, RuntimeState* state);
    Status _extract_build_column(SetSinkLocalState<is_intersect>& local_state,
                                 vectorized::Block& block, vectorized::ColumnRawPtrs& raw_ptrs,
                                 size_t& rows);

    const int _cur_child_id = 0;
    const size_t _child_quantity;
    // every child has its result expr list
    vectorized::VExprContextSPtrs _child_exprs;
    const bool _is_colocate;
    const std::vector<TExpr> _partition_exprs;
    using OperatorBase::_child;

    const std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
};
#include "common/compile_check_end.h"

} // namespace pipeline
} // namespace doris
