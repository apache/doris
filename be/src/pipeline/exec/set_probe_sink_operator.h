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

#include "common/status.h"
#include "operator.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
template <class HashTableContext, bool is_intersected>
struct HashTableProbe;
} // namespace vectorized

namespace pipeline {

template <bool is_intersect>
class SetProbeSinkOperatorX;

template <bool is_intersect>
class SetProbeSinkLocalState final : public PipelineXSinkLocalState<SetSharedState> {
public:
    ENABLE_FACTORY_CREATOR(SetProbeSinkLocalState);
    using Base = PipelineXSinkLocalState<SetSharedState>;
    using Parent = SetProbeSinkOperatorX<is_intersect>;

    SetProbeSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    int64_t* valid_element_in_hash_tbl() { return &_shared_state->valid_element_in_hash_tbl; }

private:
    friend class SetProbeSinkOperatorX<is_intersect>;
    template <class HashTableContext, bool is_intersected>
    friend struct vectorized::HashTableProbe;

    //record insert column id during probe
    std::vector<uint16_t> _probe_column_inserted_id;
    vectorized::ColumnRawPtrs _probe_columns;
    // every child has its result expr list
    vectorized::VExprContextSPtrs _child_exprs;
};

template <bool is_intersect>
class SetProbeSinkOperatorX final : public DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>> {
public:
    using Base = DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>>;
    using DataSinkOperatorXBase::operator_id;
    using Base::get_local_state;
    using typename Base::LocalState;

    friend class SetProbeSinkLocalState<is_intersect>;
    SetProbeSinkOperatorX(int child_id, int sink_id, ObjectPool* pool, const TPlanNode& tnode,
                          const DescriptorTbl& descs)
            : Base(sink_id, tnode.node_id, tnode.node_id),
              _cur_child_id(child_id),
              _is_colocate(is_intersect ? tnode.intersect_node.is_colocate
                                        : tnode.except_node.is_colocate),
              _partition_exprs(is_intersect ? tnode.intersect_node.result_expr_lists[child_id]
                                            : tnode.except_node.result_expr_lists[child_id]) {}
    ~SetProbeSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError(
                "{} should not init with TDataSink",
                DataSinkOperatorX<SetProbeSinkLocalState<is_intersect>>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;
    DataDistribution required_data_distribution() const override {
        return _is_colocate ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                            : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
    }

    bool require_shuffled_data_distribution() const override { return true; }

    std::shared_ptr<BasicSharedState> create_shared_state() const override { return nullptr; }

private:
    void _finalize_probe(SetProbeSinkLocalState<is_intersect>& local_state);
    Status _extract_probe_column(SetProbeSinkLocalState<is_intersect>& local_state,
                                 vectorized::Block& block, vectorized::ColumnRawPtrs& raw_ptrs,
                                 int child_id);
    void _refresh_hash_table(SetProbeSinkLocalState<is_intersect>& local_state);
    const int _cur_child_id;
    // every child has its result expr list
    vectorized::VExprContextSPtrs _child_exprs;
    const bool _is_colocate;
    const std::vector<TExpr> _partition_exprs;
    using OperatorBase::_child;
};

} // namespace pipeline
} // namespace doris
