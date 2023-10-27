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
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vset_operation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

template <bool is_intersect>
class SetSourceOperatorBuilder
        : public OperatorBuilder<vectorized::VSetOperationNode<is_intersect>> {
private:
    constexpr static auto builder_name =
            is_intersect ? "IntersectSourceOperator" : "ExceptSourceOperator";

public:
    SetSourceOperatorBuilder(int32_t id, ExecNode* set_node);
    [[nodiscard]] bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

template <bool is_intersect>
class SetSourceOperator : public SourceOperator<SetSourceOperatorBuilder<is_intersect>> {
public:
    SetSourceOperator(OperatorBuilderBase* builder,
                      vectorized::VSetOperationNode<is_intersect>* set_node);

    Status open(RuntimeState* /*state*/) override { return Status::OK(); }
};

template <bool is_intersect>
class SetSourceOperatorX;

template <bool is_intersect>
class SetSourceLocalState final : public PipelineXLocalState<SetDependency> {
public:
    ENABLE_FACTORY_CREATOR(SetSourceLocalState);
    using Base = PipelineXLocalState<SetDependency>;
    using Parent = SetSourceOperatorX<is_intersect>;
    SetSourceLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {};
    Status init(RuntimeState* state, LocalStateInfo& infos) override;
    Status open(RuntimeState* state) override;

private:
    friend class SetSourceOperatorX<is_intersect>;
    friend class OperatorX<SetSourceLocalState<is_intersect>>;
    std::vector<vectorized::MutableColumnPtr> _mutable_cols;
    //record build column type
    vectorized::DataTypes _left_table_data_types;
};

template <bool is_intersect>
class SetSourceOperatorX final : public OperatorX<SetSourceLocalState<is_intersect>> {
public:
    using Base = OperatorX<SetSourceLocalState<is_intersect>>;
    // for non-delay tempalte instantiation
    using OperatorXBase::operator_id;
    using typename Base::LocalState;

    SetSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                       const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs) {};
    ~SetSourceOperatorX() override = default;

    [[nodiscard]] bool is_source() const override { return true; }

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

private:
    friend class SetSourceLocalState<is_intersect>;

    void _create_mutable_cols(SetSourceLocalState<is_intersect>& local_state,
                              vectorized::Block* output_block);

    template <typename HashTableContext>
    Status _get_data_in_hashtable(SetSourceLocalState<is_intersect>& local_state,
                                  HashTableContext& hash_table_ctx, vectorized::Block* output_block,
                                  const int batch_size, SourceState& source_state);

    void _add_result_columns(SetSourceLocalState<is_intersect>& local_state,
                             vectorized::RowRefListWithFlags& value, int& block_size);
};

} // namespace pipeline
} // namespace doris
