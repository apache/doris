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

namespace pipeline {

template <bool is_intersect>
class SetSourceOperatorX;

template <bool is_intersect>
class SetSourceLocalState final : public PipelineXLocalState<SetSharedState> {
public:
    ENABLE_FACTORY_CREATOR(SetSourceLocalState);
    using Base = PipelineXLocalState<SetSharedState>;
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
    using Base::get_local_state;
    using typename Base::LocalState;

    SetSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                       const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs),
              _child_quantity(tnode.node_type == TPlanNodeType::type::INTERSECT_NODE
                                      ? tnode.intersect_node.result_expr_lists.size()
                                      : tnode.except_node.result_expr_lists.size()) {};
    ~SetSourceOperatorX() override = default;

    [[nodiscard]] bool is_source() const override { return true; }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

private:
    friend class SetSourceLocalState<is_intersect>;

    void _create_mutable_cols(SetSourceLocalState<is_intersect>& local_state,
                              vectorized::Block* output_block);

    template <typename HashTableContext>
    Status _get_data_in_hashtable(SetSourceLocalState<is_intersect>& local_state,
                                  HashTableContext& hash_table_ctx, vectorized::Block* output_block,
                                  const int batch_size, bool* eos);

    void _add_result_columns(SetSourceLocalState<is_intersect>& local_state,
                             RowRefListWithFlags& value, int& block_size);
    const int _child_quantity;
};

} // namespace pipeline
} // namespace doris
