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
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vrepeat_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

class RepeatOperatorBuilder final : public OperatorBuilder<vectorized::VRepeatNode> {
public:
    RepeatOperatorBuilder(int32_t id, ExecNode* repeat_node);

    OperatorPtr build_operator() override;
};

class RepeatOperator final : public StatefulOperator<RepeatOperatorBuilder> {
public:
    RepeatOperator(OperatorBuilderBase* operator_builder, ExecNode* repeat_node);

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;
};

class RepeatLocalState final : public PipelineXLocalState<FakeDependency> {
public:
    ENABLE_FACTORY_CREATOR(RepeatLocalState);
    using Base = PipelineXLocalState<FakeDependency>;
    RepeatLocalState(RuntimeState* state, OperatorXBase* parent);

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

private:
    friend class RepeatOperatorX;
    std::unique_ptr<vectorized::Block> _child_block;
    SourceState _child_source_state;
    bool _child_eos;
    int _repeat_id_idx;
    std::unique_ptr<vectorized::Block> _intermediate_block {};
};
class RepeatOperatorX final : public OperatorXBase {
public:
    RepeatOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override;
    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;
    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

private:
    friend class RepeatLocalState;
    Status get_repeated_block(vectorized::Block* child_block, int repeat_id_idx,
                              vectorized::Block* output_block);
    bool need_more_input_data(RuntimeState* state) const;

    Status pull(RuntimeState* state, vectorized::Block* output_block, SourceState& source_state);
    Status push(RuntimeState* state, vectorized::Block* input_block, SourceState& source_state);
    // Slot id set used to indicate those slots need to set to null.
    std::vector<std::set<SlotId>> _slot_id_set_list;
    // all slot id
    std::set<SlotId> _all_slot_ids;
    // An integer bitmap list, it indicates the bit position of the exprs not null.
    std::vector<int64_t> _repeat_id_list;
    std::vector<std::vector<int64_t>> _grouping_list;
    TupleId _output_tuple_id;
    const TupleDescriptor* _output_tuple_desc;

    std::vector<SlotDescriptor*> _output_slots;

    vectorized::VExprContextSPtrs _expr_ctxs;
};

} // namespace pipeline
} // namespace doris
