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
#include "pipeline/exec/operator.h"

namespace doris {
class RuntimeState;

namespace pipeline {

class RepeatOperatorX;

class RepeatLocalState final : public PipelineXLocalState<FakeSharedState> {
public:
    ENABLE_FACTORY_CREATOR(RepeatLocalState);
    using Parent = RepeatOperatorX;
    using Base = PipelineXLocalState<FakeSharedState>;
    RepeatLocalState(RuntimeState* state, OperatorXBase* parent);

    Status open(RuntimeState* state) override;

    Status get_repeated_block(vectorized::Block* child_block, int repeat_id_idx,
                              vectorized::Block* output_block);

    Status add_grouping_id_column(std::size_t rows, std::size_t& cur_col,
                                  vectorized::MutableColumns& columns, int repeat_id_idx);

private:
    friend class RepeatOperatorX;
    template <typename LocalStateType>
    friend class StatefulOperatorX;
    std::unique_ptr<vectorized::Block> _child_block;
    bool _child_eos = false;
    int _repeat_id_idx;
    std::unique_ptr<vectorized::Block> _intermediate_block;
    vectorized::VExprContextSPtrs _expr_ctxs;
};

class RepeatOperatorX final : public StatefulOperatorX<RepeatLocalState> {
public:
    using Base = StatefulOperatorX<RepeatLocalState>;
    RepeatOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                    const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    bool need_more_input_data(RuntimeState* state) const override;
    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) const override;
    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) const override;

private:
    friend class RepeatLocalState;

    // Slot id set used to indicate those slots need to set to null.
    std::vector<std::set<SlotId>> _slot_id_set_list;
    // all slot id
    std::set<SlotId> _all_slot_ids;
    // An integer bitmap list, it indicates the bit position of the exprs not null.
    std::vector<int64_t> _repeat_id_list;
    std::vector<std::vector<int64_t>> _grouping_list;
    TupleId _output_tuple_id;
    const TupleDescriptor* _output_tuple_desc = nullptr;

    mutable std::vector<SlotDescriptor*> _output_slots;

    vectorized::VExprContextSPtrs _expr_ctxs;
};

} // namespace pipeline
} // namespace doris
