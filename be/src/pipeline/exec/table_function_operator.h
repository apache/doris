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
#include "vec/exprs/table_function/table_function.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

class TableFunctionOperatorX;
class TableFunctionLocalState final : public PipelineXLocalState<> {
public:
    using Parent = TableFunctionOperatorX;
    ENABLE_FACTORY_CREATOR(TableFunctionLocalState);
    TableFunctionLocalState(RuntimeState* state, OperatorXBase* parent);
    ~TableFunctionLocalState() override = default;

    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override {
        for (auto* fn : _fns) {
            RETURN_IF_ERROR(fn->close());
        }
        RETURN_IF_ERROR(PipelineXLocalState<>::close(state));
        return Status::OK();
    }
    void process_next_child_row();
    Status get_expanded_block(RuntimeState* state, vectorized::Block* output_block, bool* eos);

private:
    friend class TableFunctionOperatorX;
    friend class StatefulOperatorX<TableFunctionLocalState>;

    void _copy_output_slots(std::vector<vectorized::MutableColumnPtr>& columns);
    bool _roll_table_functions(int last_eos_idx);
    // return:
    //  0: all fns are eos
    // -1: all fns are not eos
    // >0: some of fns are eos
    int _find_last_fn_eos_idx() const;
    bool _is_inner_and_empty();

    std::vector<vectorized::TableFunction*> _fns;
    vectorized::VExprContextSPtrs _vfn_ctxs;
    int64_t _cur_child_offset = -1;
    std::unique_ptr<vectorized::Block> _child_block;
    int _current_row_insert_times = 0;
    bool _child_eos = false;
};

class TableFunctionOperatorX final : public StatefulOperatorX<TableFunctionLocalState> {
public:
    using Base = StatefulOperatorX<TableFunctionLocalState>;
    TableFunctionOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                           const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status open(doris::RuntimeState* state) override;

    bool need_more_input_data(RuntimeState* state) const override {
        auto& local_state = state->get_local_state(operator_id())->cast<TableFunctionLocalState>();
        return !local_state._child_block->rows() && !local_state._child_eos;
    }

    DataDistribution required_data_distribution() const override {
        return {ExchangeType::PASSTHROUGH};
    }

    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) const override {
        auto& local_state = get_local_state(state);
        if (input_block->rows() == 0) {
            return Status::OK();
        }

        for (auto* fn : local_state._fns) {
            RETURN_IF_ERROR(fn->process_init(input_block, state));
        }
        local_state.process_next_child_row();
        return Status::OK();
    }

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) const override {
        auto& local_state = get_local_state(state);
        RETURN_IF_ERROR(local_state.get_expanded_block(state, output_block, eos));
        local_state.reached_limit(output_block, eos);
        return Status::OK();
    }

private:
    friend class TableFunctionLocalState;

    Status _prepare_output_slot_ids(const TPlanNode& tnode);

    /*  Now the output tuples for table function node is base_table_tuple + tf1 + tf2 + ...
        But not all slots are used, the real used slots are inside table_function_node.outputSlotIds.
        For case like explode_bitmap:
            SELECT a2,count(*) as a3 FROM A WHERE a1 IN
                (SELECT c1 FROM B LATERAL VIEW explode_bitmap(b1) C as c1)
            GROUP BY a2 ORDER BY a3;
        Actually we only need to output column c1, no need to output columns in bitmap table B.
        Copy large bitmap columns are very expensive and slow.

    Here we check if the slot is really used, otherwise we avoid copy it and just insert a default value.

                                              A better solution is:
                                              1. FE: create a new output tuple based on the real output slots;
    2. BE: refractor (V)TableFunctionNode output rows based no the new tuple;
    */
    [[nodiscard]] inline bool _slot_need_copy(SlotId slot_id) const {
        auto id = _output_slots[slot_id]->id();
        return (id < _output_slot_ids.size()) && (_output_slot_ids[id]);
    }

    std::vector<SlotDescriptor*> _child_slots;
    std::vector<SlotDescriptor*> _output_slots;

    vectorized::VExprContextSPtrs _vfn_ctxs;

    std::vector<vectorized::TableFunction*> _fns;
    int _fn_num = 0;

    std::vector<bool> _output_slot_ids;
    std::vector<int> _output_slot_indexs;
    std::vector<int> _useless_slot_indexs;

    std::vector<int> _child_slot_sizes;
};

} // namespace doris::pipeline
