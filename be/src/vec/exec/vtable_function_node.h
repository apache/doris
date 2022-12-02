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

#include "exec/table_function_node.h"
#include "exprs/table_function/table_function.h"

namespace doris::vectorized {

class VTableFunctionNode : public TableFunctionNode {
public:
    VTableFunctionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VTableFunctionNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    bool need_more_input_data() { return !_child_block || !_child_block->rows(); }

    Status push(RuntimeState*, vectorized::Block* input_block, bool eos) override {
        if (eos) {
            return Status::OK();
        }

        if (input_block != _child_block.get()) {
            _child_block.reset(input_block);
        }
        for (TableFunction* fn : _fns) {
            RETURN_IF_ERROR(fn->process_init(_child_block.get()));
        }
        RETURN_IF_ERROR(_process_next_child_row());
        return Status::OK();
    }

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override {
        RETURN_IF_ERROR(get_expanded_block(state, output_block, eos));
        reached_limit(output_block, eos);
        return Status::OK();
    }

private:
    Status _process_next_child_row() override;

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
    inline bool slot_need_copy(SlotId slot_id) const {
        auto id = _output_slots[slot_id]->id();
        return (id < _output_slot_ids.size()) && (_output_slot_ids[id]);
    }

    using TableFunctionNode::get_next;

    Status get_expanded_block(RuntimeState* state, Block* output_block, bool* eos);

    std::unique_ptr<Block> _child_block;
    std::vector<SlotDescriptor*> _child_slots;
    std::vector<SlotDescriptor*> _output_slots;
};

} // namespace doris::vectorized
