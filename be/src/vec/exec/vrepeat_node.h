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

#include <iosfwd>
#include <memory>
#include <set>
#include <vector>

#include "common/global_types.h"
#include "exec/exec_node.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {

class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RuntimeState;
class Status;
class SlotDescriptor;
class TupleDescriptor;

namespace vectorized {

class VRepeatNode : public ExecNode {
public:
    VRepeatNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VRepeatNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status alloc_resource(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;
    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) override;
    bool need_more_input_data() const;
    std::shared_ptr<Block> get_child_block() { return _child_block; }

    void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    Status get_repeated_block(Block* child_block, int repeat_id_idx, Block* output_block);

    // Slot id set used to indicate those slots need to set to null.
    std::vector<std::set<SlotId>> _slot_id_set_list;
    // all slot id
    std::set<SlotId> _all_slot_ids;
    // An integer bitmap list, it indicates the bit position of the exprs not null.
    std::vector<int64_t> _repeat_id_list;
    std::vector<std::vector<int64_t>> _grouping_list;
    TupleId _output_tuple_id;
    const TupleDescriptor* _output_tuple_desc;

    std::shared_ptr<Block> _child_block;
    std::unique_ptr<Block> _intermediate_block {};

    std::vector<SlotDescriptor*> _output_slots;

    VExprContextSPtrs _expr_ctxs;
    bool _child_eos;
    int _repeat_id_idx;
};
} // namespace vectorized
} // namespace doris
