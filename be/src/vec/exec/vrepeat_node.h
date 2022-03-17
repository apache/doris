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

#include "exec/repeat_node.h"

namespace doris {

class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RuntimeState;
class Status;

namespace vectorized {
class VRepeatNode : public RepeatNode {
public:
    VRepeatNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VRepeatNode() override = default;

    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;
    virtual Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    virtual Status close(RuntimeState* state) override;

protected:
    virtual void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    using RepeatNode::get_next;
    Status get_repeated_block(Block* child_block, int repeat_id_idx, Block* output_block);

    std::unique_ptr<Block> _child_block;
    std::vector<SlotDescriptor*> _child_slots;
    std::vector<SlotDescriptor*> _output_slots;

    // _virtual_tuple_id id used for GROUPING_ID().
    TupleId _virtual_tuple_id;
    const TupleDescriptor* _virtual_tuple_desc;
};
} // namespace vectorized
} // namespace doris
