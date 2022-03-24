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

namespace doris::vectorized {

class VTableFunctionNode : public TableFunctionNode {
public:
    VTableFunctionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VTableFunctionNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

private:
    Status _process_next_child_row() override;

    using TableFunctionNode::get_next;

    Status get_expanded_block(RuntimeState* state, Block* output_block, bool* eos);

    std::unique_ptr<Block> _child_block;
    std::vector<SlotDescriptor*> _child_slots;
    std::vector<SlotDescriptor*> _output_slots;
};

} // namespace doris::vectorized