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
#include "exec/exec_node.h"

namespace doris {
namespace vectorized {

class VSelectNode final : public ExecNode {
public:
    VSelectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;
    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;

private:
    // true if last get_next() call on child signalled eos
    bool _child_eos;
};
} // namespace vectorized
} // namespace doris