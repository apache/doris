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

#include "common/status.h"
#include "pipeline/exec/operator.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

class RecCTEScanOperatorX;
class RecCTEScanLocalState final : public PipelineXLocalState<> {
public:
    ENABLE_FACTORY_CREATOR(RecCTEScanLocalState);

    RecCTEScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<>(state, parent) {}
    ~RecCTEScanLocalState() override = default;

private:
    friend class RecCTEScanOperatorX;
    std::vector<vectorized::Block> _blocks;
};

class RecCTEScanOperatorX final : public OperatorX<RecCTEScanLocalState> {
public:
    RecCTEScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                        const DescriptorTbl& descs)
            : OperatorX<RecCTEScanLocalState>(pool, tnode, operator_id, descs) {}

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        auto& local_state = get_local_state(state);
        if (local_state._blocks.empty()) {
            *eos = true;
            return Status::OK();
        }
        *block = std::move(local_state._blocks.back());
        local_state._blocks.pop_back();
        *eos = false;
        return Status::OK();
    }

    bool is_source() const override { return true; }
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline