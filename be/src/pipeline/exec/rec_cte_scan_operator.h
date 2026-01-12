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
            : PipelineXLocalState<>(state, parent) {
        _scan_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                     _parent->get_name() + "_DEPENDENCY");
        state->get_query_ctx()->registe_cte_scan(state->fragment_instance_id(), parent->node_id(),
                                                 this);
    }
    ~RecCTEScanLocalState() override {
        state()->get_query_ctx()->deregiste_cte_scan(state()->fragment_instance_id(),
                                                     parent()->node_id());
    }

    Status add_block(const PBlock& pblock) {
        vectorized::Block block;
        size_t uncompressed_bytes;
        int64_t decompress_time;
        RETURN_IF_ERROR(block.deserialize(pblock, &uncompressed_bytes, &decompress_time));
        _blocks.emplace_back(std::move(block));
        return Status::OK();
    }

    void set_ready() { _scan_dependency->set_ready(); }

    std::vector<Dependency*> dependencies() const override { return {_scan_dependency.get()}; }

private:
    friend class RecCTEScanOperatorX;
    std::vector<vectorized::Block> _blocks;
    DependencySPtr _scan_dependency = nullptr;
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
        RETURN_IF_ERROR(local_state.filter_block(local_state.conjuncts(), block, block->columns()));
        local_state._blocks.pop_back();
        return Status::OK();
    }

    bool is_source() const override { return true; }
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline