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

#include <memory>

#include "common/status.h"
#include "operator.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

class MaterializationSourceOperatorX;
class MaterializationSourceLocalState final
        : public PipelineXLocalState<MaterializationSharedState> {
public:
    ENABLE_FACTORY_CREATOR(MaterializationSourceLocalState);
    using Base = PipelineXLocalState<MaterializationSharedState>;
    using Parent = MaterializationSourceOperatorX;
    MaterializationSourceLocalState(RuntimeState* state, OperatorXBase* parent)
            : Base(state, parent) {};

    Status init(doris::RuntimeState* state, doris::pipeline::LocalStateInfo& info) override {
        RETURN_IF_ERROR(Base::init(state, info));
        _max_rpc_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "MaxRpcTime", 2);
        _merge_response_timer = ADD_TIMER_WITH_LEVEL(_runtime_profile, "MergeResponseTime", 2);
        return Status::OK();
    }

private:
    RuntimeProfile::Counter* _max_rpc_timer = nullptr;
    RuntimeProfile::Counter* _merge_response_timer = nullptr;

    friend class MaterializationSourceOperatorX;
    friend class OperatorX<MaterializationSourceLocalState>;
};

class MaterializationSourceOperatorX final : public OperatorX<MaterializationSourceLocalState> {
public:
    using Base = OperatorX<MaterializationSourceLocalState>;
    MaterializationSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
                                   const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs) {};
    ~MaterializationSourceOperatorX() override = default;

    Status get_block(doris::RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris