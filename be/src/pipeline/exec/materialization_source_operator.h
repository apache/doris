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

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;

private:
    friend class MaterializationSourceOperatorX;
    friend class OperatorX<MaterializationSourceLocalState>;
};

class MaterializationSourceOperatorX final : public OperatorX<MaterializationSourceLocalState> {
public:
    using Base = OperatorX<MaterializationSourceLocalState>;
    MaterializationSourceOperatorX(ObjectPool* pool, int plan_node_id, int operator_id)
            : Base(pool, plan_node_id, operator_id) {
        _op_name = "MATERIALIZATION_SOURCE_OPERATOR";
    };
    ~MaterializationSourceOperatorX() override = default;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    const RowDescriptor& intermediate_row_desc() const override {
        return _child->intermediate_row_desc();
    }
    RowDescriptor& row_descriptor() override { return _child->row_descriptor(); }
    const RowDescriptor& row_desc() const override { return _child->row_desc(); }

private:
    bool _has_data(RuntimeState* state) const { auto& local_state = get_local_state(state); }
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris