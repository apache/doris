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

class MaterializationSinkOperatorX;
class MaterializationSinkLocalState final
        : public PipelineXSinkLocalState<MaterializationSharedState> {
public:
    ENABLE_FACTORY_CREATOR(MaterializationSinkLocalState);
    MaterializationSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}

private:
    friend class MaterializationSinkOperatorX;
    using Base = PipelineXSinkLocalState<MaterializationSharedState>;
    using Parent = MaterializationSinkOperatorX;
};

class MaterializationSinkOperatorX final : public DataSinkOperatorX<MaterializationSinkLocalState> {
public:
    using Base = DataSinkOperatorX<MaterializationSinkLocalState>;

    friend class MaterializationSinkLocalState;
    MaterializationSinkOperatorX(int child_id, int sink_id, ObjectPool* pool,
                                 const TPlanNode& tnode)
            : Base(sink_id, tnode.node_id, child_id) {
        _name = "MATERIALIZATION_SINK_OPERATOR";
    }
    ~MaterializationSinkOperatorX() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

private:
    // Materialized slot by this node. The i-th result expr list refers to a slot of RowId
    TMaterializationNode _materialization_node;
    vectorized::VExprContextSPtrs _rowid_exprs;
    bool _gc_id_map = false;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris