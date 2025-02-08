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
class DataQueue;

class MaterializationSinkOperatorX;
class MaterializationSinkLocalState final : public PipelineXSinkLocalState<DataQueueSharedState> {
public:
    ENABLE_FACTORY_CREATOR(MaterializationSinkLocalState);
    MaterializationSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;

private:
    friend class MaterializationSinkOperatorX;
    using Base = PipelineXSinkLocalState<DataQueueSharedState>;
    using Parent = MaterializationSinkOperatorX;
};

class MaterializationSinkOperatorX final : public DataSinkOperatorX<MaterializationSinkLocalState> {
public:
    using Base = DataSinkOperatorX<MaterializationSinkLocalState>;

    friend class MaterializationSinkLocalState;
    MaterializationSinkOperatorX(int sink_id, int child_id, const std::vector<int>& column_mapping,
                                 const std::vector<bool>& need_materialize)
            : Base(sink_id, child_id, child_id) {
        _name = "MATERIALIZATION_SINK_OPERATOR";
    }
    ~MaterializationSinkOperatorX() override = default;

    Status open(RuntimeState* state) override;
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

    //    std::shared_ptr<BasicSharedState> create_shared_state() const override {
    //        std::shared_ptr<BasicSharedState> ss = std::make_shared<MaterializationSharedState>();
    //        ss->id = operator_id();
    //        for (auto& dest : dests_id()) {
    //            ss->related_op_ids.insert(dest);
    //        }
    //        return ss;
    //    }
    //
private:
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris