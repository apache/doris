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

#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"

namespace doris::pipeline {

class LocalExchangeSourceOperatorX;
class LocalExchangeSourceLocalState final : public PipelineXLocalState<LocalExchangeDependency> {
public:
    using Base = PipelineXLocalState<LocalExchangeDependency>;
    ENABLE_FACTORY_CREATOR(LocalExchangeSourceLocalState);
    LocalExchangeSourceLocalState(RuntimeState* state, OperatorXBase* parent)
            : Base(state, parent) {}

    Status init(RuntimeState* state, LocalStateInfo& info) override;

private:
    friend class LocalExchangeSourceOperatorX;

    int _channel_id;
    RuntimeProfile::Counter* _get_block_failed_counter = nullptr;
};

class LocalExchangeSourceOperatorX final : public OperatorX<LocalExchangeSourceLocalState> {
public:
    using Base = OperatorX<LocalExchangeSourceLocalState>;
    LocalExchangeSourceOperatorX(ObjectPool* pool, int id) : Base(pool, -1, id) {}
    Status init(const TPlanNode& tnode, RuntimeState* state) override {
        _op_name = "LOCAL_EXCHANGE_OPERATOR";
        return Status::OK();
    }
    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status open(RuntimeState* state) override { return Status::OK(); }
    const RowDescriptor& intermediate_row_desc() const override {
        return _child_x->intermediate_row_desc();
    }
    RowDescriptor& row_descriptor() override { return _child_x->row_descriptor(); }
    const RowDescriptor& row_desc() override { return _child_x->row_desc(); }

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

private:
    friend class LocalExchangeSourceLocalState;
};

} // namespace doris::pipeline
