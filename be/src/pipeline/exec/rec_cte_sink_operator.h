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
#include "pipeline/exec/union_sink_operator.h"
#include "pipeline/rec_cte_shared_state.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {
class DataQueue;

class RecCTESinkOperatorX;
class RecCTESinkLocalState final : public PipelineXSinkLocalState<RecCTESharedState> {
public:
    ENABLE_FACTORY_CREATOR(RecCTESinkLocalState);
    RecCTESinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    Status open(RuntimeState* state) override;

private:
    friend class RecCTESinkOperatorX;
    using Base = PipelineXSinkLocalState<RecCTESharedState>;
    using Parent = RecCTESinkOperatorX;

    vectorized::VExprContextSPtrs _child_expr;
};

class RecCTESinkOperatorX MOCK_REMOVE(final) : public DataSinkOperatorX<RecCTESinkLocalState> {
public:
    using Base = DataSinkOperatorX<RecCTESinkLocalState>;

    friend class RecCTESinkLocalState;
    RecCTESinkOperatorX(int sink_id, int dest_id, const TPlanNode& tnode,
                        const DescriptorTbl& descs)
            : Base(sink_id, tnode.node_id, dest_id), _row_descriptor(descs, tnode.row_tuples) {}

    ~RecCTESinkOperatorX() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    bool reset_to_rerun(RuntimeState* state, OperatorXBase* root) const override {
        auto* shared_state = get_local_state(state)._shared_state;
        if (shared_state->ready_to_return == false) {
            THROW_IF_ERROR(root->reset(state));
            // must set_ready after root(exchange) reset
            // if next round executed before exchange source reset, it maybe cant find receiver and cause blocked forever
            shared_state->source_dep->set_ready();
            return true;
        }
        return false;
    }

    std::shared_ptr<BasicSharedState> create_shared_state() const override { return nullptr; }

    bool is_serial_operator() const override { return true; }

    DataDistribution required_data_distribution(RuntimeState* /*state*/) const override {
        return {ExchangeType::NOOP};
    }

    Status sink(RuntimeState* state, vectorized::Block* input_block, bool eos) override {
        auto& local_state = get_local_state(state);

        COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)input_block->rows());
        if (input_block->rows() != 0) {
            vectorized::Block block;
            RETURN_IF_ERROR(materialize_block(local_state._child_expr, input_block, &block, true));
            RETURN_IF_ERROR(local_state._shared_state->emplace_block(state, std::move(block)));
        }
        return Status::OK();
    }

private:
    const RowDescriptor _row_descriptor;
    vectorized::VExprContextSPtrs _child_expr;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris