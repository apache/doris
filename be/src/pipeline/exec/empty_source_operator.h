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

#include <stdint.h>

#include <memory>

#include "common/status.h"
#include "operator.h"
#include "runtime/descriptors.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::pipeline {

class EmptySourceOperatorBuilder final : public OperatorBuilderBase {
public:
    EmptySourceOperatorBuilder(int32_t id, const RowDescriptor& row_descriptor, ExecNode* exec_node)
            : OperatorBuilderBase(id, "EmptySourceOperator"),
              _row_descriptor(row_descriptor),
              _exec_node(exec_node) {}

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;

    const RowDescriptor& row_desc() override { return _row_descriptor; }

private:
    RowDescriptor _row_descriptor;
    ExecNode* _exec_node = nullptr;
};

class EmptySourceOperator final : public OperatorBase {
public:
    EmptySourceOperator(OperatorBuilderBase* builder, ExecNode* exec_node)
            : OperatorBase(builder), _exec_node(exec_node) {}

    bool can_read() override { return true; }
    bool is_pending_finish() const override { return false; }

    Status prepare(RuntimeState*) override { return Status::OK(); }

    Status open(RuntimeState*) override { return Status::OK(); }

    Status get_block(RuntimeState* /*runtime_state*/, vectorized::Block* /*block*/,
                     SourceState& result_state) override {
        result_state = SourceState::FINISHED;
        return Status::OK();
    }

    Status sink(RuntimeState*, vectorized::Block*, SourceState) override { return Status::OK(); }

    Status close(RuntimeState* state) override {
        static_cast<void>(_exec_node->close(state));
        return Status::OK();
    }

    [[nodiscard]] RuntimeProfile* get_runtime_profile() const override {
        return _exec_node->runtime_profile();
    }

private:
    ExecNode* _exec_node = nullptr;
};

} // namespace doris::pipeline
