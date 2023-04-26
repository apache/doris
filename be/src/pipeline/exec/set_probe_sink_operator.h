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

#include "common/status.h"
#include "operator.h"
#include "vec/exec/vset_operation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

namespace pipeline {

template <bool is_intersect>
class SetProbeSinkOperatorBuilder final
        : public OperatorBuilder<vectorized::VSetOperationNode<is_intersect>> {
private:
    constexpr static auto builder_name =
            is_intersect ? "IntersectProbeSinkOperator" : "ExceptProbeSinkOperator";

public:
    SetProbeSinkOperatorBuilder(int32_t id, int child_id, ExecNode* set_node);
    bool is_sink() const override { return true; }

    OperatorPtr build_operator() override;

private:
    int _child_id;
};

template <bool is_intersect>
class SetProbeSinkOperator : public StreamingOperator<SetProbeSinkOperatorBuilder<is_intersect>> {
public:
    SetProbeSinkOperator(OperatorBuilderBase* operator_builder, int child_id, ExecNode* set_node);

    bool can_write() override;

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override;
    Status open(RuntimeState* /*state*/) override { return Status::OK(); }

private:
    int _child_id;
};

} // namespace pipeline
} // namespace doris
