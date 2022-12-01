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

#include "agg_context.h"
#include "operator.h"

namespace doris {
namespace vectorized {
class AggregationNode;
class VExprContext;
class Block;
} // namespace vectorized

namespace pipeline {
class AggSinkOperatorBuilder;
class AggSinkOperator : public Operator {
public:
    AggSinkOperator(AggSinkOperatorBuilder* operator_builder, vectorized::AggregationNode*);

    Status prepare(RuntimeState*) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override;

    bool can_write() override;

    Status close(RuntimeState* state) override;

    Status finalize(doris::RuntimeState* state) override { return Status::OK(); }

private:
    vectorized::AggregationNode* _agg_node;
};

class AggSinkOperatorBuilder : public OperatorBuilder {
public:
    AggSinkOperatorBuilder(int32_t, const std::string&, vectorized::AggregationNode*);

    OperatorPtr build_operator() override;

    bool is_sink() const override;
    bool is_source() const override;

private:
    vectorized::AggregationNode* _agg_node;
};

} // namespace pipeline
} // namespace doris