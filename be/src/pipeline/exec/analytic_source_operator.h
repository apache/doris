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

#include "operator.h"
#include "vec/exec/vanalytic_eval_node.h"

namespace doris {

namespace vectorized {
class VAnalyticEvalNode;
}

namespace pipeline {

class AnalyticSourceOperatorBuilder;

class AnalyticSourceOperator : public Operator {
public:
    AnalyticSourceOperator(AnalyticSourceOperatorBuilder* operator_builder,
                           vectorized::VAnalyticEvalNode* analytic_eval_node);

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    Status close(RuntimeState* state) override;

    bool can_read() override;

private:
    vectorized::VAnalyticEvalNode* _analytic_eval_node = nullptr;
};

class AnalyticSourceOperatorBuilder : public OperatorBuilder {
public:
    AnalyticSourceOperatorBuilder(int32_t id, const std::string& name,
                                  vectorized::VAnalyticEvalNode* analytic_eval_node)
            : OperatorBuilder(id, name, analytic_eval_node),
              _analytic_eval_node(analytic_eval_node) {}

    bool is_sink() const override { return false; }

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override {
        return std::make_shared<AnalyticSourceOperator>(this, _analytic_eval_node);
    }

private:
    vectorized::VAnalyticEvalNode* _analytic_eval_node = nullptr;
};

} // namespace pipeline
} // namespace doris