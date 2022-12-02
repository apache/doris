
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

#include "analytic_sink_operator.h"

#include "common/status.h"
namespace doris::pipeline {

AnalyticSinkOperator::AnalyticSinkOperator(AnalyticSinkOperatorBuilder* operator_builder,
                                           vectorized::VAnalyticEvalNode* analytic_eval_node)
        : Operator(operator_builder), _analytic_eval_node(analytic_eval_node) {}

Status AnalyticSinkOperator::open(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::open(state));
    RETURN_IF_ERROR(_analytic_eval_node->alloc_resource(state));
    return Status::OK();
}

Status AnalyticSinkOperator::sink(RuntimeState* state, vectorized::Block* block,
                                  SourceState source_state) {
    LOG(INFO)<<"AnalyticSinkOperator::sink1: "<<block->columns()<<" "<<block->rows();
    RETURN_IF_ERROR(_analytic_eval_node->sink(state, block, source_state == SourceState::FINISHED));
    LOG(INFO)<<"AnalyticSinkOperator::sink2: "<<block->columns()<<" "<<block->rows();
    return Status::OK();
}

bool AnalyticSinkOperator::can_write() {
    return _analytic_eval_node->can_write();
}

} // namespace doris::pipeline
