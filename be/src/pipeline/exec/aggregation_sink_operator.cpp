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

#include "aggregation_sink_operator.h"

#include "vec/exec/vaggregation_node.h"

namespace doris::pipeline {

AggSinkOperator::AggSinkOperator(AggSinkOperatorBuilder* operator_builder,
                                 vectorized::AggregationNode* agg_node)
        : Operator(operator_builder), _agg_node(agg_node) {}

Status AggSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _agg_node->increase_ref();
    return Status::OK();
}

Status AggSinkOperator::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(Operator::open(state));
    RETURN_IF_ERROR(_agg_node->alloc_resource(state));
    return Status::OK();
}

bool AggSinkOperator::can_write() {
    return true;
}

Status AggSinkOperator::sink(RuntimeState* state, vectorized::Block* in_block,
                             SourceState source_state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    return _agg_node->sink(state, in_block, source_state == SourceState::FINISHED);
}

Status AggSinkOperator::close(RuntimeState* state) {
    _fresh_exec_timer(_agg_node);
    if (!_agg_node->decrease_ref()) {
        _agg_node->release_resource(state);
    }
    return Status::OK();
}

///////////////////////////////  operator template  ////////////////////////////////

AggSinkOperatorBuilder::AggSinkOperatorBuilder(int32_t id, const std::string& name,
                                               vectorized::AggregationNode* exec_node)
        : OperatorBuilder(id, name, exec_node), _agg_node(exec_node) {}

OperatorPtr AggSinkOperatorBuilder::build_operator() {
    return std::make_shared<AggSinkOperator>(this, _agg_node);
}

// use final aggregation source operator
bool AggSinkOperatorBuilder::is_sink() const {
    return true;
}

bool AggSinkOperatorBuilder::is_source() const {
    return false;
}
} // namespace doris::pipeline