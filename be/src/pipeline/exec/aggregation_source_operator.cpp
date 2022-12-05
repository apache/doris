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

#include "aggregation_source_operator.h"

#include "vec/exec/vaggregation_node.h"

namespace doris {
namespace pipeline {

AggregationSourceOperator::AggregationSourceOperator(OperatorBuilder* templ,
                                                     vectorized::AggregationNode* node)
        : Operator(templ), _agg_node(node) {}

Status AggregationSourceOperator::prepare(RuntimeState* state) {
    _agg_node->increase_ref();
    return Status::OK();
}

bool AggregationSourceOperator::can_read() {
    return _agg_node->can_read();
}

Status AggregationSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                            SourceState& source_state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    bool eos = false;
    RETURN_IF_ERROR(_agg_node->pull(state, block, &eos));
    source_state = eos ? SourceState::FINISHED : SourceState::DEPEND_ON_SOURCE;
    return Status::OK();
}

Status AggregationSourceOperator::close(RuntimeState* state) {
    _fresh_exec_timer(_agg_node);
    if (!_agg_node->decrease_ref()) {
        _agg_node->release_resource(state);
    }
    return Status::OK();
}

///////////////////////////////  operator template  ////////////////////////////////

AggregationSourceOperatorBuilder::AggregationSourceOperatorBuilder(
        int32_t id, const std::string& name, vectorized::AggregationNode* exec_node)
        : OperatorBuilder(id, name, exec_node) {}

OperatorPtr AggregationSourceOperatorBuilder::build_operator() {
    return std::make_shared<AggregationSourceOperator>(
            this, assert_cast<vectorized::AggregationNode*>(_related_exec_node));
}

} // namespace pipeline
} // namespace doris