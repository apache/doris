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

#include "sort_sink_operator.h"

#include "vec/exec/vsort_node.h"

namespace doris::pipeline {

SortSinkOperatorBuilder::SortSinkOperatorBuilder(int32_t id, const string& name,
                                                 vectorized::VSortNode* sort_node)
        : OperatorBuilder(id, name, sort_node), _sort_node(sort_node) {}

SortSinkOperator::SortSinkOperator(SortSinkOperatorBuilder* operator_builder,
                                   vectorized::VSortNode* sort_node)
        : Operator(operator_builder), _sort_node(sort_node) {}

Status SortSinkOperator::open(doris::RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(Operator::open(state));
    RETURN_IF_ERROR(_sort_node->alloc_resource(state));
    return Status::OK();
}

Status SortSinkOperator::close(doris::RuntimeState* /*state*/) {
    _fresh_exec_timer(_sort_node);
    return Status::OK();
}

Status SortSinkOperator::sink(doris::RuntimeState* state, vectorized::Block* block,
                              SourceState source_state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    // TODO pipeline when sort node's _reuse_mem is false, we should pass a new block to it.
    RETURN_IF_ERROR(_sort_node->sink(state, block, source_state == SourceState::FINISHED));
    return Status::OK();
}
} // namespace doris::pipeline
