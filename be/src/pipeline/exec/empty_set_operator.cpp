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

#include "empty_set_operator.h"

#include "vec/exec/vempty_set_node.h"

namespace doris::pipeline {

EmptySetSourceOperator::EmptySetSourceOperator(EmptySetSourceOperatorBuilder* operator_builder,
                                               vectorized::VEmptySetNode* empty_set_node)
        : Operator(operator_builder), _empty_set_node(empty_set_node) {}

bool EmptySetSourceOperator::can_read() {
    return true;
}

Status EmptySetSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                         SourceState& source_state) {
    bool eos = false;
    RETURN_IF_ERROR(_empty_set_node->get_next(state, block, &eos));
    source_state = eos ? SourceState::FINISHED : SourceState::DEPEND_ON_SOURCE;
    return Status::OK();
}

EmptySetSourceOperatorBuilder::EmptySetSourceOperatorBuilder(
        int32_t id, const string& name, vectorized::VEmptySetNode* empty_set_node)
        : OperatorBuilder(id, name, empty_set_node), _empty_set_node(empty_set_node) {}

} // namespace doris::pipeline
