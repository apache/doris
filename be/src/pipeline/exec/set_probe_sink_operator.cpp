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

#include "set_probe_sink_operator.h"

#include <glog/logging.h>

#include <memory>

#include "pipeline/exec/operator.h"
#include "vec/exec/vset_operation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::pipeline {

template <bool is_intersect>
SetProbeSinkOperatorBuilder<is_intersect>::SetProbeSinkOperatorBuilder(int32_t id, int child_id,
                                                                       ExecNode* set_node)
        : OperatorBuilder<vectorized::VSetOperationNode<is_intersect>>(id, builder_name, set_node),
          _child_id(child_id) {}

template <bool is_intersect>
OperatorPtr SetProbeSinkOperatorBuilder<is_intersect>::build_operator() {
    return std::make_shared<SetProbeSinkOperator<is_intersect>>(this, _child_id, this->_node);
}

template <bool is_intersect>
SetProbeSinkOperator<is_intersect>::SetProbeSinkOperator(OperatorBuilderBase* operator_builder,
                                                         int child_id, ExecNode* set_node)
        : StreamingOperator<SetProbeSinkOperatorBuilder<is_intersect>>(operator_builder, set_node),
          _child_id(child_id) {}

template <bool is_intersect>
Status SetProbeSinkOperator<is_intersect>::sink(RuntimeState* state, vectorized::Block* block,
                                                SourceState source_state) {
    return this->_node->sink_probe(state, _child_id, block, source_state == SourceState::FINISHED);
}

template <bool is_intersect>
bool SetProbeSinkOperator<is_intersect>::can_write() {
    DCHECK_GT(_child_id, 0);
    return this->_node->is_child_finished(_child_id - 1);
}

template class SetProbeSinkOperatorBuilder<true>;
template class SetProbeSinkOperatorBuilder<false>;
template class SetProbeSinkOperator<true>;
template class SetProbeSinkOperator<false>;

} // namespace doris::pipeline
