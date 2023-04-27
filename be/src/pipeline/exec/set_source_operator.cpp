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

#include "set_source_operator.h"

#include <memory>

#include "pipeline/exec/operator.h"
#include "vec/exec/vset_operation_node.h"

namespace doris {
class ExecNode;
} // namespace doris

namespace doris::pipeline {

template <bool is_intersect>
SetSourceOperatorBuilder<is_intersect>::SetSourceOperatorBuilder(int32_t id, ExecNode* set_node)
        : OperatorBuilder<vectorized::VSetOperationNode<is_intersect>>(id, builder_name, set_node) {
}

template <bool is_intersect>
OperatorPtr SetSourceOperatorBuilder<is_intersect>::build_operator() {
    return std::make_shared<SetSourceOperator<is_intersect>>(this, this->_node);
}

template <bool is_intersect>
SetSourceOperator<is_intersect>::SetSourceOperator(
        OperatorBuilderBase* builder, vectorized::VSetOperationNode<is_intersect>* set_node)
        : SourceOperator<SetSourceOperatorBuilder<is_intersect>>(builder, set_node) {}

template class SetSourceOperatorBuilder<true>;
template class SetSourceOperatorBuilder<false>;
template class SetSourceOperator<true>;
template class SetSourceOperator<false>;

} // namespace doris::pipeline
