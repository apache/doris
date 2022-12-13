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

#include "pipeline/exec/union_source_operator.h"

#include <opentelemetry/common/threadlocal.h>

#include "common/status.h"
#include "pipeline/exec/data_queue.h"

namespace doris {
namespace vectorized {
class Block;
}

namespace pipeline {

UnionSourceOperatorBuilder::UnionSourceOperatorBuilder(int32_t id, ExecNode* node,
                                                       std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "UnionSourceOperatorBuilder", node), _data_queue(queue) {};

OperatorPtr UnionSourceOperatorBuilder::build_operator() {
    return std::make_shared<UnionSourceOperator>(this, _node, _data_queue);
}

UnionSourceOperator::UnionSourceOperator(OperatorBuilderBase* operator_builder, ExecNode* node,
                                         std::shared_ptr<DataQueue> queue)
        : SourceOperator(operator_builder, node), _data_queue(queue) {};

//queue have data, could read
bool UnionSourceOperator::can_read() {
    return _data_queue->remaining_has_data();
}

Status UnionSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                      SourceState& source_state) {
    auto output_block = _data_queue->get_block_from_queue();
    block->swap(*output_block);

    //queue have no data any more, child could be colsed, and have exectue const expr
    source_state = (!_data_queue->remaining_has_data() && this->_node->check_node_eos(state))
                           ? SourceState::FINISHED
                           : SourceState::DEPEND_ON_SOURCE;
    return Status::OK();
}
} // namespace pipeline
} // namespace doris