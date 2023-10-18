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

#include "streaming_aggregation_source_operator.h"

#include <utility>

#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

StreamingAggSourceOperator::StreamingAggSourceOperator(OperatorBuilderBase* templ, ExecNode* node,
                                                       std::shared_ptr<DataQueue> queue)
        : SourceOperator(templ, node), _data_queue(std::move(queue)) {}

bool StreamingAggSourceOperator::can_read() {
    return _data_queue->has_data_or_finished();
}

Status StreamingAggSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                             SourceState& source_state) {
    bool eos = false;
    if (!_data_queue->data_exhausted()) {
        std::unique_ptr<vectorized::Block> agg_block;
        RETURN_IF_ERROR(_data_queue->get_block_from_queue(&agg_block));

        if (_data_queue->data_exhausted()) {
            RETURN_IF_ERROR(_node->pull(state, block, &eos));
        } else {
            block->swap(*agg_block);
            agg_block->clear_column_data(_node->row_desc().num_materialized_slots());
            _data_queue->push_free_block(std::move(agg_block));
        }
    } else {
        RETURN_IF_ERROR(_node->pull(state, block, &eos));
    }

    source_state = eos ? SourceState::FINISHED : SourceState::DEPEND_ON_SOURCE;

    return Status::OK();
}

StreamingAggSourceOperatorBuilder::StreamingAggSourceOperatorBuilder(
        int32_t id, ExecNode* exec_node, std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "StreamingAggSourceOperator", exec_node),
          _data_queue(std::move(queue)) {}

OperatorPtr StreamingAggSourceOperatorBuilder::build_operator() {
    return std::make_shared<StreamingAggSourceOperator>(this, _node, _data_queue);
}

StreamingAggSourceOperatorX::StreamingAggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                         const DescriptorTbl& descs)
        : Base(pool, tnode, descs) {}

Status StreamingAggSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                              SourceState& source_state) {
    CREATE_LOCAL_STATE_RETURN_STATUS_IF_ERROR(local_state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    if (!local_state._shared_state->data_queue->data_exhausted()) {
        std::unique_ptr<vectorized::Block> agg_block;
        DCHECK(local_state._dependency->read_blocked_by() == nullptr);
        RETURN_IF_ERROR(local_state._shared_state->data_queue->get_block_from_queue(&agg_block));

        if (local_state._shared_state->data_queue->data_exhausted()) {
            RETURN_IF_ERROR(Base::get_block(state, block, source_state));
        } else {
            block->swap(*agg_block);
            agg_block->clear_column_data(row_desc().num_materialized_slots());
            local_state._shared_state->data_queue->push_free_block(std::move(agg_block));
        }
    } else {
        RETURN_IF_ERROR(Base::get_block(state, block, source_state));
    }

    return Status::OK();
}

Status StreamingAggSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    _op_name = "STREAMING_AGGREGATION_OPERATOR";
    return Status::OK();
}

} // namespace pipeline
} // namespace doris
