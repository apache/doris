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

#include "streaming_aggregation_sink_operator.h"

#include "vec/exec/vaggregation_node.h"

namespace doris::pipeline {

StreamingAggSinkOperator::StreamingAggSinkOperator(OperatorBuilderBase* operator_builder,
                                                   ExecNode* agg_node,
                                                   std::shared_ptr<DataQueue> queue)
        : StreamingOperator(operator_builder, agg_node), _data_queue(std::move(queue)) {}

Status StreamingAggSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(StreamingOperator::prepare(state));
    _queue_byte_size_counter =
            ADD_COUNTER(_runtime_profile.get(), "MaxSizeInBlockQueue", TUnit::BYTES);
    _queue_size_counter = ADD_COUNTER(_runtime_profile.get(), "MaxSizeOfBlockQueue", TUnit::UNIT);
    return Status::OK();
}

bool StreamingAggSinkOperator::can_write() {
    // sink and source in diff threads
    return _data_queue->has_enough_space_to_push();
}

Status StreamingAggSinkOperator::sink(RuntimeState* state, vectorized::Block* in_block,
                                      SourceState source_state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    Status ret = Status::OK();
    if (in_block && in_block->rows() > 0) {
        auto block_from_ctx = _data_queue->get_free_block();
        RETURN_IF_ERROR(_node->do_pre_agg(in_block, block_from_ctx.get()));
        if (block_from_ctx->rows() == 0) {
            _data_queue->push_free_block(std::move(block_from_ctx));
        } else {
            _data_queue->push_block(std::move(block_from_ctx));
        }
    }

    if (UNLIKELY(source_state == SourceState::FINISHED)) {
        _data_queue->set_finish();
    }
    return Status::OK();
}

Status StreamingAggSinkOperator::close(RuntimeState* state) {
    if (_data_queue && !_data_queue->is_finish()) {
        // finish should be set, if not set here means error.
        _data_queue->set_canceled();
    }
    if (_data_queue) {
        COUNTER_SET(_queue_size_counter, _data_queue->max_size_of_queue());
        COUNTER_SET(_queue_byte_size_counter, _data_queue->max_bytes_in_queue());
    }
    return StreamingOperator::close(state);
}

StreamingAggSinkOperatorBuilder::StreamingAggSinkOperatorBuilder(int32_t id, ExecNode* exec_node,
                                                                 std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "StreamingAggSinkOperator", exec_node),
          _data_queue(std::move(queue)) {}

OperatorPtr StreamingAggSinkOperatorBuilder::build_operator() {
    return std::make_shared<StreamingAggSinkOperator>(this, _node, _data_queue);
}

} // namespace doris::pipeline
