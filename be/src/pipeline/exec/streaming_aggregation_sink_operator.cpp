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

StreamingAggSinkOperator::StreamingAggSinkOperator(
        StreamingAggSinkOperatorBuilder* operator_builder, vectorized::AggregationNode* agg_node,
        std::shared_ptr<AggContext> agg_context)
        : Operator(operator_builder), _agg_node(agg_node), _agg_context(std::move(agg_context)) {}

Status StreamingAggSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _queue_byte_size_counter =
            ADD_COUNTER(_runtime_profile.get(), "MaxSizeInBlockQueue", TUnit::BYTES);
    _queue_size_counter = ADD_COUNTER(_runtime_profile.get(), "MaxSizeOfBlockQueue", TUnit::UNIT);
    return Status::OK();
}

Status StreamingAggSinkOperator::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(Operator::open(state));
    RETURN_IF_ERROR(_agg_node->alloc_resource(state));
    return Status::OK();
}

bool StreamingAggSinkOperator::can_write() {
    // sink and source in diff threads
    return _agg_context->has_enough_space_to_push();
}

Status StreamingAggSinkOperator::sink(RuntimeState* state, vectorized::Block* in_block,
                                      SourceState source_state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    Status ret = Status::OK();
    if (in_block && in_block->rows() > 0) {
        auto bock_from_ctx = _agg_context->get_free_block();
        RETURN_IF_ERROR(_agg_node->do_pre_agg(in_block, bock_from_ctx.get()));
        if (bock_from_ctx->rows() == 0) {
            _agg_context->return_free_block(std::move(bock_from_ctx));
        } else {
            _agg_context->push_block(std::move(bock_from_ctx));
        }
    }

    if (UNLIKELY(source_state == SourceState::FINISHED)) {
        _agg_context->set_finish();
    }
    return Status::OK();
}

Status StreamingAggSinkOperator::close(RuntimeState* state) {
    _fresh_exec_timer(_agg_node);
    if (_agg_context && !_agg_context->is_finish()) {
        // finish should be set, if not set here means error.
        _agg_context->set_canceled();
    }
    COUNTER_SET(_queue_size_counter, _agg_context->max_size_of_queue());
    COUNTER_SET(_queue_byte_size_counter, _agg_context->max_bytes_in_queue());
    return Status::OK();
}

///////////////////////////////  operator template  ////////////////////////////////

StreamingAggSinkOperatorBuilder::StreamingAggSinkOperatorBuilder(
        int32_t id, const std::string& name, vectorized::AggregationNode* exec_node,
        std::shared_ptr<AggContext> agg_context)
        : OperatorBuilder(id, name, exec_node),
          _agg_node(exec_node),
          _agg_context(std::move(agg_context)) {}

OperatorPtr StreamingAggSinkOperatorBuilder::build_operator() {
    return std::make_shared<StreamingAggSinkOperator>(this, _agg_node, _agg_context);
}

// use final aggregation source operator
bool StreamingAggSinkOperatorBuilder::is_sink() const {
    return true;
}

bool StreamingAggSinkOperatorBuilder::is_source() const {
    return false;
}
} // namespace doris::pipeline
