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

#include "vec/exec/vaggregation_node.h"

namespace doris {
namespace pipeline {
StreamingAggSourceOperator::StreamingAggSourceOperator(OperatorBuilder* templ,
                                                       vectorized::AggregationNode* node,
                                                       std::shared_ptr<AggContext> agg_context)
        : Operator(templ), _agg_node(node), _agg_context(std::move(agg_context)) {}

Status StreamingAggSourceOperator::prepare(RuntimeState* state) {
    _agg_node->increase_ref();
    return Status::OK();
}

bool StreamingAggSourceOperator::can_read() {
    return _agg_context->has_data_or_finished();
}

Status StreamingAggSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                             SourceState& source_state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    bool eos = false;
    if (!_agg_context->data_exhausted()) {
        std::unique_ptr<vectorized::Block> agg_block;
        RETURN_IF_ERROR(_agg_context->get_block(&agg_block));

        if (_agg_context->data_exhausted()) {
            RETURN_IF_ERROR(_agg_node->pull(state, block, &eos));
        } else {
            block->swap(*agg_block);
            agg_block->clear_column_data(_agg_node->row_desc().num_materialized_slots());
            _agg_context->return_free_block(std::move(agg_block));
        }
    } else {
        RETURN_IF_ERROR(_agg_node->pull(state, block, &eos));
    }

    source_state = eos ? SourceState::FINISHED : SourceState::DEPEND_ON_SOURCE;

    return Status::OK();
}

Status StreamingAggSourceOperator::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _fresh_exec_timer(_agg_node);
    if (!_agg_node->decrease_ref()) {
        _agg_node->release_resource(state);
    }
    return Operator::close(state);
}

///////////////////////////////  operator template  ////////////////////////////////

StreamingAggSourceOperatorBuilder::StreamingAggSourceOperatorBuilder(
        int32_t id, const std::string& name, vectorized::AggregationNode* exec_node,
        std::shared_ptr<AggContext> agg_context)
        : OperatorBuilder(id, name, exec_node), _agg_context(std::move(agg_context)) {}

OperatorPtr StreamingAggSourceOperatorBuilder::build_operator() {
    return std::make_shared<StreamingAggSourceOperator>(
            this, assert_cast<vectorized::AggregationNode*>(_related_exec_node), _agg_context);
}

} // namespace pipeline
} // namespace doris
