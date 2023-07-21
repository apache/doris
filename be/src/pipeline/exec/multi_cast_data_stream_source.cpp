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

#include "multi_cast_data_stream_source.h"

#include <functional>

#include "common/status.h"
#include "pipeline/exec/multi_cast_data_streamer.h"
#include "pipeline/exec/operator.h"
#include "vec/core/block.h"

namespace doris::pipeline {

MultiCastDataStreamerSourceOperatorBuilder::MultiCastDataStreamerSourceOperatorBuilder(
        int32_t id, const int consumer_id, std::shared_ptr<MultiCastDataStreamer>& data_streamer,
        const TDataStreamSink& sink)
        : OperatorBuilderBase(id, "MultiCastDataStreamerSourceOperator"),
          _consumer_id(consumer_id),
          _multi_cast_data_streamer(data_streamer),
          _t_data_stream_sink(sink) {}

OperatorPtr MultiCastDataStreamerSourceOperatorBuilder::build_operator() {
    return std::make_shared<MultiCastDataStreamerSourceOperator>(
            this, _consumer_id, _multi_cast_data_streamer, _t_data_stream_sink);
}

const RowDescriptor& MultiCastDataStreamerSourceOperatorBuilder::row_desc() {
    return _multi_cast_data_streamer->row_desc();
}

MultiCastDataStreamerSourceOperator::MultiCastDataStreamerSourceOperator(
        OperatorBuilderBase* operator_builder, const int consumer_id,
        std::shared_ptr<MultiCastDataStreamer>& data_streamer, const TDataStreamSink& sink)
        : OperatorBase(operator_builder),
          vectorized::RuntimeFilterConsumer(sink.dest_node_id, sink.runtime_filters,
                                            data_streamer->row_desc(), _conjuncts),
          _consumer_id(consumer_id),
          _multi_cast_data_streamer(data_streamer),
          _t_data_stream_sink(sink) {}

Status MultiCastDataStreamerSourceOperator::prepare(doris::RuntimeState* state) {
    RETURN_IF_ERROR(vectorized::RuntimeFilterConsumer::init(state));
    // init profile for runtime filter
    RuntimeFilterConsumer::_init_profile(_multi_cast_data_streamer->profile());
    if (_t_data_stream_sink.__isset.output_exprs) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_data_stream_sink.output_exprs,
                                                             _output_expr_contexts));
        RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_expr_contexts, state, row_desc()));
    }

    if (_t_data_stream_sink.__isset.conjuncts) {
        RETURN_IF_ERROR(
                vectorized::VExpr::create_expr_trees(_t_data_stream_sink.conjuncts, _conjuncts));
        RETURN_IF_ERROR(vectorized::VExpr::prepare(_conjuncts, state, row_desc()));
    }
    return Status::OK();
}

Status MultiCastDataStreamerSourceOperator::open(doris::RuntimeState* state) {
    return _acquire_runtime_filter();
}

bool MultiCastDataStreamerSourceOperator::runtime_filters_are_ready_or_timeout() {
    return vectorized::RuntimeFilterConsumer::runtime_filters_are_ready_or_timeout();
}

bool MultiCastDataStreamerSourceOperator::can_read() {
    return _multi_cast_data_streamer->can_read(_consumer_id);
}

Status MultiCastDataStreamerSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                                      SourceState& source_state) {
    bool eos = false;
    vectorized::Block tmp_block;
    vectorized::Block* output_block = block;
    if (!_output_expr_contexts.empty()) {
        output_block = &tmp_block;
    }
    _multi_cast_data_streamer->pull(_consumer_id, output_block, &eos);

    if (!_conjuncts.empty()) {
        RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, output_block,
                                                               output_block->columns()));
    }

    if (!_output_expr_contexts.empty() && output_block->rows() > 0) {
        RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
                _output_expr_contexts, *output_block, block));
        materialize_block_inplace(*block);
    }
    if (eos) {
        source_state = SourceState::FINISHED;
    }
    return Status::OK();
}

Status MultiCastDataStreamerSourceOperator::close(doris::RuntimeState* state) {
    _multi_cast_data_streamer->close_sender(_consumer_id);
    return OperatorBase::close(state);
}

RuntimeProfile* MultiCastDataStreamerSourceOperator::get_runtime_profile() const {
    return _multi_cast_data_streamer->profile();
}

} // namespace doris::pipeline
