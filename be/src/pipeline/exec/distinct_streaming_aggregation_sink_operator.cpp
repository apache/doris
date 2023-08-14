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

#include "distinct_streaming_aggregation_sink_operator.h"

#include <gen_cpp/Metrics_types.h>

#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "vec/exec/distinct_vaggregation_node.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

DistinctStreamingAggSinkOperator::DistinctStreamingAggSinkOperator(
        OperatorBuilderBase* operator_builder, ExecNode* agg_node, std::shared_ptr<DataQueue> queue)
        : StreamingOperator(operator_builder, agg_node), _data_queue(std::move(queue)) {}

bool DistinctStreamingAggSinkOperator::can_write() {
    // sink and source in diff threads
    return _data_queue->has_enough_space_to_push();
}

Status DistinctStreamingAggSinkOperator::sink(RuntimeState* state, vectorized::Block* in_block,
                                              SourceState source_state) {
    if (in_block && in_block->rows() > 0) {
        if (_output_block == nullptr) {
            _output_block = _data_queue->get_free_block();
        }
        RETURN_IF_ERROR(
                _node->_distinct_pre_agg_with_serialized_key(in_block, _output_block.get()));

        // get enough data or reached limit rows, need push block to queue
        if (_node->limit() != -1 &&
            (_output_block->rows() + _output_distinct_rows) >= _node->limit()) {
            auto limit_rows = _node->limit() - _output_distinct_rows;
            _output_block->set_num_rows(limit_rows);
            _output_distinct_rows += limit_rows;
            _data_queue->push_block(std::move(_output_block));
        } else if (_output_block->rows() >= state->batch_size()) {
            _output_distinct_rows += _output_block->rows();
            _data_queue->push_block(std::move(_output_block));
        }
    }

    // reach limit or source finish
    if ((UNLIKELY(source_state == SourceState::FINISHED)) || reached_limited_rows()) {
        if (_output_block != nullptr) { //maybe the last block with eos
            _output_distinct_rows += _output_block->rows();
            _data_queue->push_block(std::move(_output_block));
        }
        _data_queue->set_finish();
        return Status::Error<ErrorCode::END_OF_FILE>("");
    }
    return Status::OK();
}

Status DistinctStreamingAggSinkOperator::close(RuntimeState* state) {
    if (_data_queue && !_data_queue->is_finish()) {
        // finish should be set, if not set here means error.
        _data_queue->set_canceled();
    }
    return StreamingOperator::close(state);
}

DistinctStreamingAggSinkOperatorBuilder::DistinctStreamingAggSinkOperatorBuilder(
        int32_t id, ExecNode* exec_node, std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "DistinctStreamingAggSinkOperator", exec_node),
          _data_queue(std::move(queue)) {}

OperatorPtr DistinctStreamingAggSinkOperatorBuilder::build_operator() {
    return std::make_shared<DistinctStreamingAggSinkOperator>(this, _node, _data_queue);
}

} // namespace doris::pipeline
