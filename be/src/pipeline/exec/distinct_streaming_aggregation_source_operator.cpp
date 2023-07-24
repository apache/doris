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

#include "distinct_streaming_aggregation_source_operator.h"

#include <utility>

#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/operator.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/distinct_vaggregation_node.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {
DistinctStreamingAggSourceOperator::DistinctStreamingAggSourceOperator(
        OperatorBuilderBase* templ, ExecNode* node, std::shared_ptr<DataQueue> queue)
        : SourceOperator(templ, node), _data_queue(std::move(queue)) {}

bool DistinctStreamingAggSourceOperator::can_read() {
    return _data_queue->has_data_or_finished();
}

Status DistinctStreamingAggSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                                     SourceState& source_state) {
    bool eos = false;
    std::unique_ptr<vectorized::Block> agg_block;
    RETURN_IF_ERROR(_data_queue->get_block_from_queue(&agg_block));
    if (agg_block != nullptr) {
        block->swap(*agg_block);
        agg_block->clear_column_data(_node->row_desc().num_materialized_slots());
        _data_queue->push_free_block(std::move(agg_block));
    }
    if (_data_queue->data_exhausted()) { //the sink is eos or reached limit
        eos = true;
    }
    reinterpret_cast<vectorized::DistinctAggregationNode*>(_node)->update_num_rows_returned(
            block->rows());
    source_state = eos ? SourceState::FINISHED : SourceState::DEPEND_ON_SOURCE;
    return Status::OK();
}

DistinctStreamingAggSourceOperatorBuilder::DistinctStreamingAggSourceOperatorBuilder(
        int32_t id, ExecNode* exec_node, std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "DistinctStreamingAggSourceOperator", exec_node),
          _data_queue(std::move(queue)) {}

OperatorPtr DistinctStreamingAggSourceOperatorBuilder::build_operator() {
    return std::make_shared<DistinctStreamingAggSourceOperator>(this, _node, _data_queue);
}

} // namespace pipeline
} // namespace doris
