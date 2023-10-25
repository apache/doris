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

Status DistinctStreamingAggSourceOperator::pull_data(RuntimeState* state, vectorized::Block* block,
                                                     bool* eos) {
    std::unique_ptr<vectorized::Block> agg_block;
    RETURN_IF_ERROR(_data_queue->get_block_from_queue(&agg_block));
    if (agg_block != nullptr) {
        block->swap(*agg_block);
        agg_block->clear_column_data(block->columns());
        _data_queue->push_free_block(std::move(agg_block));
    }
    if (_data_queue->data_exhausted()) { //the sink is eos or reached limit
        *eos = true;
    }
    _node->_make_nullable_output_key(block);
    if (_node->is_streaming_preagg() == false) {
        // dispose the having clause, should not be execute in prestreaming agg
        RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_node->get_conjuncts(), block,
                                                               block->columns()));
    }

    rows_have_returned += block->rows();
    return Status::OK();
}

Status DistinctStreamingAggSourceOperator::get_block(RuntimeState* state, vectorized::Block* block,
                                                     SourceState& source_state) {
    bool eos = false;
    RETURN_IF_ERROR(_node->get_next_after_projects(
            state, block, &eos,
            std::bind(&DistinctStreamingAggSourceOperator::pull_data, this, std::placeholders::_1,
                      std::placeholders::_2, std::placeholders::_3)));
    if (UNLIKELY(eos)) {
        _node->set_num_rows_returned(rows_have_returned);
        source_state = SourceState::FINISHED;
    } else {
        source_state = SourceState::DEPEND_ON_SOURCE;
    }
    return Status::OK();
}

DistinctStreamingAggSourceOperatorBuilder::DistinctStreamingAggSourceOperatorBuilder(
        int32_t id, ExecNode* exec_node, std::shared_ptr<DataQueue> queue)
        : OperatorBuilder(id, "DistinctStreamingAggSourceOperator", exec_node),
          _data_queue(std::move(queue)) {}

OperatorPtr DistinctStreamingAggSourceOperatorBuilder::build_operator() {
    return std::make_shared<DistinctStreamingAggSourceOperator>(this, _node, _data_queue);
}

DistinctStreamingAggSourceOperatorX::DistinctStreamingAggSourceOperatorX(ObjectPool* pool,
                                                                         const TPlanNode& tnode,
                                                                         int operator_id,
                                                                         const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs) {
    if (tnode.agg_node.__isset.use_streaming_preaggregation) {
        _is_streaming_preagg = tnode.agg_node.use_streaming_preaggregation;
        if (_is_streaming_preagg) {
            DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
            DCHECK(_limit == -1) << "Preaggs have no limits";
        }
    } else {
        _is_streaming_preagg = false;
    }
}

Status DistinctStreamingAggSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                      SourceState& source_state) {
    CREATE_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    std::unique_ptr<vectorized::Block> agg_block;
    RETURN_IF_ERROR(local_state._shared_state->data_queue->get_block_from_queue(&agg_block));
    if (agg_block != nullptr) {
        block->swap(*agg_block);
        agg_block->clear_column_data(block->columns());
        local_state._shared_state->data_queue->push_free_block(std::move(agg_block));
    }

    local_state._dependency->_make_nullable_output_key(block);
    if (_is_streaming_preagg == false) {
        // dispose the having clause, should not be execute in prestreaming agg
        RETURN_IF_ERROR(
                vectorized::VExprContext::filter_block(_conjuncts, block, block->columns()));
    }

    if (UNLIKELY(local_state._shared_state->data_queue->data_exhausted())) {
        source_state = SourceState::FINISHED;
    } else {
        source_state = SourceState::DEPEND_ON_SOURCE;
    }
    return Status::OK();
}

Status DistinctStreamingAggSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));
    _op_name = "DISTINCT_STREAMING_AGGREGATION_OPERATOR";
    return Status::OK();
}

} // namespace pipeline
} // namespace doris
