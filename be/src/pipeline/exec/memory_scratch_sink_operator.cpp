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

#include "memory_scratch_sink_operator.h"

#include <memory>

#include "common/object_pool.h"
#include "pipeline/exec/operator.h"
#include "runtime/record_batch_queue.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::pipeline {

Status MemoryScratchSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    // create queue
    state->exec_env()->result_queue_mgr()->create_queue(state->fragment_instance_id(), &_queue);

    auto& p = _parent->cast<MemoryScratchSinkOperatorX>();
    _output_vexpr_ctxs.resize(p._output_vexpr_ctxs.size());
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._output_vexpr_ctxs[i]->clone(state, _output_vexpr_ctxs[i]));
    }
    _queue_dependency =
            Dependency::create_shared(p.operator_id(), p.node_id(), "QueueDependency", true);
    _queue->set_dep(_queue_dependency);
    return Status::OK();
}

Status MemoryScratchSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_queue != nullptr) {
        _queue->blocking_put(nullptr);
    }
    RETURN_IF_ERROR(Base::close(state, exec_status));
    return Status::OK();
}

MemoryScratchSinkOperatorX::MemoryScratchSinkOperatorX(const RowDescriptor& row_desc,
                                                       int operator_id,
                                                       const std::vector<TExpr>& t_output_expr)
        : DataSinkOperatorX(operator_id, 0), _row_desc(row_desc), _t_output_expr(t_output_expr) {}

Status MemoryScratchSinkOperatorX::init(const TDataSink& thrift_sink) {
    RETURN_IF_ERROR(DataSinkOperatorX<MemoryScratchSinkLocalState>::init(thrift_sink));
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
    return Status::OK();
}

Status MemoryScratchSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<MemoryScratchSinkLocalState>::prepare(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    _timezone_obj = state->timezone_obj();
    return Status::OK();
}

Status MemoryScratchSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<MemoryScratchSinkLocalState>::open(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_vexpr_ctxs, state));
    return Status::OK();
}

Status MemoryScratchSinkOperatorX::sink(RuntimeState* state, vectorized::Block* input_block,
                                        bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    if (nullptr == input_block || 0 == input_block->rows()) {
        return Status::OK();
    }
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)input_block->rows());
    std::shared_ptr<arrow::RecordBatch> result;
    // Exec vectorized expr here to speed up, block.rows() == 0 means expr exec
    // failed, just return the error status
    vectorized::Block block;
    RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
            local_state._output_vexpr_ctxs, *input_block, &block));
    std::shared_ptr<arrow::Schema> block_arrow_schema;
    // After expr executed, use recaculated schema as final schema
    RETURN_IF_ERROR(convert_block_arrow_schema(block, &block_arrow_schema));
    RETURN_IF_ERROR(convert_to_arrow_batch(block, block_arrow_schema, arrow::default_memory_pool(),
                                           &result, _timezone_obj));
    local_state._queue->blocking_put(result);
    if (local_state._queue->size() < 10) {
        local_state._queue_dependency->block();
    }
    return Status::OK();
}

} // namespace doris::pipeline
