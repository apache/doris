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

#include "result_sink_operator.h"

#include <memory>

#include "common/object_pool.h"
#include "exec/rowid_fetcher.h"
#include "pipeline/exec/operator.h"
#include "runtime/buffer_control_block.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/sink/vmysql_result_writer.h"
#include "vec/sink/vresult_sink.h"

namespace doris {
class DataSink;
} // namespace doris

namespace doris::pipeline {

ResultSinkOperatorBuilder::ResultSinkOperatorBuilder(int32_t id, DataSink* sink)
        : DataSinkOperatorBuilder(id, "ResultSinkOperator", sink) {};

OperatorPtr ResultSinkOperatorBuilder::build_operator() {
    return std::make_shared<ResultSinkOperator>(this, _sink);
}

ResultSinkOperator::ResultSinkOperator(OperatorBuilderBase* operator_builder, DataSink* sink)
        : DataSinkOperator(operator_builder, sink) {};

bool ResultSinkOperator::can_write() {
    return _sink->_sender->can_sink();
}

Status ResultSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<>::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    auto fragment_instance_id = state->fragment_instance_id();
    // create sender
    std::shared_ptr<BufferControlBlock> sender = nullptr;
    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
            state->fragment_instance_id(), vectorized::RESULT_SINK_BUFFER_SIZE, &_sender, true,
            state->execution_timeout()));
    return Status::OK();
}

Status ResultSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(PipelineXSinkLocalState<>::open(state));
    auto& p = _parent->cast<ResultSinkOperatorX>();
    _output_vexpr_ctxs.resize(p._output_vexpr_ctxs.size());
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._output_vexpr_ctxs[i]->clone(state, _output_vexpr_ctxs[i]));
    }
    // create writer based on sink type
    switch (p._sink_type) {
    case TResultSinkType::MYSQL_PROTOCAL:
        _writer.reset(new (std::nothrow) vectorized::VMysqlResultWriter(
                _sender.get(), _output_vexpr_ctxs, _profile));
        break;
    default:
        return Status::InternalError("Unknown result sink type");
    }

    RETURN_IF_ERROR(_writer->init(state));
    return Status::OK();
}

ResultSinkOperatorX::ResultSinkOperatorX(const RowDescriptor& row_desc,
                                         const std::vector<TExpr>& t_output_expr,
                                         const TResultSink& sink)
        : DataSinkOperatorX(0), _row_desc(row_desc), _t_output_expr(t_output_expr) {
    if (!sink.__isset.type || sink.type == TResultSinkType::MYSQL_PROTOCAL) {
        _sink_type = TResultSinkType::MYSQL_PROTOCAL;
    } else {
        _sink_type = sink.type;
    }
    _fetch_option = sink.fetch_option;
    _name = "ResultSink";
}

Status ResultSinkOperatorX::prepare(RuntimeState* state) {
    auto fragment_instance_id = state->fragment_instance_id();
    auto title = fmt::format("VDataBufferSender (dst_fragment_instance_id={:x}-{:x})",
                             fragment_instance_id.hi, fragment_instance_id.lo);
    // prepare output_expr
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
    if (_fetch_option.use_two_phase_fetch) {
        for (auto& expr_ctx : _output_vexpr_ctxs) {
            // Must materialize if it a slot, or the slot column id will be -1
            expr_ctx->set_force_materialize_slot();
        }
    }
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    return Status::OK();
}

Status ResultSinkOperatorX::open(RuntimeState* state) {
    return vectorized::VExpr::open(_output_vexpr_ctxs, state);
}

Status ResultSinkOperatorX::sink(RuntimeState* state, vectorized::Block* block,
                                 SourceState source_state) {
    auto& local_state = state->get_sink_local_state(id())->cast<ResultSinkLocalState>();
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    if (_fetch_option.use_two_phase_fetch && block->rows() > 0) {
        RETURN_IF_ERROR(_second_phase_fetch_data(state, block));
    }
    RETURN_IF_ERROR(local_state._writer->append_block(*block));
    if (_fetch_option.use_two_phase_fetch) {
        // Block structure may be changed by calling _second_phase_fetch_data().
        // So we should clear block in case of unmatched columns
        block->clear();
    }
    return Status::OK();
}

Status ResultSinkOperatorX::_second_phase_fetch_data(RuntimeState* state,
                                                     vectorized::Block* final_block) {
    auto row_id_col = final_block->get_by_position(final_block->columns() - 1);
    CHECK(row_id_col.name == BeConsts::ROWID_COL);
    auto tuple_desc = _row_desc.tuple_descriptors()[0];
    FetchOption fetch_option;
    fetch_option.desc = tuple_desc;
    fetch_option.t_fetch_opt = _fetch_option;
    fetch_option.runtime_state = state;
    RowIDFetcher id_fetcher(fetch_option);
    RETURN_IF_ERROR(id_fetcher.init());
    RETURN_IF_ERROR(id_fetcher.fetch(row_id_col.column, final_block));
    return Status::OK();
}

Status ResultSinkLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    Status final_status = Status::OK();
    if (_writer) {
        // close the writer
        Status st = _writer->close();
        if (!st.ok() && final_status.ok()) {
            // close file writer failed, should return this error to client
            final_status = st;
        }
    }

    // close sender, this is normal path end
    if (_sender) {
        if (_writer) {
            _sender->update_num_written_rows(_writer->get_written_rows());
        }
        _sender->update_max_peak_memory_bytes();
        _sender->close(final_status);
    }
    state->exec_env()->result_mgr()->cancel_at_time(
            time(nullptr) + config::result_buffer_cancelled_interval_time,
            state->fragment_instance_id());
    RETURN_IF_ERROR(PipelineXSinkLocalState<>::close(state));
    return final_status;
}

bool ResultSinkOperatorX::can_write(RuntimeState* state) {
    return state->get_sink_local_state(id())->cast<ResultSinkLocalState>()._sender->can_sink();
}
} // namespace doris::pipeline
