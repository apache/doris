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

#include "vec/sink/vresult_sink.h"

#include "runtime/buffer_control_block.h"
#include "runtime/exec_env.h"
#include "runtime/file_result_writer.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/vmysql_result_writer.h"

namespace doris {
namespace vectorized {

VResultSink::VResultSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_output_expr,
                         const TResultSink& sink, int buffer_size)
        : _row_desc(row_desc), _t_output_expr(t_output_expr), _buf_size(buffer_size) {
    if (!sink.__isset.type || sink.type == TResultSinkType::MYSQL_PROTOCAL) {
        _sink_type = TResultSinkType::MYSQL_PROTOCAL;
    } else {
        _sink_type = sink.type;
    }

    _name = "ResultSink";
}

VResultSink::~VResultSink() = default;

Status VResultSink::prepare_exprs(RuntimeState* state) {
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(
            VExpr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_vexpr_ctxs));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    return Status::OK();
}

Status VResultSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    auto fragment_instance_id = state->fragment_instance_id();
    auto title = fmt::format("VDataBufferSender (dst_fragment_instance_id={:x}-{:x})",
                             fragment_instance_id.hi, fragment_instance_id.lo);
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title));
    // prepare output_expr
    RETURN_IF_ERROR(prepare_exprs(state));

    // create sender
    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
            state->fragment_instance_id(), _buf_size, &_sender, state->enable_pipeline_exec()));

    // create writer based on sink type
    switch (_sink_type) {
    case TResultSinkType::MYSQL_PROTOCAL:
        _writer.reset(new (std::nothrow)
                              VMysqlResultWriter(_sender.get(), _output_vexpr_ctxs, _profile));
        break;
    default:
        return Status::InternalError("Unknown result sink type");
    }

    RETURN_IF_ERROR(_writer->init(state));
    return Status::OK();
}

Status VResultSink::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VResultSink::open");
    return VExpr::open(_output_vexpr_ctxs, state);
}

Status VResultSink::send(RuntimeState* state, RowBatch* batch) {
    return Status::NotSupported("Not Implemented Result Sink::send scalar");
}

Status VResultSink::send(RuntimeState* state, Block* block, bool eos) {
    INIT_AND_SCOPE_SEND_SPAN(state->get_tracer(), _send_span, "VResultSink::send");
    // The memory consumption in the process of sending the results is not check query memory limit.
    // Avoid the query being cancelled when the memory limit is reached after the query result comes out.
    STOP_CHECK_THREAD_MEM_TRACKER_LIMIT();
    return _writer->append_block(*block);
}

Status VResultSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VResultSink::close");
    Status final_status = exec_status;

    if (_writer) {
        // close the writer
        Status st = _writer->close();
        if (!st.ok() && exec_status.ok()) {
            // close file writer failed, should return this error to client
            final_status = st;
        }
    }

    // close sender, this is normal path end
    if (_sender) {
        if (_writer) _sender->update_num_written_rows(_writer->get_written_rows());
        _sender->update_max_peak_memory_bytes();
        _sender->close(final_status);
    }
    state->exec_env()->result_mgr()->cancel_at_time(
            time(nullptr) + config::result_buffer_cancelled_interval_time,
            state->fragment_instance_id());

    VExpr::close(_output_vexpr_ctxs, state);
    return DataSink::close(state, exec_status);
}

void VResultSink::set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
    _sender->set_query_statistics(statistics);
}

} // namespace vectorized
} // namespace doris
