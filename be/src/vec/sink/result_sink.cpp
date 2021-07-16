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

#include "vec/sink/result_sink.h"

#include "runtime/buffer_control_block.h"
#include "runtime/exec_env.h"
#include "runtime/file_result_writer.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/mysql_result_writer.h"

namespace doris {
namespace vectorized {
ResultSink::ResultSink(const RowDescriptor& row_desc, const std::vector<TExpr>& t_output_expr,
                       const TResultSink& sink, int buffer_size)
        : _row_desc(row_desc), _t_output_expr(t_output_expr), _buf_size(buffer_size) {
    if (!sink.__isset.type || sink.type == TResultSinkType::MYSQL_PROTOCAL) {
        _sink_type = TResultSinkType::MYSQL_PROTOCAL;
    } else {
        _sink_type = sink.type;
    }

    if (_sink_type == TResultSinkType::FILE) {
        CHECK(sink.__isset.file_options);
        _file_opts.reset(new ResultFileOptions(sink.file_options));
    }

    _name = "ResultSink";
}

ResultSink::~ResultSink() {}

Status ResultSink::prepare_exprs(RuntimeState* state) {
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(
            VExpr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_vexpr_ctxs));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::prepare(_output_vexpr_ctxs, state, _row_desc, _expr_mem_tracker));
    return Status::OK();
}
Status ResultSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    auto fragment_instance_id = state->fragment_instance_id();
    auto title = fmt::format("DataBufferSender (dst_fragment_instance_id={:x}-{:x})",
                             fragment_instance_id.hi, fragment_instance_id.lo);
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title));
    // prepare output_expr
    RETURN_IF_ERROR(prepare_exprs(state));

    // create sender
    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(state->fragment_instance_id(),
                                                                   _buf_size, &_sender));

    // create writer based on sink type
    switch (_sink_type) {
    case TResultSinkType::MYSQL_PROTOCAL:
        _writer.reset(new (std::nothrow)
                              MysqlResultWriter(_sender.get(), _output_vexpr_ctxs, _profile));
        break;
    case TResultSinkType::FILE:
        CHECK(_file_opts.get() != nullptr);
        // TODO:
        /*      _writer.reset(new (std::nothrow) FileResultWriter(_file_opts.get(), _output_expr_ctxs,*/
        /*_profile, _sender.get()));*/
        break;
    default:
        return Status::InternalError("Unknown result sink type");
    }

    RETURN_IF_ERROR(_writer->init(state));
    return Status::OK();
}

Status ResultSink::open(RuntimeState* state) {
    return VExpr::open(_output_vexpr_ctxs, state);
}

Status ResultSink::send(RuntimeState* state, RowBatch* batch) {
    return Status::NotSupported("Not Implemented Result Sink::send scalar");
}

Status ResultSink::send(RuntimeState* state, Block* block) {
    return _writer->append_block(*block);
}

Status ResultSink::close(RuntimeState* state, Status exec_status) {
    if (_closed || _writer == nullptr|| _sender == nullptr) {
        return Status::OK();
    }

    Status final_status = exec_status;
    // close the writer
    Status st = _writer->close();
    if (!st.ok() && exec_status.ok()) {
        // close file writer failed, should return this error to client
        final_status = st;
    }

    // close sender, this is normal path end
    if (_sender) {
        _sender->update_num_written_rows(_writer->get_written_rows());
        _sender->close(final_status);
    }
    state->exec_env()->result_mgr()->cancel_at_time(
            time(NULL) + config::result_buffer_cancelled_interval_time,
            state->fragment_instance_id());

    VExpr::close(_output_vexpr_ctxs, state);
    _closed = true;
    return Status::OK();
}

void ResultSink::set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
    _sender->set_query_statistics(statistics);
}

} // namespace vectorized
} // namespace doris
