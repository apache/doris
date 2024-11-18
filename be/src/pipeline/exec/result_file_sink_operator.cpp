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

#include "result_file_sink_operator.h"

#include <memory>
#include <random>

#include "pipeline/exec/exchange_sink_buffer.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/result_sink_operator.h"
#include "runtime/buffer_control_block.h"
#include "runtime/result_buffer_mgr.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::pipeline {

ResultFileSinkLocalState::ResultFileSinkLocalState(DataSinkOperatorXBase* parent,
                                                   RuntimeState* state)
        : AsyncWriterSink<vectorized::VFileResultWriter, ResultFileSinkOperatorX>(parent, state) {}

ResultFileSinkOperatorX::ResultFileSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                                                 const std::vector<TExpr>& t_output_expr)
        : DataSinkOperatorX(operator_id, 0), _row_desc(row_desc), _t_output_expr(t_output_expr) {}

ResultFileSinkOperatorX::ResultFileSinkOperatorX(
        int operator_id, const RowDescriptor& row_desc, const TResultFileSink& sink,
        const std::vector<TPlanFragmentDestination>& destinations,
        const std::vector<TExpr>& t_output_expr, DescriptorTbl& descs)
        : DataSinkOperatorX(operator_id, 0),
          _row_desc(row_desc),
          _t_output_expr(t_output_expr),
          _dests(destinations),
          _output_row_descriptor(descs.get_tuple_descriptor(sink.output_tuple_id), false) {
    CHECK_EQ(destinations.size(), 1);
}

ResultFileSinkLocalState::~ResultFileSinkLocalState() = default;

Status ResultFileSinkOperatorX::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSinkOperatorX<ResultFileSinkLocalState>::init(tsink));
    const auto& sink = tsink.result_file_sink;
    CHECK(sink.__isset.file_options);
    _file_opts = std::make_unique<ResultFileOptions>(sink.file_options);
    CHECK(sink.__isset.storage_backend_type);
    _storage_type = sink.storage_backend_type;

    //for impl csv_with_name and csv_with_names_and_types
    _header_type = sink.header_type;
    _header = sink.header;

    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
    return Status::OK();
}

Status ResultFileSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<ResultFileSinkLocalState>::open(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    if (state->query_options().enable_parallel_outfile) {
        RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(state->query_id(), _buf_size,
                                                                       &_sender, state));
    }
    return vectorized::VExpr::open(_output_vexpr_ctxs, state);
}

Status ResultFileSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _sender_id = info.sender_id;

    auto& p = _parent->cast<ResultFileSinkOperatorX>();
    CHECK(p._file_opts.get() != nullptr);
    // create sender
    if (state->query_options().enable_parallel_outfile) {
        _sender = _parent->cast<ResultFileSinkOperatorX>()._sender;
    } else {
        RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
                state->fragment_instance_id(), p._buf_size, &_sender, state));
    }
    _sender->set_dependency(state->fragment_instance_id(), _dependency->shared_from_this());

    // create writer
    _writer.reset(new (std::nothrow) vectorized::VFileResultWriter(
            p._file_opts.get(), p._storage_type, state->fragment_instance_id(), _output_vexpr_ctxs,
            _sender, nullptr, state->return_object_data_as_binary(), p._output_row_descriptor,
            _async_writer_dependency, _finish_dependency));
    _writer->set_header_info(p._header_type, p._header);
    return Status::OK();
}

Status ResultFileSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (Base::_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(_close_timer);
    SCOPED_TIMER(exec_time_counter());

    if (_closed) {
        return Status::OK();
    }

    Status final_status = exec_status;
    // For pipelinex engine, the writer is closed in async thread process_block
    if (_writer) {
        Status st = _writer->get_writer_status();
        if (!st.ok() && exec_status.ok()) {
            // close file writer failed, should return this error to client
            final_status = st;
        }
    }
    // close sender, this is normal path end
    if (_sender) {
        _sender->update_return_rows(_writer == nullptr ? 0 : _writer->get_written_rows());
        RETURN_IF_ERROR(_sender->close(state->fragment_instance_id(), final_status));
    }
    state->exec_env()->result_mgr()->cancel_at_time(
            time(nullptr) + config::result_buffer_cancelled_interval_time,
            state->fragment_instance_id());

    return Base::close(state, exec_status);
}

Status ResultFileSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    return local_state.sink(state, in_block, eos);
}

} // namespace doris::pipeline
