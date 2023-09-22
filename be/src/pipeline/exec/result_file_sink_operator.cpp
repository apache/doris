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

#include "pipeline/exec/operator.h"
#include "runtime/buffer_control_block.h"
#include "runtime/result_buffer_mgr.h"
#include "vec/sink/vresult_file_sink.h"

namespace doris {
class DataSink;
} // namespace doris

namespace doris::pipeline {

ResultFileSinkOperatorBuilder::ResultFileSinkOperatorBuilder(int32_t id, DataSink* sink)
        : DataSinkOperatorBuilder(id, "ResultSinkOperator", sink) {};

OperatorPtr ResultFileSinkOperatorBuilder::build_operator() {
    return std::make_shared<ResultFileSinkOperator>(this, _sink);
}

ResultFileSinkOperator::ResultFileSinkOperator(OperatorBuilderBase* operator_builder,
                                               DataSink* sink)
        : DataSinkOperator(operator_builder, sink) {};

ResultFileSinkLocalState::ResultFileSinkLocalState(DataSinkOperatorXBase* parent,
                                                   RuntimeState* state)
        : AsyncWriterSink<vectorized::VFileResultWriter>(
                  parent, state, parent->cast<ResultFileSinkOperatorX>()._row_desc,
                  parent->cast<ResultFileSinkOperatorX>()._t_output_expr) {}

ResultFileSinkOperatorX::ResultFileSinkOperatorX(const RowDescriptor& row_desc,
                                                 const std::vector<TExpr>& t_output_expr)
        : DataSinkOperatorX(0), _row_desc(row_desc), _t_output_expr(t_output_expr) {}

ResultFileSinkOperatorX::ResultFileSinkOperatorX(
        RuntimeState* state, ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
        const TResultFileSink& sink, const std::vector<TPlanFragmentDestination>& destinations,
        bool send_query_statistics_with_every_batch, const std::vector<TExpr>& t_output_expr,
        DescriptorTbl& descs)
        : DataSinkOperatorX(0),
          _row_desc(row_desc),
          _t_output_expr(t_output_expr),
          _output_row_descriptor(descs.get_tuple_descriptor(sink.output_tuple_id), false) {
    _is_top_sink = false;
    CHECK_EQ(destinations.size(), 1);
}

Status ResultFileSinkOperatorX::init(const TDataSink& tsink) {
    auto& sink = tsink.result_file_sink;
    CHECK(sink.__isset.file_options);
    _file_opts.reset(new vectorized::ResultFileOptions(sink.file_options));
    CHECK(sink.__isset.storage_backend_type);
    _storage_type = sink.storage_backend_type;

    //for impl csv_with_name and csv_with_names_and_types
    _header_type = sink.header_type;
    _header = sink.header;

    return Status::OK();
}

Status ResultFileSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));

    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<ResultFileSinkOperatorX>();
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
    CHECK(p._file_opts.get() != nullptr);
    if (p._is_top_sink) {
        // create sender
        RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
                state->fragment_instance_id(), p._buf_size, &_sender, state->enable_pipeline_exec(),
                state->execution_timeout()));
        // create writer
        _writer.reset(new (std::nothrow) vectorized::VFileResultWriter(
                p._file_opts.get(), p._storage_type, state->fragment_instance_id(),
                _output_vexpr_ctxs, _sender.get(), nullptr, state->return_object_data_as_binary(),
                p._output_row_descriptor));
    } else {
        // init channel
        _output_block = vectorized::Block::create_unique(
                p._output_row_descriptor.tuple_descriptors()[0]->slots(), 1);
        _writer.reset(new (std::nothrow) vectorized::VFileResultWriter(
                p._file_opts.get(), p._storage_type, state->fragment_instance_id(),
                _output_vexpr_ctxs, nullptr, _output_block.get(),
                state->return_object_data_as_binary(), p._output_row_descriptor));
    }
    _writer->set_header_info(p._header_type, p._header);
    return Status::OK();
}

Status ResultFileSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(profile()->total_time_counter());
    SCOPED_TIMER(_open_timer);
    return Base::open(state);
}

Status ResultFileSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (Base::_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(_close_timer);
    SCOPED_TIMER(profile()->total_time_counter());

    auto& p = _parent->cast<ResultFileSinkOperatorX>();
    if (_closed) {
        return Status::OK();
    }

    Status final_status = exec_status;
    // close the writer
    if (_writer && _writer->need_normal_close()) {
        Status st = _writer->close();
        if (!st.ok() && exec_status.ok()) {
            // close file writer failed, should return this error to client
            final_status = st;
        }
    }
    if (p._is_top_sink) {
        // close sender, this is normal path end
        if (_sender) {
            _sender->update_num_written_rows(_writer == nullptr ? 0 : _writer->get_written_rows());
            _sender->close(final_status);
        }
        state->exec_env()->result_mgr()->cancel_at_time(
                time(nullptr) + config::result_buffer_cancelled_interval_time,
                state->fragment_instance_id());
    } else {
        _output_block->clear();
    }

    return Base::close(state, exec_status);
}

Status ResultFileSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                     SourceState source_state) {
    CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    return local_state.sink(state, in_block, source_state);
}

bool ResultFileSinkOperatorX::is_pending_finish(RuntimeState* state) const {
    auto& local_state = state->get_sink_local_state(id())->cast<ResultFileSinkLocalState>();
    return local_state.is_pending_finish();
}

WriteDependency* ResultFileSinkOperatorX::wait_for_dependency(RuntimeState* state) {
    CREATE_SINK_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state);
    return local_state.write_blocked_by();
}

} // namespace doris::pipeline
