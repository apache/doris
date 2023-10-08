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

#include "vec/sink/vresult_file_sink.h"

#include <gen_cpp/DataSinks_types.h>
#include <glog/logging.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <time.h>

#include <new>
#include <ostream>

#include "common/config.h"
#include "common/object_pool.h"
#include "runtime/buffer_control_block.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "util/telemetry/telemetry.h"
#include "vec/exprs/vexpr.h"

namespace doris {
class QueryStatistics;
class TExpr;
} // namespace doris

namespace doris::vectorized {

VResultFileSink::VResultFileSink(const RowDescriptor& row_desc,
                                 const std::vector<TExpr>& t_output_expr)
        : AsyncWriterSink<VFileResultWriter, VRESULT_FILE_SINK>(row_desc, t_output_expr) {}

VResultFileSink::VResultFileSink(RuntimeState* state, ObjectPool* pool, int sender_id,
                                 const RowDescriptor& row_desc, const TResultFileSink& sink,
                                 const std::vector<TPlanFragmentDestination>& destinations,
                                 bool send_query_statistics_with_every_batch,
                                 const std::vector<TExpr>& t_output_expr, DescriptorTbl& descs)
        : AsyncWriterSink<VFileResultWriter, VRESULT_FILE_SINK>(row_desc, t_output_expr),
          _output_row_descriptor(descs.get_tuple_descriptor(sink.output_tuple_id), false) {
    _is_top_sink = false;
    CHECK_EQ(destinations.size(), 1);
    _stream_sender.reset(new VDataStreamSender(state, pool, sender_id, row_desc, sink.dest_node_id,
                                               destinations,
                                               send_query_statistics_with_every_batch));
}

Status VResultFileSink::init(const TDataSink& tsink) {
    if (!_is_top_sink) {
        RETURN_IF_ERROR(_stream_sender->init(tsink));
    }

    auto& sink = tsink.result_file_sink;
    CHECK(sink.__isset.file_options);
    _file_opts.reset(new ResultFileOptions(sink.file_options));
    CHECK(sink.__isset.storage_backend_type);
    _storage_type = sink.storage_backend_type;

    //for impl csv_with_name and csv_with_names_and_types
    _header_type = sink.header_type;
    _header = sink.header;

    return VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs);
}

Status VResultFileSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AsyncWriterSink::prepare(state));

    CHECK(_file_opts.get() != nullptr);
    if (_is_top_sink) {
        // create sender
        RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
                state->fragment_instance_id(), _buf_size, &_sender, state->enable_pipeline_exec(),
                state->execution_timeout()));
        // create writer
        _writer.reset(new (std::nothrow) VFileResultWriter(
                _file_opts.get(), _storage_type, state->fragment_instance_id(), _output_vexpr_ctxs,
                _sender.get(), nullptr, state->return_object_data_as_binary(),
                _output_row_descriptor));
    } else {
        // init channel
        _output_block =
                Block::create_unique(_output_row_descriptor.tuple_descriptors()[0]->slots(), 1);
        _writer.reset(new (std::nothrow) VFileResultWriter(
                _file_opts.get(), _storage_type, state->fragment_instance_id(), _output_vexpr_ctxs,
                nullptr, _output_block.get(), state->return_object_data_as_binary(),
                _output_row_descriptor));
        RETURN_IF_ERROR(_stream_sender->prepare(state));
        _profile->add_child(_stream_sender->profile(), true, nullptr);
    }
    _writer->set_header_info(_header_type, _header);
    return Status::OK();
}

Status VResultFileSink::open(RuntimeState* state) {
    if (!_is_top_sink) {
        RETURN_IF_ERROR(_stream_sender->open(state));
    }
    return AsyncWriterSink::open(state);
}

Status VResultFileSink::close(RuntimeState* state, Status exec_status) {
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
    if (_is_top_sink) {
        // close sender, this is normal path end
        if (_sender) {
            _sender->update_num_written_rows(_writer == nullptr ? 0 : _writer->get_written_rows());
            static_cast<void>(_sender->close(final_status));
        }
        static_cast<void>(state->exec_env()->result_mgr()->cancel_at_time(
                time(nullptr) + config::result_buffer_cancelled_interval_time,
                state->fragment_instance_id()));
    } else {
        if (final_status.ok()) {
            auto st = _stream_sender->send(state, _output_block.get(), true);
            if (!st.template is<ErrorCode::END_OF_FILE>()) {
                RETURN_IF_ERROR(st);
            }
        }
        RETURN_IF_ERROR(_stream_sender->close(state, final_status));
        _output_block->clear();
    }

    _closed = true;
    return Status::OK();
}

void VResultFileSink::set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
    if (_is_top_sink) {
        _sender->set_query_statistics(statistics);
    } else {
        _stream_sender->set_query_statistics(statistics);
    }
}

} // namespace doris::vectorized
