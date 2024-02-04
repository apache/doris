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
#include <gen_cpp/PaloInternalService_types.h>
#include <glog/logging.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <time.h>

#include <new>
#include <ostream>

#include "common/config.h"
#include "common/object_pool.h"
#include "runtime/buffer_control_block.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "util/telemetry/telemetry.h"
#include "util/uid_util.h"
#include "vec/exprs/vexpr.h"
#include "vec/runtime/vfile_result_writer.h"

namespace doris {
class TExpr;
} // namespace doris

namespace doris::vectorized {

VResultFileSink::VResultFileSink(ObjectPool* pool, const RowDescriptor& row_desc,
                                 const TResultFileSink& sink, int per_channel_buffer_size,
                                 const std::vector<TExpr>& t_output_expr)
        : _t_output_expr(t_output_expr), _row_desc(row_desc) {
    CHECK(sink.__isset.file_options);
    _file_opts.reset(new ResultFileOptions(sink.file_options));
    CHECK(sink.__isset.storage_backend_type);
    _storage_type = sink.storage_backend_type;
    _is_top_sink = true;

    _name = "VResultFileSink";
    //for impl csv_with_name and csv_with_names_and_types
    _header_type = sink.header_type;
    _header = sink.header;
}

VResultFileSink::VResultFileSink(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
                                 const TResultFileSink& sink,
                                 const std::vector<TPlanFragmentDestination>& destinations,
                                 int per_channel_buffer_size,
                                 const std::vector<TExpr>& t_output_expr, DescriptorTbl& descs)
        : _t_output_expr(t_output_expr),
          _output_row_descriptor(descs.get_tuple_descriptor(sink.output_tuple_id), false),
          _row_desc(row_desc) {
    CHECK(sink.__isset.file_options);
    _file_opts.reset(new ResultFileOptions(sink.file_options));
    CHECK(sink.__isset.storage_backend_type);
    _storage_type = sink.storage_backend_type;
    _is_top_sink = false;
    CHECK_EQ(destinations.size(), 1);
    _stream_sender.reset(new VDataStreamSender(pool, sender_id, row_desc, sink.dest_node_id,
                                               destinations, per_channel_buffer_size));

    _name = "VResultFileSink";
    //for impl csv_with_name and csv_with_names_and_types
    _header_type = sink.header_type;
    _header = sink.header;
}

Status VResultFileSink::init(const TDataSink& tsink) {
    if (!_is_top_sink) {
        RETURN_IF_ERROR(_stream_sender->init(tsink));
    }
    return Status::OK();
}

Status VResultFileSink::prepare_exprs(RuntimeState* state) {
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    return Status::OK();
}

Status VResultFileSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    std::stringstream title;
    title << "VResultFileSink (fragment_instance_id=" << print_id(state->fragment_instance_id())
          << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    // prepare output_expr
    RETURN_IF_ERROR(prepare_exprs(state));

    CHECK(_file_opts.get() != nullptr);
    if (_is_top_sink) {
        // create sender
        RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
                state->fragment_instance_id(), _buf_size, &_sender, state->enable_pipeline_exec(),
                state->execution_timeout()));
        // create writer
        _writer.reset(new (std::nothrow) VFileResultWriter(
                _file_opts.get(), _storage_type, state->fragment_instance_id(), _output_vexpr_ctxs,
                _profile, _sender.get(), nullptr, state->return_object_data_as_binary(),
                _output_row_descriptor));
    } else {
        // init channel
        _output_block =
                Block::create_unique(_output_row_descriptor.tuple_descriptors()[0]->slots(), 1);
        _writer.reset(new (std::nothrow) VFileResultWriter(
                _file_opts.get(), _storage_type, state->fragment_instance_id(), _output_vexpr_ctxs,
                _profile, nullptr, _output_block.get(), state->return_object_data_as_binary(),
                _output_row_descriptor));
        RETURN_IF_ERROR(_stream_sender->prepare(state));
        _profile->add_child(_stream_sender->profile(), true, nullptr);
    }
    _writer->set_header_info(_header_type, _header);
    RETURN_IF_ERROR(_writer->init(state));
    return Status::OK();
}

Status VResultFileSink::open(RuntimeState* state) {
    if (!_is_top_sink) {
        RETURN_IF_ERROR(_stream_sender->open(state));
    }
    return VExpr::open(_output_vexpr_ctxs, state);
}

Status VResultFileSink::send(RuntimeState* state, Block* block, bool eos) {
    RETURN_IF_ERROR(_writer->append_block(*block));
    return Status::OK();
}

Status VResultFileSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }

    Status final_status = exec_status;
    // close the writer
    if (_writer) {
        Status st = _writer->close();
        if (!st.ok() && exec_status.ok()) {
            // close file writer failed, should return this error to client
            final_status = st;
        }
    }
    if (_is_top_sink) {
        // close sender, this is normal path end
        if (_sender) {
            _sender->update_return_rows(_writer == nullptr ? 0 : _writer->get_written_rows());
            _sender->close(final_status);
        }
        state->exec_env()->result_mgr()->cancel_at_time(
                time(nullptr) + config::result_buffer_cancelled_interval_time,
                state->fragment_instance_id());
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

} // namespace doris::vectorized
