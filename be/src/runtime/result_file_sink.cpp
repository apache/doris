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

#include "runtime/result_file_sink.h"

#include "common/config.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "runtime/exec_env.h"
#include "runtime/file_result_writer.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"

namespace doris {

ResultFileSink::ResultFileSink(const RowDescriptor& row_desc,
                               const std::vector<TExpr>& t_output_expr, const TResultFileSink& sink)
        : DataStreamSender(nullptr, 0, row_desc), _t_output_expr(t_output_expr) {
    CHECK(sink.__isset.file_options);
    _file_opts.reset(new ResultFileOptions(sink.file_options));
    CHECK(sink.__isset.storage_backend_type);
    _storage_type = sink.storage_backend_type;
    _is_top_sink = true;

    _name = "ResultFileSink";
    //for impl csv_with_name and csv_with_names_and_types
    _header_type = sink.header_type;
    _header = sink.header;
}

ResultFileSink::ResultFileSink(const RowDescriptor& row_desc,
                               const std::vector<TExpr>& t_output_expr, const TResultFileSink& sink,
                               const std::vector<TPlanFragmentDestination>& destinations,
                               ObjectPool* pool, int sender_id, DescriptorTbl& descs)
        : DataStreamSender(pool, sender_id, row_desc),
          _t_output_expr(t_output_expr),
          _output_row_descriptor(descs.get_tuple_descriptor(sink.output_tuple_id), false) {
    CHECK(sink.__isset.file_options);
    _file_opts.reset(new ResultFileOptions(sink.file_options));
    CHECK(sink.__isset.storage_backend_type);
    _storage_type = sink.storage_backend_type;
    _is_top_sink = false;
    DCHECK_EQ(destinations.size(), 1);
    _channel_shared_ptrs.emplace_back(new Channel(
            this, _output_row_descriptor, destinations[0].brpc_server,
            destinations[0].fragment_instance_id, sink.dest_node_id, _buf_size, true, true));
    _channels.push_back(_channel_shared_ptrs.back().get());

    _name = "ResultFileSink";
    //for impl csv_with_name and csv_with_names_and_types
    _header_type = sink.header_type;
    _header = sink.header;
}

ResultFileSink::~ResultFileSink() {
    if (_output_batch != nullptr) {
        delete _output_batch;
    }
}

Status ResultFileSink::init(const TDataSink& tsink) {
    return Status::OK();
}

Status ResultFileSink::prepare_exprs(RuntimeState* state) {
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state, _row_desc));
    return Status::OK();
}

Status ResultFileSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    std::stringstream title;
    title << "DataBufferSender (dst_fragment_instance_id="
          << print_id(state->fragment_instance_id()) << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    // prepare output_expr
    RETURN_IF_ERROR(prepare_exprs(state));

    CHECK(_file_opts.get() != nullptr);
    if (_is_top_sink) {
        // create sender
        RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
                state->fragment_instance_id(), _buf_size, &_sender));
        // create writer
        _writer.reset(new (std::nothrow) FileResultWriter(
                _file_opts.get(), _storage_type, state->fragment_instance_id(), _output_expr_ctxs,
                _profile, _sender.get(), nullptr, state->return_object_data_as_binary()));
    } else {
        // init channel
        _profile = _pool->add(new RuntimeProfile(title.str()));
        _state = state;
        _serialize_batch_timer = ADD_TIMER(profile(), "SerializeBatchTime");
        _bytes_sent_counter = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
        _local_bytes_send_counter = ADD_COUNTER(profile(), "LocalBytesSent", TUnit::BYTES);
        _uncompressed_bytes_counter =
                ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
        // create writer
        _output_batch = new RowBatch(_output_row_descriptor, 1024);
        _writer.reset(new (std::nothrow) FileResultWriter(
                _file_opts.get(), _storage_type, state->fragment_instance_id(), _output_expr_ctxs,
                _profile, nullptr, _output_batch, state->return_object_data_as_binary()));
    }
    _writer->set_header_info(_header_type, _header);
    RETURN_IF_ERROR(_writer->init(state));
    for (int i = 0; i < _channels.size(); ++i) {
        RETURN_IF_ERROR(_channels[i]->init(state));
    }
    return Status::OK();
}

Status ResultFileSink::open(RuntimeState* state) {
    return Expr::open(_output_expr_ctxs, state);
}

Status ResultFileSink::send(RuntimeState* state, RowBatch* batch) {
    RETURN_IF_ERROR(_writer->append_row_batch(batch));
    return Status::OK();
}

Status ResultFileSink::close(RuntimeState* state, Status exec_status) {
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
            _sender->update_num_written_rows(_writer == nullptr ? 0 : _writer->get_written_rows());
            _sender->close(final_status);
        }
        state->exec_env()->result_mgr()->cancel_at_time(
                time(nullptr) + config::result_buffer_cancelled_interval_time,
                state->fragment_instance_id());
    } else {
        if (final_status.ok()) {
            RETURN_IF_ERROR(serialize_batch(_output_batch, _cur_pb_batch, _channels.size()));
            for (auto channel : _channels) {
                RETURN_IF_ERROR(channel->send_batch(_cur_pb_batch));
            }
        }
        Status final_st = Status::OK();
        for (int i = 0; i < _channels.size(); ++i) {
            Status st = _channels[i]->close(state);
            if (!st.ok() && final_st.ok()) {
                final_st = st;
            }
        }
        // wait all channels to finish
        for (int i = 0; i < _channels.size(); ++i) {
            Status st = _channels[i]->close_wait(state);
            if (!st.ok() && final_st.ok()) {
                final_st = st;
            }
        }
        // release row batch
        _output_batch->reset();
    }

    Expr::close(_output_expr_ctxs, state);

    _closed = true;
    return Status::OK();
}

void ResultFileSink::set_query_statistics(std::shared_ptr<QueryStatistics> statistics) {
    if (_is_top_sink) {
        _sender->set_query_statistics(statistics);
    } else {
        _query_statistics = statistics;
    }
}

} // namespace doris
