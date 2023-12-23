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
#include "runtime/buffer_control_block.h"
#include "runtime/result_buffer_mgr.h"
#include "vec/sink/vdata_stream_sender.h"
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
        : AsyncWriterSink<vectorized::VFileResultWriter, ResultFileSinkOperatorX>(parent, state),
          _serializer(this) {}

ResultFileSinkOperatorX::ResultFileSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                                                 const std::vector<TExpr>& t_output_expr)
        : DataSinkOperatorX(operator_id, 0),
          _row_desc(row_desc),
          _t_output_expr(t_output_expr),
          _is_top_sink(true) {}

ResultFileSinkOperatorX::ResultFileSinkOperatorX(
        int operator_id, const RowDescriptor& row_desc, const TResultFileSink& sink,
        const std::vector<TPlanFragmentDestination>& destinations,
        bool send_query_statistics_with_every_batch, const std::vector<TExpr>& t_output_expr,
        DescriptorTbl& descs)
        : DataSinkOperatorX(operator_id, 0),
          _row_desc(row_desc),
          _t_output_expr(t_output_expr),
          _dests(destinations),
          _send_query_statistics_with_every_batch(send_query_statistics_with_every_batch),
          _output_row_descriptor(descs.get_tuple_descriptor(sink.output_tuple_id), false),
          _is_top_sink(false) {
    CHECK_EQ(destinations.size(), 1);
}

Status ResultFileSinkOperatorX::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSinkOperatorX<ResultFileSinkLocalState>::init(tsink));
    auto& sink = tsink.result_file_sink;
    CHECK(sink.__isset.file_options);
    _file_opts.reset(new vectorized::ResultFileOptions(sink.file_options));
    CHECK(sink.__isset.storage_backend_type);
    _storage_type = sink.storage_backend_type;

    //for impl csv_with_name and csv_with_names_and_types
    _header_type = sink.header_type;
    _header = sink.header;

    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));

    return Status::OK();
}

Status ResultFileSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<ResultFileSinkLocalState>::prepare(state));
    return vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc);
}

Status ResultFileSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<ResultFileSinkLocalState>::open(state));
    return vectorized::VExpr::open(_output_vexpr_ctxs, state);
}

Status ResultFileSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    _sender_id = info.sender_id;

    _brpc_wait_timer = ADD_TIMER(_profile, "BrpcSendTime.Wait");
    _local_send_timer = ADD_TIMER(_profile, "LocalSendTime");
    _brpc_send_timer = ADD_TIMER(_profile, "BrpcSendTime");
    _split_block_distribute_by_channel_timer =
            ADD_TIMER(_profile, "SplitBlockDistributeByChannelTime");
    _brpc_send_timer = ADD_TIMER(_profile, "BrpcSendTime");
    auto& p = _parent->cast<ResultFileSinkOperatorX>();
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

        std::map<int64_t, int64_t> fragment_id_to_channel_index;
        for (int i = 0; i < p._dests.size(); ++i) {
            _channels.push_back(new vectorized::Channel(
                    this, p._row_desc, p._dests[i].brpc_server, state->fragment_instance_id(),
                    info.tsink.result_file_sink.dest_node_id, false,
                    p._send_query_statistics_with_every_batch));
        }
        std::random_device rd;
        std::mt19937 g(rd());
        shuffle(_channels.begin(), _channels.end(), g);

        int local_size = 0;
        for (int i = 0; i < _channels.size(); ++i) {
            RETURN_IF_ERROR(_channels[i]->init(state));
            if (_channels[i]->is_local()) {
                local_size++;
            }
        }
        _only_local_exchange = local_size == _channels.size();
    }
    _writer->set_dependency(_async_writer_dependency.get(), _finish_dependency.get());
    _writer->set_header_info(p._header_type, p._header);
    return Status::OK();
}

Status ResultFileSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    return Base::open(state);
}

Status ResultFileSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (Base::_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(_close_timer);
    SCOPED_TIMER(exec_time_counter());

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
            static_cast<void>(_sender->close(final_status));
        }
        static_cast<void>(state->exec_env()->result_mgr()->cancel_at_time(
                time(nullptr) + config::result_buffer_cancelled_interval_time,
                state->fragment_instance_id()));
    } else {
        if (final_status.ok()) {
            bool all_receiver_eof = true;
            for (auto channel : _channels) {
                if (!channel->is_receiver_eof()) {
                    all_receiver_eof = false;
                    break;
                }
            }
            if (all_receiver_eof) {
                return Status::EndOfFile("all data stream channels EOF");
            }
            // 1. serialize depends on it is not local exchange
            // 2. send block
            // 3. rollover block
            if (_only_local_exchange) {
                if (!_output_block->empty()) {
                    Status status;
                    for (auto channel : _channels) {
                        if (!channel->is_receiver_eof()) {
                            status = channel->send_local_block(_output_block.get());
                            HANDLE_CHANNEL_STATUS(state, channel, status);
                        }
                    }
                }
            } else {
                {
                    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                    bool serialized = false;
                    RETURN_IF_ERROR(_serializer.next_serialized_block(
                            _output_block.get(), _block_holder->get_block(), _channels.size(),
                            &serialized, true));
                    if (serialized) {
                        auto cur_block = _serializer.get_block()->to_block();
                        if (!cur_block.empty()) {
                            RETURN_IF_ERROR(_serializer.serialize_block(
                                    &cur_block, _block_holder->get_block(), _channels.size()));
                        } else {
                            _block_holder->get_block()->Clear();
                        }
                        Status status;
                        for (auto channel : _channels) {
                            if (!channel->is_receiver_eof()) {
                                if (channel->is_local()) {
                                    status = channel->send_local_block(&cur_block);
                                } else {
                                    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
                                    status = channel->send_broadcast_block(_block_holder, true);
                                }
                                HANDLE_CHANNEL_STATUS(state, channel, status);
                            }
                        }
                        cur_block.clear_column_data();
                        _serializer.get_block()->set_mutable_columns(cur_block.mutate_columns());
                    }
                }
            }
        }
        _output_block->clear();
    }

    return Base::close(state, exec_status);
}

template <typename ChannelPtrType>
void ResultFileSinkLocalState::_handle_eof_channel(RuntimeState* state, ChannelPtrType channel,
                                                   Status st) {
    channel->set_receiver_eof(st);
    // Chanel will not send RPC to the downstream when eof, so close chanel by OK status.
    static_cast<void>(channel->close(state, Status::OK()));
}

Status ResultFileSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                     SourceState source_state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    return local_state.sink(state, in_block, source_state);
}

} // namespace doris::pipeline
