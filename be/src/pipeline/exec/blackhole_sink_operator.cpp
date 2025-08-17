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

#include "blackhole_sink_operator.h"

#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>

#include <sstream>

#include "common/logging.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "util/mysql_row_buffer.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
namespace pipeline {

BlackholeSinkOperatorX::BlackholeSinkOperatorX(
        int operator_id, const int dest_id, const TDataStreamSink& sink,
        const std::vector<TPlanFragmentDestination>& destinations)
        : Base(operator_id, 0, dest_id), _t_data_stream_sink(sink), _destinations(destinations) {}

Status BlackholeSinkOperatorX::prepare(RuntimeState* state) {
    std::shared_ptr<ResultBlockBufferBase> sender_base = nullptr;
    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(
            state->query_id(), 4096, &sender_base, state, false, nullptr));
    _sender = std::dynamic_pointer_cast<vectorized::MySQLResultBlockBuffer>(sender_base);
    return Status::OK();
}

Status BlackholeSinkOperatorX::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSinkOperatorXBase::init(tsink));
    return Status::OK();
}

Status BlackholeSinkOperatorX::sink(RuntimeState* state, vectorized::Block* block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)block->rows());

    if (block && block->rows() > 0) {
        RETURN_IF_ERROR(_process_block(state, block));
    }

    if (eos) {
        _collect_cache_metrics(state, local_state);

        RETURN_IF_ERROR(_send_cache_metrics_batch(state, local_state));
    }

    return Status::OK();
}

Status BlackholeSinkOperatorX::_process_block(RuntimeState* state, vectorized::Block* block) {
    auto& local_state = get_local_state(state);

    // Update metrics - count rows and bytes processed
    local_state._rows_processed += block->rows();
    local_state._bytes_processed += block->bytes();

    // Update runtime counters
    if (local_state._rows_processed_timer) {
        COUNTER_UPDATE(local_state._rows_processed_timer, block->rows());
    }
    if (local_state._bytes_processed_timer) {
        COUNTER_UPDATE(local_state._bytes_processed_timer, block->bytes());
    }

    // The BLACKHOLE discards the data
    // We don't write the block anywhere - it's effectively sent to /dev/null
    return Status::OK();
}

void BlackholeSinkOperatorX::_collect_cache_metrics(RuntimeState* state,
                                                    BlackholeSinkLocalState& local_state) {
    auto io_context = state->get_query_ctx()->resource_ctx()->io_context();
    local_state._scan_rows = io_context->scan_rows();
    local_state._scan_bytes = io_context->scan_bytes();
    local_state._scan_bytes_from_local_storage = io_context->scan_bytes_from_local_storage();
    local_state._scan_bytes_from_remote_storage = io_context->scan_bytes_from_remote_storage();
}

Status BlackholeSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    // Initialize performance counters
    _rows_processed_timer = ADD_COUNTER(custom_profile(), "RowsProcessed", TUnit::UNIT);
    _bytes_processed_timer = ADD_COUNTER(custom_profile(), "BytesProcessed", TUnit::BYTES);

    static const std::string timer_name = "WaitForDependencyTime";
    _wait_for_dependency_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), timer_name, 1);
    auto fragment_instance_id = state->fragment_instance_id();

    _sender = _parent->cast<BlackholeSinkOperatorX>()._sender;

    if (_dependency) {
        _sender->set_dependency(fragment_instance_id, _dependency->shared_from_this());
    } else {
        // Create a fake dependency for blackhole sink
        auto fake_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                         "BlackholeSinkFakeDependency");
        _sender->set_dependency(fragment_instance_id, fake_dependency);
    }

    return Status::OK();
}

Status BlackholeSinkLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(Base::open(state));

    return Status::OK();
}

Status BlackholeSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);

    if (_sender) {
        auto fragment_instance_id = state->fragment_instance_id();
        int64_t processed_rows = _rows_processed;
        Status close_status = _sender->close(fragment_instance_id, exec_status, processed_rows);
        if (!close_status.ok()) {
            LOG(WARNING) << "Failed to close result buffer: " << close_status
                         << ", fragment_instance_id: " << print_id(fragment_instance_id);
        }
    }

    return Base::close(state, exec_status);
}

Status BlackholeSinkOperatorX::close(RuntimeState* state) {
    return Status::OK();
}

Status BlackholeSinkOperatorX::_send_cache_metrics_batch(RuntimeState* state,
                                                         BlackholeSinkLocalState& local_state) {
    if (!local_state._sender) {
        LOG(INFO) << "No result sender available, skipping cache metrics batch";
        return Status::OK();
    }

    MysqlRowBuffer<> row_buffer;

    // Push values for each column
    row_buffer.push_bigint(local_state._rows_processed);
    row_buffer.push_bigint(local_state._bytes_processed);

    // Create the result batch
    auto result = std::make_shared<TFetchDataResult>();
    result->result_batch.rows.resize(1);
    result->result_batch.rows[0].assign(row_buffer.buf(), row_buffer.length());

    // Add attach_infos for additional metadata
    std::map<std::string, std::string> attach_infos;
    attach_infos.insert(std::make_pair("be_id", std::to_string(state->backend_id())));
    attach_infos.insert(std::make_pair("ScanRows", std::to_string(local_state._scan_rows)));
    attach_infos.insert(std::make_pair("ScanBytes", std::to_string(local_state._scan_bytes)));
    attach_infos.insert(std::make_pair("ScanBytesFromLocalStorage",
                                       std::to_string(local_state._scan_bytes_from_local_storage)));
    attach_infos.insert(
            std::make_pair("ScanBytesFromRemoteStorage",
                           std::to_string(local_state._scan_bytes_from_remote_storage)));

    result->result_batch.__set_attached_infos(attach_infos);

    Status status;
    if (local_state._sender) {
        status = local_state._sender->add_batch(state, result);
    } else {
        status = Status::InternalError("Failed to cast sender to MySQLResultBlockBuffer");
    }

    if (!status.ok()) {
        LOG(WARNING) << "Failed to send cache metrics batch to FE: " << status;
        return status;
    }
    return Status::OK();
}

} // namespace pipeline
} // namespace doris
