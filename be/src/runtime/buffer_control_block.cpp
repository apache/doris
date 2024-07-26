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

#include "runtime/buffer_control_block.h"

#include <gen_cpp/Data_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <limits>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/record_batch.h"
#include "arrow/type_fwd.h"
#include "pipeline/dependency.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/thrift_util.h"

namespace doris {

void GetResultBatchCtx::on_failure(const Status& status) {
    DCHECK(!status.ok()) << "status is ok, errmsg=" << status;
    status.to_protobuf(result->mutable_status());
    {
        // call by result sink
        done->Run();
    }
    delete this;
}

void GetResultBatchCtx::on_close(int64_t packet_seq, QueryStatistics* statistics) {
    Status status;
    status.to_protobuf(result->mutable_status());
    if (statistics != nullptr) {
        statistics->to_pb(result->mutable_query_statistics());
    }
    result->set_packet_seq(packet_seq);
    result->set_eos(true);
    { done->Run(); }
    delete this;
}

void GetResultBatchCtx::on_data(const std::unique_ptr<TFetchDataResult>& t_result,
                                int64_t packet_seq, bool eos) {
    Status st = Status::OK();
    if (t_result != nullptr) {
        uint8_t* buf = nullptr;
        uint32_t len = 0;
        ThriftSerializer ser(false, 4096);
        st = ser.serialize(&t_result->result_batch, &len, &buf);
        if (st.ok()) {
            result->set_row_batch(std::string((const char*)buf, len));
            result->set_packet_seq(packet_seq);
            result->set_eos(eos);
        } else {
            LOG(WARNING) << "TFetchDataResult serialize failed, errmsg=" << st;
        }
    } else {
        result->set_empty_batch(true);
        result->set_packet_seq(packet_seq);
        result->set_eos(eos);
    }

    /// The size limit of proto buffer message is 2G
    if (result->ByteSizeLong() > std::numeric_limits<int32_t>::max()) {
        st = Status::InternalError("Message size exceeds 2GB: {}", result->ByteSizeLong());
        result->clear_row_batch();
        result->set_empty_batch(true);
    }
    st.to_protobuf(result->mutable_status());
    { done->Run(); }
    delete this;
}

BufferControlBlock::BufferControlBlock(const TUniqueId& id, int buffer_size, int batch_size)
        : _fragment_id(id),
          _is_close(false),
          _is_cancelled(false),
          _buffer_rows(0),
          _buffer_limit(buffer_size),
          _packet_num(0),
          _batch_size(batch_size) {
    _query_statistics = std::make_unique<QueryStatistics>();
}

BufferControlBlock::~BufferControlBlock() {
    cancel();
}

Status BufferControlBlock::init() {
    return Status::OK();
}

Status BufferControlBlock::add_batch(RuntimeState* state,
                                     std::unique_ptr<TFetchDataResult>& result) {
    std::unique_lock<std::mutex> l(_lock);

    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    int num_rows = result->result_batch.rows.size();
    if (_waiting_rpc.empty()) {
        // Merge result into batch to reduce rpc times
        if (!_fe_result_batch_queue.empty() &&
            ((_fe_result_batch_queue.back()->result_batch.rows.size() + num_rows) <
             _buffer_limit) &&
            !result->eos) {
            std::vector<std::string>& back_rows = _fe_result_batch_queue.back()->result_batch.rows;
            std::vector<std::string>& result_rows = result->result_batch.rows;
            back_rows.insert(back_rows.end(), std::make_move_iterator(result_rows.begin()),
                             std::make_move_iterator(result_rows.end()));
        } else {
            _instance_rows_in_queue.emplace_back();
            _fe_result_batch_queue.push_back(std::move(result));
        }
        _buffer_rows += num_rows;
        _instance_rows[state->fragment_instance_id()] += num_rows;
        _instance_rows_in_queue.back()[state->fragment_instance_id()] += num_rows;
    } else {
        auto* ctx = _waiting_rpc.front();
        _waiting_rpc.pop_front();
        ctx->on_data(result, _packet_num);
        _packet_num++;
    }

    _update_dependency();
    return Status::OK();
}

Status BufferControlBlock::add_arrow_batch(RuntimeState* state,
                                           std::shared_ptr<arrow::RecordBatch>& result) {
    std::unique_lock<std::mutex> l(_lock);

    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    int num_rows = result->num_rows();

    // TODO: merge RocordBatch, ToStructArray -> Make again

    _arrow_flight_batch_queue.push_back(std::move(result));
    _buffer_rows += num_rows;
    _instance_rows_in_queue.emplace_back();
    _instance_rows[state->fragment_instance_id()] += num_rows;
    _instance_rows_in_queue.back()[state->fragment_instance_id()] += num_rows;
    _arrow_data_arrival.notify_one();
    _update_dependency();
    return Status::OK();
}

void BufferControlBlock::get_batch(GetResultBatchCtx* ctx) {
    std::lock_guard<std::mutex> l(_lock);
    if (!_status.ok()) {
        ctx->on_failure(_status);
        _update_dependency();
        return;
    }
    if (_is_cancelled) {
        ctx->on_failure(Status::Cancelled("Cancelled"));
        _update_dependency();
        return;
    }
    if (!_fe_result_batch_queue.empty()) {
        // get result
        std::unique_ptr<TFetchDataResult> result = std::move(_fe_result_batch_queue.front());
        _fe_result_batch_queue.pop_front();
        _buffer_rows -= result->result_batch.rows.size();
        for (auto it : _instance_rows_in_queue.front()) {
            _instance_rows[it.first] -= it.second;
        }
        _instance_rows_in_queue.pop_front();

        ctx->on_data(result, _packet_num);
        _packet_num++;
        _update_dependency();
        return;
    }
    if (_is_close) {
        ctx->on_close(_packet_num, _query_statistics.get());
        _update_dependency();
        return;
    }
    // no ready data, push ctx to waiting list
    _waiting_rpc.push_back(ctx);
    _update_dependency();
}

Status BufferControlBlock::get_arrow_batch(std::shared_ptr<arrow::RecordBatch>* result) {
    std::unique_lock<std::mutex> l(_lock);
    if (!_status.ok()) {
        return _status;
    }
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    while (_arrow_flight_batch_queue.empty() && !_is_cancelled && !_is_close) {
        _arrow_data_arrival.wait_for(l, std::chrono::seconds(1));
    }

    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    if (!_arrow_flight_batch_queue.empty()) {
        *result = std::move(_arrow_flight_batch_queue.front());
        _arrow_flight_batch_queue.pop_front();
        _buffer_rows -= (*result)->num_rows();
        for (auto it : _instance_rows_in_queue.front()) {
            _instance_rows[it.first] -= it.second;
        }
        _instance_rows_in_queue.pop_front();
        _packet_num++;
        _update_dependency();
        return Status::OK();
    }

    // normal path end
    if (_is_close) {
        _update_dependency();
        return Status::OK();
    }
    return Status::InternalError("Get Arrow Batch Abnormal Ending");
}

Status BufferControlBlock::close(const TUniqueId& id, Status exec_status) {
    std::unique_lock<std::mutex> l(_lock);
    auto it = _result_sink_dependencys.find(id);
    if (it != _result_sink_dependencys.end()) {
        it->second->set_always_ready();
        _result_sink_dependencys.erase(it);
    }
    if (!_result_sink_dependencys.empty()) {
        return Status::OK();
    }

    _is_close = true;
    _status = exec_status;
    _arrow_data_arrival.notify_all();

    if (!_waiting_rpc.empty()) {
        if (_status.ok()) {
            for (auto& ctx : _waiting_rpc) {
                ctx->on_close(_packet_num, _query_statistics.get());
            }
        } else {
            for (auto& ctx : _waiting_rpc) {
                ctx->on_failure(_status);
            }
        }
        _waiting_rpc.clear();
    }
    return Status::OK();
}

void BufferControlBlock::cancel() {
    std::unique_lock<std::mutex> l(_lock);
    _is_cancelled = true;
    _arrow_data_arrival.notify_all();
    for (auto& ctx : _waiting_rpc) {
        ctx->on_failure(Status::Cancelled("Cancelled"));
    }
    _waiting_rpc.clear();
    _update_dependency();
}

void BufferControlBlock::set_dependency(
        const TUniqueId& id, std::shared_ptr<pipeline::Dependency> result_sink_dependency) {
    std::unique_lock<std::mutex> l(_lock);
    _result_sink_dependencys[id] = result_sink_dependency;
    _update_dependency();
}

void BufferControlBlock::_update_dependency() {
    if (_is_cancelled) {
        for (auto it : _result_sink_dependencys) {
            it.second->set_ready();
        }
        return;
    }

    for (auto it : _result_sink_dependencys) {
        if (_instance_rows[it.first] > _batch_size) {
            it.second->block();
        } else {
            it.second->set_ready();
        }
    }
}

} // namespace doris
