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

#include "runtime/result_block_buffer.h"

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

#include "arrow/type_fwd.h"
#include "common/config.h"
#include "pipeline/dependency.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "util/thrift_util.h"
#include "vec/core/block.h"
#include "vec/sink/varrow_flight_result_writer.h"
#include "vec/sink/vmysql_result_writer.h"

namespace doris {

template <typename ResultCtxType>
ResultBlockBuffer<ResultCtxType>::ResultBlockBuffer(TUniqueId id, RuntimeState* state,
                                                    int buffer_size)
        : _fragment_id(std::move(id)),
          _is_close(false),
          _batch_size(state->batch_size()),
          _timezone(state->timezone()),
          _be_exec_version(state->be_exec_version()),
          _fragment_transmission_compression_type(state->fragement_transmission_compression_type()),
          _buffer_limit(buffer_size) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::QUERY,
            fmt::format("ResultBlockBuffer#FragmentInstanceId={}", print_id(_fragment_id)));
}

template <typename ResultCtxType>
Status ResultBlockBuffer<ResultCtxType>::close(const TUniqueId& id, Status exec_status,
                                               int64_t num_rows) {
    std::unique_lock<std::mutex> l(_lock);
    _returned_rows.fetch_add(num_rows);
    // close will be called multiple times and error status needs to be collected.
    if (!exec_status.ok()) {
        _status = exec_status;
    }

    auto it = _result_sink_dependencies.find(id);
    if (it != _result_sink_dependencies.end()) {
        it->second->set_always_ready();
        _result_sink_dependencies.erase(it);
    } else {
        _status = Status::InternalError("Instance {} is not found in ResultBlockBuffer",
                                        print_id(id));
    }
    if (!_result_sink_dependencies.empty()) {
        return _status;
    }

    _is_close = true;
    _arrow_data_arrival.notify_all();

    if (!_waiting_rpc.empty()) {
        if (_status.ok()) {
            for (auto& ctx : _waiting_rpc) {
                ctx->on_close(_packet_num, _returned_rows);
            }
        } else {
            for (auto& ctx : _waiting_rpc) {
                ctx->on_failure(_status);
            }
        }
        _waiting_rpc.clear();
    }

    return _status;
}

template <typename ResultCtxType>
void ResultBlockBuffer<ResultCtxType>::cancel(const Status& reason) {
    std::unique_lock<std::mutex> l(_lock);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    if (_status.ok()) {
        _status = reason;
    }
    _arrow_data_arrival.notify_all();
    for (auto& ctx : _waiting_rpc) {
        ctx->on_failure(reason);
    }
    _waiting_rpc.clear();
    _update_dependency();
    _result_batch_queue.clear();
}

template <typename ResultCtxType>
void ResultBlockBuffer<ResultCtxType>::set_dependency(
        const TUniqueId& id, std::shared_ptr<pipeline::Dependency> result_sink_dependency) {
    std::unique_lock<std::mutex> l(_lock);
    _result_sink_dependencies[id] = result_sink_dependency;
    _update_dependency();
}

template <typename ResultCtxType>
void ResultBlockBuffer<ResultCtxType>::_update_dependency() {
    if (!_status.ok()) {
        for (auto it : _result_sink_dependencies) {
            it.second->set_ready();
        }
        return;
    }

    for (auto it : _result_sink_dependencies) {
        if (_instance_rows[it.first] > _batch_size) {
            it.second->block();
        } else {
            it.second->set_ready();
        }
    }
}

template <typename ResultCtxType>
Status ResultBlockBuffer<ResultCtxType>::get_batch(std::shared_ptr<ResultCtxType> ctx) {
    std::lock_guard<std::mutex> l(_lock);
    SCOPED_ATTACH_TASK(_mem_tracker);
    Defer defer {[&]() { _update_dependency(); }};
    if (!_status.ok()) {
        ctx->on_failure(_status);
        return _status;
    }
    if (!_result_batch_queue.empty()) {
        auto result = _result_batch_queue.front();
        _result_batch_queue.pop_front();
        for (auto it : _instance_rows_in_queue.front()) {
            _instance_rows[it.first] -= it.second;
        }
        _instance_rows_in_queue.pop_front();
        RETURN_IF_ERROR(ctx->on_data(result, _packet_num, this));
        _packet_num++;
        return Status::OK();
    }
    if (_is_close) {
        if (!_status.ok()) {
            ctx->on_failure(_status);
            return Status::OK();
        }
        ctx->on_close(_packet_num, _returned_rows);
        LOG(INFO) << fmt::format(
                "ResultBlockBuffer finished, fragment_id={}, is_close={}, is_cancelled={}, "
                "packet_num={}, peak_memory_usage={}",
                print_id(_fragment_id), _is_close, !_status.ok(), _packet_num,
                _mem_tracker->peak_consumption());
        return Status::OK();
    }
    // no ready data, push ctx to waiting list
    _waiting_rpc.push_back(ctx);
    return Status::OK();
}

template <typename ResultCtxType>
Status ResultBlockBuffer<ResultCtxType>::add_batch(RuntimeState* state,
                                                   std::shared_ptr<InBlockType>& result) {
    std::unique_lock<std::mutex> l(_lock);

    if (!_status.ok()) {
        return _status;
    }

    if (_waiting_rpc.empty()) {
        auto sz = 0;
        auto num_rows = 0;
        size_t batch_size = 0;
        if constexpr (std::is_same_v<InBlockType, vectorized::Block>) {
            num_rows = result->rows();
            batch_size = result->bytes();
        } else if constexpr (std::is_same_v<InBlockType, TFetchDataResult>) {
            num_rows = result->result_batch.rows.size();
            for (const auto& row : result->result_batch.rows) {
                batch_size += row.size();
            }
        }
        if (!_result_batch_queue.empty()) {
            if constexpr (std::is_same_v<InBlockType, vectorized::Block>) {
                sz = _result_batch_queue.back()->rows();
            } else if constexpr (std::is_same_v<InBlockType, TFetchDataResult>) {
                sz = _result_batch_queue.back()->result_batch.rows.size();
            }
            if (sz + num_rows < _buffer_limit &&
                (batch_size + _last_batch_bytes) <= config::thrift_max_message_size) {
                if constexpr (std::is_same_v<InBlockType, vectorized::Block>) {
                    auto last_block = _result_batch_queue.back();
                    for (size_t i = 0; i < last_block->columns(); i++) {
                        last_block->mutate_columns()[i]->insert_range_from(
                                *result->get_by_position(i).column, 0, num_rows);
                    }
                } else {
                    std::vector<std::string>& back_rows =
                            _result_batch_queue.back()->result_batch.rows;
                    std::vector<std::string>& result_rows = result->result_batch.rows;
                    back_rows.insert(back_rows.end(), std::make_move_iterator(result_rows.begin()),
                                     std::make_move_iterator(result_rows.end()));
                }
                _last_batch_bytes += batch_size;
            } else {
                _instance_rows_in_queue.emplace_back();
                _result_batch_queue.push_back(std::move(result));
                _last_batch_bytes = batch_size;
                _arrow_data_arrival
                        .notify_one(); // Only valid for get_arrow_batch(std::shared_ptr<vectorized::Block>,)
            }
        } else {
            _instance_rows_in_queue.emplace_back();
            _result_batch_queue.push_back(std::move(result));
            _last_batch_bytes = batch_size;
            _arrow_data_arrival
                    .notify_one(); // Only valid for get_arrow_batch(std::shared_ptr<vectorized::Block>,)
        }
        _instance_rows[state->fragment_instance_id()] += num_rows;
        _instance_rows_in_queue.back()[state->fragment_instance_id()] += num_rows;
    } else {
        auto ctx = _waiting_rpc.front();
        _waiting_rpc.pop_front();
        RETURN_IF_ERROR(ctx->on_data(result, _packet_num, this));
        _packet_num++;
    }

    _update_dependency();
    return Status::OK();
}

template class ResultBlockBuffer<vectorized::GetArrowResultBatchCtx>;
template class ResultBlockBuffer<vectorized::GetResultBatchCtx>;

} // namespace doris
