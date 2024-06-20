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

#include "vec/sink/autoinc_buffer.h"

#include <gen_cpp/HeartbeatService_types.h>

#include <mutex>
#include <string>

#include "common/status.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/runtime_profile.h"
#include "util/thrift_rpc_helper.h"
#include "vec/sink/vtablet_block_convertor.h"

namespace doris::vectorized {

AutoIncIDBuffer::AutoIncIDBuffer(int64_t db_id, int64_t table_id, int64_t column_id)
        : _db_id(db_id),
          _table_id(table_id),
          _column_id(column_id),
          _rpc_token(GlobalAutoIncBuffers::GetInstance()->create_token()) {}

void AutoIncIDBuffer::set_batch_size_at_least(size_t batch_size) {
    if (batch_size > _batch_size) {
        _batch_size = batch_size;
    }
}

Result<int64_t> AutoIncIDBuffer::_fetch_ids_from_fe(size_t length) {
    constexpr uint32_t FETCH_AUTOINC_MAX_RETRY_TIMES = 3;
    _rpc_status = Status::OK();
    for (uint32_t retry_times = 0; retry_times < FETCH_AUTOINC_MAX_RETRY_TIMES; retry_times++) {
        TAutoIncrementRangeRequest request;
        TAutoIncrementRangeResult result;
        TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
        request.__set_db_id(_db_id);
        request.__set_table_id(_table_id);
        request.__set_column_id(_column_id);
        request.__set_length(length);

        int64_t get_auto_inc_range_rpc_ns = 0;
        {
            SCOPED_RAW_TIMER(&get_auto_inc_range_rpc_ns);
            _rpc_status = ThriftRpcHelper::rpc<FrontendServiceClient>(
                    master_addr.hostname, master_addr.port,
                    [&request, &result](FrontendServiceConnection& client) {
                        client->getAutoIncrementRange(result, request);
                    });
        }
        LOG(INFO) << "[auto-inc-range][start=" << result.start << ",length=" << result.length
                  << "][elapsed=" << get_auto_inc_range_rpc_ns / 1000000 << " ms]";

        if (!_rpc_status.ok()) {
            LOG(WARNING)
                    << "Failed to fetch auto-incremnt range, encounter rpc failure. retry_time="
                    << retry_times << ", errmsg=" << _rpc_status.to_string();
            continue;
        }
        if (result.length != length) [[unlikely]] {
            _rpc_status = Status::RpcError<true>(
                    "Failed to fetch auto-incremnt range, request length={}, but get "
                    "result.length={}, retry_time={}",
                    length, result.length, retry_times);
            continue;
        }

        return result.start;
    }
    CHECK(!_rpc_status.ok());
    return _rpc_status;
}

void AutoIncIDBuffer::_get_autoinc_ranges_from_buffers(
        size_t& request_length, std::vector<std::pair<int64_t, size_t>>* result) {
    std::lock_guard<std::mutex> lock {_latch};
    while (request_length > 0 && !_buffers.empty()) {
        auto& autoinc_range = _buffers.front();
        CHECK_GT(autoinc_range.length, 0);
        auto min_length = std::min(request_length, autoinc_range.length);
        result->emplace_back(autoinc_range.start, min_length);
        autoinc_range.consume(min_length);
        request_length -= min_length;
        _current_volume -= min_length;
        if (autoinc_range.empty()) {
            _buffers.pop_front();
        }
    }
}

Status AutoIncIDBuffer::sync_request_ids(size_t request_length,
                                         std::vector<std::pair<int64_t, size_t>>* result) {
    std::unique_lock<std::mutex> lock(_mutex);
    int current = current_volume();
    if (current < request_length) {
        // ids from _buffers is NOT sufficient for current request,
        // first, we wait for any potential asynchronous fetch task
        if (_is_fetching) {
            _rpc_token->wait();
        }
        if (!_rpc_status.ok()) {
            return _rpc_status;
        }
        // then try to feed the request_length
        _get_autoinc_ranges_from_buffers(request_length, result);
        // If ids from buffers is still not enough for request, we should fetch ids from fe synchronously.
        // After that, if current_volume <= low_water_level_mark, we should launch an asynchronous fetch task.
        // However, in order to reduce latency, we combine these two RPCs into one

        // it's guarenteed that there's no background task here, so no need to lock _latch
        if (request_length > 0 || _current_volume <= _low_water_level_mark()) {
            int64_t remained_length = _prefetch_size();
            int64_t start = DORIS_TRY(_fetch_ids_from_fe(request_length + remained_length));
            if (request_length > 0) {
                result->emplace_back(start, request_length);
            }
            start += request_length;
            _buffers.emplace_back(start, remained_length);
            _current_volume += remained_length;
        }

    } else if (current <= _low_water_level_mark()) {
        // ids from _buffers is sufficient for current request,
        // but current_volume <= low_water_level_mark, need to launch an asynchronous fetch task if no such task exists
        _get_autoinc_ranges_from_buffers(request_length, result);
        CHECK_EQ(request_length, 0);
        if (!_is_fetching) {
            RETURN_IF_ERROR(_launch_async_fetch_task(_prefetch_size()));
        }
    } else {
        // ids from _buffers is sufficient for current request,
        // and current_volume > low_water_level_mark, no need to launch asynchronous fetch task
        _get_autoinc_ranges_from_buffers(request_length, result);
        CHECK_EQ(request_length, 0);
    }
    return Status::OK();
}

Status AutoIncIDBuffer::_launch_async_fetch_task(size_t length) {
    RETURN_IF_ERROR(_rpc_token->submit_func([=, this]() {
        auto&& res = _fetch_ids_from_fe(length);
        if (!res.has_value()) [[unlikely]] {
            return;
        }
        int64_t start = res.value();
        {
            std::lock_guard<std::mutex> lock {_latch};
            _buffers.emplace_back(start, length);
            _current_volume += length;
        }
        _is_fetching = false;
    }));
    _is_fetching = true;
    return Status::OK();
}

} // namespace doris::vectorized