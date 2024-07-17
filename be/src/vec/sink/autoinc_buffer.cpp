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

#include <chrono>
#include <mutex>

#include "common/logging.h"
#include "common/status.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/runtime_profile.h"
#include "util/thrift_rpc_helper.h"

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
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    for (uint32_t retry_times = 0; retry_times < FETCH_AUTOINC_MAX_RETRY_TIMES; retry_times++) {
        TAutoIncrementRangeRequest request;
        TAutoIncrementRangeResult result;
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

        if (_rpc_status.is<ErrorCode::NOT_MASTER>()) {
            LOG_WARNING(
                    "Failed to fetch auto-incremnt range, requested to non-master FE@{}:{}, change "
                    "to request to FE@{}:{}. retry_time={}, db_id={}, table_id={}, column_id={}",
                    master_addr.hostname, master_addr.port, result.master_address.hostname,
                    result.master_address.port, retry_times, _db_id, _table_id, _column_id);
            master_addr = result.master_address;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        if (!_rpc_status.ok()) {
            LOG_WARNING(
                    "Failed to fetch auto-incremnt range, encounter rpc failure. "
                    "errmsg={}, retry_time={}, db_id={}, table_id={}, column_id={}",
                    _rpc_status.to_string(), retry_times, _db_id, _table_id, _column_id);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        if (result.length != length) [[unlikely]] {
            auto msg = fmt::format(
                    "Failed to fetch auto-incremnt range, request length={}, but get "
                    "result.length={}, retry_time={}, db_id={}, table_id={}, column_id={}",
                    length, result.length, retry_times, _db_id, _table_id, _column_id);
            LOG(WARNING) << msg;
            _rpc_status = Status::RpcError<true>(msg);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }

        LOG_INFO(
                "get auto-incremnt range from FE@{}:{}, start={}, length={}, elapsed={}ms, "
                "retry_time={}, db_id={}, table_id={}, column_id={}",
                master_addr.hostname, master_addr.port, result.start, result.length,
                get_auto_inc_range_rpc_ns / 1000000, retry_times, _db_id, _table_id, _column_id);
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
        _current_volume -= min_length;
        request_length -= min_length;
        if (autoinc_range.empty()) {
            _buffers.pop_front();
        }
    }
}

Status AutoIncIDBuffer::sync_request_ids(size_t request_length,
                                         std::vector<std::pair<int64_t, size_t>>* result) {
    std::lock_guard<std::mutex> lock(_mutex);
    while (request_length > 0) {
        _get_autoinc_ranges_from_buffers(request_length, result);
        if (request_length == 0) {
            break;
        }
        if (!_is_fetching) {
            RETURN_IF_ERROR(
                    _launch_async_fetch_task(std::max<size_t>(request_length, _prefetch_size())));
        }
        _rpc_token->wait();
        CHECK(!_is_fetching);
        if (!_rpc_status.ok()) {
            return _rpc_status;
        }
    }
    CHECK_EQ(request_length, 0);
    if (!_is_fetching && _current_volume < _low_water_level_mark()) {
        RETURN_IF_ERROR(_launch_async_fetch_task(_prefetch_size()));
    }
    return Status::OK();
}

Status AutoIncIDBuffer::_launch_async_fetch_task(size_t length) {
    _is_fetching = true;
    RETURN_IF_ERROR(_rpc_token->submit_func([=, this]() {
        auto&& res = _fetch_ids_from_fe(length);
        if (!res.has_value()) [[unlikely]] {
            _is_fetching = false;
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
    return Status::OK();
}

} // namespace doris::vectorized