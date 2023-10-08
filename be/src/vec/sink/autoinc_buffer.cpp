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

void AutoIncIDBuffer::_wait_for_prefetching() {
    if (_is_fetching) {
        _rpc_token->wait();
    }
}

Status AutoIncIDBuffer::sync_request_ids(size_t length,
                                         std::vector<std::pair<int64_t, size_t>>* result) {
    std::unique_lock<std::mutex> lock(_mutex);
    _prefetch_ids(_prefetch_size());
    if (_front_buffer.second > 0) {
        auto min_length = std::min(_front_buffer.second, length);
        length -= min_length;
        result->emplace_back(_front_buffer.first, min_length);
        _front_buffer.first += min_length;
        _front_buffer.second -= min_length;
    }
    if (length > 0) {
        _wait_for_prefetching();
        if (!_rpc_status.ok()) {
            return _rpc_status;
        }

        {
            std::lock_guard<std::mutex> lock(_backend_buffer_latch);
            std::swap(_front_buffer, _backend_buffer);
        }

        DCHECK(length <= _front_buffer.second);
        result->emplace_back(_front_buffer.first, length);
        _front_buffer.first += length;
        _front_buffer.second -= length;
    }
    return Status::OK();
}

void AutoIncIDBuffer::_prefetch_ids(size_t length) {
    if (_front_buffer.second > _low_water_level_mark() || _is_fetching) {
        return;
    }
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    _is_fetching = true;
    static_cast<void>(_rpc_token->submit_func([=, this]() {
        TAutoIncrementRangeRequest request;
        TAutoIncrementRangeResult result;
        request.__set_db_id(_db_id);
        request.__set_table_id(_table_id);
        request.__set_column_id(_column_id);
        request.__set_length(length);

        int64_t get_auto_inc_range_rpc_ns;
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

        if (!_rpc_status.ok() || result.length <= 0) {
            LOG(WARNING) << "Failed to fetch auto-incremnt range, encounter rpc failure."
                         << "errmsg=" << _rpc_status.to_string();
            return;
        }

        {
            std::lock_guard<std::mutex> lock(_backend_buffer_latch);
            _backend_buffer = {result.start, result.length};
        }
        _is_fetching = false;
    }));
}

} // namespace doris::vectorized