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

#pragma once
#include <list>

#include "common/config.h"
#include "common/status.h"
#include "util/threadpool.h"

namespace doris {
namespace stream_load {

class VOlapTableSink;
class OlapTableBlockConvertor;

struct FetchAutoIncIDExecutor {
    FetchAutoIncIDExecutor();
    static FetchAutoIncIDExecutor* GetInstance() {
        static FetchAutoIncIDExecutor instance;
        return &instance;
    }

    std::unique_ptr<ThreadPool> _pool;
};

struct AutoIncIDAllocator {
    int64_t next_id() {
        DCHECK(!ids.empty());
        if (ids.front().second > 0) {
            --ids.front().second;
            --total_count;
            return ids.front().first++;
        }
        ids.pop_front();
        DCHECK(!ids.empty() && ids.front().second > 0);
        --ids.front().second;
        --total_count;
        return ids.front().first++;
    }

    void insert_ids(int64_t start, size_t length) {
        total_count += length;
        ids.emplace_back(start, length);
    }

    size_t total_count {0};
    std::list<std::pair<int64_t, size_t>> ids;
};

class AutoIncIDBuffer {
    // GenericReader::_MIN_BATCH_SIZE = 4064
    static constexpr size_t MIN_BATCH_SIZE = 4064;

public:
    // all public functions are thread safe
    AutoIncIDBuffer(int64_t _db_id, int64_t _table_id, int64_t column_id);
    void set_batch_size_at_least(size_t batch_size);
    Status sync_request_ids(size_t length, std::vector<std::pair<int64_t, size_t>>* result);

private:
    void _prefetch_ids(size_t length);
    [[nodiscard]] size_t _prefetch_size() const {
        return _batch_size * config::auto_inc_prefetch_size_ratio;
    }
    [[nodiscard]] size_t _low_water_level_mark() const {
        return _batch_size * config::auto_inc_low_water_level_mark_size_ratio;
    };
    void _wait_for_prefetching();

    std::atomic<size_t> _batch_size {MIN_BATCH_SIZE};

    int64_t _db_id;
    int64_t _table_id;
    int64_t _column_id;

    std::unique_ptr<ThreadPoolToken> _rpc_token;
    Status _rpc_status {Status::OK()};
    std::atomic<bool> _is_fetching {false};

    std::pair<int64_t, size_t> _front_buffer {0, 0};
    std::pair<int64_t, size_t> _backend_buffer {0, 0};
    std::mutex _backend_buffer_latch; // for _backend_buffer
    std::mutex _mutex;
};

class GlobalAutoIncBuffers {
public:
    static GlobalAutoIncBuffers* GetInstance() {
        static GlobalAutoIncBuffers buffers;
        return &buffers;
    }

    ~GlobalAutoIncBuffers() {
        for (auto [_, buffer] : _buffers) {
            delete buffer;
        }
    }

    AutoIncIDBuffer* get_auto_inc_buffer(int64_t db_id, int64_t table_id, int64_t column_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto key = std::make_tuple(db_id, table_id, column_id);
        auto it = _buffers.find(key);
        if (it == _buffers.end()) {
            _buffers.emplace(std::make_pair(key, new AutoIncIDBuffer(db_id, table_id, column_id)));
        }
        return _buffers[{db_id, table_id, column_id}];
    }

private:
    std::map<std::tuple<int64_t, int64_t, int64_t>, AutoIncIDBuffer*> _buffers;
    std::mutex _mutex;
};

} // namespace stream_load
} // namespace doris