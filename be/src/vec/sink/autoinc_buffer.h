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
#include "common/factory_creator.h"
#include "common/status.h"
#include "util/threadpool.h"

namespace doris::vectorized {

class VOlapTableSink;
class OlapTableBlockConvertor;

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
    ENABLE_FACTORY_CREATOR(AutoIncIDBuffer);
    // GenericReader::_MIN_BATCH_SIZE = 4064
    static constexpr size_t MIN_BATCH_SIZE = 4064;

public:
    // all public functions are thread safe
    AutoIncIDBuffer(int64_t _db_id, int64_t _table_id, int64_t column_id);
    void set_batch_size_at_least(size_t batch_size);
    Status sync_request_ids(size_t request_length, std::vector<std::pair<int64_t, size_t>>* result);

    struct AutoIncRange {
        int64_t start;
        size_t length;

        bool empty() const { return length == 0; }

        void consume(size_t l) {
            start += l;
            length -= l;
        }
    };

private:
    [[nodiscard]] size_t _prefetch_size() const {
        return _batch_size * config::auto_inc_prefetch_size_ratio;
    }

    [[nodiscard]] size_t _low_water_level_mark() const {
        return _batch_size * config::auto_inc_low_water_level_mark_size_ratio;
    };

    void _get_autoinc_ranges_from_buffers(size_t& request_length,
                                          std::vector<std::pair<int64_t, size_t>>* result);

    Status _launch_async_fetch_task(size_t length);

    Result<int64_t> _fetch_ids_from_fe(size_t length);

    std::atomic<size_t> _batch_size {MIN_BATCH_SIZE};

    int64_t _db_id;
    int64_t _table_id;
    int64_t _column_id;

    std::unique_ptr<ThreadPoolToken> _rpc_token;
    Status _rpc_status {Status::OK()};

    std::atomic<bool> _is_fetching {false};

    std::mutex _mutex;

    mutable std::mutex _latch;
    std::list<AutoIncRange> _buffers;
    size_t _current_volume {0};
};

class GlobalAutoIncBuffers {
public:
    static GlobalAutoIncBuffers* GetInstance() {
        static GlobalAutoIncBuffers buffers;
        return &buffers;
    }

    GlobalAutoIncBuffers() {
        static_cast<void>(ThreadPoolBuilder("AsyncFetchAutoIncIDExecutor")
                                  .set_min_threads(config::auto_inc_fetch_thread_num)
                                  .set_max_threads(config::auto_inc_fetch_thread_num)
                                  .set_max_queue_size(std::numeric_limits<int>::max())
                                  .build(&_fetch_autoinc_id_executor));
    }
    ~GlobalAutoIncBuffers() = default;

    std::unique_ptr<ThreadPoolToken> create_token() {
        return _fetch_autoinc_id_executor->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    }

    std::shared_ptr<AutoIncIDBuffer> get_auto_inc_buffer(int64_t db_id, int64_t table_id,
                                                         int64_t column_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto key = std::make_tuple(db_id, table_id, column_id);
        auto it = _buffers.find(key);
        if (it == _buffers.end()) {
            _buffers.emplace(key, AutoIncIDBuffer::create_shared(db_id, table_id, column_id));
        }
        return _buffers[{db_id, table_id, column_id}];
    }

private:
    std::unique_ptr<ThreadPool> _fetch_autoinc_id_executor;
    std::map<std::tuple<int64_t, int64_t, int64_t>, std::shared_ptr<AutoIncIDBuffer>> _buffers;
    std::mutex _mutex;
};

} // namespace doris::vectorized