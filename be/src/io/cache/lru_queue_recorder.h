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

#include <concurrentqueue.h>

#include <boost/lockfree/spsc_queue.hpp>

#include "io/cache/file_cache_common.h"

namespace doris::io {

class LRUQueue;

enum class CacheLRULogType {
    ADD = 0, // all of the integer types
    REMOVE = 1,
    MOVETOBACK = 2,
    INVALID = 3,
};

struct CacheLRULog {
    CacheLRULogType type = CacheLRULogType::INVALID;
    UInt128Wrapper hash;
    size_t offset;
    size_t size;

    CacheLRULog(CacheLRULogType t, UInt128Wrapper h, size_t o, size_t s)
            : type(t), hash(h), offset(o), size(s) {}
};

using CacheLRULogQueue = moodycamel::ConcurrentQueue<std::unique_ptr<CacheLRULog>>;

class LRUQueueRecorder {
public:
    LRUQueueRecorder(BlockFileCache* mgr) : _mgr(mgr) {
        _lru_queue_update_cnt_from_last_dump[FileCacheType::DISPOSABLE] = 0;
        _lru_queue_update_cnt_from_last_dump[FileCacheType::NORMAL] = 0;
        _lru_queue_update_cnt_from_last_dump[FileCacheType::INDEX] = 0;
        _lru_queue_update_cnt_from_last_dump[FileCacheType::TTL] = 0;
        _lru_queue_update_cnt_from_last_dump[FileCacheType::COLD_NORMAL] = 0;
    }
    void record_queue_event(FileCacheType type, CacheLRULogType log_type, const UInt128Wrapper hash,
                            const size_t offset, const size_t size);
    void replay_queue_event(FileCacheType type);
    void evaluate_queue_diff(LRUQueue& base, std::string name,
                             std::lock_guard<std::mutex>& cache_lock);
    size_t get_lru_queue_update_cnt_from_last_dump(FileCacheType type);
    void reset_lru_queue_update_cnt_from_last_dump(FileCacheType type);

    CacheLRULogQueue& get_lru_log_queue(FileCacheType type);
    LRUQueue& get_shadow_queue(FileCacheType type);

public:
    std::mutex _mutex_lru_log;

private:
    LRUQueue _shadow_index_queue;
    LRUQueue _shadow_normal_queue;
    LRUQueue _shadow_disposable_queue;
    LRUQueue _shadow_ttl_queue;
    LRUQueue _shadow_cold_normal_queue;

    CacheLRULogQueue _ttl_lru_log_queue;
    CacheLRULogQueue _index_lru_log_queue;
    CacheLRULogQueue _normal_lru_log_queue;
    CacheLRULogQueue _disposable_lru_log_queue;
    CacheLRULogQueue _cold_normal_lru_log_queue;

    std::unordered_map<FileCacheType, size_t> _lru_queue_update_cnt_from_last_dump;

    BlockFileCache* _mgr;
};

} // namespace doris::io
