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

#include <gen_cpp/internal_service.pb.h>
#include <stdint.h>

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "gutil/ref_counted.h"
#include "olap/lru_cache.h"
#include "olap/memtable_memory_limiter.h"
#include "runtime/load_channel.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/countdown_latch.h"
#include "util/uid_util.h"

namespace doris {

class PTabletWriterCancelRequest;
class PTabletWriterOpenRequest;
class Thread;

// LoadChannelMgr -> LoadChannel -> TabletsChannel -> DeltaWriter
// All dispatched load data for this backend is routed from this class
class LoadChannelMgr {
public:
    LoadChannelMgr();

    Status init(int64_t process_mem_limit);

    // open a new load channel if not exist
    Status open(const PTabletWriterOpenRequest& request);

    Status add_batch(const PTabletWriterAddBlockRequest& request,
                     PTabletWriterAddBlockResult* response);

    // cancel all tablet stream for 'load_id' load
    Status cancel(const PTabletWriterCancelRequest& request);

    void stop();

private:
    Status _get_load_channel(std::shared_ptr<LoadChannel>& channel, bool& is_eof,
                             const UniqueId& load_id, const PTabletWriterAddBlockRequest& request);

    void _finish_load_channel(UniqueId load_id);

    Status _start_bg_worker();

    class LastSuccessChannelCache : public LRUCachePolicy {
    public:
        LastSuccessChannelCache(size_t capacity)
                : LRUCachePolicy(CachePolicy::CacheType::LAST_SUCCESS_CHANNEL_CACHE, capacity,
                                 LRUCacheType::SIZE, -1, DEFAULT_LRU_CACHE_NUM_SHARDS,
                                 DEFAULT_LRU_CACHE_ELEMENT_COUNT_CAPACITY, false) {}
    };

protected:
    // lock protect the load channel map
    std::mutex _lock;
    // load id -> load channel
    std::unordered_map<UniqueId, std::shared_ptr<LoadChannel>> _load_channels;
    std::unique_ptr<LastSuccessChannelCache> _last_success_channels;

    MemTableMemoryLimiter* _memtable_memory_limiter = nullptr;

    CountDownLatch _stop_background_threads_latch;
    // thread to clean timeout load channels
    scoped_refptr<Thread> _load_channels_clean_thread;
    Status _start_load_channels_clean();
};

} // namespace doris
