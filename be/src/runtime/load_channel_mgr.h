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
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "gutil/ref_counted.h"
#include "olap/lru_cache.h"
#include "runtime/load_channel.h"
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
    ~LoadChannelMgr();

    Status init(int64_t process_mem_limit);

    // open a new load channel if not exist
    Status open(const PTabletWriterOpenRequest& request);

    Status open_partition(const OpenPartitionRequest& params);

    Status add_batch(const PTabletWriterAddBlockRequest& request,
                     PTabletWriterAddBlockResult* response);

    // cancel all tablet stream for 'load_id' load
    Status cancel(const PTabletWriterCancelRequest& request);

    void refresh_mem_tracker() {
        std::lock_guard<std::mutex> l(_lock);
        _refresh_mem_tracker_without_lock();
    }
    MemTrackerLimiter* mem_tracker() { return _mem_tracker.get(); }

private:
    Status _get_load_channel(std::shared_ptr<LoadChannel>& channel, bool& is_eof,
                             const UniqueId& load_id, const PTabletWriterAddBlockRequest& request);

    void _finish_load_channel(UniqueId load_id);
    // check if the total load mem consumption exceeds limit.
    // If yes, it will pick a load channel to try to reduce memory consumption.
    void _handle_mem_exceed_limit();

    Status _start_bg_worker();

    // lock should be held when calling this method
    void _refresh_mem_tracker_without_lock() {
        _mem_usage = 0;
        for (auto& kv : _load_channels) {
            _mem_usage += kv.second->mem_consumption();
        }
        THREAD_MEM_TRACKER_TRANSFER_TO(_mem_usage - _mem_tracker->consumption(),
                                       _mem_tracker.get());
    }

protected:
    // lock protect the load channel map
    std::mutex _lock;
    // load id -> load channel
    std::unordered_map<UniqueId, std::shared_ptr<LoadChannel>> _load_channels;
    Cache* _last_success_channel = nullptr;

    // check the total load channel mem consumption of this Backend
    int64_t _mem_usage = 0;
    std::unique_ptr<MemTrackerLimiter> _mem_tracker;
    int64_t _load_hard_mem_limit = -1;
    int64_t _load_soft_mem_limit = -1;
    bool _soft_reduce_mem_in_progress = false;

    // If hard limit reached, one thread will trigger load channel flush,
    // other threads should wait on the condition variable.
    bool _should_wait_flush = false;
    std::condition_variable _wait_flush_cond;

    CountDownLatch _stop_background_threads_latch;
    // thread to clean timeout load channels
    scoped_refptr<Thread> _load_channels_clean_thread;
    Status _start_load_channels_clean();
};

} // namespace doris
