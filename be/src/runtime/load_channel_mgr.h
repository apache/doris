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

#include <ctime>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/ref_counted.h"
#include "runtime/tablets_channel.h"
#include "util/countdown_latch.h"
#include "util/thread.h"
#include "util/uid_util.h"
#include "olap/lru_cache.h"

namespace doris {

class Cache;
class LoadChannel;

// LoadChannelMgr -> LoadChannel -> TabletsChannel -> DeltaWriter
// All dispatched load data for this backend is routed from this class
class LoadChannelMgr {
public:
    LoadChannelMgr();
    virtual ~LoadChannelMgr();

    Status init(int64_t process_mem_limit);

    // open a new load channel if not exist
    Status open(const PTabletWriterOpenRequest& request);

    Status add_batch(const PTabletWriterAddBatchRequest& request,
                     PTabletWriterAddBatchResult* response);
    
    virtual Status add_block(const PTabletWriterAddBlockRequest& request,
                             PTabletWriterAddBlockResult* response) {
        return Status::NotSupported("Not Implemented add_block");
    }

    // cancel all tablet stream for 'load_id' load
    Status cancel(const PTabletWriterCancelRequest& request);

protected:
    virtual LoadChannel* _create_load_channel(const UniqueId& load_id, int64_t mem_limit, int64_t timeout_s,
                                              const std::shared_ptr<MemTracker>& mem_tracker, bool is_high_priority,
                                              const std::string& sender_ip);

    template<typename Request>
    Status _get_load_channel(std::shared_ptr<LoadChannel>& channel,
                             bool& is_eof,
                             const UniqueId load_id,
                             const Request& request) {
        is_eof = false;
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_channels.find(load_id);
        if (it == _load_channels.end()) {
            auto handle = _last_success_channel->lookup(load_id.to_string());
            // success only when eos be true
            if (handle != nullptr) {
                _last_success_channel->release(handle);
                if (request.has_eos() && request.eos()) {
                    is_eof = true;
                    return Status::OK();
                }
            }
            return Status::InternalError(strings::Substitute(
                    "fail to add batch in load channel. unknown load_id=$0", load_id.to_string()));
        }
        channel = it->second;
        return Status::OK();
    }
    void _finish_load_channel(const UniqueId load_id);
    // check if the total load mem consumption exceeds limit.
    // If yes, it will pick a load channel to try to reduce memory consumption.
    void _handle_mem_exceed_limit();

    Status _start_bg_worker();

protected:
    // lock protect the load channel map
    std::mutex _lock;
    // load id -> load channel
    std::unordered_map<UniqueId, std::shared_ptr<LoadChannel>> _load_channels;
    Cache* _last_success_channel = nullptr;

    // check the total load mem consumption of this Backend
    std::shared_ptr<MemTracker> _mem_tracker;

    CountDownLatch _stop_background_threads_latch;
    // thread to clean timeout load channels
    scoped_refptr<Thread> _load_channels_clean_thread;
    Status _start_load_channels_clean();
};

} // namespace doris
