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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "common/status.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"
#include "util/uid_util.h"

namespace doris {

class PTabletWriterOpenRequest;
class PTabletWriterAddBlockRequest;
class PTabletWriterAddBlockResult;
class OpenPartitionRequest;
class BaseTabletsChannel;

// A LoadChannel manages tablets channels for all indexes
// corresponding to a certain load job
class LoadChannel {
public:
    LoadChannel(const UniqueId& load_id, int64_t timeout_s, bool is_high_priority,
                std::string sender_ip, int64_t backend_id, bool enable_profile);
    ~LoadChannel();

    // open a new load channel if not exist
    Status open(const PTabletWriterOpenRequest& request);

    // this batch must belong to a index in one transaction
    Status add_batch(const PTabletWriterAddBlockRequest& request,
                     PTabletWriterAddBlockResult* response);

    // return true if this load channel has been opened and all tablets channels are closed then.
    bool is_finished();

    Status cancel();

    time_t last_updated_time() const { return _last_updated_time.load(); }

    const UniqueId& load_id() const { return _load_id; }

    int64_t timeout() const { return _timeout_s; }

    bool is_high_priority() const { return _is_high_priority; }

    RuntimeProfile::Counter* get_mgr_add_batch_timer() { return _mgr_add_batch_timer; }
    RuntimeProfile::Counter* get_handle_mem_limit_timer() { return _handle_mem_limit_timer; }

protected:
    Status _get_tablets_channel(std::shared_ptr<BaseTabletsChannel>& channel, bool& is_finished,
                                int64_t index_id);

    Status _handle_eos(BaseTabletsChannel* channel, const PTabletWriterAddBlockRequest& request,
                       PTabletWriterAddBlockResult* response);

    void _init_profile();
    // thread safety
    void _report_profile(PTabletWriterAddBlockResult* response);

private:
    UniqueId _load_id;
    int64_t _txn_id = 0;

    SpinLock _profile_serialize_lock;
    std::unique_ptr<RuntimeProfile> _profile;
    RuntimeProfile* _self_profile = nullptr;
    RuntimeProfile::Counter* _add_batch_number_counter = nullptr;
    RuntimeProfile::Counter* _peak_memory_usage_counter = nullptr;
    RuntimeProfile::Counter* _add_batch_timer = nullptr;
    RuntimeProfile::Counter* _add_batch_times = nullptr;
    RuntimeProfile::Counter* _mgr_add_batch_timer = nullptr;
    RuntimeProfile::Counter* _handle_mem_limit_timer = nullptr;
    RuntimeProfile::Counter* _handle_eos_timer = nullptr;

    // lock protect the tablets channel map
    std::mutex _lock;
    // index id -> tablets channel
    std::unordered_map<int64_t, std::shared_ptr<BaseTabletsChannel>> _tablets_channels;
    // index id -> (received rows, filtered rows)
    std::unordered_map<int64_t, std::pair<size_t, size_t>> _tablets_channels_rows;
    SpinLock _tablets_channels_lock;
    // This is to save finished channels id, to handle the retry request.
    std::unordered_set<int64_t> _finished_channel_ids;
    // set to true if at least one tablets channel has been opened
    bool _opened = false;

    QueryThreadContext _query_thread_context;

    std::atomic<time_t> _last_updated_time;

    // the timeout of this load job.
    // Timed out channels will be periodically deleted by LoadChannelMgr.
    int64_t _timeout_s;

    // true if this is a high priority load task
    bool _is_high_priority = false;

    // the ip where tablet sink locate
    std::string _sender_ip;

    int64_t _backend_id;

    bool _enable_profile;
};

inline std::ostream& operator<<(std::ostream& os, LoadChannel& load_channel) {
    os << "LoadChannel(id=" << load_channel.load_id()
       << ", last_update_time=" << static_cast<uint64_t>(load_channel.last_updated_time())
       << ", is high priority: " << load_channel.is_high_priority() << ")";
    return os;
}

} // namespace doris
