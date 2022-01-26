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

#include <mutex>
#include <ostream>
#include <unordered_map>
#include <unordered_set>

#include "common/status.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/mem_tracker.h"
#include "util/uid_util.h"

namespace doris {

class Cache;
class TabletsChannel;

// A LoadChannel manages tablets channels for all indexes
// corresponding to a certain load job
class LoadChannel {
public:
    LoadChannel(const UniqueId& load_id, int64_t mem_limit, int64_t timeout_s,
                bool is_high_priority, const std::string& sender_ip);
    ~LoadChannel();

    // open a new load channel if not exist
    Status open(const PTabletWriterOpenRequest& request);

    // this batch must belong to a index in one transaction
    Status add_batch(const PTabletWriterAddBatchRequest& request,
                     PTabletWriterAddBatchResult* response);

    // return true if this load channel has been opened and all tablets channels are closed then.
    bool is_finished();

    Status cancel();

    time_t last_updated_time() const { return _last_updated_time.load(); }

    const UniqueId& load_id() const { return _load_id; }

    // check if this load channel mem consumption exceeds limit.
    // If yes, it will pick a tablets channel to try to reduce memory consumption.
    // If force is true, even if this load channel does not exceeds limit, it will still
    // try to reduce memory.
    void handle_mem_exceed_limit(bool force);

    int64_t mem_consumption() const { return _mem_tracker->consumption(); }

    int64_t timeout() const { return _timeout_s; }

    bool is_high_priority() const { return _is_high_priority; }

private:
    // when mem consumption exceeds limit, should call this method to find the channel
    // that consumes the largest memory(, and then we can reduce its memory usage).
    bool _find_largest_consumption_channel(std::shared_ptr<TabletsChannel>* channel);

    UniqueId _load_id;
    // Tracks the total memory consumed by current load job on this BE
    std::shared_ptr<MemTracker> _mem_tracker;

    // lock protect the tablets channel map
    std::mutex _lock;
    // index id -> tablets channel
    std::unordered_map<int64_t, std::shared_ptr<TabletsChannel>> _tablets_channels;
    // This is to save finished channels id, to handle the retry request.
    std::unordered_set<int64_t> _finished_channel_ids;
    // set to true if at least one tablets channel has been opened
    bool _opened = false;

    std::atomic<time_t> _last_updated_time;

    // the timeout of this load job.
    // Timed out channels will be periodically deleted by LoadChannelMgr.
    int64_t _timeout_s;

    // true if this is a high priority load task
    bool _is_high_priority = false;

    // the ip where tablet sink locate
    std::string _sender_ip = "";
};

inline std::ostream& operator<<(std::ostream& os, const LoadChannel& load_channel) {
    os << "LoadChannel(id=" << load_channel.load_id() << ", mem=" << load_channel.mem_consumption()
        << ", last_update_time=" << static_cast<uint64_t>(load_channel.last_updated_time())
        << ", is high priority: " << load_channel.is_high_priority() << ")";
    return os;
}

} // namespace doris
