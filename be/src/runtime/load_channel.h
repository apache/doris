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
#include "runtime/tablets_channel.h"
#include "runtime/thread_context.h"
#include "util/uid_util.h"

namespace doris {

class Cache;

// A LoadChannel manages tablets channels for all indexes
// corresponding to a certain load job
class LoadChannel {
public:
    LoadChannel(const UniqueId& load_id, int64_t mem_limit, int64_t timeout_s,
                bool is_high_priority, const std::string& sender_ip, bool is_vec);
    ~LoadChannel();

    // open a new load channel if not exist
    Status open(const PTabletWriterOpenRequest& request);

    // this batch must belong to a index in one transaction
    template <typename TabletWriterAddRequest, typename TabletWriterAddResult>
    Status add_batch(const TabletWriterAddRequest& request, TabletWriterAddResult* response);

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

protected:
    Status _get_tablets_channel(std::shared_ptr<TabletsChannel>& channel, bool& is_finished,
                                const int64_t index_id);

    template <typename Request, typename Response>
    Status _handle_eos(std::shared_ptr<TabletsChannel>& channel, const Request& request,
                       Response* response) {
        bool finished = false;
        auto index_id = request.index_id();
        RETURN_IF_ERROR(channel->close(request.sender_id(), request.backend_id(), &finished,
                                       request.partition_ids(), response->mutable_tablet_vec()));
        if (finished) {
            std::lock_guard<std::mutex> l(_lock);
            _tablets_channels.erase(index_id);
            _finished_channel_ids.emplace(index_id);
        }
        return Status::OK();
    }

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

    // true if this load is vectorized
    bool _is_vec = false;
};

template <typename TabletWriterAddRequest, typename TabletWriterAddResult>
Status LoadChannel::add_batch(const TabletWriterAddRequest& request,
                              TabletWriterAddResult* response) {
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
    int64_t index_id = request.index_id();
    // 1. get tablets channel
    std::shared_ptr<TabletsChannel> channel;
    bool is_finished;
    Status st = _get_tablets_channel(channel, is_finished, index_id);
    if (!st.ok() || is_finished) {
        return st;
    }

    // 2. check if mem consumption exceed limit
    handle_mem_exceed_limit(false);

    // 3. add batch to tablets channel
    if constexpr (std::is_same_v<TabletWriterAddRequest, PTabletWriterAddBatchRequest>) {
        if (request.has_row_batch()) {
            RETURN_IF_ERROR(channel->add_batch(request, response));
        }
    } else {
        if (request.has_block()) {
            RETURN_IF_ERROR(channel->add_batch(request, response));
        }
    }

    // 4. handle eos
    if (request.has_eos() && request.eos()) {
        st = _handle_eos(channel, request, response);
        if (!st.ok()) {
            return st;
        }
    }
    _last_updated_time.store(time(nullptr));
    return st;
}

inline std::ostream& operator<<(std::ostream& os, const LoadChannel& load_channel) {
    os << "LoadChannel(id=" << load_channel.load_id() << ", mem=" << load_channel.mem_consumption()
       << ", last_update_time=" << static_cast<uint64_t>(load_channel.last_updated_time())
       << ", is high priority: " << load_channel.is_high_priority() << ")";
    return os;
}

} // namespace doris
