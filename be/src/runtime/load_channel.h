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
#include "runtime/memory/mem_tracker.h"
#include "runtime/tablets_channel.h"
#include "runtime/thread_context.h"
#include "util/uid_util.h"

namespace doris {

class Cache;

// A LoadChannel manages tablets channels for all indexes
// corresponding to a certain load job
class LoadChannel {
public:
    LoadChannel(const UniqueId& load_id, std::unique_ptr<MemTracker> mem_tracker, int64_t timeout_s,
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
    // The method will not return until the chosen tablet channels finished memtable
    // flush.
    template <typename TabletWriterAddResult>
    Status handle_mem_exceed_limit(TabletWriterAddResult* response);

    int64_t mem_consumption() {
        int64_t mem_usage = 0;
        {
            std::lock_guard<SpinLock> l(_tablets_channels_lock);
            for (auto& it : _tablets_channels) {
                mem_usage += it.second->mem_consumption();
            }
        }
        _mem_tracker->set_consumption(mem_usage);
        return mem_usage;
    }

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
        RETURN_IF_ERROR(channel->close(
                this, request.sender_id(), request.backend_id(), &finished, request.partition_ids(),
                response->mutable_tablet_vec(), response->mutable_tablet_errors(),
                request.slave_tablet_nodes(), response->mutable_success_slave_tablet_node_ids(),
                request.write_single_replica()));
        if (finished) {
            std::lock_guard<std::mutex> l(_lock);
            {
                std::lock_guard<SpinLock> l(_tablets_channels_lock);
                _tablets_channels.erase(index_id);
            }
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
    std::unique_ptr<MemTracker> _mem_tracker;

    // lock protect the tablets channel map
    std::mutex _lock;
    // index id -> tablets channel
    std::unordered_map<int64_t, std::shared_ptr<TabletsChannel>> _tablets_channels;
    SpinLock _tablets_channels_lock;
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
    int64_t index_id = request.index_id();
    // 1. get tablets channel
    std::shared_ptr<TabletsChannel> channel;
    bool is_finished;
    Status st = _get_tablets_channel(channel, is_finished, index_id);
    if (!st.ok() || is_finished) {
        return st;
    }

    // 2. add batch to tablets channel
    if constexpr (std::is_same_v<TabletWriterAddRequest, PTabletWriterAddBatchRequest>) {
        if (request.has_row_batch()) {
            RETURN_IF_ERROR(channel->add_batch(request, response));
        }
    } else {
        if (request.has_block()) {
            RETURN_IF_ERROR(channel->add_batch(request, response));
        }
    }

    // 3. handle eos
    if (request.has_eos() && request.eos()) {
        st = _handle_eos(channel, request, response);
        if (!st.ok()) {
            return st;
        }
    }
    _last_updated_time.store(time(nullptr));
    return st;
}

inline std::ostream& operator<<(std::ostream& os, LoadChannel& load_channel) {
    os << "LoadChannel(id=" << load_channel.load_id() << ", mem=" << load_channel.mem_consumption()
       << ", last_update_time=" << static_cast<uint64_t>(load_channel.last_updated_time())
       << ", is high priority: " << load_channel.is_high_priority() << ")";
    return os;
}

template <typename TabletWriterAddResult>
Status LoadChannel::handle_mem_exceed_limit(TabletWriterAddResult* response) {
    bool found = false;
    std::shared_ptr<TabletsChannel> channel;
    {
        // lock so that only one thread can check mem limit
        std::lock_guard<SpinLock> l(_tablets_channels_lock);
        found = _find_largest_consumption_channel(&channel);
    }
    // Release lock so that other threads can still call add_batch concurrently.
    if (found) {
        DCHECK(channel != nullptr);
        return channel->reduce_mem_usage(response);
    } else {
        // should not happen, add log to observe
        LOG(WARNING) << "fail to find suitable tablets-channel when memory exceed. "
                     << "load_id=" << _load_id;
    }
    return Status::OK();
}

} // namespace doris
