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
#include <time.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/memtable_memory_limiter.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/tablets_channel.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"
//#include <gen_cpp/internal_service.pb.h>

namespace doris {

class PTabletWriterOpenRequest;
class OpenPartitionRequest;

// A LoadChannel manages tablets channels for all indexes
// corresponding to a certain load job
class LoadChannel {
public:
    LoadChannel(const UniqueId& load_id, int64_t timeout_s, bool is_high_priority,
                const std::string& sender_ip, int64_t backend_id, bool enable_profile);
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

    std::unordered_map<int64_t, std::shared_ptr<TabletsChannel>> get_tablets_channels() {
        std::lock_guard<SpinLock> l(_tablets_channels_lock);
        return _tablets_channels;
    }

protected:
    Status _get_tablets_channel(std::shared_ptr<TabletsChannel>& channel, bool& is_finished,
                                const int64_t index_id);

    Status _handle_eos(std::shared_ptr<TabletsChannel>& channel,
                       const PTabletWriterAddBlockRequest& request,
                       PTabletWriterAddBlockResult* response) {
        _self_profile->add_info_string("EosHost", fmt::format("{}", request.backend_id()));
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

    void _init_profile();
    // thread safety
    void _report_profile(PTabletWriterAddBlockResult* response);

private:
    UniqueId _load_id;

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
