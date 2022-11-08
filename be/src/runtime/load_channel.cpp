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

#include "runtime/load_channel.h"

#include "olap/lru_cache.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/tablets_channel.h"
#include "runtime/thread_context.h"

namespace doris {

LoadChannel::LoadChannel(const UniqueId& load_id, std::unique_ptr<MemTracker> mem_tracker,
                         int64_t timeout_s, bool is_high_priority, const std::string& sender_ip,
                         bool is_vec)
        : _load_id(load_id),
          _mem_tracker(std::move(mem_tracker)),
          _timeout_s(timeout_s),
          _is_high_priority(is_high_priority),
          _sender_ip(sender_ip),
          _is_vec(is_vec) {
    // _last_updated_time should be set before being inserted to
    // _load_channels in load_channel_mgr, or it may be erased
    // immediately by gc thread.
    _last_updated_time.store(time(nullptr));
}

LoadChannel::~LoadChannel() {
    LOG(INFO) << "load channel removed. mem peak usage=" << _mem_tracker->peak_consumption()
              << ", info=" << _mem_tracker->debug_string() << ", load_id=" << _load_id
              << ", is high priority=" << _is_high_priority << ", sender_ip=" << _sender_ip
              << ", is_vec=" << _is_vec;
}

Status LoadChannel::open(const PTabletWriterOpenRequest& params) {
    int64_t index_id = params.index_id();
    std::shared_ptr<TabletsChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _tablets_channels.find(index_id);
        if (it != _tablets_channels.end()) {
            channel = it->second;
        } else {
            // create a new tablets channel
            TabletsChannelKey key(params.id(), index_id);
            channel.reset(new TabletsChannel(key, _load_id, _is_high_priority, _is_vec));
            {
                std::lock_guard<SpinLock> l(_tablets_channels_lock);
                _tablets_channels.insert({index_id, channel});
            }
        }
    }

    RETURN_IF_ERROR(channel->open(params));

    _opened = true;
    _last_updated_time.store(time(nullptr));
    return Status::OK();
}

Status LoadChannel::_get_tablets_channel(std::shared_ptr<TabletsChannel>& channel,
                                         bool& is_finished, const int64_t index_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto it = _tablets_channels.find(index_id);
    if (it == _tablets_channels.end()) {
        if (_finished_channel_ids.find(index_id) != _finished_channel_ids.end()) {
            // this channel is already finished, just return OK
            is_finished = true;
            return Status::OK();
        }
        std::stringstream ss;
        ss << "load channel " << _load_id << " add batch with unknown index id: " << index_id;
        return Status::InternalError(ss.str());
    }

    is_finished = false;
    channel = it->second;
    return Status::OK();
}

// lock should be held when calling this method
bool LoadChannel::_find_largest_consumption_channel(std::shared_ptr<TabletsChannel>* channel) {
    int64_t max_consume = 0;
    for (auto& it : _tablets_channels) {
        if (it.second->mem_consumption() > max_consume) {
            max_consume = it.second->mem_consumption();
            *channel = it.second;
        }
    }
    return max_consume > 0;
}

bool LoadChannel::is_finished() {
    if (!_opened) {
        return false;
    }
    std::lock_guard<std::mutex> l(_lock);
    return _tablets_channels.empty();
}

Status LoadChannel::cancel() {
    std::lock_guard<std::mutex> l(_lock);
    for (auto& it : _tablets_channels) {
        it.second->cancel();
    }
    return Status::OK();
}

} // namespace doris