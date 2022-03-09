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

#include "vload_channel.h"
#include "vtablets_channel.h"

namespace doris {

namespace vectorized {

VLoadChannel::VLoadChannel(const UniqueId& load_id, int64_t mem_limit, int64_t timeout_s,
                           const std::shared_ptr<MemTracker>& mem_tracker, bool is_high_priority,
                           const std::string& sender_ip)
        : LoadChannel(load_id, mem_limit, timeout_s, mem_tracker, is_high_priority, sender_ip) {
}

Status VLoadChannel::open(const PTabletWriterOpenRequest& params) {
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
            channel.reset(new VTabletsChannel(key, _mem_tracker, _is_high_priority));
            _tablets_channels.insert({index_id, channel});
        }
    }

    RETURN_IF_ERROR(channel->open(params));

    _opened = true;
    _last_updated_time.store(time(nullptr));
    return Status::OK();
}

Status VLoadChannel::add_block(const PTabletWriterAddBlockRequest& request,
                               PTabletWriterAddBlockResult* response) {
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
    if (request.has_block()) {
        RETURN_IF_ERROR(channel->add_block(request, response));
    }

    // 4. handle eos
    if (request.has_eos() && request.eos()) {
        if (request.has_eos() && request.eos()) {
        st = _handle_eos(channel, request, response);
        if (!st.ok()) {
            return st;
        }
    }
    }
    _last_updated_time.store(time(nullptr));
    return st;
}

} // namespace vectorized

} // namespace doris