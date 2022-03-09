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

#include "vec/runtime/vload_channel_mgr.h"
#include "vec/runtime/vload_channel.h"

namespace doris {

namespace vectorized {

VLoadChannelMgr::VLoadChannelMgr() : LoadChannelMgr() {}

VLoadChannelMgr::~VLoadChannelMgr() {}

LoadChannel*
VLoadChannelMgr::_create_load_channel(const UniqueId& load_id, int64_t mem_limit, int64_t timeout_s,
                                      const std::shared_ptr<MemTracker>& mem_tracker, bool is_high_priority,
                                      const std::string& sender_ip) {
    return new VLoadChannel(load_id, mem_limit, timeout_s, mem_tracker, is_high_priority, sender_ip);
}

Status VLoadChannelMgr::add_block(const PTabletWriterAddBlockRequest& request,
                                  PTabletWriterAddBlockResult* response) {
    UniqueId load_id(request.id());
    // 1. get load channel
    std::shared_ptr<LoadChannel> channel;
    bool is_eof;
    auto status = _get_load_channel(channel, is_eof, load_id, request);
    if (!status.ok() || is_eof) {
        return status;
    }

    if (!channel->is_high_priority()) {
        // 2. check if mem consumption exceed limit
        // If this is a high priority load task, do not handle this.
        // because this may block for a while, which may lead to rpc timeout.
        _handle_mem_exceed_limit();
    }

    // 3. add batch to load channel
    // batch may not exist in request(eg: eos request without batch),
    // this case will be handled in load channel's add batch method.
    RETURN_IF_ERROR(channel->add_block(request, response));

    // 4. handle finish
    if (channel->is_finished()) {
        _finish_load_channel(load_id);
    }
    return Status::OK();

}

} // namespace vectorized

} // namespace doris