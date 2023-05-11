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

#include "util/open_partition_closure.h"

#include <google/protobuf/stubs/common.h>

#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "service/brpc.h"
#include "vec/sink/vtablet_sink.h"

namespace doris {
OpenPartitionClosure::OpenPartitionClosure(VNodeChannel* vnode_channel, IndexChannel* index_channel,
                                           int64_t partition_id, int64_t retry_count)
        : vnode_channel(vnode_channel),
          index_channel(index_channel),
          partition_id(partition_id),
          retry_count(retry_count) {}
OpenPartitionClosure::~OpenPartitionClosure() {};

void OpenPartitionClosure::Run() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
    if (cntl.Failed()) {
        if (retry_count < _max_retry_count) {
            vnode_channel->open_partition(partition_id, ++retry_count);
        } else {
            std::stringstream ss;
            ss << "failed to open partition, error=" << berror(this->cntl.ErrorCode())
               << ", error_text=" << this->cntl.ErrorText();
            LOG(WARNING) << ss.str() << " " << vnode_channel->channel_info();
            vnode_channel->cancel("Open partition error");
            index_channel->mark_as_failed(vnode_channel->node_id(), vnode_channel->host(),
                                          fmt::format("{}, open failed, err: {}",
                                                      vnode_channel->channel_info(), ss.str()),
                                          -1);
        }
    }
}

void OpenPartitionClosure::join() {
    brpc::Join(cntl.call_id());
}
} // namespace doris