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

#include "runtime/result_queue_mgr.h"

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gen_cpp/DorisExternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/exec_env.h"

namespace doris {

ResultQueueMgr::ResultQueueMgr() : _max_sink_batch_count(config::max_memory_sink_batch_count) {
}
ResultQueueMgr::~ResultQueueMgr() {
}

Status ResultQueueMgr::fetch_result(const TUniqueId& fragment_instance_id, std::shared_ptr<TScanRowBatch>* result, bool *eos) {
    shared_block_queue_t queue;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _fragment_queue_map.find(fragment_instance_id);
        if (_fragment_queue_map.end() != iter) {
            queue = iter->second;
        } else {
            return Status::InternalError("fragment_instance_id does not exists");
        }
    }
    bool sucess = queue->blocking_get(result);
    if (sucess) {
        // sentinel nullptr indicates scan end
        if (*result == nullptr) {
            *eos = true;
            // put sentinel for consistency, avoid repeated invoking fetch result when hava no rowbatch
            if (queue != nullptr) {
                queue->blocking_put(nullptr);
            }
        } else {
            *eos = false;
        }
    } else {
        *eos = true;
    }
    return Status::OK();
}

void ResultQueueMgr::create_queue(const TUniqueId& fragment_instance_id, shared_block_queue_t* queue) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _fragment_queue_map.find(fragment_instance_id);
    if (iter != _fragment_queue_map.end()) {
        *queue = iter->second;
    } else {
        // the blocking queue size = 20 (default), in this way, one queue have 20 * 1024 rows at most
        shared_block_queue_t tmp(new BlockingQueue<std::shared_ptr<TScanRowBatch>>(_max_sink_batch_count));
        _fragment_queue_map.insert(std::make_pair(fragment_instance_id, tmp));
        *queue = tmp;
    }
}

Status ResultQueueMgr::cancel(const TUniqueId& fragment_instance_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _fragment_queue_map.find(fragment_instance_id);
    if (iter != _fragment_queue_map.end()) {
        // remove this queue from map
        _fragment_queue_map.erase(fragment_instance_id);
    }
    return Status::OK();
}

}
