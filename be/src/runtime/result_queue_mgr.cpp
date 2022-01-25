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
#include "util/arrow/row_batch.h"
#include "util/doris_metrics.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(result_block_queue_count, MetricUnit::NOUNIT);

ResultQueueMgr::ResultQueueMgr() {
    // Each BlockingQueue has a limited size (default 20, by config::max_memory_sink_batch_count),
    // it's not needed to count the actual size of all BlockingQueue.
    REGISTER_HOOK_METRIC(result_block_queue_count, [this]() {
        std::lock_guard<std::mutex> l(_lock);
        return _fragment_queue_map.size();
    });
}

ResultQueueMgr::~ResultQueueMgr() {
    DEREGISTER_HOOK_METRIC(result_block_queue_count);
}

Status ResultQueueMgr::fetch_result(const TUniqueId& fragment_instance_id,
                                    std::shared_ptr<arrow::RecordBatch>* result, bool* eos) {
    BlockQueueSharedPtr queue;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _fragment_queue_map.find(fragment_instance_id);
        if (_fragment_queue_map.end() != iter) {
            queue = iter->second;
        } else {
            return Status::InternalError("fragment_instance_id does not exists");
        }
    }
    // check queue status before get result
    RETURN_IF_ERROR(queue->status());
    bool success = queue->blocking_get(result);
    if (success) {
        // sentinel nullptr indicates scan end
        if (*result == nullptr) {
            *eos = true;
            // put sentinel for consistency, avoid repeated invoking fetch result when have no rowbatch
            queue->blocking_put(nullptr);
        } else {
            *eos = false;
        }
    } else {
        *eos = true;
    }
    return Status::OK();
}

void ResultQueueMgr::create_queue(const TUniqueId& fragment_instance_id,
                                  BlockQueueSharedPtr* queue) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _fragment_queue_map.find(fragment_instance_id);
    if (iter != _fragment_queue_map.end()) {
        *queue = iter->second;
    } else {
        // the blocking queue size = 20 (default), in this way, one queue have 20 * 1024 rows at most
        BlockQueueSharedPtr tmp(new RecordBatchQueue(config::max_memory_sink_batch_count));
        _fragment_queue_map.insert(std::make_pair(fragment_instance_id, tmp));
        *queue = tmp;
    }
}

Status ResultQueueMgr::cancel(const TUniqueId& fragment_instance_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _fragment_queue_map.find(fragment_instance_id);
    if (iter != _fragment_queue_map.end()) {
        // first remove RecordBatch from queue
        // avoid MemoryScratchSink block on send or close operation
        iter->second->shutdown();
        // remove this queue from map
        _fragment_queue_map.erase(fragment_instance_id);
    }
    return Status::OK();
}

void ResultQueueMgr::update_queue_status(const TUniqueId& fragment_instance_id,
                                         const Status& status) {
    if (status.ok()) {
        return;
    }
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _fragment_queue_map.find(fragment_instance_id);
    if (iter != _fragment_queue_map.end()) {
        iter->second->update_status(status);
    }
}

} // namespace doris
