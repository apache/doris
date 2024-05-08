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

#include "olap/task/engine_index_change_task.h"

#include "olap/storage_engine.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/doris_metrics.h"

namespace doris {

EngineIndexChangeTask::EngineIndexChangeTask(
        StorageEngine& engine, const TAlterInvertedIndexReq& alter_inverted_index_request)
        : _engine(engine), _alter_inverted_index_req(alter_inverted_index_request) {
    _mem_tracker = MemTrackerLimiter::create_shared(
            MemTrackerLimiter::Type::SCHEMA_CHANGE,
            fmt::format("EngineIndexChangeTask#tabletId={}",
                        std::to_string(_alter_inverted_index_req.tablet_id)),
            engine.memory_limitation_bytes_per_thread_for_schema_change());
}

EngineIndexChangeTask::~EngineIndexChangeTask() = default;

Status EngineIndexChangeTask::execute() {
    DorisMetrics::instance()->alter_inverted_index_requests_total->increment(1);
    uint64_t start = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    Status res = _engine.process_index_change_task(_alter_inverted_index_req);

    if (!res.ok()) {
        LOG(WARNING) << "failed to do index change task. res=" << res
                     << " tablet_id=" << _alter_inverted_index_req.tablet_id
                     << ", job_id=" << _alter_inverted_index_req.job_id
                     << ", schema_hash=" << _alter_inverted_index_req.schema_hash;
        DorisMetrics::instance()->alter_inverted_index_requests_failed->increment(1);
        return res;
    }

    uint64_t end = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    LOG(INFO) << "success to execute index change task. res=" << res
              << " tablet_id=" << _alter_inverted_index_req.tablet_id
              << ", job_id=" << _alter_inverted_index_req.job_id
              << ", schema_hash=" << _alter_inverted_index_req.schema_hash
              << ", start time=" << start << ", end time=" << end
              << ", cost time=" << (end - start);
    return res;
} // execute

} // namespace doris
