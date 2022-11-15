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

#include "olap/task/engine_alter_tablet_task.h"

#include "olap/schema_change.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"

namespace doris {

EngineAlterTabletTask::EngineAlterTabletTask(const TAlterTabletReqV2& request)
        : _alter_tablet_req(request) {
    _mem_tracker = std::make_shared<MemTrackerLimiter>(
            MemTrackerLimiter::Type::SCHEMA_CHANGE,
            fmt::format("EngineAlterTabletTask#baseTabletId={}:newTabletId={}",
                        std::to_string(_alter_tablet_req.base_tablet_id),
                        std::to_string(_alter_tablet_req.new_tablet_id)),
            config::memory_limitation_per_thread_for_schema_change_bytes);
}

Status EngineAlterTabletTask::execute() {
    SCOPED_ATTACH_TASK(_mem_tracker);
    DorisMetrics::instance()->create_rollup_requests_total->increment(1);

    Status res = SchemaChangeHandler::process_alter_tablet_v2(_alter_tablet_req);
    if (!res.ok()) {
        DorisMetrics::instance()->create_rollup_requests_failed->increment(1);
        return res;
    }
    return res;
} // execute

} // namespace doris
