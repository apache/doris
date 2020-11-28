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

namespace doris {

using std::to_string;

EngineAlterTabletTask::EngineAlterTabletTask(const TAlterTabletReqV2& request, int64_t signature,
                                             const TTaskType::type task_type,
                                             std::vector<string>* error_msgs,
                                             const string& process_name)
        : _alter_tablet_req(request),
          _signature(signature),
          _task_type(task_type),
          _error_msgs(error_msgs),
          _process_name(process_name) {}

OLAPStatus EngineAlterTabletTask::execute() {
    DorisMetrics::instance()->create_rollup_requests_total->increment(1);

    SchemaChangeHandler handler;
    OLAPStatus res = handler.process_alter_tablet_v2(_alter_tablet_req);

    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to do alter task. res=" << res
                     << " base_tablet_id=" << _alter_tablet_req.base_tablet_id
                     << ", base_schema_hash=" << _alter_tablet_req.base_schema_hash
                     << ", new_tablet_id=" << _alter_tablet_req.new_tablet_id
                     << ", new_schema_hash=" << _alter_tablet_req.new_schema_hash;
        DorisMetrics::instance()->create_rollup_requests_failed->increment(1);
        return res;
    }

    LOG(INFO) << "success to create new alter tablet. res=" << res
              << " base_tablet_id=" << _alter_tablet_req.base_tablet_id << ", base_schema_hash"
              << _alter_tablet_req.base_schema_hash
              << ", new_tablet_id=" << _alter_tablet_req.new_tablet_id
              << ", new_schema_hash=" << _alter_tablet_req.new_schema_hash;
    return res;
} // execute

} // namespace doris
