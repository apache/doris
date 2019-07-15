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

#include "olap/task/engine_schema_change_task.h"

#include "olap/schema_change.h"

namespace doris {

using std::to_string;

EngineSchemaChangeTask::EngineSchemaChangeTask(const TAlterTabletReq& alter_tablet_request,
        int64_t signature, const TTaskType::type task_type, vector<string>* error_msgs,
        const string& process_name):
        _alter_tablet_req(alter_tablet_request),
        _signature(signature),
        _task_type(task_type),
        _error_msgs(error_msgs),
        _process_name(process_name) { }

OLAPStatus EngineSchemaChangeTask::execute() {
    OLAPStatus status = OLAP_SUCCESS;
    // create different alter task according task type
    switch (_task_type) {
        case TTaskType::ROLLUP:
            status = _create_rollup_tablet(_alter_tablet_req);
            break;
        case TTaskType::SCHEMA_CHANGE:
            status = _schema_change(_alter_tablet_req);
            break;
        default:
            break;
    }
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << _process_name << " failed. "
                     << "signature: " << _signature << " status: " << status;
    }

    return status;
} // execute

OLAPStatus EngineSchemaChangeTask::_create_rollup_tablet(const TAlterTabletReq& request) {
    LOG(INFO) << "begin to create rollup tablet. base_tablet_id=" << request.base_tablet_id
              << ", base_schema_hash=" << request.base_schema_hash
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id
              << ", new_schema_hash=" << request.new_tablet_req.tablet_schema.schema_hash;

    DorisMetrics::create_rollup_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_tablet(ROLLUP, request);

    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to do rollup. res=" << res
                     << " base_tablet_id=" << request.base_tablet_id
                     << ", base_schema_hash=" << request.base_schema_hash
                     << ", new_tablet_id=" << request.new_tablet_req.tablet_id
                     << ", new_schema_hash=" << request.new_tablet_req.tablet_schema.schema_hash;
        DorisMetrics::create_rollup_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << "success to create rollup tablet. res=" << res
              << " base_tablet_id=" << request.base_tablet_id
              << ", base_schema_hash" << request.base_schema_hash
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id
              << ", new_schema_hash=" << request.new_tablet_req.tablet_schema.schema_hash;
    return res;
} // create_rollup_tablet

OLAPStatus EngineSchemaChangeTask::_schema_change(const TAlterTabletReq& request) {
    LOG(INFO) << "begin to alter tablet. base_tablet_id=" << request.base_tablet_id
              << ", base_schema_hash=" << request.base_schema_hash
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id
              << ", new_schema_hash=" << request.new_tablet_req.tablet_schema.schema_hash;

    DorisMetrics::schema_change_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_tablet(SCHEMA_CHANGE, request);

    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to do alter tablet. res=" << res
                     << ", base_tablet_id=" << request.base_tablet_id
                     << ", base_schema_hash=" << request.base_schema_hash
                     << ", new_tablet_id=" << request.new_tablet_req.tablet_id
                     << ", new_schema_hash=" << request.new_tablet_req.tablet_schema.schema_hash;
        DorisMetrics::schema_change_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << "success to do alter tablet."
              << " base_tablet_id=" << request.base_tablet_id
              << ", base_schema_hash" << request.base_schema_hash
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id
              << ", new_schema_hash=" << request.new_tablet_req.tablet_schema.schema_hash;
    return res;
}

} // doris
