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
        _process_name(process_name) {

}

OLAPStatus EngineSchemaChangeTask::execute() {
    OLAPStatus status = OLAP_SUCCESS;
    TTabletId base_tablet_id = _alter_tablet_req.base_tablet_id;
    TSchemaHash base_schema_hash = _alter_tablet_req.base_schema_hash;

    // Check last alter tablet status, if failed delete tablet file
    // Do not need to adjust delete success or not
    // Because if delete failed create rollup will failed
    // Check lastest alter tablet status
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(base_tablet_id, base_schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "failed to get tablet. tablet=" << base_tablet_id
                     << ", schema_hash=" << base_schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // Delete failed alter table tablet file
    if (tablet->has_alter_task() && tablet->alter_state() == ALTER_FAILED) {
        // !!! could not drop failed tablet
        // alter tablet job is in finishing state in fe, be restarts, then the tablet is in ALTER_TABLE_FAILED state
        // if drop the old tablet, then data is lost
        status = StorageEngine::instance()->tablet_manager()->drop_tablet(_alter_tablet_req.new_tablet_req.tablet_id,
                                                        _alter_tablet_req.new_tablet_req.tablet_schema.schema_hash);

        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to drop tablet after failing alter_tablet task, status=" << status
                         << ", signature=" << _signature;
            _error_msgs->push_back("failed to drop tablet after failing alter_tablet task, "
                                    "signature: " + to_string(_signature));
            return status;
        }
    }

    // if there is one running alter task, then not start current task
    if (!tablet->has_alter_task()
            || tablet->alter_state() == ALTER_FINISHED
            || tablet->alter_state() == ALTER_FAILED) {
        // Create rollup table
        switch (_task_type) {
            case TTaskType::ROLLUP:
                status = _create_rollup_tablet(_alter_tablet_req);
                break;
            case TTaskType::SCHEMA_CHANGE:
                status = _schema_change(_alter_tablet_req);
                break;
            default:
                // pass
                break;
        }
        if (status != OLAPStatus::OLAP_SUCCESS) {
            LOG(WARNING) << _process_name << " failed. signature: " << _signature << " status: " << status;
        }
    }

    return status;
} // execute

OLAPStatus EngineSchemaChangeTask::_create_rollup_tablet(const TAlterTabletReq& request) {
    LOG(INFO) << "begin to create rollup tablet. base_tablet_id=" << request.base_tablet_id
              << ", base_schema_hash" << request.base_schema_hash
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id
              << ", new_schema_hash=" << request.new_tablet_req.tablet_schema.schema_hash;

    DorisMetrics::create_rollup_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_tablet(ROLLUP, request);

    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to do rollup. res=" << res
                     << " base_tablet_id=" << request.base_tablet_id
                     << ", base_schema_hash" << request.base_schema_hash
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
