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

    // Check last schema change status, if failed delete tablet file
    // Do not need to adjust delete success or not
    // Because if delete failed create rollup will failed
    // Check lastest schema change status
    AlterTabletState alter_tablet_state
        = TabletManager::instance()->show_alter_tablet_state(base_tablet_id, base_schema_hash);
    LOG(INFO) << "get alter table state:" << alter_tablet_state << ", signature:" << _signature;

    // Delete failed alter table tablet file
    if (alter_tablet_state == ALTER_FAILED) {
        // !!! could not drop failed tablet
        // schema change job is in finishing state in fe, be restarts, then the tablet is in ALTER_TABLE_FAILED state
        // if drop the old tablet, then data is lost
        status = TabletManager::instance()->drop_tablet(_alter_tablet_req.new_tablet_req.tablet_id, 
                                                            _alter_tablet_req.new_tablet_req.tablet_schema.schema_hash);

        if (status != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("delete failed rollup file failed, status: %d, "
                                "signature: %ld.",
                                status, _signature);
            _error_msgs->push_back("delete failed rollup file failed, "
                                    "signature: " + to_string(_signature));
        }
    }

    if (status == OLAP_SUCCESS) {
        // if there is one running alter task, then not start current task
        if (alter_tablet_state == ALTER_FINISHED
                || alter_tablet_state == ALTER_FAILED 
                || alter_tablet_state == ALTER_NONE) {
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
    }

    return status;
} // execute

OLAPStatus EngineSchemaChangeTask::_create_rollup_tablet(const TAlterTabletReq& request) {
    LOG(INFO) << "begin to create rollup tablet. "
              << "old_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id;

    DorisMetrics::create_rollup_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_tablet(ROLLUP, request);

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to do rollup. "
                         "[base_tablet=%ld new_tablet=%ld] [res=%d]",
                         request.base_tablet_id, request.new_tablet_req.tablet_id, res);
        DorisMetrics::create_rollup_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << "success to create rollup tablet. res=" << res
              << ", old_tablet_id=" << request.base_tablet_id 
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id;
    return res;
} // create_rollup_tablet

OLAPStatus EngineSchemaChangeTask::_schema_change(const TAlterTabletReq& request) {
    LOG(INFO) << "begin to schema change. old_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id;

    DorisMetrics::schema_change_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_tablet(SCHEMA_CHANGE, request);

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to do schema change. "
                         "[base_tablet=%ld new_tablet=%ld] [res=%d]",
                         request.base_tablet_id, request.new_tablet_req.tablet_id, res);
        DorisMetrics::schema_change_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << "success to submit schema change."
              << "old_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id;
    return res;
}

} // doris
