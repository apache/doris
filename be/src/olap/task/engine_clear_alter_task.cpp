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

#include "olap/task/engine_clear_alter_task.h"

namespace doris {

EngineClearAlterTask::EngineClearAlterTask(const TClearAlterTaskRequest& request)
    :_clear_alter_task_req(request) {

}

OLAPStatus EngineClearAlterTask::execute() {
    OLAPStatus status = _clear_alter_task(_clear_alter_task_req.tablet_id, _clear_alter_task_req.schema_hash);

    return status;
}

OLAPStatus EngineClearAlterTask::_clear_alter_task(const TTabletId tablet_id,
                                const TSchemaHash schema_hash) {
    LOG(INFO) << "begin to process clear alter task. tablet_id=" << tablet_id
              << ", schema_hash=" << schema_hash;
    TabletSharedPtr tablet = TabletManager::instance()->get_tablet(tablet_id, schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("can't find tablet when process clear alter task. ",
                         "[tablet_id=%ld, schema_hash=%d]", tablet_id, schema_hash);
        return OLAP_SUCCESS;
    }

    // get schema change info
    AlterTabletType type;
    TTabletId related_tablet_id;
    TSchemaHash related_schema_hash;
    vector<Version> schema_change_versions;
    tablet->obtain_header_rdlock();
    bool ret = tablet->get_schema_change_request(
            &related_tablet_id, &related_schema_hash, &schema_change_versions, &type);
    tablet->release_header_lock();
    if (!ret) {
        return OLAP_SUCCESS;
    } else if (!schema_change_versions.empty()) {
        OLAP_LOG_WARNING("find alter task unfinished when process clear alter task. ",
                         "[tablet=%s versions_to_change_size=%d]",
                         tablet->full_name().c_str(), schema_change_versions.size());
        return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
    }

    // clear schema change info
    tablet->obtain_header_wrlock();
    tablet->clear_schema_change_request();
    OLAPStatus res = tablet->save_tablet_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save header. [res=" << res << " tablet='" << tablet->full_name() << "']";
    } else {
        LOG(INFO) << "clear alter task on tablet. [tablet='" << tablet->full_name() << "']";
    }
    tablet->release_header_lock();

    // clear related tablet's schema change info
    TabletSharedPtr related_tablet = TabletManager::instance()->get_tablet(related_tablet_id, related_schema_hash);
    if (related_tablet.get() == NULL) {
        OLAP_LOG_WARNING("related tablet not found when process clear alter task. "
                         "[tablet_id=%ld schema_hash=%d "
                         "related_tablet_id=%ld related_schema_hash=%d]",
                         tablet_id, schema_hash, related_tablet_id, related_schema_hash);
    } else {
        related_tablet->obtain_header_wrlock();
        related_tablet->clear_schema_change_request();
        res = related_tablet->save_tablet_meta();
        if (res != OLAP_SUCCESS) {
            LOG(FATAL) << "fail to save header. [res=" << res << " tablet='"
                       << related_tablet->full_name() << "']";
        } else {
            LOG(INFO) << "clear alter task on tablet. [tablet='" << related_tablet->full_name() << "']";
        }
        related_tablet->release_header_lock();
    }

    LOG(INFO) << "finish to process clear alter task."
              << "tablet_id=" << related_tablet_id
              << ", schema_hash=" << related_schema_hash;
    return OLAP_SUCCESS;
}

} // doris