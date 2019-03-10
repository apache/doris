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
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("can't find tablet when process clear alter task. ",
                         "[tablet_id=%ld, schema_hash=%d]", tablet_id, schema_hash);
        return OLAP_SUCCESS;
    }

    // get schema change info
    tablet->obtain_header_rdlock();
    bool has_alter_task = tablet->has_alter_task();
    tablet->release_header_lock();

    if (!has_alter_task) {
        return OLAP_SUCCESS;
    }

    tablet->obtain_header_rdlock();
    const AlterTabletTask& alter_task = tablet->alter_task();
    AlterTabletState alter_state = alter_task.alter_state();
    TTabletId related_tablet_id = alter_task.related_tablet_id();
    TSchemaHash related_schema_hash = alter_task.related_schema_hash();
    tablet->release_header_lock();

    if (alter_state == ALTER_RUNNING) {
        LOG(WARNING) << "find alter task unfinished when process clear alter task. "
                     << "tablet=" << tablet->full_name();
        return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
    }

    // clear schema change info
    OLAPStatus res = tablet->protected_delete_alter_task();

    // clear related tablet's schema change info
    TabletSharedPtr related_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(related_tablet_id, related_schema_hash);
    if (related_tablet.get() == NULL) {
        LOG(WARNING) << "related tablet not found when process clear alter task."
                     << " tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
                     << ", related_tablet_id=" << related_tablet_id
                     << ", related_schema_hash=" << related_schema_hash;
    } else {
        res = related_tablet->protected_delete_alter_task();
    }

    LOG(INFO) << "finish to process clear alter task."
              << "tablet_id=" << related_tablet_id
              << ", schema_hash=" << related_schema_hash;
    return res;
}

} // doris
