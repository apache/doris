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

#include "tablet_migration_action.h"

#include "olap/storage_engine.h"
#include "olap/task/engine_storage_migration_task.h"

namespace doris {
TabletMigrationAction::TabletMigrationAction() : BaseHttpHandler("tablet_migration") {
    _init_migration_action();
}

void TabletMigrationAction::_init_migration_action() {
    int32_t max_thread_num = config::max_tablet_migration_threads;
    int32_t min_thread_num = config::min_tablet_migration_threads;
    ThreadPoolBuilder("MigrationTaskThreadPool")
            .set_min_threads(min_thread_num)
            .set_max_threads(max_thread_num)
            .build(&_migration_thread_pool);
}

Status TabletMigrationAction::_execute_tablet_migration(TabletSharedPtr tablet,
                                                        DataDir* dest_store) {
    int64_t tablet_id = tablet->tablet_id();
    int32_t schema_hash = tablet->schema_hash();
    string dest_disk = dest_store->path();
    EngineStorageMigrationTask engine_task(tablet, dest_store);
    Status res = StorageEngine::instance()->execute_task(&engine_task);
    if (!res.ok()) {
        LOG(WARNING) << "tablet migrate failed. tablet_id=" << tablet_id
                     << ", schema_hash=" << schema_hash << ", dest_disk=" << dest_disk
                     << ", status:" << res;
    } else {
        LOG(INFO) << "tablet migrate success. tablet_id=" << tablet_id
                  << ", schema_hash=" << schema_hash << ", dest_disk=" << dest_disk;
    }
    return res;
}

Status TabletMigrationAction::_check_param(brpc::Controller* cntl, int64_t& tablet_id,
                                           int32_t& schema_hash, string& dest_disk, string& goal) {
    const std::string& req_tablet_id = *get_param(cntl, "tablet_id");
    const std::string& req_schema_hash = *get_param(cntl, "schema_hash");
    try {
        tablet_id = std::stoull(req_tablet_id);
        schema_hash = std::stoul(req_schema_hash);
    } catch (const std::exception& e) {
        LOG(WARNING) << "invalid argument.tablet_id:" << req_tablet_id
                     << ", schema_hash:" << req_schema_hash;
        return Status::InternalError("Convert failed, {}", e.what());
    }
    dest_disk = *get_param(cntl, "disk");
    goal = *get_param(cntl, "goal");
    if (goal != "run" && goal != "status") {
        return Status::InternalError("invalid goal argument.");
    }
    return Status::OK();
}

Status _check_migrate_request(int64_t tablet_id, int32_t schema_hash, string dest_disk,
                              TabletSharedPtr& tablet, DataDir** dest_store) {
    tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "no tablet for tablet_id:" << tablet_id;
        return Status::NotFound("Tablet not found");
    }

    // request specify the data dir
    *dest_store = StorageEngine::instance()->get_store(dest_disk);
    if (*dest_store == nullptr) {
        LOG(WARNING) << "data dir not found: " << dest_disk;
        return Status::NotFound("Disk not found");
    }

    if (tablet->data_dir() == *dest_store) {
        LOG(WARNING) << "tablet already exist in destine disk: " << dest_disk;
        return Status::AlreadyExist("Tablet already exist in destination disk");
    }

    // check local disk capacity
    int64_t tablet_size = tablet->tablet_local_size();
    if ((*dest_store)->reach_capacity_limit(tablet_size)) {
        LOG(WARNING) << "reach the capacity limit of path: " << (*dest_store)->path()
                     << ", tablet size: " << tablet_size;
        return Status::InternalError("Insufficient disk capacity");
    }

    return Status::OK();
}

} // namespace doris