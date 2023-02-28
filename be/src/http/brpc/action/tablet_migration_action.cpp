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

#include <brpc/http_method.h>

#include "olap/storage_engine.h"
#include "olap/task/engine_storage_migration_task.h"

namespace doris {
TabletMigrationHandler::TabletMigrationHandler() : BaseHttpHandler("tablet_migration") {
    _init_migration_action();
}

void TabletMigrationHandler::_init_migration_action() {
    int32_t max_thread_num = config::max_tablet_migration_threads;
    int32_t min_thread_num = config::min_tablet_migration_threads;
    ThreadPoolBuilder("MigrationTaskThreadPool")
            .set_min_threads(min_thread_num)
            .set_max_threads(max_thread_num)
            .build(&_migration_thread_pool);
}

Status TabletMigrationHandler::_execute_tablet_migration(TabletSharedPtr tablet,
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

Status TabletMigrationHandler::_check_param(brpc::Controller* cntl, int64_t& tablet_id,
                                            int32_t& schema_hash, string& dest_disk, string& goal) {
    const std::string* req_tablet_id = get_param(cntl, "tablet_id");
    const std::string* req_schema_hash = get_param(cntl, "schema_hash");
    try {
        if (req_tablet_id == nullptr || req_schema_hash == nullptr) {
            return Status::InternalError("invalid null argument.");
        }
        tablet_id = std::stoull(*req_tablet_id);
        schema_hash = std::stoul(*req_schema_hash);
    } catch (const std::exception& e) {
        LOG(WARNING) << "invalid argument.tablet_id:" << *req_tablet_id
                     << ", schema_hash:" << *req_schema_hash;
        return Status::InternalError("Convert failed, {}", e.what());
    }
    const std::string* dest_disk_ptr = get_param(cntl, "disk");
    const std::string* goal_ptr = get_param(cntl, "goal");
    if (dest_disk_ptr == nullptr || goal_ptr == nullptr) {
        return Status::InternalError("invalid null argument.");
    }
    dest_disk = *dest_disk_ptr;
    goal = *goal_ptr;
    if (goal != "run" && goal != "status") {
        return Status::InternalError("invalid goal argument.");
    }
    return Status::OK();
}

bool TabletMigrationHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_GET;
}

Status TabletMigrationHandler::_check_migrate_request(int64_t tablet_id, int32_t schema_hash,
                                                      string dest_disk, TabletSharedPtr& tablet,
                                                      DataDir** dest_store) {
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

void TabletMigrationHandler::handle_sync(brpc::Controller* cntl) {
    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    string dest_disk;
    string goal;
    Status status = _check_param(cntl, tablet_id, schema_hash, dest_disk, goal);
    if (status.ok()) {
        if (goal == "run") {
            MigrationTask current_task(tablet_id, schema_hash, dest_disk);
            TabletSharedPtr tablet;
            DataDir* dest_store;
            Status status =
                    _check_migrate_request(tablet_id, schema_hash, dest_disk, tablet, &dest_store);
            if (status.ok()) {
                do {
                    {
                        std::unique_lock<std::mutex> lock(_migration_status_mutex);
                        std::map<MigrationTask, std::string>::iterator it_task =
                                _migration_tasks.find(current_task);
                        if (it_task != _migration_tasks.end()) {
                            status = Status::AlreadyExist(
                                    "There is a migration task for this tablet already exists. "
                                    "dest_disk is {} .",
                                    (it_task->first)._dest_disk);
                            break;
                        }
                        _migration_tasks[current_task] = "submitted";
                    }
                    auto st = _migration_thread_pool->submit_func([&, dest_disk, current_task]() {
                        {
                            std::unique_lock<std::mutex> lock(_migration_status_mutex);
                            _migration_tasks[current_task] = "running";
                        }
                        Status result_status = _execute_tablet_migration(tablet, dest_store);
                        {
                            std::unique_lock<std::mutex> lock(_migration_status_mutex);
                            std::map<MigrationTask, std::string>::iterator it_task =
                                    _migration_tasks.find(current_task);
                            if (it_task != _migration_tasks.end()) {
                                _migration_tasks.erase(it_task);
                            }
                            std::pair<MigrationTask, Status> finished_task =
                                    std::make_pair(current_task, result_status);
                            if (_finished_migration_tasks.size() >=
                                config::finished_migration_tasks_size) {
                                _finished_migration_tasks.pop_front();
                            }
                            _finished_migration_tasks.push_back(finished_task);
                        }
                    });
                    if (!st.ok()) {
                        status = Status::InternalError("Migration task submission failed");
                        std::unique_lock<std::mutex> lock(_migration_status_mutex);
                        std::map<MigrationTask, std::string>::iterator it_task =
                                _migration_tasks.find(current_task);
                        if (it_task != _migration_tasks.end()) {
                            _migration_tasks.erase(it_task);
                        }
                    }
                } while (false);
            }
            std::string status_result;
            if (!status.ok()) {
                status_result = status.to_json();
            } else {
                status_result =
                        "{\"status\": \"Success\", \"msg\": \"migration task is successfully "
                        "submitted.\"}";
            }
            on_succ_json(cntl, status_result);
        } else {
            DCHECK(goal == "status");
            MigrationTask current_task(tablet_id, schema_hash);
            std::string status_result;
            do {
                std::unique_lock<std::mutex> lock(_migration_status_mutex);
                std::map<MigrationTask, std::string>::iterator it_task =
                        _migration_tasks.find(current_task);
                if (it_task != _migration_tasks.end()) {
                    status_result = "{\"status\": \"Success\", \"msg\": \"migration task is " +
                                    it_task->second + "\", \"dest_disk\": \"" +
                                    (it_task->first)._dest_disk + "\"}";
                    break;
                }

                int i = _finished_migration_tasks.size() - 1;
                for (; i >= 0; i--) {
                    MigrationTask finished_task = _finished_migration_tasks[i].first;
                    if (finished_task._tablet_id == tablet_id &&
                        finished_task._schema_hash == schema_hash) {
                        status = _finished_migration_tasks[i].second;
                        if (status.ok()) {
                            status_result =
                                    "{\"status\": \"Success\", \"msg\": \"migration task has "
                                    "finished successfully\", \"dest_disk\": \"" +
                                    finished_task._dest_disk + "\"}";
                        }
                        break;
                    }
                }
                if (i < 0) {
                    status = Status::NotFound("Migration task not found");
                }
            } while (false);
            if (!status.ok()) {
                status_result = status.to_json();
            }
            on_succ_json(cntl, status_result);
        }
    } else {
        std::string status_result = status.to_json();
        on_succ_json(cntl, status_result);
    }
}

} // namespace doris