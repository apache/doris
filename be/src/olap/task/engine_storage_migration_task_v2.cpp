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

#include "olap/task/engine_storage_migration_task_v2.h"

#include "olap/storage_migration_v2.h"
#include "runtime/mem_tracker.h"

namespace doris {

using std::to_string;

EngineStorageMigrationTaskV2::EngineStorageMigrationTaskV2(const TStorageMigrationReqV2& request)
        : _storage_migration_req(request) {
    _mem_tracker = MemTracker::create_tracker(
            config::memory_limitation_per_thread_for_storage_migration_bytes,
            fmt::format("EngineStorageMigrationTaskV2: {}-{}",
                        std::to_string(_storage_migration_req.base_tablet_id),
                        std::to_string(_storage_migration_req.new_tablet_id)),
            StorageEngine::instance()->storage_migration_mem_tracker(), MemTrackerLevel::TASK);
}

Status EngineStorageMigrationTaskV2::execute() {
    DorisMetrics::instance()->storage_migrate_v2_requests_total->increment(1);
    StorageMigrationV2Handler* storage_migration_v2_handler = StorageMigrationV2Handler::instance();
    Status res = storage_migration_v2_handler->process_storage_migration_v2(_storage_migration_req);

    if (!res.ok()) {
        LOG(WARNING) << "failed to do storage migration task. res=" << res
                     << " base_tablet_id=" << _storage_migration_req.base_tablet_id
                     << ", base_schema_hash=" << _storage_migration_req.base_schema_hash
                     << ", new_tablet_id=" << _storage_migration_req.new_tablet_id
                     << ", new_schema_hash=" << _storage_migration_req.new_schema_hash;
        DorisMetrics::instance()->storage_migrate_v2_requests_failed->increment(1);
        return res;
    }

    LOG(INFO) << "success to create new storage migration v2. res=" << res
              << " base_tablet_id=" << _storage_migration_req.base_tablet_id << ", base_schema_hash"
              << _storage_migration_req.base_schema_hash
              << ", new_tablet_id=" << _storage_migration_req.new_tablet_id
              << ", new_schema_hash=" << _storage_migration_req.new_schema_hash;
    return res;
} // execute

} // namespace doris
