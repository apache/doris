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

#pragma once

#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/tablet_fwd.h"
#include "olap/task/engine_task.h"

namespace doris {
class DataDir;
class StorageEngine;

/// This task is used to migrate the specified tablet to the specified data directory.
// Usually used for storage medium migration, or migration of tablets between disks.
class EngineStorageMigrationTask final : public EngineTask {
public:
    Status execute() override;

    EngineStorageMigrationTask(StorageEngine& engine, TabletSharedPtr tablet, DataDir* dest_store);
    ~EngineStorageMigrationTask() override;

private:
    Status _migrate();
    // check if task is timeout
    bool _is_timeout();
    Status _get_versions(int32_t start_version, int32_t* end_version,
                         std::vector<RowsetSharedPtr>* consistent_rowsets);
    Status _check_running_txns();
    // caller should not hold migration lock, and 'migration_wlock' should not be nullptr
    // ownership of the migration lock is transferred to the caller if check succ
    Status _check_running_txns_until_timeout(
            std::unique_lock<std::shared_timed_mutex>* migration_wlock);

    // if the size less than threshold, return true
    bool _is_rowsets_size_less_than_threshold(
            const std::vector<RowsetSharedPtr>& consistent_rowsets);

    Status _gen_and_write_header_to_hdr_file(uint64_t shard, const std::string& full_path,
                                             const std::vector<RowsetSharedPtr>& consistent_rowsets,
                                             int64_t end_version);
    Status _reload_tablet(const std::string& full_path);

    void _generate_new_header(uint64_t new_shard,
                              const std::vector<RowsetSharedPtr>& consistent_rowsets,
                              TabletMetaSharedPtr new_tablet_meta, int64_t end_version);

    // TODO: hkp
    // rewrite this function
    Status _copy_index_and_data_files(const std::string& full_path,
                                      const std::vector<RowsetSharedPtr>& consistent_rowsets) const;

private:
    StorageEngine& _engine;
    // tablet to do migrated
    TabletSharedPtr _tablet;
    // destination data dir
    DataDir* _dest_store = nullptr;
    int64_t _task_start_time;
    std::vector<PendingRowsetGuard> _pending_rs_guards;
}; // EngineTask

} // namespace doris
