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

#include "olap/task/engine_storage_migration_task.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <unistd.h>

#include <algorithm>
#include <ctime>
#include <memory>
#include <mutex>
#include <new>
#include <ostream>
#include <set>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/numbers.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/txn_manager.h"
#include "util/doris_metrics.h"
#include "util/uid_util.h"

namespace doris {

using std::stringstream;

EngineStorageMigrationTask::EngineStorageMigrationTask(StorageEngine& engine,
                                                       TabletSharedPtr tablet, DataDir* dest_store)
        : _engine(engine), _tablet(std::move(tablet)), _dest_store(dest_store) {
    _task_start_time = time(nullptr);
    _mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "EngineStorageMigrationTask");
}

EngineStorageMigrationTask::~EngineStorageMigrationTask() = default;

Status EngineStorageMigrationTask::execute() {
    return _migrate();
}

Status EngineStorageMigrationTask::_get_versions(int32_t start_version, int32_t* end_version,
                                                 std::vector<RowsetSharedPtr>* consistent_rowsets) {
    std::shared_lock rdlock(_tablet->get_header_lock());
    // check if tablet is in cooldown, we don't support migration in this case
    if (_tablet->tablet_meta()->cooldown_meta_id().initialized()) {
        LOG(WARNING) << "currently not support migrate tablet with cooldowned remote data. tablet="
                     << _tablet->tablet_id();
        return Status::NotSupported(
                "currently not support migrate tablet with cooldowned remote data");
    }
    const RowsetSharedPtr last_version = _tablet->get_rowset_with_max_version();
    if (last_version == nullptr) {
        return Status::InternalError("failed to get rowset with max version, tablet={}",
                                     _tablet->tablet_id());
    }

    *end_version = last_version->end_version();
    if (*end_version < start_version) {
        // rowsets are empty
        VLOG_DEBUG << "consistent rowsets empty. tablet=" << _tablet->tablet_id()
                   << ", start_version=" << start_version << ", end_version=" << *end_version;
        return Status::OK();
    }
    return _tablet->capture_consistent_rowsets_unlocked(Version(start_version, *end_version),
                                                        consistent_rowsets);
}

bool EngineStorageMigrationTask::_is_timeout() {
    int64_t time_elapsed = time(nullptr) - _task_start_time;
    int64_t timeout = std::max<int64_t>(config::migration_task_timeout_secs,
                                        _tablet->tablet_local_size() >> 20);
    if (time_elapsed > timeout) {
        LOG(WARNING) << "migration failed due to timeout, time_elapsed=" << time_elapsed
                     << ", tablet=" << _tablet->tablet_id();
        return true;
    }
    return false;
}

Status EngineStorageMigrationTask::_check_running_txns() {
    // need hold migration lock outside
    int64_t partition_id;
    std::set<int64_t> transaction_ids;
    // check if this tablet has related running txns. if yes, can not do migration.
    _engine.txn_manager()->get_tablet_related_txns(_tablet->tablet_id(), _tablet->tablet_uid(),
                                                   &partition_id, &transaction_ids);
    if (!transaction_ids.empty()) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("tablet {} has unfinished txns",
                                                               _tablet->tablet_id());
    }
    return Status::OK();
}

Status EngineStorageMigrationTask::_check_running_txns_until_timeout(
        std::unique_lock<std::shared_timed_mutex>* migration_wlock) {
    // caller should not hold migration lock, and 'migration_wlock' should not be nullptr
    // ownership of the migration_wlock is transferred to the caller if check succ
    DCHECK_NE(migration_wlock, nullptr);
    Status res = Status::OK();
    do {
        // to avoid invalid loops, the lock is guaranteed to be acquired here
        {
            std::unique_lock<std::shared_timed_mutex> wlock(_tablet->get_migration_lock());
            if (_tablet->tablet_state() == TABLET_SHUTDOWN) {
                return Status::Error<ErrorCode::INTERNAL_ERROR, false>("tablet {} has deleted",
                                                                       _tablet->tablet_id());
            }
            res = _check_running_txns();
            if (res.ok()) {
                // transfer the lock to the caller
                *migration_wlock = std::move(wlock);
                return res;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    } while (!_is_timeout());
    return res;
}

Status EngineStorageMigrationTask::_gen_and_write_header_to_hdr_file(
        uint64_t shard, const std::string& full_path,
        const std::vector<RowsetSharedPtr>& consistent_rowsets, int64_t end_version) {
    // need hold migration lock and push lock outside
    int64_t tablet_id = _tablet->tablet_id();
    int32_t schema_hash = _tablet->schema_hash();
    TabletMetaSharedPtr new_tablet_meta(new (std::nothrow) TabletMeta());
    {
        std::shared_lock rdlock(_tablet->get_header_lock());
        _generate_new_header(shard, consistent_rowsets, new_tablet_meta, end_version);
    }
    std::string new_meta_file = full_path + "/" + std::to_string(tablet_id) + ".hdr";
    RETURN_IF_ERROR(new_tablet_meta->save(new_meta_file));

    // it will change rowset id and its create time
    // rowset create time is useful when load tablet from meta to check which tablet is the tablet to load
    _pending_rs_guards = DORIS_TRY(_engine.snapshot_mgr()->convert_rowset_ids(
            full_path, tablet_id, _tablet->replica_id(), _tablet->table_id(),
            _tablet->partition_id(), schema_hash));
    return Status::OK();
}

Status EngineStorageMigrationTask::_reload_tablet(const std::string& full_path) {
    if (_tablet->tablet_state() == TABLET_SHUTDOWN) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("tablet {} has deleted",
                                                               _tablet->tablet_id());
    }
    // need hold migration lock and push lock outside
    int64_t tablet_id = _tablet->tablet_id();
    int32_t schema_hash = _tablet->schema_hash();
    RETURN_IF_ERROR(_engine.tablet_manager()->load_tablet_from_dir(_dest_store, tablet_id,
                                                                   schema_hash, full_path, false));

    // if old tablet finished schema change, then the schema change status of the new tablet is DONE
    // else the schema change status of the new tablet is FAILED
    TabletSharedPtr new_tablet = _engine.tablet_manager()->get_tablet(tablet_id);
    if (new_tablet == nullptr) {
        return Status::NotFound("could not find tablet {}", tablet_id);
    }
    return Status::OK();
}

// if the size less than threshold, return true
bool EngineStorageMigrationTask::_is_rowsets_size_less_than_threshold(
        const std::vector<RowsetSharedPtr>& consistent_rowsets) {
    size_t total_size = 0;
    for (const auto& rs : consistent_rowsets) {
        total_size += rs->index_disk_size() + rs->data_disk_size();
    }
    if (total_size < config::migration_remaining_size_threshold_mb) {
        return true;
    }
    return false;
}

Status EngineStorageMigrationTask::_migrate() {
    int64_t tablet_id = _tablet->tablet_id();
    LOG(INFO) << "begin to process tablet migrate. "
              << "tablet_id=" << tablet_id << ", dest_store=" << _dest_store->path();

    RETURN_IF_ERROR(_engine.tablet_manager()->register_transition_tablet(_tablet->tablet_id(),
                                                                         "disk migrate"));
    Defer defer {[&]() {
        _engine.tablet_manager()->unregister_transition_tablet(_tablet->tablet_id(),
                                                               "disk migrate");
    }};

    DorisMetrics::instance()->storage_migrate_requests_total->increment(1);
    int32_t start_version = 0;
    int32_t end_version = 0;
    std::vector<RowsetSharedPtr> consistent_rowsets;

    // During migration, if the rowsets being migrated undergoes a compaction operation,
    // that will result in incorrect delete bitmaps after migration for mow table. Therefore,
    // compaction will be prohibited for the mow table when migration. Moreover, it is useless
    // to perform a compaction operation on the migration data, as the migration still migrates
    // the data of rowsets before the compaction operation.
    std::unique_lock base_compaction_lock(_tablet->get_base_compaction_lock(), std::defer_lock);
    std::unique_lock cumu_compaction_lock(_tablet->get_cumulative_compaction_lock(),
                                          std::defer_lock);
    if (_tablet->enable_unique_key_merge_on_write()) {
        base_compaction_lock.lock();
        cumu_compaction_lock.lock();
    }

    // try hold migration lock first
    Status res;
    uint64_t shard = 0;
    std::string full_path;
    {
        std::unique_lock<std::shared_timed_mutex> migration_wlock(_tablet->get_migration_lock(),
                                                                  std::chrono::seconds(1));
        if (!migration_wlock.owns_lock()) {
            return Status::InternalError("could not own migration_wlock");
        }

        // check if this tablet has related running txns. if yes, can not do migration.
        RETURN_IF_ERROR(_check_running_txns());

        std::lock_guard<std::mutex> lock(_tablet->get_push_lock());
        // get versions to be migrate
        RETURN_IF_ERROR(_get_versions(start_version, &end_version, &consistent_rowsets));

        // TODO(ygl): the tablet should not under schema change or rollup or load
        shard = _dest_store->get_shard();

        auto shard_path = fmt::format("{}/{}/{}", _dest_store->path(), DATA_PREFIX, shard);
        full_path = SnapshotManager::get_schema_hash_full_path(_tablet, shard_path);
        // if dir already exist then return err, it should not happen.
        // should not remove the dir directly, for safety reason.
        bool exists = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(full_path, &exists));
        if (exists) {
            return Status::AlreadyExist("schema hash path {} already exist, skip this path",
                                        full_path);
        }
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(full_path));
    }

    std::vector<RowsetSharedPtr> temp_consistent_rowsets(consistent_rowsets);
    do {
        // migrate all index and data files but header file
        res = _copy_index_and_data_files(full_path, temp_consistent_rowsets);
        if (!res.ok()) {
            break;
        }
        std::unique_lock<std::shared_timed_mutex> migration_wlock;
        res = _check_running_txns_until_timeout(&migration_wlock);
        if (!res.ok()) {
            break;
        }
        std::lock_guard<std::mutex> lock(_tablet->get_push_lock());
        start_version = end_version;
        // clear temp rowsets before get remaining versions
        temp_consistent_rowsets.clear();
        // get remaining versions
        res = _get_versions(end_version + 1, &end_version, &temp_consistent_rowsets);
        if (!res.ok()) {
            break;
        }
        if (start_version < end_version) {
            // we have remaining versions to be migrated
            consistent_rowsets.insert(consistent_rowsets.end(), temp_consistent_rowsets.begin(),
                                      temp_consistent_rowsets.end());
            LOG(INFO) << "we have remaining versions to be migrated. start_version="
                      << start_version << " end_version=" << end_version;
            // if the remaining size is less than config::migration_remaining_size_threshold_mb(default 10MB),
            // we take the lock to complete it to avoid long-term competition with other tasks
            if (_is_rowsets_size_less_than_threshold(temp_consistent_rowsets)) {
                // force to copy the remaining data and index
                res = _copy_index_and_data_files(full_path, temp_consistent_rowsets);
                if (!res.ok()) {
                    break;
                }
            } else {
                if (_is_timeout()) {
                    res = Status::TimedOut("failed to migrate due to timeout");
                    break;
                }
                // there is too much remaining data here.
                // in order not to affect other tasks, release the lock and then copy it
                continue;
            }
        }

        // generate new tablet meta and write to hdr file
        res = _gen_and_write_header_to_hdr_file(shard, full_path, consistent_rowsets, end_version);
        if (!res.ok()) {
            break;
        }
        res = _reload_tablet(full_path);
        if (!res.ok()) {
            break;
        }

        break;
    } while (true);

    if (!res.ok()) {
        // we should remove the dir directly for avoid disk full of junk data, and it's safe to remove
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(full_path));
        RETURN_IF_ERROR(DataDir::delete_tablet_parent_path_if_empty(full_path));
    }
    return res;
}

// TODO(ygl): lost some information here, such as cumulative layer point
void EngineStorageMigrationTask::_generate_new_header(
        uint64_t new_shard, const std::vector<RowsetSharedPtr>& consistent_rowsets,
        TabletMetaSharedPtr new_tablet_meta, int64_t end_version) {
    _tablet->generate_tablet_meta_copy_unlocked(*new_tablet_meta);

    std::vector<RowsetMetaSharedPtr> rs_metas;
    for (auto& rs : consistent_rowsets) {
        rs_metas.push_back(rs->rowset_meta());
    }
    new_tablet_meta->revise_rs_metas(std::move(rs_metas));
    if (_tablet->keys_type() == UNIQUE_KEYS && _tablet->enable_unique_key_merge_on_write()) {
        DeleteBitmap bm = _tablet->tablet_meta()->delete_bitmap().snapshot(end_version);
        new_tablet_meta->revise_delete_bitmap_unlocked(bm);
    }
    new_tablet_meta->set_shard_id(new_shard);
    // should not save new meta here, because new tablet may failed
    // should not remove the old meta here, because the new header maybe not valid
    // remove old meta after the new tablet is loaded successfully
}

Status EngineStorageMigrationTask::_copy_index_and_data_files(
        const string& full_path, const std::vector<RowsetSharedPtr>& consistent_rowsets) const {
    for (const auto& rs : consistent_rowsets) {
        RETURN_IF_ERROR(rs->copy_files_to(full_path, rs->rowset_id()));
    }
    return Status::OK();
}

} // namespace doris
