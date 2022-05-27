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

#include <ctime>

#include "olap/snapshot_manager.h"
#include "olap/tablet_meta_manager.h"

namespace doris {

using std::stringstream;

const int CHECK_TXNS_MAX_WAIT_TIME_SECS = 60;

EngineStorageMigrationTask::EngineStorageMigrationTask(const TabletSharedPtr& tablet,
                                                       DataDir* dest_store)
        : _tablet(tablet), _dest_store(dest_store) {
    _task_start_time = time(nullptr);
}

Status EngineStorageMigrationTask::execute() {
    return _migrate();
}

Status EngineStorageMigrationTask::_get_versions(int32_t start_version, int32_t* end_version,
                                                 std::vector<RowsetSharedPtr>* consistent_rowsets) {
    std::shared_lock rdlock(_tablet->get_header_lock());
    const RowsetSharedPtr last_version = _tablet->rowset_with_max_version();
    if (last_version == nullptr) {
        LOG(WARNING) << "failed to get rowset with max version, tablet=" << _tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_WRITE_PROTOBUF_ERROR);
    }

    *end_version = last_version->end_version();
    if (*end_version < start_version) {
        // rowsets are empty
        VLOG_DEBUG << "consistent rowsets empty. tablet=" << _tablet->full_name()
                   << ", start_version=" << start_version << ", end_version=" << *end_version;
        return Status::OK();
    }
    _tablet->capture_consistent_rowsets(Version(start_version, *end_version), consistent_rowsets);
    if (consistent_rowsets->empty()) {
        LOG(WARNING) << "fail to capture consistent rowsets. tablet=" << _tablet->full_name()
                     << ", version=" << *end_version;
        return Status::OLAPInternalError(OLAP_ERR_WRITE_PROTOBUF_ERROR);
    }
    return Status::OK();
}

bool EngineStorageMigrationTask::_is_timeout() {
    int64_t time_elapsed = time(nullptr) - _task_start_time;
    if (time_elapsed > config::migration_task_timeout_secs) {
        LOG(WARNING) << "migration failed due to timeout, time_eplapsed=" << time_elapsed
                     << ", tablet=" << _tablet->full_name();
        return true;
    }
    return false;
}

Status EngineStorageMigrationTask::_check_running_txns() {
    // need hold migration lock outside
    int64_t partition_id;
    std::set<int64_t> transaction_ids;
    // check if this tablet has related running txns. if yes, can not do migration.
    StorageEngine::instance()->txn_manager()->get_tablet_related_txns(
            _tablet->tablet_id(), _tablet->schema_hash(), _tablet->tablet_uid(), &partition_id,
            &transaction_ids);
    if (transaction_ids.size() > 0) {
        return Status::OLAPInternalError(OLAP_ERR_HEADER_HAS_PENDING_DATA);
    }
    return Status::OK();
}

Status EngineStorageMigrationTask::_check_running_txns_until_timeout(
        std::unique_lock<std::shared_mutex>* migration_wlock) {
    // caller should not hold migration lock, and 'migration_wlock' should not be nullptr
    // ownership of the migration_wlock is transferred to the caller if check succ
    DCHECK_NE(migration_wlock, nullptr);
    Status res = Status::OK();
    int try_times = 1;
    do {
        // to avoid invalid loops, the lock is guaranteed to be acquired here
        std::unique_lock<std::shared_mutex> wlock(_tablet->get_migration_lock());
        res = _check_running_txns();
        if (res.ok()) {
            // transfer the lock to the caller
            *migration_wlock = std::move(wlock);
            return res;
        }
        LOG(INFO) << "check running txns fail, try again until timeout."
                  << " tablet=" << _tablet->full_name() << ", try times=" << try_times
                  << ", res=" << res;
        // unlock and sleep for a while, try again
        wlock.unlock();
        sleep(std::min(config::sleep_one_second * try_times, CHECK_TXNS_MAX_WAIT_TIME_SECS));
        ++try_times;
    } while (!_is_timeout());
    return res;
}

Status EngineStorageMigrationTask::_gen_and_write_header_to_hdr_file(
        uint64_t shard, const std::string& full_path,
        const std::vector<RowsetSharedPtr>& consistent_rowsets) {
    // need hold migration lock and push lock outside
    Status res = Status::OK();
    int64_t tablet_id = _tablet->tablet_id();
    int32_t schema_hash = _tablet->schema_hash();
    TabletMetaSharedPtr new_tablet_meta(new (std::nothrow) TabletMeta());
    {
        std::shared_lock rdlock(_tablet->get_header_lock());
        _generate_new_header(shard, consistent_rowsets, new_tablet_meta);
    }
    std::string new_meta_file = full_path + "/" + std::to_string(tablet_id) + ".hdr";
    res = new_tablet_meta->save(new_meta_file);
    if (!res.ok()) {
        LOG(WARNING) << "failed to save meta to path: " << new_meta_file;
        return res;
    }

    // reset tablet id and rowset id
    res = TabletMeta::reset_tablet_uid(new_meta_file);
    if (!res.ok()) {
        LOG(WARNING) << "errors while set tablet uid: '" << new_meta_file;
        return res;
    }
    // it will change rowset id and its create time
    // rowset create time is useful when load tablet from meta to check which tablet is the tablet to load
    res = SnapshotManager::instance()->convert_rowset_ids(full_path, tablet_id, schema_hash);
    if (!res.ok()) {
        LOG(WARNING) << "failed to convert rowset id when do storage migration"
                     << " path = " << full_path;
        return res;
    }
    return res;
}

Status EngineStorageMigrationTask::_reload_tablet(const std::string& full_path) {
    // need hold migration lock and push lock outside
    Status res = Status::OK();
    int64_t tablet_id = _tablet->tablet_id();
    int32_t schema_hash = _tablet->schema_hash();
    res = StorageEngine::instance()->tablet_manager()->load_tablet_from_dir(
            _dest_store, tablet_id, schema_hash, full_path, false);
    if (!res.ok()) {
        LOG(WARNING) << "failed to load tablet from new path. tablet_id=" << tablet_id
                     << " schema_hash=" << schema_hash << " path = " << full_path;
        return res;
    }

    // if old tablet finished schema change, then the schema change status of the new tablet is DONE
    // else the schema change status of the new tablet is FAILED
    TabletSharedPtr new_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
    if (new_tablet == nullptr) {
        LOG(WARNING) << "tablet not found. tablet_id=" << tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }
    return res;
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

    DorisMetrics::instance()->storage_migrate_requests_total->increment(1);
    int32_t start_version = 0;
    int32_t end_version = 0;
    std::vector<RowsetSharedPtr> consistent_rowsets;

    // try hold migration lock first
    Status res = Status::OK();
    uint64_t shard = 0;
    string full_path;
    {
        std::unique_lock<std::shared_mutex> migration_wlock(_tablet->get_migration_lock(),
                                                            std::try_to_lock);
        if (!migration_wlock.owns_lock()) {
            return Status::OLAPInternalError(OLAP_ERR_RWLOCK_ERROR);
        }

        // check if this tablet has related running txns. if yes, can not do migration.
        res = _check_running_txns();
        if (!res.ok()) {
            LOG(WARNING) << "could not migration because has unfinished txns, "
                         << " tablet=" << _tablet->full_name();
            return res;
        }

        std::lock_guard<std::mutex> lock(_tablet->get_push_lock());
        // get versions to be migrate
        res = _get_versions(start_version, &end_version, &consistent_rowsets);
        if (!res.ok()) {
            return res;
        }

        // TODO(ygl): the tablet should not under schema change or rollup or load
        res = _dest_store->get_shard(&shard);
        if (!res.ok()) {
            LOG(WARNING) << "fail to get shard from store: " << _dest_store->path();
            return res;
        }
        FilePathDescStream root_path_desc_s;
        root_path_desc_s << _dest_store->path_desc() << DATA_PREFIX << "/" << shard;
        FilePathDesc full_path_desc = SnapshotManager::instance()->get_schema_hash_full_path(
                _tablet, root_path_desc_s.path_desc());
        full_path = full_path_desc.filepath;
        // if dir already exist then return err, it should not happen.
        // should not remove the dir directly, for safety reason.
        if (FileUtils::check_exist(full_path)) {
            LOG(INFO) << "schema hash path already exist, skip this path. "
                      << "full_path=" << full_path;
            return Status::OLAPInternalError(OLAP_ERR_FILE_ALREADY_EXIST);
        }

        Status st = FileUtils::create_dir(full_path);
        if (!st.ok()) {
            res = Status::OLAPInternalError(OLAP_ERR_CANNOT_CREATE_DIR);
            LOG(WARNING) << "fail to create path. path=" << full_path
                         << ", error:" << st.to_string();
            return res;
        }
    }

    std::vector<RowsetSharedPtr> temp_consistent_rowsets(consistent_rowsets);
    do {
        // migrate all index and data files but header file
        res = _copy_index_and_data_files(full_path, temp_consistent_rowsets);
        if (!res.ok()) {
            LOG(WARNING) << "fail to copy index and data files when migrate. res=" << res;
            break;
        }
        std::unique_lock<std::shared_mutex> migration_wlock;
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
                    LOG(WARNING)
                            << "fail to copy the remaining index and data files when migrate. res="
                            << res;
                    break;
                }
            } else {
                if (_is_timeout()) {
                    res = Status::OLAPInternalError(OLAP_ERR_HEADER_HAS_PENDING_DATA);
                    break;
                }
                // there is too much remaining data here.
                // in order not to affect other tasks, release the lock and then copy it
                continue;
            }
        }

        // generate new tablet meta and write to hdr file
        res = _gen_and_write_header_to_hdr_file(shard, full_path, consistent_rowsets);
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
        FileUtils::remove_all(full_path);
    }
    return res;
}

// TODO(ygl): lost some information here, such as cumulative layer point
void EngineStorageMigrationTask::_generate_new_header(
        uint64_t new_shard, const std::vector<RowsetSharedPtr>& consistent_rowsets,
        TabletMetaSharedPtr new_tablet_meta) {
    _tablet->generate_tablet_meta_copy_unlocked(new_tablet_meta);

    std::vector<RowsetMetaSharedPtr> rs_metas;
    for (auto& rs : consistent_rowsets) {
        rs_metas.push_back(rs->rowset_meta());
    }
    new_tablet_meta->revise_rs_metas(std::move(rs_metas));
    new_tablet_meta->set_shard_id(new_shard);
    // should not save new meta here, because new tablet may failed
    // should not remove the old meta here, because the new header maybe not valid
    // remove old meta after the new tablet is loaded successfully
}

Status EngineStorageMigrationTask::_copy_index_and_data_files(
        const string& full_path, const std::vector<RowsetSharedPtr>& consistent_rowsets) const {
    Status status = Status::OK();
    for (const auto& rs : consistent_rowsets) {
        status = rs->copy_files_to(full_path, rs->rowset_id());
        if (!status.ok()) {
            Status ret = FileUtils::remove_all(full_path);
            if (!ret.ok()) {
                LOG(FATAL) << "remove storage migration path failed. "
                           << "full_path:" << full_path << " error: " << ret.to_string();
            }
            break;
        }
    }
    return status;
}

} // namespace doris
