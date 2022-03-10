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

#include "olap/snapshot_manager.h"
#include "olap/tablet_meta_manager.h"

namespace doris {

using std::stringstream;

EngineStorageMigrationTask::EngineStorageMigrationTask(const TabletSharedPtr& tablet,
                                                       DataDir* dest_store)
        : _tablet(tablet), _dest_store(dest_store) {}

OLAPStatus EngineStorageMigrationTask::execute() {
    return _migrate();
}

OLAPStatus EngineStorageMigrationTask::_migrate() {
    int64_t tablet_id = _tablet->tablet_id();
    int32_t schema_hash = _tablet->schema_hash();
    LOG(INFO) << "begin to process tablet migrate. "
              << "tablet_id=" << tablet_id << ", dest_store=" << _dest_store->path();

    DorisMetrics::instance()->storage_migrate_requests_total->increment(1);

    // try hold migration lock first
    OLAPStatus res = OLAP_SUCCESS;
    UniqueWriteLock migration_wlock(_tablet->get_migration_lock(), std::try_to_lock);
    if (!migration_wlock.owns_lock()) {
        return OLAP_ERR_RWLOCK_ERROR;
    }

    // check if this tablet has related running txns. if yes, can not do migration.
    int64_t partition_id;
    std::set<int64_t> transaction_ids;
    StorageEngine::instance()->txn_manager()->get_tablet_related_txns(
            tablet_id, schema_hash, _tablet->tablet_uid(), &partition_id, &transaction_ids);
    if (transaction_ids.size() > 0) {
        LOG(WARNING) << "could not migration because has unfinished txns, "
                     << " tablet=" << _tablet->full_name();
        return OLAP_ERR_HEADER_HAS_PENDING_DATA;
    }

    _tablet->obtain_push_lock();

    // TODO(ygl): the tablet should not under schema change or rollup or load
    do {
        std::vector<RowsetSharedPtr> consistent_rowsets;
        {
            ReadLock rdlock(_tablet->get_header_lock());
            // get all versions to be migrate
            const RowsetSharedPtr last_version = _tablet->rowset_with_max_version();
            if (last_version == nullptr) {
                res = OLAP_ERR_VERSION_NOT_EXIST;
                LOG(WARNING) << "failed to get rowset with max version, tablet="
                             << _tablet->full_name();
                break;
            }
            int32_t end_version = last_version->end_version();
            res = _tablet->capture_consistent_rowsets(Version(0, end_version), &consistent_rowsets);
            if (consistent_rowsets.empty()) {
                res = OLAP_ERR_VERSION_NOT_EXIST;
                LOG(WARNING) << "fail to capture consistent rowsets. tablet=" << _tablet->full_name()
                             << ", version=" << end_version;
                break;
            }
        }

        uint64_t shard = 0;
        res = _dest_store->get_shard(&shard);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to get shard from store: " << _dest_store->path();
            break;
        }
        FilePathDescStream root_path_desc_s;
        root_path_desc_s << _dest_store->path_desc() << DATA_PREFIX << "/" << shard;
        FilePathDesc full_path_desc = SnapshotManager::instance()->get_schema_hash_full_path(
                _tablet, root_path_desc_s.path_desc());
        string full_path = full_path_desc.filepath;
        // if dir already exist then return err, it should not happen.
        // should not remove the dir directly, for safety reason.
        if (FileUtils::check_exist(full_path)) {
            LOG(INFO) << "schema hash path already exist, skip this path. "
                      << "full_path=" << full_path;
            res = OLAP_ERR_FILE_ALREADY_EXIST;
            break;
        }

        Status st = FileUtils::create_dir(full_path);
        if (!st.ok()) {
            res = OLAP_ERR_CANNOT_CREATE_DIR;
            LOG(WARNING) << "fail to create path. path=" << full_path
                         << ", error:" << st.to_string();
            break;
        }

        // migrate all index and data files but header file
        res = _copy_index_and_data_files(full_path, consistent_rowsets);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to copy index and data files when migrate. res=" << res;
            break;
        }

        // generate new tablet meta and write to hdr file
        TabletMetaSharedPtr new_tablet_meta(new (std::nothrow) TabletMeta());
        {
            ReadLock rdlock(_tablet->get_header_lock());
            _generate_new_header(shard, consistent_rowsets, new_tablet_meta);
        }
        std::string new_meta_file = full_path + "/" + std::to_string(tablet_id) + ".hdr";
        res = new_tablet_meta->save(new_meta_file);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to save meta to path: " << new_meta_file;
            break;
        }

        // reset tablet id and rowset id
        res = TabletMeta::reset_tablet_uid(new_meta_file);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "errors while set tablet uid: '" << new_meta_file;
            break;
        }
        // it will change rowset id and its create time
        // rowset create time is useful when load tablet from meta to check which tablet is the tablet to load
        res = SnapshotManager::instance()->convert_rowset_ids(full_path, tablet_id, schema_hash);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to convert rowset id when do storage migration"
                         << " path = " << full_path;
            break;
        }

        res = StorageEngine::instance()->tablet_manager()->load_tablet_from_dir(
                _dest_store, tablet_id, schema_hash, full_path, false);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to load tablet from new path. tablet_id=" << tablet_id
                         << " schema_hash=" << schema_hash << " path = " << full_path;
            break;
        }

        // if old tablet finished schema change, then the schema change status of the new tablet is DONE
        // else the schema change status of the new tablet is FAILED
        TabletSharedPtr new_tablet =
                StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
        if (new_tablet == nullptr) {
            LOG(WARNING) << "tablet not found. tablet_id=" << tablet_id
                         << " schema_hash=" << schema_hash;
            res = OLAP_ERR_TABLE_NOT_FOUND;
            break;
        }
    } while (0);

    _tablet->release_push_lock();

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

OLAPStatus EngineStorageMigrationTask::_copy_index_and_data_files(
        const string& full_path, const std::vector<RowsetSharedPtr>& consistent_rowsets) const {
    OLAPStatus status = OLAP_SUCCESS;
    for (const auto& rs : consistent_rowsets) {
        status = rs->copy_files_to(full_path);
        if (status != OLAP_SUCCESS) {
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
