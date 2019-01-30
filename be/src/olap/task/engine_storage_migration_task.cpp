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

EngineStorageMigrationTask::EngineStorageMigrationTask(TStorageMediumMigrateReq& storage_medium_migrate_req) :
        _storage_medium_migrate_req(storage_medium_migrate_req) {

}

OLAPStatus EngineStorageMigrationTask::execute() {
    OLAPStatus res = OLAP_SUCCESS;
    res = _storage_medium_migrate(
        _storage_medium_migrate_req.tablet_id,
        _storage_medium_migrate_req.schema_hash,
        _storage_medium_migrate_req.storage_medium);
    return res;
}

OLAPStatus EngineStorageMigrationTask::_storage_medium_migrate(
        TTabletId tablet_id, TSchemaHash schema_hash,
        TStorageMedium::type storage_medium) {
    LOG(INFO) << "begin to process storage media migrate. "
              << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
              << ", dest_storage_medium=" << storage_medium;
    DorisMetrics::storage_migrate_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;
    TabletSharedPtr tablet = TabletManager::instance()->get_tablet(tablet_id, schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("can't find tablet. [tablet_id=%ld schema_hash=%d]",
                tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // judge case when no need to migrate
    uint32_t count = StorageEngine::instance()->available_storage_medium_type_count();
    if (count <= 1) {
        LOG(INFO) << "available storage medium type count is less than 1, "
                  << "no need to migrate. count=" << count;
        return OLAP_SUCCESS;
    }

    TStorageMedium::type src_storage_medium = tablet->data_dir()->storage_medium();
    if (src_storage_medium == storage_medium) {
        LOG(INFO) << "tablet is already on specified storage medium. "
                  << "storage_medium=" << storage_medium;
        return OLAP_SUCCESS;
    }

    tablet->obtain_push_lock();

    do {
        // get all versions to be migrate
        tablet->obtain_header_rdlock();
        const RowsetSharedPtr lastest_version = tablet->rowset_with_max_version();
        if (lastest_version == NULL) {
            tablet->release_header_lock();
            res = OLAP_ERR_VERSION_NOT_EXIST;
            OLAP_LOG_WARNING("tablet has not any version.");
            break;
        }

        int32_t end_version = lastest_version->end_version();
        vector<RowsetSharedPtr> consistent_rowsets;
        res = tablet->capture_consistent_rowsets(Version(0, end_version), &consistent_rowsets);
        if (consistent_rowsets.empty()) {
            tablet->release_header_lock();
            res = OLAP_ERR_VERSION_NOT_EXIST;
            LOG(WARNING) << "fail to capture consistent rowsets. tablet=" << tablet->full_name()
                         << ", version=" << end_version;
            break;
        }
        tablet->release_header_lock();

        // generate schema hash path where files will be migrated
        auto stores = StorageEngine::instance()->get_stores_for_create_tablet(storage_medium);
        if (stores.empty()) {
            res = OLAP_ERR_INVALID_ROOT_PATH;
            OLAP_LOG_WARNING("fail to get root path for create tablet.");
            break;
        }

        uint64_t shard = 0;
        res = stores[0]->get_shard(&shard);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to get root path shard. [res=%d]", res);
            break;
        }

        stringstream root_path_stream;
        root_path_stream << stores[0]->path() << DATA_PREFIX << "/" << shard;
        string schema_hash_path = SnapshotManager::instance()->get_schema_hash_full_path(tablet, root_path_stream.str());
        if (check_dir_existed(schema_hash_path)) {
            VLOG(3) << "schema hash path already exist, remove it. "
                    << "schema_hash_path=" << schema_hash_path;
            remove_all_dir(schema_hash_path);
        }
        create_dirs(schema_hash_path);

        // migrate all index and data files but header file
        res = _copy_index_and_data_files(schema_hash_path, tablet, consistent_rowsets);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to copy index and data files when migrate. [res=%d]", res);
            break;
        }

        // generate new header file from the old
        TabletMeta* new_tablet_meta = new(std::nothrow) TabletMeta();
        if (new_tablet_meta == NULL) {
            OLAP_LOG_WARNING("new olap header failed");
            return OLAP_ERR_BUFFER_OVERFLOW;
        }
        res = _generate_new_header(stores[0], shard, tablet, consistent_rowsets, new_tablet_meta);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to generate new header file from the old. [res=%d]", res);
            break;
        }

        // load the new tablet into OLAPEngine
        auto tablet = Tablet::create_from_tablet_meta(new_tablet_meta, stores[0]);
        if (tablet == NULL) {
            OLAP_LOG_WARNING("failed to create from header");
            res = OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR;
            break;
        }
        res = TabletManager::instance()->add_tablet(tablet_id, schema_hash, tablet, false);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to add tablet to StorageEngine. [res=%d]", res);
            break;
        }

        // if old tablet finished schema change, then the schema change status of the new tablet is DONE
        // else the schema change status of the new tablet is FAILED
        TabletSharedPtr new_tablet = TabletManager::instance()->get_tablet(tablet_id, schema_hash);
        if (new_tablet.get() == NULL) {
            OLAP_LOG_WARNING("get null tablet. [tablet_id=%ld schema_hash=%d]",
                             tablet_id, schema_hash);
            return OLAP_ERR_TABLE_NOT_FOUND;
        }
        AlterTabletState alter_state = tablet->alter_state();
        if (alter_state == ALTER_FINISHED) {
            new_tablet->set_alter_state(ALTER_FINISHED);
        } else {
            new_tablet->set_alter_state(ALTER_NONE);
        }
    } while (0);

    tablet->release_push_lock();

    return res;
}


OLAPStatus EngineStorageMigrationTask::_generate_new_header(
        DataDir* store,
        const uint64_t new_shard,
        const TabletSharedPtr& tablet,
        const std::vector<RowsetSharedPtr>& consistent_rowsets, TabletMeta* new_tablet_meta) {
    if (store == nullptr) {
        LOG(WARNING) << "fail to generate new header for store is null";
        return OLAP_ERR_HEADER_INIT_FAILED;
    }
    OLAPStatus res = OLAP_SUCCESS;

    DataDir* ref_store =
            StorageEngine::instance()->get_store(tablet->storage_root_path_name());
    TabletMetaManager::get_header(ref_store, tablet->tablet_id(), tablet->schema_hash(), new_tablet_meta);
    SnapshotManager::instance()->update_header_file_info(consistent_rowsets, new_tablet_meta);
    new_tablet_meta->set_shard_id(new_shard);

    res = TabletMetaManager::save(store, tablet->tablet_id(), tablet->schema_hash(), new_tablet_meta);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to save olap header to new db. [res=%d]", res);
        return res;
    }

    // delete old header
    // TODO: make sure atomic update
    TabletMetaManager::remove(ref_store, tablet->tablet_id(), tablet->schema_hash());
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to delete olap header to old db. res=" << res;
    }
    return res;
}

OLAPStatus EngineStorageMigrationTask::_copy_index_and_data_files(
        const string& schema_hash_path,
        const TabletSharedPtr& ref_tablet,
        std::vector<RowsetSharedPtr>& consistent_rowsets) {
    // TODO(lcy). copy function should be implemented
    for (auto& rs : consistent_rowsets) {
        std::vector<std::string> success_files;
        RETURN_NOT_OK(rs->make_snapshot(schema_hash_path, &success_files));
    }
    return OLAP_SUCCESS;
}

} // doris
