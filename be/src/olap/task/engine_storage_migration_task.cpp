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
    uint32_t count = StorageEngine::get_instance()->available_storage_medium_type_count();
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

    vector<RowsetReaderSharedPtr> rs_readers;
    tablet->obtain_push_lock();

    do {
        // get all versions to be migrate
        tablet->obtain_header_rdlock();
        const PDelta* lastest_version = tablet->lastest_version();
        if (lastest_version == NULL) {
            tablet->release_header_lock();
            res = OLAP_ERR_VERSION_NOT_EXIST;
            OLAP_LOG_WARNING("tablet has not any version.");
            break;
        }

        int32_t end_version = lastest_version->end_version();
        tablet->capture_rs_readers(Version(0, end_version), &rs_readers);
        if (rs_readers.empty()) {
            tablet->release_header_lock();
            res = OLAP_ERR_VERSION_NOT_EXIST;
            OLAP_LOG_WARNING("fail to acquire data souces. [tablet='%s' version=%d]",
                    tablet->full_name().c_str(), end_version);
            break;
        }

        vector<VersionEntity> version_entity_vec;
        tablet->list_version_entities(&version_entity_vec);
        tablet->release_header_lock();

        // generate schema hash path where files will be migrated
        auto stores = StorageEngine::get_instance()->get_stores_for_create_tablet(storage_medium);
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
        res = _copy_index_and_data_files(schema_hash_path, tablet, version_entity_vec);
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
        res = _generate_new_header(stores[0], shard, tablet, version_entity_vec, new_tablet_meta);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to generate new header file from the old. [res=%d]", res);
            break;
        }

        // load the new tablet into OLAPEngine
        auto tablet = Tablet::create_from_header(new_tablet_meta, stores[0]);
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
        SchemaChangeStatus tablet_status = tablet->schema_change_status();
        if (tablet->schema_change_status().status == AlterTableStatus::ALTER_TABLE_FINISHED) {
            new_tablet->set_schema_change_status(tablet_status.status,
                                                 tablet_status.schema_hash,
                                                 tablet_status.version);
        } else {
            new_tablet->set_schema_change_status(AlterTableStatus::ALTER_TABLE_FAILED,
                                                 tablet_status.schema_hash,
                                                 tablet_status.version);
        }
    } while (0);

    tablet->release_push_lock();
    tablet->release_rs_readers(&rs_readers);

    return res;
}


OLAPStatus EngineStorageMigrationTask::_generate_new_header(
        DataDir* store,
        const uint64_t new_shard,
        const TabletSharedPtr& tablet,
        const vector<VersionEntity>& version_entity_vec, TabletMeta* new_tablet_meta) {
    if (store == nullptr) {
        LOG(WARNING) << "fail to generate new header for store is null";
        return OLAP_ERR_HEADER_INIT_FAILED;
    }
    OLAPStatus res = OLAP_SUCCESS;

    DataDir* ref_store =
            StorageEngine::get_instance()->get_store(tablet->storage_root_path_name());
    TabletMetaManager::get_header(ref_store, tablet->tablet_id(), tablet->schema_hash(), new_tablet_meta);
    SnapshotManager::instance()->update_header_file_info(version_entity_vec, new_tablet_meta);
    new_tablet_meta->set_shard(new_shard);

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
        vector<VersionEntity>& version_entity_vec) {
    std::stringstream prefix_stream;
    prefix_stream << schema_hash_path << "/" << ref_tablet->tablet_id();
    std::string tablet_path_prefix = prefix_stream.str();
    for (VersionEntity& entity : version_entity_vec) {
        Version version = entity.version;
        VersionHash v_hash = entity.version_hash;
        for (SegmentGroupEntity segment_group_entity : entity.segment_group_vec) {
            int32_t segment_group_id = segment_group_entity.segment_group_id;
            for (int seg_id = 0; seg_id < segment_group_entity.num_segments; ++seg_id) {
                string index_path =
                    SnapshotManager::instance()->construct_index_file_path(tablet_path_prefix, version, v_hash, segment_group_id, seg_id);
                string ref_tablet_index_path = ref_tablet->construct_index_file_path(
                        version, v_hash, segment_group_id, seg_id);
                Status res = FileUtils::copy_file(ref_tablet_index_path, index_path);
                if (!res.ok()) {
                    LOG(WARNING) << "fail to copy index file."
                                 << "dest=" << index_path
                                 << ", src=" << ref_tablet_index_path;
                    return OLAP_ERR_COPY_FILE_ERROR;
                }

                string data_path =
                    SnapshotManager::instance()->construct_data_file_path(tablet_path_prefix, version, v_hash, segment_group_id, seg_id);
                string ref_tablet_data_path = ref_tablet->construct_data_file_path(
                    version, v_hash, segment_group_id, seg_id);
                res = FileUtils::copy_file(ref_tablet_data_path, data_path);
                if (!res.ok()) {
                    LOG(WARNING) << "fail to copy data file."
                                 << "dest=" << index_path
                                 << ", src=" << ref_tablet_index_path;
                    return OLAP_ERR_COPY_FILE_ERROR;
                }
            }
        }
    }

    return OLAP_SUCCESS;
}

} // doris
