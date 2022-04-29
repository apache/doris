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

#include "olap/storage_migration_v2.h"

#include <pthread.h>
#include <signal.h>

#include <algorithm>
#include <vector>
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"

#include "agent/cgroups_mgr.h"
#include "common/resource_tls.h"
#include "env/env_util.h"
#include "olap/merger.h"
#include "olap/row.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/wrapper_field.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"

using std::deque;
using std::list;
using std::nothrow;
using std::pair;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(storage_migration_mem_consumption, MetricUnit::BYTES, "",
                                   mem_consumption, Labels({{"type", "storage_migration"}}));

StorageMigrationV2Handler::StorageMigrationV2Handler()
        : _mem_tracker(MemTracker::create_tracker(
                  -1, "StorageMigrationV2Handler",
                  StorageEngine::instance()->storage_migration_mem_tracker())) {
    REGISTER_HOOK_METRIC(storage_migration_mem_consumption,
                         [this]() { return _mem_tracker->consumption(); });
}

StorageMigrationV2Handler::~StorageMigrationV2Handler() {
    DEREGISTER_HOOK_METRIC(storage_migration_mem_consumption);
}

Status StorageMigrationV2Handler::process_storage_migration_v2(
        const TStorageMigrationReqV2& request) {
    LOG(INFO) << "begin to do request storage_migration: base_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_id
              << ", migration_version=" << request.migration_version;

    TabletSharedPtr base_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            request.base_tablet_id, request.base_schema_hash);
    if (base_tablet == nullptr) {
        LOG(WARNING) << "fail to find base tablet. base_tablet=" << request.base_tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }
    // Lock schema_change_lock util schema change info is stored in tablet header
    std::unique_lock<std::mutex> schema_change_lock(base_tablet->get_schema_change_lock(),
                                                    std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "failed to obtain schema change lock. "
                     << "base_tablet=" << request.base_tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TRY_LOCK_FAILED);
    }

    Status res = _do_process_storage_migration_v2(request);
    LOG(INFO) << "finished storage_migration process, res=" << res;
    return res;
}

Status StorageMigrationV2Handler::_do_process_storage_migration_v2(
        const TStorageMigrationReqV2& request) {
    Status res = Status::OK();
    TabletSharedPtr base_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            request.base_tablet_id, request.base_schema_hash);
    if (base_tablet == nullptr) {
        LOG(WARNING) << "fail to find base tablet. base_tablet=" << request.base_tablet_id
                     << ", base_schema_hash=" << request.base_schema_hash;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    // new tablet has to exist
    TabletSharedPtr new_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            request.new_tablet_id, request.new_schema_hash);
    if (new_tablet == nullptr) {
        LOG(WARNING) << "fail to find new tablet."
                     << " new_tablet=" << request.new_tablet_id
                     << ", new_schema_hash=" << request.new_schema_hash;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    // check if tablet's state is not_ready, if it is ready, it means the tablet already finished
    // check whether the tablet's max continuous version == request.version
    if (new_tablet->tablet_state() != TABLET_NOTREADY) {
        res = _validate_migration_result(new_tablet, request);
        LOG(INFO) << "tablet's state=" << new_tablet->tablet_state()
                  << " the convert job already finished, check its version"
                  << " res=" << res;
        return res;
    }

    LOG(INFO) << "finish to validate storage_migration request. begin to migrate data from base "
                 "tablet "
                 "to new tablet"
              << " base_tablet=" << base_tablet->full_name()
              << " new_tablet=" << new_tablet->full_name();

    std::shared_lock base_migration_rlock(base_tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_RWLOCK_ERROR);
    }
    std::shared_lock new_migration_rlock(new_tablet->get_migration_lock(), std::try_to_lock);
    if (!new_migration_rlock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_RWLOCK_ERROR);
    }

    std::vector<Version> versions_to_be_changed;
    std::vector<RowsetReaderSharedPtr> rs_readers;
    // delete handlers for new tablet
    DeleteHandler delete_handler;
    std::vector<ColumnId> return_columns;

    // begin to find deltas to convert from base tablet to new tablet so that
    // obtain base tablet and new tablet's push lock and header write lock to prevent loading data
    {
        std::lock_guard<std::mutex> base_tablet_lock(base_tablet->get_push_lock());
        std::lock_guard<std::mutex> new_tablet_lock(new_tablet->get_push_lock());
        std::lock_guard<std::shared_mutex> base_tablet_wlock(base_tablet->get_header_lock());
        std::lock_guard<std::shared_mutex> new_tablet_wlock(new_tablet->get_header_lock());
        // check if the tablet has alter task
        // if it has alter task, it means it is under old alter process
        size_t num_cols = base_tablet->tablet_schema().num_columns();
        return_columns.resize(num_cols);
        for (int i = 0; i < num_cols; ++i) {
            return_columns[i] = i;
        }

        // reader_context is stack variables, it's lifetime should keep the same
        // with rs_readers
        RowsetReaderContext reader_context;
        reader_context.reader_type = READER_ALTER_TABLE;
        reader_context.tablet_schema = &base_tablet->tablet_schema();
        reader_context.need_ordered_result = true;
        reader_context.delete_handler = &delete_handler;
        reader_context.return_columns = &return_columns;
        // for schema change, seek_columns is the same to return_columns
        reader_context.seek_columns = &return_columns;
        reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();

        do {
            // get history data to be converted and it will check if there is hold in base tablet
            res = _get_versions_to_be_changed(base_tablet, &versions_to_be_changed);
            if (!res.ok()) {
                LOG(WARNING) << "fail to get version to be changed. res=" << res;
                break;
            }

            // should check the max_version >= request.migration_version, if not the convert is useless
            RowsetSharedPtr max_rowset = base_tablet->rowset_with_max_version();
            if (max_rowset == nullptr || max_rowset->end_version() < request.migration_version) {
                LOG(WARNING) << "base tablet's max version="
                             << (max_rowset == nullptr ? 0 : max_rowset->end_version())
                             << " is less than request version=" << request.migration_version;
                res = Status::OLAPInternalError(OLAP_ERR_VERSION_NOT_EXIST);
                break;
            }
            // before calculating version_to_be_changed,
            // remove all data from new tablet, prevent to rewrite data(those double pushed when wait)
            LOG(INFO) << "begin to remove all data from new tablet to prevent rewrite."
                      << " new_tablet=" << new_tablet->full_name();
            std::vector<RowsetSharedPtr> rowsets_to_delete;
            std::vector<std::pair<Version, RowsetSharedPtr>> version_rowsets;
            new_tablet->acquire_version_and_rowsets(&version_rowsets);
            for (auto& pair : version_rowsets) {
                if (pair.first.second <= max_rowset->end_version()) {
                    rowsets_to_delete.push_back(pair.second);
                }
            }
            std::vector<RowsetSharedPtr> empty_vec;
            new_tablet->modify_rowsets(empty_vec, rowsets_to_delete);
            // inherit cumulative_layer_point from base_tablet
            // check if new_tablet.ce_point > base_tablet.ce_point?
            new_tablet->set_cumulative_layer_point(-1);
            // save tablet meta
            new_tablet->save_meta();
            for (auto& rowset : rowsets_to_delete) {
                // do not call rowset.remove directly, using gc thread to delete it
                StorageEngine::instance()->add_unused_rowset(rowset);
            }

            // init one delete handler
            int32_t end_version = -1;
            for (auto& version : versions_to_be_changed) {
                if (version.second > end_version) {
                    end_version = version.second;
                }
            }

            res = delete_handler.init(base_tablet->tablet_schema(),
                                      base_tablet->delete_predicates(), end_version);
            if (!res.ok()) {
                LOG(WARNING) << "init delete handler failed. base_tablet="
                             << base_tablet->full_name() << ", end_version=" << end_version;

                // release delete handlers which have been inited successfully.
                delete_handler.finalize();
                break;
            }

            // acquire data sources correspond to history versions
            base_tablet->capture_rs_readers(versions_to_be_changed, &rs_readers);
            if (rs_readers.size() < 1) {
                LOG(WARNING) << "fail to acquire all data sources. "
                             << "version_num=" << versions_to_be_changed.size()
                             << ", data_source_num=" << rs_readers.size();
                res = Status::OLAPInternalError(OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS);
                break;
            }

            for (auto& rs_reader : rs_readers) {
                res = rs_reader->init(&reader_context);
                if (!res.ok()) {
                    LOG(WARNING) << "failed to init rowset reader: " << base_tablet->full_name();
                    break;
                }
            }

        } while (0);
    }

    do {
        if (!res.ok()) {
            break;
        }
        StorageMigrationParams sm_params;
        sm_params.base_tablet = base_tablet;
        sm_params.new_tablet = new_tablet;
        sm_params.ref_rowset_readers = rs_readers;
        sm_params.delete_handler = &delete_handler;

        res = _convert_historical_rowsets(sm_params);
        if (!res.ok()) {
            break;
        }
        // set state to ready
        std::lock_guard<std::shared_mutex> new_wlock(new_tablet->get_header_lock());
        res = new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);
        if (!res.ok()) {
            break;
        }
        new_tablet->save_meta();
    } while (0);

    if (res.ok()) {
        // _validate_migration_result should be outside the above while loop.
        // to avoid requiring the header lock twice.
        res = _validate_migration_result(new_tablet, request);
    }

    // if failed convert history data, then just remove the new tablet
    if (!res.ok()) {
        LOG(WARNING) << "failed to alter tablet. base_tablet=" << base_tablet->full_name()
                     << ", drop new_tablet=" << new_tablet->full_name();
        // do not drop the new tablet and its data. GC thread will
    }

    return res;
}

Status StorageMigrationV2Handler::_get_versions_to_be_changed(
        TabletSharedPtr base_tablet, std::vector<Version>* versions_to_be_changed) {
    RowsetSharedPtr rowset = base_tablet->rowset_with_max_version();
    if (rowset == nullptr) {
        LOG(WARNING) << "Tablet has no version. base_tablet=" << base_tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS);
    }

    std::vector<Version> span_versions;
    RETURN_NOT_OK(base_tablet->capture_consistent_versions(Version(0, rowset->version().second),
                                                           &span_versions));
    versions_to_be_changed->insert(versions_to_be_changed->end(), span_versions.begin(),
                                   span_versions.end());

    return Status::OK();
}

Status StorageMigrationV2Handler::_convert_historical_rowsets(
        const StorageMigrationParams& sm_params) {
    LOG(INFO) << "begin to convert historical rowsets for new_tablet from base_tablet."
              << " base_tablet=" << sm_params.base_tablet->full_name()
              << ", new_tablet=" << sm_params.new_tablet->full_name();

    // find end version
    int32_t end_version = -1;
    for (size_t i = 0; i < sm_params.ref_rowset_readers.size(); ++i) {
        if (sm_params.ref_rowset_readers[i]->version().second > end_version) {
            end_version = sm_params.ref_rowset_readers[i]->version().second;
        }
    }

    Status res = Status::OK();
    for (auto& rs_reader : sm_params.ref_rowset_readers) {
        VLOG_TRACE << "begin to convert a history rowset. version=" << rs_reader->version().first
                   << "-" << rs_reader->version().second;

        TabletSharedPtr new_tablet = sm_params.new_tablet;

        RowsetWriterContext writer_context;
        writer_context.rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.tablet_uid = new_tablet->tablet_uid();
        writer_context.tablet_id = new_tablet->tablet_id();
        writer_context.partition_id = new_tablet->partition_id();
        writer_context.tablet_schema_hash = new_tablet->schema_hash();
        // linked schema change can't change rowset type, therefore we preserve rowset type in schema change now
        writer_context.rowset_type = rs_reader->rowset()->rowset_meta()->rowset_type();
        if (sm_params.new_tablet->tablet_meta()->preferred_rowset_type() == BETA_ROWSET) {
            writer_context.rowset_type = BETA_ROWSET;
        }
        writer_context.path_desc = new_tablet->tablet_path_desc();
        writer_context.tablet_schema = &(new_tablet->tablet_schema());
        writer_context.rowset_state = VISIBLE;
        writer_context.version = rs_reader->version();
        writer_context.segments_overlap = rs_reader->rowset()->rowset_meta()->segments_overlap();

        std::unique_ptr<RowsetWriter> rowset_writer;
        Status status = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        if (!status.ok()) {
            res = Status::OLAPInternalError(OLAP_ERR_ROWSET_BUILDER_INIT);
            goto PROCESS_ALTER_EXIT;
        }

        if ((res = _generate_rowset_writer(sm_params.base_tablet->tablet_path_desc(),
                                           sm_params.new_tablet->tablet_path_desc(), rs_reader,
                                           rowset_writer.get(), new_tablet)) != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to add_rowset. version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second;
            new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                       rowset_writer->rowset_id().to_string());
            goto PROCESS_ALTER_EXIT;
        }
        new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                   rowset_writer->rowset_id().to_string());
        // Add the new version of the data to the header
        // In order to prevent the occurrence of deadlock, we must first lock the old table, and then lock the new table
        std::lock_guard<std::mutex> lock(sm_params.new_tablet->get_push_lock());
        RowsetSharedPtr new_rowset = rowset_writer->build();
        if (new_rowset == nullptr) {
            LOG(WARNING) << "failed to build rowset, exit alter process";
            goto PROCESS_ALTER_EXIT;
        }
        res = sm_params.new_tablet->add_rowset(new_rowset, false);
        if (res == Status::OLAPInternalError(OLAP_ERR_PUSH_VERSION_ALREADY_EXIST)) {
            LOG(WARNING) << "version already exist, version revert occurred. "
                         << "tablet=" << sm_params.new_tablet->full_name() << ", version='"
                         << rs_reader->version().first << "-" << rs_reader->version().second;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            res = Status::OK();
        } else if (!res.ok()) {
            LOG(WARNING) << "failed to register new version. "
                         << " tablet=" << sm_params.new_tablet->full_name()
                         << ", version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            goto PROCESS_ALTER_EXIT;
        } else {
            VLOG_NOTICE << "register new version. tablet=" << sm_params.new_tablet->full_name()
                        << ", version=" << rs_reader->version().first << "-"
                        << rs_reader->version().second;
        }

        VLOG_TRACE << "succeed to convert a history version."
                   << " version=" << rs_reader->version().first << "-"
                   << rs_reader->version().second;
    }
// XXX:The SchemaChange state should not be canceled at this time, because the new Delta has to be converted to the old and new Schema version
PROCESS_ALTER_EXIT : {
    // save tablet meta here because rowset meta is not saved during add rowset
    std::lock_guard<std::shared_mutex> new_wlock(sm_params.new_tablet->get_header_lock());
    sm_params.new_tablet->save_meta();
}
    if (res.ok()) {
        Version test_version(0, end_version);
        res = sm_params.new_tablet->check_version_integrity(test_version);
    }

    LOG(INFO) << "finish converting rowsets for new_tablet from base_tablet. "
              << "base_tablet=" << sm_params.base_tablet->full_name()
              << ", new_tablet=" << sm_params.new_tablet->full_name();
    return res;
}

Status StorageMigrationV2Handler::_validate_migration_result(
        TabletSharedPtr new_tablet, const TStorageMigrationReqV2& request) {
    Version max_continuous_version = {-1, 0};
    new_tablet->max_continuous_version_from_beginning(&max_continuous_version);
    LOG(INFO) << "find max continuous version of tablet=" << new_tablet->full_name()
              << ", start_version=" << max_continuous_version.first
              << ", end_version=" << max_continuous_version.second;
    if (max_continuous_version.second < request.migration_version) {
        return Status::OLAPInternalError(OLAP_ERR_VERSION_NOT_EXIST);
    }

    std::vector<std::pair<Version, RowsetSharedPtr>> version_rowsets;
    {
        std::shared_lock rdlock(new_tablet->get_header_lock(), std::try_to_lock);
        new_tablet->acquire_version_and_rowsets(&version_rowsets);
    }
    for (auto& pair : version_rowsets) {
        RowsetSharedPtr rowset = pair.second;
        if (!rowset->check_file_exist()) {
            return Status::OLAPInternalError(OLAP_ERR_FILE_NOT_EXIST);
        }
    }
    return Status::OK();
}

Status StorageMigrationV2Handler::_generate_rowset_writer(const FilePathDesc& src_desc,
                                                          const FilePathDesc& dst_desc,
                                                          RowsetReaderSharedPtr rowset_reader,
                                                          RowsetWriter* new_rowset_writer,
                                                          TabletSharedPtr new_tablet) {
    if (!src_desc.is_remote() && dst_desc.is_remote()) {
        string remote_file_param_path = dst_desc.filepath + REMOTE_FILE_PARAM;
        rapidjson::StringBuffer strbuf;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
        writer.StartObject();
        writer.Key(TABLET_UID.c_str());
        writer.String(TabletUid(new_tablet->tablet_uid()).to_string().c_str());
        writer.Key(STORAGE_NAME.c_str());
        writer.String(dst_desc.storage_name.c_str());
        writer.EndObject();
        Status st = env_util::write_string_to_file(
                Env::Default(), Slice(std::string(strbuf.GetString())), remote_file_param_path);
        // strbuf.GetString() format: {"tablet_uid": "a84cfb67d3ad3d62-87fd8b3ae9bdad84", "storage_name": "s3_name"}
        if (!st.ok()) {
            LOG(WARNING) << "fail to write tablet_uid and storage_name. path="
                         << remote_file_param_path << ", error:" << st.to_string();
            return Status::OLAPInternalError(OLAP_ERR_COPY_FILE_ERROR);
        }
        LOG(INFO) << "write storage_param successfully: " << remote_file_param_path;
    }

    return new_rowset_writer->add_rowset_for_migration(rowset_reader->rowset());
}

} // namespace doris
