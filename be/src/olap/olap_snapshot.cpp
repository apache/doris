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

#include "olap/olap_engine.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>

#include <algorithm>
#include <iterator>
#include <map>
#include <set>

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>

#include "common/status.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/column_data.h"
#include "olap/olap_define.h"
#include "olap/rowset.h"
#include "olap/olap_table.h"
#include "olap/olap_header_manager.h"
#include "olap/push_handler.h"
#include "olap/store.h"
#include "util/file_utils.h"
#include "util/doris_metrics.h"

using boost::filesystem::canonical;
using boost::filesystem::copy_file;
using boost::filesystem::copy_option;
using boost::filesystem::path;
using std::map;
using std::nothrow;
using std::set;
using std::string;
using std::stringstream;
using std::vector;
using std::list;

namespace doris {

OLAPStatus OLAPEngine::make_snapshot(
        const TSnapshotRequest& request,
        string* snapshot_path) {
    OLAPStatus res = OLAP_SUCCESS;
    if (snapshot_path == nullptr) {
        OLAP_LOG_WARNING("output parameter cannot be NULL");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    OLAPTablePtr ref_olap_table = get_table(request.tablet_id, request.schema_hash);
    if (ref_olap_table.get() == NULL) {
        OLAP_LOG_WARNING("failed to get olap table. [table=%ld schema_hash=%d]",
                request.tablet_id, request.schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    if (request.__isset.missing_version) {
        res = _create_incremental_snapshot_files(ref_olap_table, request, snapshot_path);
        // if all nodes has been upgraded, it can be removed
        (const_cast<TSnapshotRequest&>(request)).__set_allow_incremental_clone(true);
    } else {
        res = _create_snapshot_files(ref_olap_table, request, snapshot_path);
    }

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to make snapshot. [res=%d table=%ld schema_hash=%d]",
                res, request.tablet_id, request.schema_hash);
        return res;
    }

    VLOG(3) << "success to make snapshot. [path='" << snapshot_path << "']";
    return res;
}

OLAPStatus OLAPEngine::release_snapshot(const string& snapshot_path) {
    // 如果请求的snapshot_path位于root/snapshot文件夹下，则认为是合法的，可以删除
    // 否则认为是非法请求，返回错误结果
    auto stores = get_stores();
    for (auto store : stores) {
        path boost_root_path(store->path());
        string abs_path = canonical(boost_root_path).string();

        if (snapshot_path.compare(0, abs_path.size(), abs_path) == 0
                && snapshot_path.compare(abs_path.size(),
                        SNAPSHOT_PREFIX.size(), SNAPSHOT_PREFIX) == 0) {
            remove_all_dir(snapshot_path);
            VLOG(3) << "success to release snapshot path. [path='" << snapshot_path << "']";

            return OLAP_SUCCESS;
        }
    }

    LOG(WARNING) << "released snapshot path illegal. [path='" << snapshot_path << "']";
    return OLAP_ERR_CE_CMD_PARAMS_ERROR;
}

OLAPStatus OLAPEngine::_calc_snapshot_id_path(
        const OLAPTablePtr& olap_table,
        string* out_path) {
    OLAPStatus res = OLAP_SUCCESS;
    if (out_path == nullptr) {
        OLAP_LOG_WARNING("output parameter cannot be NULL");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // get current timestamp string
    string time_str;
    if ((res = gen_timestamp_string(&time_str)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to generate time_string when move file to trash."
                "[err code=%d]", res);
        return res;
    }

    stringstream snapshot_id_path_stream;
    MutexLock auto_lock(&_snapshot_mutex); // will automatically unlock when function return.
    snapshot_id_path_stream << olap_table->storage_root_path_name() << SNAPSHOT_PREFIX
                            << "/" << time_str << "." << _snapshot_base_id++;
    *out_path = snapshot_id_path_stream.str();
    return res;
}

string OLAPEngine::_get_schema_hash_full_path(
        const OLAPTablePtr& ref_olap_table,
        const string& location) const {
    stringstream schema_full_path_stream;
    schema_full_path_stream << location
                            << "/" << ref_olap_table->tablet_id()
                            << "/" << ref_olap_table->schema_hash();
    string schema_full_path = schema_full_path_stream.str();

    return schema_full_path;
}

string OLAPEngine::_get_header_full_path(
        const OLAPTablePtr& ref_olap_table,
        const std::string& schema_hash_path) const {
    stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << ref_olap_table->tablet_id() << ".hdr";
    return header_name_stream.str();
}

void OLAPEngine::_update_header_file_info(
        const vector<VersionEntity>& shortest_versions,
        OLAPHeader* header) {
    // clear schema_change_status
    header->clear_schema_change_status();
    // remove all old version and add new version
    header->delete_all_versions();

    for (const VersionEntity& entity : shortest_versions) {
        Version version = entity.version;
        VersionHash v_hash = entity.version_hash;
        for (RowSetEntity rowset : entity.rowset_vec) {
            int32_t rowset_id = rowset.rowset_id;
            const std::vector<KeyRange>* column_statistics = nullptr;
            if (!rowset.key_ranges.empty()) {
                column_statistics = &(rowset.key_ranges);
            }
            header->add_version(version, v_hash, rowset_id, rowset.num_segments, rowset.index_size,
                                rowset.data_size, rowset.num_rows, rowset.empty, column_statistics);
        }
    }
}

OLAPStatus OLAPEngine::_link_index_and_data_files(
        const string& schema_hash_path,
        const OLAPTablePtr& ref_olap_table,
        const vector<VersionEntity>& version_entity_vec) {
    OLAPStatus res = OLAP_SUCCESS;

    std::stringstream prefix_stream;
    prefix_stream << schema_hash_path << "/" << ref_olap_table->tablet_id();
    std::string tablet_path_prefix = prefix_stream.str();
    for (const VersionEntity& entity : version_entity_vec) {
        Version version = entity.version;
        VersionHash v_hash = entity.version_hash;
        for (RowSetEntity rowset : entity.rowset_vec) {
            int32_t rowset_id = rowset.rowset_id;
            for (int seg_id = 0; seg_id < rowset.num_segments; ++seg_id) {
                std::string index_path =
                    _construct_index_file_path(tablet_path_prefix, version, v_hash, rowset_id, seg_id);
                std::string ref_table_index_path =
                    ref_olap_table->construct_index_file_path(version, v_hash, rowset_id, seg_id);
                res = _create_hard_link(ref_table_index_path, index_path);
                if (res != OLAP_SUCCESS) {
                    LOG(WARNING) << "fail to create hard link. "
                        << " schema_hash_path=" << schema_hash_path
                        << " from_path=" << ref_table_index_path
                        << " to_path=" << index_path;
                    return res;
                }

                std:: string data_path =
                    _construct_data_file_path(tablet_path_prefix, version, v_hash, rowset_id, seg_id);
                std::string ref_table_data_path =
                    ref_olap_table->construct_data_file_path(version, v_hash, rowset_id, seg_id);
                res = _create_hard_link(ref_table_data_path, data_path);
                if (res != OLAP_SUCCESS) {
                    LOG(WARNING) << "fail to create hard link."
                        << "tablet_path_prefix=" << tablet_path_prefix << ", "
                        << "from_path=" << ref_table_data_path << ", to_path=" << data_path;
                    return res;
                }
            }
        }
    }

    return res;
}

OLAPStatus OLAPEngine::_copy_index_and_data_files(
        const string& schema_hash_path,
        const OLAPTablePtr& ref_olap_table,
        vector<VersionEntity>& version_entity_vec) {
    std::stringstream prefix_stream;
    prefix_stream << schema_hash_path << "/" << ref_olap_table->tablet_id();
    std::string tablet_path_prefix = prefix_stream.str();
    for (VersionEntity& entity : version_entity_vec) {
        Version version = entity.version;
        VersionHash v_hash = entity.version_hash;
        for (RowSetEntity rowset : entity.rowset_vec) {
            int32_t rowset_id = rowset.rowset_id;
            for (int seg_id = 0; seg_id < rowset.num_segments; ++seg_id) {
                string index_path =
                    _construct_index_file_path(tablet_path_prefix, version, v_hash, rowset_id, seg_id);
                string ref_table_index_path = ref_olap_table->construct_index_file_path(
                        version, v_hash, rowset_id, seg_id);
                Status res = FileUtils::copy_file(ref_table_index_path, index_path);
                if (!res.ok()) {
                    LOG(WARNING) << "fail to copy index file."
                                 << "dest=" << index_path << ", "
                                 << "src=" << ref_table_index_path;
                    return OLAP_ERR_COPY_FILE_ERROR;
                }

                string data_path =
                    _construct_data_file_path(tablet_path_prefix, version, v_hash, rowset_id, seg_id);
                string ref_table_data_path = ref_olap_table->construct_data_file_path(
                    version, v_hash, rowset_id, seg_id);
                res = FileUtils::copy_file(ref_table_data_path, data_path);
                if (!res.ok()) {
                    LOG(WARNING) << "fail to copy data file."
                                 << "dest=" << index_path << ", "
                                 << "src=" << ref_table_index_path;
                    return OLAP_ERR_COPY_FILE_ERROR;
                }
            }
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPEngine::_create_snapshot_files(
        const OLAPTablePtr& ref_olap_table,
        const TSnapshotRequest& request,
        string* snapshot_path) {
    OLAPStatus res = OLAP_SUCCESS;
    if (snapshot_path == nullptr) {
        OLAP_LOG_WARNING("output parameter cannot be NULL");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    string snapshot_id_path;
    res = _calc_snapshot_id_path(ref_olap_table, &snapshot_id_path);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to calc snapshot_id_path, [ref table=%s]",
                ref_olap_table->storage_root_path_name().c_str());
        return res;
    }

    string schema_full_path = _get_schema_hash_full_path(
            ref_olap_table, snapshot_id_path);
    string header_path = _get_header_full_path(ref_olap_table, schema_full_path);
    if (check_dir_existed(schema_full_path)) {
        OLAP_LOG_TRACE("remove the old schema_full_path.");
        remove_all_dir(schema_full_path);
    }
    create_dirs(schema_full_path);

    path boost_path(snapshot_id_path);
    string snapshot_id = canonical(boost_path).string();

    bool header_locked = false;
    ref_olap_table->obtain_header_rdlock();
    header_locked = true;

    vector<ColumnData*> olap_data_sources;
    OLAPHeader* new_olap_header = nullptr;
    do {
        // get latest version
        const PDelta* lastest_version = NULL;
        lastest_version = ref_olap_table->lastest_version();
        if (lastest_version == NULL) {
            OLAP_LOG_WARNING("table has not any version. [path='%s']",
                    ref_olap_table->full_name().c_str());
            res = OLAP_ERR_VERSION_NOT_EXIST;
            break;
        }

        // get snapshot version, use request.version if specified
        int32_t version = lastest_version->end_version();
        if (request.__isset.version) {
            if (lastest_version->end_version() < request.version
                    || (lastest_version->start_version() == lastest_version->end_version()
                    && lastest_version->end_version() == request.version
                    && lastest_version->version_hash() != request.version_hash)) {
                OLAP_LOG_WARNING("invalid make snapshot request. "
                        "[version=%d version_hash=%ld req_version=%d req_version_hash=%ld]",
                        lastest_version->end_version(), lastest_version->version_hash(),
                        request.version, request.version_hash);
                res = OLAP_ERR_INPUT_PARAMETER_ERROR;
                break;
            }

            version = request.version;
        }

        // get shortest version path
        vector<Version> shortest_path;
        vector<VersionEntity> shortest_versions;
        res = ref_olap_table->select_versions_to_span(Version(0, version), &shortest_path);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to select versions to span. [res=%d]", res);
            break;
        }

        for (const Version& version : shortest_path) {
            shortest_versions.push_back(ref_olap_table->get_version_entity_by_version(version));
        }

        // get data source and add reference count for prevent to delete data files
        ref_olap_table->acquire_data_sources_by_versions(shortest_path, &olap_data_sources);
        if (olap_data_sources.size() == 0) {
            OLAP_LOG_WARNING("failed to acquire data sources. [table='%s', version=%d]",
                    ref_olap_table->full_name().c_str(), version);
            res = OLAP_ERR_OTHER_ERROR;
            break;
        }

        // load table header, in order to remove versions that not in shortest version path
        OlapStore* store = ref_olap_table->store();
        new_olap_header = new(nothrow) OLAPHeader();
        if (new_olap_header == NULL) {
            OLAP_LOG_WARNING("fail to malloc OLAPHeader.");
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        res = OlapHeaderManager::get_header(store, ref_olap_table->tablet_id(), ref_olap_table->schema_hash(), new_olap_header);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load header. res=" << res
                    << "tablet_id=" << ref_olap_table->tablet_id() << ", schema_hash=" << ref_olap_table->schema_hash();
            break;
        }

        ref_olap_table->release_header_lock();
        header_locked = false;
        _update_header_file_info(shortest_versions, new_olap_header);

        // save new header to snapshot header path
        res = new_olap_header->save(header_path);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to save header. [res=%d tablet_id=%ld, schema_hash=%d, headerpath=%s]",
                    res, ref_olap_table->tablet_id(), ref_olap_table->schema_hash(), header_path.c_str());
            break;
        }

        res = _link_index_and_data_files(schema_full_path, ref_olap_table, shortest_versions);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to create hard link. [path=" << snapshot_id_path << "]";
            break;
        }

        // append a single delta if request.version is end_version of cumulative delta
        if (request.__isset.version) {
            for (const VersionEntity& entity : shortest_versions) {
                if (entity.version.second == request.version) {
                    if (entity.version.first != request.version) {
                        res = _append_single_delta(request, store);
                        if (res != OLAP_SUCCESS) {
                            OLAP_LOG_WARNING("fail to append single delta. [res=%d]", res);
                        }
                    }
                    break;
                }
            }
        }
    } while (0);

    SAFE_DELETE(new_olap_header);

    if (header_locked) {
        OLAP_LOG_TRACE("release header lock.");
        ref_olap_table->release_header_lock();
    }

    if (ref_olap_table.get() != NULL) {
        OLAP_LOG_TRACE("release data sources.");
        ref_olap_table->release_data_sources(&olap_data_sources);
    }

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to make snapshot, try to delete the snapshot path. [path=%s]",
                         snapshot_id_path.c_str());

        if (check_dir_existed(snapshot_id_path)) {
            VLOG(3) << "remove snapshot path. [path=" << snapshot_id_path << "]";
            remove_all_dir(snapshot_id_path);
        }
    } else {
        *snapshot_path = snapshot_id;
    }

    return res;
}

OLAPStatus OLAPEngine::_create_incremental_snapshot_files(
        const OLAPTablePtr& ref_olap_table,
        const TSnapshotRequest& request,
        string* snapshot_path) {
    OLAP_LOG_INFO("begin to create incremental snapshot files. [table=%ld schema_hash=%d]",
                  request.tablet_id, request.schema_hash);
    OLAPStatus res = OLAP_SUCCESS;

    if (snapshot_path == nullptr) {
        OLAP_LOG_WARNING("output parameter cannot be NULL");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    string snapshot_id_path;
    res = _calc_snapshot_id_path(ref_olap_table, &snapshot_id_path);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to calc snapshot_id_path, [ref table=%s]",
                ref_olap_table->storage_root_path_name().c_str());
        return res;
    }

    string schema_full_path = _get_schema_hash_full_path(ref_olap_table, snapshot_id_path);
    if (check_dir_existed(schema_full_path)) {
        OLAP_LOG_TRACE("remove the old schema_full_path.");
        remove_all_dir(schema_full_path);
    }
    create_dirs(schema_full_path);

    path boost_path(snapshot_id_path);
    string snapshot_id = canonical(boost_path).string();

    ref_olap_table->obtain_header_rdlock();

    do {
        // save header to snapshot path
        OLAPHeader olap_header;
        res = OlapHeaderManager::get_header(ref_olap_table->store(),
                ref_olap_table->tablet_id(), ref_olap_table->schema_hash(), &olap_header);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load header. res=" << res << "tablet_id="
                    << ref_olap_table->tablet_id() << ", schema_hash=" << ref_olap_table->schema_hash();
            break;
        }
        string header_path = _get_header_full_path(ref_olap_table, schema_full_path);
        res = olap_header.save(header_path);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to save header to path:" << header_path;
            remove_dir(header_path);
            break;
        }

        for (int64_t missing_version : request.missing_version) {

            // find missing version
            const PDelta* incremental_delta =
                ref_olap_table->get_incremental_delta(Version(missing_version, missing_version));
            if (incremental_delta != nullptr) {
                OLAP_LOG_DEBUG("success to find missing version when snapshot, "
                               "begin to link files. [table=%ld schema_hash=%d version=%ld]",
                               request.tablet_id, request.schema_hash, missing_version);
                // link files
                for (uint32_t i = 0; i < incremental_delta->rowset(0).num_segments(); i++) {
                    int32_t rowset_id = incremental_delta->rowset(0).rowset_id();
                    string from = ref_olap_table->construct_incremental_index_file_path(
                                Version(missing_version, missing_version),
                                incremental_delta->version_hash(), rowset_id, i);
                    string to = schema_full_path + '/' + basename(from.c_str());
                    if ((res = _create_hard_link(from, to)) != OLAP_SUCCESS) {
                        break;
                    }

                    from = ref_olap_table->construct_incremental_data_file_path(
                                Version(missing_version, missing_version),
                                incremental_delta->version_hash(), rowset_id, i);
                    to = schema_full_path + '/' + basename(from.c_str());
                    if ((res = _create_hard_link(from, to)) != OLAP_SUCCESS) {
                        break;
                    }
                }

                if (res != OLAP_SUCCESS) {
                    break;
                }

            } else {
                OLAP_LOG_WARNING("failed to find missing version when snapshot. "
                                 "[table=%ld schema_hash=%d version=%ld]",
                                 request.tablet_id, request.schema_hash, missing_version);
                res = OLAP_ERR_VERSION_NOT_EXIST;
                break;
            }
        }

    } while (0);

    ref_olap_table->release_header_lock();

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to make incremental snapshot, try to delete the snapshot path. "
                         "[path=%s]", snapshot_id_path.c_str());

        if (check_dir_existed(snapshot_id_path)) {
            VLOG(3) << "remove snapshot path. [path=" << snapshot_id_path << "]";
            remove_all_dir(snapshot_id_path);
        }
    } else {
        *snapshot_path = snapshot_id;
    }

    return res;
}

OLAPStatus OLAPEngine::_append_single_delta(
        const TSnapshotRequest& request, OlapStore* store) {
    OLAPStatus res = OLAP_SUCCESS;
    string root_path = store->path();
    OLAPHeader* new_olap_header = new(nothrow) OLAPHeader();
    if (new_olap_header == NULL) {
        OLAP_LOG_WARNING("fail to malloc OLAPHeader.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    res = OlapHeaderManager::get_header(store, request.tablet_id, request.schema_hash, new_olap_header);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to create tablet from header file. [tablet_id=%ld, schema_hash=%d]",
                         request.tablet_id, request.schema_hash);
        return res;
    }
    auto tablet = OLAPTable::create_from_header(new_olap_header, store);
    if (tablet == NULL) {
        OLAP_LOG_WARNING("fail to load tablet. [res=%d tablet_id='%ld, schema_hash=%d']",
                         res, request.tablet_id, request.schema_hash);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    res = tablet->load();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to load tablet. [res=" << res << " header_path=" << store->path();
        return res;
    }

    const PDelta* lastest_version = tablet->lastest_version();
    if (lastest_version->start_version() != request.version) {
        TPushReq empty_push;
        empty_push.tablet_id = request.tablet_id;
        empty_push.schema_hash = request.schema_hash;
        empty_push.version = request.version + 1;
        empty_push.version_hash = 0;
        
        PushHandler handler;
        res = handler.process(tablet, empty_push, PUSH_NORMAL, NULL);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to push empty version. [res=%d version=%d]",
                             res, empty_push.version);
            return res;
        }
    }

    return res;
}

string OLAPEngine::_construct_index_file_path(
        const string& tablet_path_prefix,
        const Version& version,
        VersionHash version_hash,
        int32_t rowset_id, int32_t segment) const {
    return OLAPTable::construct_file_path(tablet_path_prefix, version, version_hash, rowset_id, segment, "idx");
}

string OLAPEngine::_construct_data_file_path(
        const string& tablet_path_prefix,
        const Version& version,
        VersionHash version_hash,
        int32_t rowset_id, int32_t segment) const {
    return OLAPTable::construct_file_path(tablet_path_prefix, version, version_hash, rowset_id, segment, "dat");
}

OLAPStatus OLAPEngine::_create_hard_link(const string& from_path, const string& to_path) {
    if (link(from_path.c_str(), to_path.c_str()) == 0) {
        OLAP_LOG_TRACE("success to create hard link from path=%s to path=%s]",
                from_path.c_str(), to_path.c_str());
        return OLAP_SUCCESS;
    } else {
        OLAP_LOG_WARNING("failed to create hard link from path=%s to path=%s errno=%d",
                from_path.c_str(), to_path.c_str(), errno);
        return OLAP_ERR_OTHER_ERROR;
    }
}

OLAPStatus OLAPEngine::storage_medium_migrate(
        TTabletId tablet_id, TSchemaHash schema_hash,
        TStorageMedium::type storage_medium) {
    OLAP_LOG_INFO("begin to process storage media migrate. "
                  "[tablet_id=%ld schema_hash=%d dest_storage_medium=%d]",
                  tablet_id, schema_hash, storage_medium);
    DorisMetrics::storage_migrate_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;
    OLAPTablePtr tablet = get_table(tablet_id, schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("can't find olap table. [tablet_id=%ld schema_hash=%d]",
                tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // judge case when no need to migrate
    uint32_t count = available_storage_medium_type_count();
    if (count <= 1) {
        OLAP_LOG_INFO("available storage medium type count is less than 1, "
                "no need to migrate. [count=%u]", count);
        return OLAP_SUCCESS;
    }

    TStorageMedium::type src_storage_medium = tablet->store()->storage_medium();
    if (src_storage_medium == storage_medium) {
        OLAP_LOG_INFO("tablet is already on specified storage medium. "
                "[storage_medium='%d']", storage_medium);
        return OLAP_SUCCESS;
    }

    vector<ColumnData*> olap_data_sources;
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
        tablet->acquire_data_sources(Version(0, end_version), &olap_data_sources);
        if (olap_data_sources.size() == 0) {
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
        auto stores = get_stores_for_create_table(storage_medium);
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
        string schema_hash_path = _get_schema_hash_full_path(tablet, root_path_stream.str());
        if (check_dir_existed(schema_hash_path)) {
            OLAP_LOG_DEBUG("schema hash path already exist, remove it. [schema_hash_path='%s']",
                    schema_hash_path.c_str());
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
        OLAPHeader* new_olap_header = new(std::nothrow) OLAPHeader();
        if (new_olap_header == NULL) {
            OLAP_LOG_WARNING("new olap header failed");
            return OLAP_ERR_BUFFER_OVERFLOW;
        }
        res = _generate_new_header(stores[0], shard, tablet, version_entity_vec, new_olap_header);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to generate new header file from the old. [res=%d]", res);
            break;
        }

        // load the new tablet into OLAPEngine
        auto olap_table = OLAPTable::create_from_header(new_olap_header, stores[0]);
        if (olap_table == NULL) {
            OLAP_LOG_WARNING("failed to create from header");
            res = OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR;
            break;
        }
        res = add_table(tablet_id, schema_hash, olap_table);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to add tablet to OLAPEngine. [res=%d]", res);
            break;
        }

        // if old table finished schema change, then the schema change status of the new table is DONE
        // else the schema change status of the new table is FAILED
        OLAPTablePtr new_tablet = get_table(tablet_id, schema_hash);
        if (new_tablet.get() == NULL) {
            OLAP_LOG_WARNING("get null olap table. [tablet_id=%ld schema_hash=%d]",
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
    tablet->release_data_sources(&olap_data_sources);

    return res;
}

OLAPStatus OLAPEngine::_generate_new_header(
        OlapStore* store,
        const uint64_t new_shard,
        const OLAPTablePtr& tablet,
        const vector<VersionEntity>& version_entity_vec, OLAPHeader* new_olap_header) {
    if (store == nullptr) {
        LOG(WARNING) << "fail to generate new header for store is null";
        return OLAP_ERR_HEADER_INIT_FAILED;
    }
    OLAPStatus res = OLAP_SUCCESS;

    OlapStore* ref_store =
            OLAPEngine::get_instance()->get_store(tablet->storage_root_path_name());
    OlapHeaderManager::get_header(ref_store, tablet->tablet_id(), tablet->schema_hash(), new_olap_header);
    _update_header_file_info(version_entity_vec, new_olap_header);
    new_olap_header->set_shard(new_shard);

    res = OlapHeaderManager::save(store, tablet->tablet_id(), tablet->schema_hash(), new_olap_header);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to save olap header to new db. [res=%d]", res);
        return res;
    }

    // delete old header
    // TODO: make sure atomic update
    OlapHeaderManager::remove(ref_store, tablet->tablet_id(), tablet->schema_hash());
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to delete olap header to old db. res=" << res;
    }
    return res;
}

}  // namespace doris
