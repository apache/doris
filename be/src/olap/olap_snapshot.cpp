// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/olap_snapshot.h"

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
#include "olap/olap_data.h"
#include "olap/olap_define.h"
#include "olap/olap_engine.h"
#include "olap/olap_index.h"
#include "olap/olap_table.h"
#include "olap/push_handler.h"
#include "util/file_utils.h"

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

namespace palo {

OLAPSnapshot::OLAPSnapshot(): _base_id(0) {}

OLAPSnapshot::~OLAPSnapshot() {}

OLAPStatus OLAPSnapshot::make_snapshot(
        const TSnapshotRequest& request,
        string* snapshot_path) {
    OLAPStatus res = OLAP_SUCCESS;
    if (snapshot_path == nullptr) {
        OLAP_LOG_WARNING("output parameter cannot be NULL");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    SmartOLAPTable ref_olap_table =
            OLAPEngine::get_instance()->get_table(request.tablet_id, request.schema_hash);
    if (ref_olap_table.get() == NULL) {
        OLAP_LOG_WARNING("failed to get olap table. [table=%ld schema_hash=%d]",
                request.tablet_id, request.schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    res = _create_snapshot_files(ref_olap_table, request, snapshot_path);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to make snapshot. [res=%d table=%ld schema_hash=%d]",
                res, request.tablet_id, request.schema_hash);
        return res;
    }

    OLAP_LOG_TRACE("success to make snapshot. [path='%s']", snapshot_path->c_str());
    return res;
}

OLAPStatus OLAPSnapshot::release_snapshot(const string& snapshot_path) {
    // 如果请求的snapshot_path位于root/snapshot文件夹下，则认为是合法的，可以删除
    // 否则认为是非法请求，返回错误结果
    OLAPRootPath::RootPathVec all_available_root_path;
    OLAPRootPath::get_instance()->get_all_available_root_path(&all_available_root_path);

    for (std::string& root_path : all_available_root_path) {
        path boost_root_path(root_path);
        string abs_path = canonical(boost_root_path).string();

        if (snapshot_path.compare(0, abs_path.size(), abs_path) == 0
                && snapshot_path.compare(abs_path.size(),
                        SNAPSHOT_PREFIX.size(), SNAPSHOT_PREFIX) == 0) {
            remove_all_dir(snapshot_path);
            OLAP_LOG_TRACE("success to release snapshot path. [path='%s']", snapshot_path.c_str());

            return OLAP_SUCCESS;
        }
    }

    OLAP_LOG_WARNING("released snapshot path illegal. [path='%s']", snapshot_path.c_str());
    return OLAP_ERR_CE_CMD_PARAMS_ERROR;
}

OLAPStatus OLAPSnapshot::_calc_snapshot_id_path(
        const SmartOLAPTable& olap_table,
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
    AutoMutexLock auto_lock(&_mutex); // will automatically unlock when function return.
    snapshot_id_path_stream << olap_table->storage_root_path_name() << SNAPSHOT_PREFIX
                            << "/" << time_str << "." << _base_id++;
    *out_path = snapshot_id_path_stream.str();
    return res;
}

string OLAPSnapshot::_get_schema_hash_full_path(
        const SmartOLAPTable& ref_olap_table,
        const string& location) const {
    stringstream schema_full_path_stream;
    schema_full_path_stream << location
                            << "/" << ref_olap_table->tablet_id()
                            << "/" << ref_olap_table->schema_hash();
    string schema_full_path = schema_full_path_stream.str();

    return schema_full_path;
}

string OLAPSnapshot::_get_header_full_path(
        const SmartOLAPTable& ref_olap_table,
        const std::string& schema_hash_path) const {
    stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << ref_olap_table->tablet_id() << ".hdr";
    return header_name_stream.str();
}

void OLAPSnapshot::_update_header_file_info(
        const vector<VersionEntity>& shortest_versions,
        OLAPHeader* olap_header) {
    // clear schema_change_status
    olap_header->clear_schema_change_status();
    // remove all old version and add new version
    olap_header->delete_all_versions();

    for (uint32_t i = 0; i < shortest_versions.size(); ++i) {
        if (shortest_versions[i].column_statistics.size() == 0) {
            olap_header->add_version(
                    shortest_versions[i].version,
                    shortest_versions[i].version_hash,
                    shortest_versions[i].num_segments,
                    0,
                    shortest_versions[i].index_size,
                    shortest_versions[i].data_size,
                    shortest_versions[i].num_rows);
        } else {
            olap_header->add_version(
                    shortest_versions[i].version,
                    shortest_versions[i].version_hash,
                    shortest_versions[i].num_segments,
                    0,
                    shortest_versions[i].index_size,
                    shortest_versions[i].data_size,
                    shortest_versions[i].num_rows,
                    const_cast<std::vector<std::pair<Field *, Field *> >*> \
                    (&shortest_versions[i].column_statistics));
        }
    }
}

OLAPStatus OLAPSnapshot::_link_index_and_data_files(
        const string& header_path,
        const SmartOLAPTable& ref_olap_table,
        const vector<VersionEntity>& version_entity_vec) {
    OLAPStatus res = OLAP_SUCCESS;

    for (const VersionEntity& entity : version_entity_vec) {
        for (uint32_t i = 0; i < entity.num_segments; ++i) {
            string index_path = _construct_index_file_path(
                    header_path, entity.version, entity.version_hash, i);
            string ref_table_index_path = ref_olap_table->construct_index_file_path(
                    entity.version, entity.version_hash, i);
            res = _create_hard_link(ref_table_index_path, index_path);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to create hard link. [header_path=%s from_path=%s to_path=%s]",
                        header_path.c_str(), ref_table_index_path.c_str(), index_path.c_str());
                return res;
            }

            string data_path = _construct_data_file_path(
                    header_path, entity.version, entity.version_hash, i);
            string ref_table_data_path = ref_olap_table->construct_data_file_path(
                    entity.version, entity.version_hash, i);
            res = _create_hard_link(ref_table_data_path, data_path);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to create hard link. [header_path=%s from_path=%s to_path=%s]",
                        header_path.c_str(), ref_table_data_path.c_str(), data_path.c_str());
                return res;
            }
        }
    }

    return res;
}

OLAPStatus OLAPSnapshot::_copy_index_and_data_files(
        const string& header_path,
        const SmartOLAPTable& ref_olap_table,
        vector<VersionEntity>& version_entity_vec) {
    for (const VersionEntity& entity : version_entity_vec) {
        for (uint32_t i = 0; i < entity.num_segments; ++i) {
            string index_path;
            string data_path;
            string ref_table_index_path;
            string ref_table_data_path;

            index_path = _construct_index_file_path(
                    header_path, entity.version, entity.version_hash, i);
            ref_table_index_path = ref_olap_table->construct_index_file_path(
                    entity.version, entity.version_hash, i);
            Status status = FileUtils::copy_file(ref_table_index_path, index_path);
            if (!status.ok()) {
                OLAP_LOG_WARNING("fail to copy file. [src='%s' dest='%s']",
                        ref_table_index_path.c_str(), index_path.c_str());
                return OLAP_ERR_COPY_FILE_ERROR;
            }
            data_path = _construct_data_file_path(
                    header_path, entity.version, entity.version_hash, i);
            ref_table_data_path = ref_olap_table->construct_data_file_path(
                    entity.version, entity.version_hash, i);
            status = FileUtils::copy_file(ref_table_data_path, data_path);
            if (!status.ok()) {
                OLAP_LOG_WARNING("fail to copy file. [src='%s' dest='%s']",
                        ref_table_data_path.c_str(), data_path.c_str());
                return OLAP_ERR_COPY_FILE_ERROR;
            }
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPSnapshot::_create_snapshot_files(
        const SmartOLAPTable& ref_olap_table,
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

    vector<IData*> olap_data_sources;
    OLAPHeader* new_olap_header = nullptr;
    do {
        // get latest version
        const FileVersionMessage* latest_version = NULL;
        latest_version = ref_olap_table->latest_version();
        if (latest_version == NULL) {
            OLAP_LOG_WARNING("table has not any version. [path='%s']",
                    ref_olap_table->full_name().c_str());
            res = OLAP_ERR_VERSION_NOT_EXIST;
            break;
        }

        // get snapshot version, use request.version if specified
        int32_t version = latest_version->end_version();
        if (request.__isset.version) {
            if (latest_version->end_version() < request.version
                    || (latest_version->start_version() == latest_version->end_version()
                    && latest_version->end_version() == request.version
                    && latest_version->version_hash() != request.version_hash)) {
                OLAP_LOG_WARNING("invalid make snapshot request. "
                        "[version=%d version_hash=%ld req_version=%d req_version_hash=%ld]",
                        latest_version->end_version(), latest_version->version_hash(),
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
        string old_header_path = ref_olap_table->header_file_name();
        new_olap_header = new(nothrow) OLAPHeader(old_header_path);
        if (new_olap_header == NULL) {
            OLAP_LOG_WARNING("fail to malloc OLAPHeader.");
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        res = new_olap_header->load();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to load header. [res=%d header_file=%s]",
                    res, old_header_path.c_str());
            break;
        }

        ref_olap_table->release_header_lock();
        header_locked = false;
        _update_header_file_info(shortest_versions, new_olap_header);

        // save new header
        if ((res = new_olap_header->save(header_path)) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("save header error. [table='%s' path='%s']",
                    ref_olap_table->full_name().c_str(), header_path.c_str());
            break;
        }

        res = _link_index_and_data_files(header_path, ref_olap_table, shortest_versions);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create hard link. [path=%s]", snapshot_id_path.c_str());
            break;
        }

        // append a single delta if request.version is end_version of cumulative delta
        if (request.__isset.version) {
            for (const VersionEntity& entity : shortest_versions) {
                if (entity.version.second == request.version) {
                    if (entity.version.first != request.version) {
                        res = _append_single_delta(request, header_path);
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
            OLAP_LOG_DEBUG("remove snapshot path. [path=%s]", snapshot_id_path.c_str());
            remove_all_dir(snapshot_id_path);
        }
    } else {
        *snapshot_path = snapshot_id;
    }

    return res;
}

OLAPStatus OLAPSnapshot::_append_single_delta(const TSnapshotRequest& request, const string& header_path) {
    OLAPStatus res = OLAP_SUCCESS;

    OLAPTable* tablet = OLAPTable::create_from_header_file(
            request.tablet_id, request.schema_hash, header_path);
    if (tablet == NULL) {
        OLAP_LOG_WARNING("fail to create tablet from header file. [header_path='%s']",
                         header_path.c_str());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    res = tablet->load();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to load tablet. [res=%d header_path='%s']",
                         res, header_path.c_str());
        return res;
    }

    const FileVersionMessage* latest_version = tablet->latest_version();
    if (latest_version->start_version() != request.version) {
        TPushReq empty_push;
        empty_push.tablet_id = request.tablet_id;
        empty_push.schema_hash = request.schema_hash;
        empty_push.version = request.version + 1;
        empty_push.version_hash = 0;
        
        PushHandler handler;
        SmartOLAPTable smart_tablet(tablet);
        res = handler.process(smart_tablet, empty_push, PUSH_NORMAL, NULL);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to push empty version. [res=%d version=%d]",
                             res, empty_push.version);
            return res;
        }
    }

    return res;
}

string OLAPSnapshot::_construct_index_file_path(
        const string& header_path,
        const Version& version,
        VersionHash version_hash,
        uint32_t segment) const {
    return OLAPTable::construct_file_path(header_path, version, version_hash, segment, "idx");
}

string OLAPSnapshot::_construct_data_file_path(
        const string& header_path,
        const Version& version,
        VersionHash version_hash,
        uint32_t segment) const {
    return OLAPTable::construct_file_path(header_path, version, version_hash, segment, "dat");
}

OLAPStatus OLAPSnapshot::_create_hard_link(const string& from_path, const string& to_path) {
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

OLAPStatus OLAPSnapshot::storage_medium_migrate(
        TTabletId tablet_id, TSchemaHash schema_hash,
        TStorageMedium::type storage_medium) {
    OLAPStatus res = OLAP_SUCCESS;
    SmartOLAPTable tablet = OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("can't find olap table. [tablet_id=%ld schema_hash=%d]",
                tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // judge case when no need to migrate
    uint32_t count = OLAPRootPath::get_instance()->available_storage_medium_type_count();
    if (count <= 1) {
        OLAP_LOG_INFO("available storage medium type count is less than 1, "
                "no need to migrate. [count=%u]", count);
        return OLAP_SUCCESS;
    }

    TStorageMedium::type src_storage_medium = TStorageMedium::HDD;
    if (OLAPRootPath::is_ssd_disk(tablet->storage_root_path_name())) {
        src_storage_medium = TStorageMedium::SSD;
    }

    if (src_storage_medium == storage_medium) {
        OLAP_LOG_INFO("tablet is already on specified storage medium. "
                "[storage_medium='%d']", storage_medium);
        return OLAP_SUCCESS;
    }

    vector<IData*> olap_data_sources;
    tablet->obtain_push_lock();

    do {
        // get all versions to be migrate
        tablet->obtain_header_rdlock();
        const FileVersionMessage* latest_version = tablet->latest_version();
        if (latest_version == NULL) {
            tablet->release_header_lock();
            res = OLAP_ERR_VERSION_NOT_EXIST;
            OLAP_LOG_WARNING("tablet has not any version.");
            break;
        }

        int32_t end_version = latest_version->end_version();
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
        vector<string> root_path_vec;
        OLAPRootPath::get_instance()->get_root_path_for_create_table(storage_medium, &root_path_vec);
        if (root_path_vec.size() == 0) {
            res = OLAP_ERR_INVALID_ROOT_PATH;
            OLAP_LOG_WARNING("fail to get root path for create tablet.");
            break;
        }

        uint64_t shard = 0;
        res = OLAPRootPath::get_instance()->get_root_path_shard(root_path_vec[0], &shard);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to get root path shard. [res=%d]", res);
            break;
        }

        stringstream root_path_stream;
        root_path_stream << root_path_vec[0] << DATA_PREFIX << "/" << shard;
        string schema_hash_path = _get_schema_hash_full_path(tablet, root_path_stream.str());
        if (check_dir_existed(schema_hash_path)) {
            OLAP_LOG_DEBUG("schema hash path already exist, remove it. [schema_hash_path='%s']",
                    schema_hash_path.c_str());
            remove_all_dir(schema_hash_path);
        }
        create_dirs(schema_hash_path);

        // migrate all index and data files but header file
        string new_header_path = _get_header_full_path(tablet, schema_hash_path);
        res = _copy_index_and_data_files(new_header_path, tablet, version_entity_vec);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to copy index and data files when migrate. [res=%d]", res);
            break;
        }

        // generate new header file from the old
        res = _generate_new_header(tablet, new_header_path, version_entity_vec);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to generate new header file from the old. [res=%d]", res);
            break;
        }

        // load the new tablet into OLAPEngine
        res = OLAPEngine::get_instance()->load_one_tablet(
                tablet_id, schema_hash, schema_hash_path);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to add tablet to OLAPEngine. [res=%d]", res);
            break;
        }

        // if old table finished schema change, then the schema change status of the new table is DONE
        // else the schema change status of the new table is FAILED
        SmartOLAPTable new_tablet =
                OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
        if (new_tablet.get() == NULL) {
            OLAP_LOG_WARNING("get null olap table. [tablet_id=%ld schema_hash=%d]",
                             tablet_id, schema_hash);
            return OLAP_ERR_TABLE_NOT_FOUND;
        }
        SchemaChangeStatus tablet_status = tablet->schema_change_status();
        if (tablet->schema_change_status().status == AlterTableStatus::ALTER_TABLE_DONE) {
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

OLAPStatus OLAPSnapshot::_generate_new_header(
        const SmartOLAPTable& tablet,
        const string& new_header_path,
        const vector<VersionEntity>& version_entity_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAPHeader* new_olap_header = NULL;

    {
        AutoRWLock auto_lock(tablet->get_header_lock_ptr(), true);
        new_olap_header = new(nothrow) OLAPHeader(tablet->header_file_name());
        if (new_olap_header == NULL) {
            OLAP_LOG_WARNING("fail to malloc OLAPHeader. [size=%d]", sizeof(OLAPHeader));
            return OLAP_ERR_MALLOC_ERROR;
        }

        res = new_olap_header->load();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to load header. [res=%d header_file=%s]",
                    res, tablet->header_file_name().c_str());
            SAFE_DELETE(new_olap_header);
            return res;
        }
    }

    _update_header_file_info(version_entity_vec, new_olap_header);

    res = new_olap_header->save(new_header_path);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to save olap header to new path. [res=%d new_header_path='%s']",
                res, new_header_path.c_str());
    }

    SAFE_DELETE(new_olap_header);
    return res;
}

}  // namespace palo
