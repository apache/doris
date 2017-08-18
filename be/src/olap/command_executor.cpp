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

#include "olap/command_executor.h"

#include <dirent.h>
#include <unistd.h>

#include <cstring>
#include <iomanip>
#include <list>
#include <map>
#include <set>
#include <sstream>
#include <vector>

#include <boost/regex.hpp>
#include <boost/scoped_array.hpp>
#include <thrift/protocol/TDebugProtocol.h>

#include "olap/base_expansion_handler.h"
#include "olap/delete_handler.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_engine.h"
#include "olap/olap_server.h"
#include "olap/olap_table.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/schema_change.h"
#include "olap/utils.h"
#include "util/palo_metrics.h"

using apache::thrift::ThriftDebugString;
using std::map;
using std::make_pair;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::vector;
using std::list;
using google::protobuf::RepeatedPtrField;

namespace palo {

OLAPStatus CommandExecutor::compute_checksum(
        TTabletId tablet_id,
        TSchemaHash schema_hash,
        TVersion version,
        TVersionHash version_hash,
        uint32_t* checksum) {
    OLAP_LOG_INFO("begin to process compute checksum. "
                  "[tablet_id=%ld schema_hash=%d version=%ld]",
                  tablet_id, schema_hash, version);
    OLAPStatus res = OLAP_SUCCESS;

    if (checksum == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    SmartOLAPTable tablet =
            OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
    if (NULL == tablet.get()) {
        OLAP_LOG_WARNING("can't find tablet. [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    {
        AutoRWLock auto_lock(tablet->get_header_lock_ptr(), true);
        const FileVersionMessage* message = tablet->latest_version();
        if (message == NULL) {
            OLAP_LOG_FATAL("fail to get latest version. [tablet_id=%ld]", tablet_id);
            return OLAP_ERR_VERSION_NOT_EXIST;
        }

        if (message->end_version() == version
                && message->version_hash() != version_hash) {
            OLAP_LOG_WARNING("fail to check latest version hash. "
                             "[res=%d tablet_id=%ld version_hash=%ld request_version_hash=%ld]",
                             res, tablet_id, message->version_hash(), version_hash);
            return OLAP_ERR_CE_CMD_PARAMS_ERROR;
        }
    }

    Reader reader;
    ReaderParams reader_params;
    reader_params.olap_table = tablet;
    reader_params.reader_type = READER_CHECKSUM;
    reader_params.version = Version(0, version);

    // ignore float and double type considering to precision lose
    for (size_t i = 0; i < tablet->tablet_schema().size(); ++i) {
        FieldType type = tablet->get_field_type_by_index(i);
        if (type == OLAP_FIELD_TYPE_FLOAT || type == OLAP_FIELD_TYPE_DOUBLE) {
            continue;
        }

        reader_params.return_columns.push_back(i);
    }

    res = reader.init(reader_params);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("initiate reader fail. [res=%d]", res);
        return res;
    }

    RowCursor row;
    res = row.init(tablet->tablet_schema(), reader_params.return_columns);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to init row cursor. [res=%d]", res);
        return res;
    }

    RowCursor tmp_row;
    res = tmp_row.init(tablet->tablet_schema(), reader_params.return_columns);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to init row cursor. [res=%d]", res);
        return res;
    }
    
    bool eof = false;
    int64_t raw_rows_read = 0;
    uint32_t tmp_checksum = CRC32_INIT;
    while (true) {
        OLAPStatus res = reader.next_row_with_aggregation(&tmp_row, &raw_rows_read, &eof);
        if (res == OLAP_SUCCESS && eof) {
            OLAP_LOG_DEBUG("reader reads to the end.");
            break;
        } else if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to read in reader. [res=%d]", res);
            return res;
        }

        // reset buffer and copy from tmp_row to avoid invalid content in varchar buffer
        row.reset_buf();
        row.copy(tmp_row);
        tmp_checksum = olap_crc32(tmp_checksum, row.get_buf(), row.get_buf_len());
    }
    
    OLAP_LOG_INFO("success to finish compute checksum. [checksum=%u]", tmp_checksum);
    *checksum = tmp_checksum;
    return OLAP_SUCCESS;
}

OLAPStatus CommandExecutor::push(
        const TPushReq& request,
        vector<TTabletInfo>* tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_INFO("begin to process push. [tablet_id=%ld version=%ld]",
                  request.tablet_id, request.version);

    time_t start = time(NULL);
    if (PaloMetrics::palo_push_count() != NULL) {
        PaloMetrics::palo_push_count()->increment(1);
    }

    if (tablet_info_vec == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    SmartOLAPTable olap_table = OLAPEngine::get_instance()->get_table(
            request.tablet_id, request.schema_hash);
    if (NULL == olap_table.get()) {
        OLAP_LOG_WARNING("false to find table. [table=%ld schema_hash=%d]",
                         request.tablet_id, request.schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    PushType type = PUSH_NORMAL;
    if (request.push_type == TPushType::LOAD_DELETE) {
        type = PUSH_FOR_LOAD_DELETE;
    }

    PushHandler push_handler;
    res = push_handler.process(olap_table, request, type, tablet_info_vec);

    time_t cost = time(NULL) - start;
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process push. cost: %ld [res=%d table=%s]",
                         cost, res, olap_table->full_name().c_str());
        return res;
    }

    OLAP_LOG_INFO("success to finish push. cost: %ld. [table=%s]",
            cost, olap_table->full_name().c_str());
    return res;
}

OLAPStatus CommandExecutor::base_expansion(
        TTabletId tablet_id,
        TSchemaHash schema_hash,
        TVersion version) {
    OLAP_LOG_INFO("begin to process base expansion. "
                  "[tablet_id=%ld schema_hash=%d version=%ld]",
                  tablet_id, schema_hash, version);
    OLAPStatus res = OLAP_SUCCESS;

    SmartOLAPTable table =
            OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
    if (NULL == table.get()) {
        OLAP_LOG_WARNING("can't find olap table. [table=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    BaseExpansionHandler base_expansion_handler;
    res = base_expansion_handler.init(table, true);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init BaseExpansionHandler. [res=%d]", res);
        return res;
    }

    res = base_expansion_handler.run();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process base_expansion. [res=%d]", res);
    }

    OLAP_LOG_INFO("success to finish base expansion.");
    return res;
}

OLAPStatus CommandExecutor::create_table(const TCreateTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAPTable* olap_table = NULL;
    bool is_table_added = false;

    OLAP_LOG_INFO("begin to process create table. [tablet=%ld, schema_hash=%d]",
                  request.tablet_id, request.tablet_schema.schema_hash);

    if (PaloMetrics::palo_request_count() != NULL) {
        PaloMetrics::palo_request_count()->increment(1);
    }

    // 1. Make sure create_table operation is idempotent:
    //    return success if table with same tablet_id and schema_hash exist,
    //           false if table with same tablet_id but different schema_hash exist
    if (OLAPEngine::get_instance()->check_tablet_id_exist(request.tablet_id)) {
        SmartOLAPTable table = OLAPEngine::get_instance()->get_table(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (table.get() != NULL) {
            OLAP_LOG_INFO("create table success for table already exist.");
            return OLAP_SUCCESS;
        } else {
            OLAP_LOG_WARNING("table with different schema hash already exists.");
            return OLAP_ERR_CE_TABLET_ID_EXIST;
        }
    }

    // 2. Lock to ensure that all create_table operation execute in serial
    static MutexLock create_table_lock;
    AutoMutexLock auto_lock(&create_table_lock);

    do {
        // 3. Create table with only header, no deltas
        olap_table = OLAPEngine::get_instance()->create_table(request, NULL, false, NULL);
        if (olap_table == NULL) {
            res = OLAP_ERR_CE_CMD_PARAMS_ERROR;
            OLAP_LOG_WARNING("fail to create olap table. [res=%d]", res);
            break;
        }

        // 4. Add table to OlapEngine will make it visiable to user
        res = OLAPEngine::get_instance()->add_table(
                request.tablet_id, request.tablet_schema.schema_hash, olap_table);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to add table to OLAPEngine. [res=%d]", res);
            break;
        }
        is_table_added = true;
        
        SmartOLAPTable olap_table_ptr = OLAPEngine::get_instance()->get_table(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (olap_table_ptr.get() == NULL) {
            res = OLAP_ERR_TABLE_NOT_FOUND;
            OLAP_LOG_WARNING("fail to get table. [res=%d]", res);
            break;
        }

        // 5. Register table into OLAPRootPath, so that we can manage table from
        // the perspective of root path.
        // Example: unregister all tables when a bad disk found.
        res = OLAPRootPath::get_instance()->register_table_into_root_path(olap_table_ptr.get());
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to register table into OLAPRootPath. [res=%d, root_path=%s]",
                    res, olap_table_ptr->storage_root_path_name().c_str());
            break;
        }

        // 6. Create init version if request.version set
        if (request.__isset.version) {
            res = _create_init_version(olap_table_ptr, request);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to create initial version for table. [res=%d]", res);
            }
        }
    } while (0);

    // 7. clear environment
    if (res != OLAP_SUCCESS) {
        if (is_table_added) {
            OLAPStatus status = OLAPEngine::get_instance()->drop_table(
                    request.tablet_id, request.tablet_schema.schema_hash);
            if (status !=  OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to drop table when create table failed. [res=%d]", res);
            }
        } else if (NULL != olap_table) {
            olap_table->delete_all_files();
            SAFE_DELETE(olap_table);
        }
    }

    OLAP_LOG_INFO("finish to process create table. [res=%d]", res);
    return res;
}

SmartOLAPTable CommandExecutor::get_table(TTabletId tablet_id, TSchemaHash schema_hash) {
    OLAP_LOG_DEBUG("begin to process get_table. [table=%ld schema_hash=%d]",
                   tablet_id, schema_hash);
    return OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
}

OLAPStatus CommandExecutor::drop_table(const TDropTabletReq& request) {
    OLAP_LOG_INFO("begin to process drop table. [table=%ld schema_hash=%d]",
                  request.tablet_id, request.schema_hash);

    if (PaloMetrics::palo_request_count() != NULL) {
        PaloMetrics::palo_request_count()->increment(1);
    }

    OLAPStatus res = OLAPEngine::get_instance()->drop_table(
            request.tablet_id, request.schema_hash);
    if (res != OLAP_SUCCESS && res != OLAP_ERR_TABLE_NOT_FOUND) {
        OLAP_LOG_WARNING("fail to process drop table. [status=%d]", res);
        return res;
    }

    OLAP_LOG_INFO("success to process drop table.");
    return OLAP_SUCCESS;
}

OLAPStatus CommandExecutor::report_all_tablets_info(
        map<TTabletId, TTablet>* tablets_info) {
    OLAP_LOG_INFO("begin to process report all tablets info.");

    if (PaloMetrics::palo_request_count() != NULL) {
        PaloMetrics::palo_request_count()->increment(1);
    }

    OLAPStatus res = OLAP_SUCCESS;

    res = OLAPEngine::get_instance()->report_all_tablets_info(tablets_info);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process report all tablets info. [res=%d]", res);
        return res;
    }

    OLAP_LOG_INFO("success to process report all tablets info. [tablet_num=%u]",
                  tablets_info->size());
    return OLAP_SUCCESS;
}

OLAPStatus CommandExecutor::report_tablet_info(TTabletInfo* tablet_info) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_INFO("begin to process report tablet info. "
                  "[table=%ld schema_hash=%d]",
                  tablet_info->tablet_id, tablet_info->schema_hash);

    if (PaloMetrics::palo_request_count() != NULL) {
        PaloMetrics::palo_request_count()->increment(1);
    }

    res = OLAPEngine::get_instance()->report_tablet_info(tablet_info);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get tablet info. [res=%d]", res);
        return res;
    }

    OLAP_LOG_INFO("success to process report tablet info.");
    return OLAP_SUCCESS;
}

OLAPStatus CommandExecutor::schema_change(const TAlterTabletReq& request) {
    OLAP_LOG_INFO("begin to schema change. [base_table=%ld new_table=%ld]",
                  request.base_tablet_id, request.new_tablet_req.tablet_id);

    if (PaloMetrics::palo_request_count() != NULL) {
        PaloMetrics::palo_request_count()->increment(1);
    }

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_table(ALTER_TABLET_SCHEMA_CHANGE, request);

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to submit schema_change. "
                         "[base_table=%ld new_table=%ld] [res=%d]",
                         request.base_tablet_id, request.new_tablet_req.tablet_id, res);
        return res;
    }

    OLAP_LOG_INFO("success to submit schema change. "
                  "[base_table=%ld new_table=%ld]",
                  request.base_tablet_id, request.new_tablet_req.tablet_id);
    return res;
}

OLAPStatus CommandExecutor::create_rollup_table(const TAlterTabletReq& request) {
    OLAP_LOG_INFO("begin to create rollup table. "
                  "[base_table=%ld new_table=%ld]",
                  request.base_tablet_id, request.new_tablet_req.tablet_id);

    if (PaloMetrics::palo_request_count() != NULL) {
        PaloMetrics::palo_request_count()->increment(1);
    }

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_table(ALTER_TABLET_CREATE_ROLLUP_TABLE, request);

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to create rollup table. "
                         "[base_table=%ld new_table=%ld] [res=%d]",
                         request.base_tablet_id, request.new_tablet_req.tablet_id, res);
        return res;
    }

    OLAP_LOG_INFO("success to create rollup table. "
                  "[base_table=%ld new_table=%ld] [res=%d]",
                  request.base_tablet_id, request.new_tablet_req.tablet_id, res);
    return res;
}

AlterTableStatus CommandExecutor::show_alter_table_status(
        TTabletId tablet_id,
        TSchemaHash schema_hash) {
    OLAP_LOG_INFO("begin to process show alter table status. "
                  "[table=%ld schema_hash=%d]",
                  tablet_id, schema_hash);

    if (PaloMetrics::palo_request_count() != NULL) {
        PaloMetrics::palo_request_count()->increment(1);
    }

    AlterTableStatus status = ALTER_TABLE_DONE;

    SmartOLAPTable table = OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
    if (table.get() == NULL) {
        OLAP_LOG_WARNING("fail to get table. [table=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        status = ALTER_TABLE_FAILED;
    } else {
        status = table->schema_change_status().status;
    }

    return status;
}

OLAPStatus CommandExecutor::make_snapshot(
        TTabletId tablet_id,
        TSchemaHash schema_hash,
        string* snapshot_path) {
    TSnapshotRequest request;
    request.tablet_id = tablet_id;
    request.schema_hash = schema_hash;
    return this->make_snapshot(request, snapshot_path);
}

OLAPStatus CommandExecutor::make_snapshot(
        const TSnapshotRequest& request,
        std::string* snapshot_path) {
    OLAP_LOG_INFO("begin to process make snapshot. "
                  "[table=%ld, schema_hash=%d]",
                  request.tablet_id, request.schema_hash);

    OLAPStatus res = OLAP_SUCCESS;
    res = OLAPSnapshot::get_instance()->make_snapshot(request, snapshot_path);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process make snapshot. [res=%d]", res);
        return res;
    }

    OLAP_LOG_INFO("success to process make snapshot. [path=%s]",
                  snapshot_path->c_str());
    return res;
}

OLAPStatus CommandExecutor::release_snapshot(const string& snapshot_path) {
    OLAP_LOG_INFO("begin to process release snapshot. [path='%s']",
                  snapshot_path.c_str());
    OLAPStatus res = OLAP_SUCCESS;

    res = OLAPSnapshot::get_instance()->release_snapshot(snapshot_path);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process release snapshot. [res=%d]", res);
        return res;
    }

    OLAP_LOG_TRACE("success to process release snapshot[path=%s].",
                   snapshot_path.c_str());
    return res;
}

OLAPStatus CommandExecutor::obtain_shard_path(
        TStorageMedium::type storage_medium, std::string* shard_path) {
    OLAP_LOG_INFO("begin to process obtain root path. [storage_medium=%d]", storage_medium);
    OLAPStatus res = OLAP_SUCCESS;

    if (shard_path == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    OLAPRootPath::RootPathVec root_paths;
    OLAPRootPath::get_instance()->get_root_path_for_create_table(storage_medium, &root_paths);
    if (root_paths.size() == 0) {
        OLAP_LOG_WARNING("no available disk can be used to create table.");
        return OLAP_ERR_NO_AVAILABLE_ROOT_PATH;
    }

    uint64_t shard = 0;
    res = OLAPRootPath::get_instance()->get_root_path_shard(root_paths[0], &shard);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get root path shard. [res=%d]", res);
        return res;
    }

    stringstream root_path_stream;
    root_path_stream << root_paths[0] << DATA_PREFIX << "/" << shard;
    *shard_path = root_path_stream.str();

    OLAP_LOG_INFO("success to process obtain root path. [path='%s']",
                  shard_path->c_str());
    return res;
}

OLAPStatus CommandExecutor::load_header(
        const string& shard_path,
        const TCloneReq& request) {
    OLAP_LOG_INFO("begin to process load headers. "
                  "[tablet_id=%ld schema_hash=%d]",
                  request.tablet_id, request.schema_hash);
    OLAPStatus res = OLAP_SUCCESS;

    stringstream schema_hash_path_stream;
    schema_hash_path_stream << shard_path 
                            << "/" << request.tablet_id
                            << "/" << request.schema_hash;
    res =  OLAPEngine::get_instance()->load_one_tablet(
            request.tablet_id, request.schema_hash,
            schema_hash_path_stream.str());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process load headers. [res=%d]", res);
        return res;
    }

    OLAP_LOG_INFO("success to process load headers.");
    return res;
}

OLAPStatus CommandExecutor::load_header(
        const string& shard_path,
        TTabletId tablet_id,
        TSchemaHash schema_hash) {
    OLAP_LOG_INFO("begin to process load headers. [tablet_id=%ld schema_hash=%d]",
                  tablet_id, schema_hash);
    OLAPStatus res = OLAP_SUCCESS;

    stringstream schema_hash_path_stream;
    schema_hash_path_stream << shard_path 
                            << "/" << tablet_id
                            << "/" << schema_hash;
    res =  OLAPEngine::get_instance()->load_one_tablet(
            tablet_id, schema_hash,
            schema_hash_path_stream.str());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process load headers. [res=%d]", res);
        return res;
    }

    OLAP_LOG_INFO("success to process load headers.");
    return res;
}

OLAPStatus CommandExecutor::storage_medium_migrate(const TStorageMediumMigrateReq& request) {
    OLAP_LOG_INFO("begin to process storage media migrate. "
                  "[tablet_id=%ld schema_hash=%d dest_storage_medium=%d]",
                  request.tablet_id, request.schema_hash, request.storage_medium);

    if (PaloMetrics::palo_request_count() != NULL) {
        PaloMetrics::palo_request_count()->increment(1);
    }

    OLAPStatus res = OLAP_SUCCESS;
    res = OLAPSnapshot::get_instance()->storage_medium_migrate(
            request.tablet_id, request.schema_hash, request.storage_medium);

    OLAP_LOG_INFO("finish to process storage media migrate. [res=%d]", res);
    return res;
}

OLAPStatus CommandExecutor::reload_root_path(const string& root_paths) {
    OLAP_LOG_INFO("begin to process reload root path. [path='%s']",
                  root_paths.c_str());
    OLAPStatus res = OLAP_SUCCESS;

    static MutexLock reload_root_path_lock;
    reload_root_path_lock.lock();
    res = OLAPRootPath::get_instance()->reload_root_paths(root_paths.c_str());
    reload_root_path_lock.unlock();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process reload root path. [res=%d]", res);
        return res;
    }

    OLAP_LOG_INFO("success to finish reload root path.");
    return res;
}

OLAPStatus CommandExecutor::delete_data(
        const TPushReq& request,
        vector<TTabletInfo>* tablet_info_vec) {
    OLAP_LOG_INFO("begin to process delete data. [request='%s']",
                  ThriftDebugString(request).c_str());

    if (PaloMetrics::palo_request_count() != NULL) {
        PaloMetrics::palo_request_count()->increment(1);
    }

    OLAPStatus res = OLAP_SUCCESS;

    if (tablet_info_vec == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    // 1. Get all tablets with same tablet_id
    SmartOLAPTable table = OLAPEngine::get_instance()->get_table(
            request.tablet_id, request.schema_hash);
    if (table.get() == NULL) {
        OLAP_LOG_WARNING("can't find table. [table=%ld schema_hash=%d]",
                         request.tablet_id, request.schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 2. Process delete data by push interface
    PushHandler push_handler;
    res = push_handler.process(
            table, request, PUSH_FOR_DELETE, tablet_info_vec);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to push empty version for delete data. "
                         "[res=%d table='%s']",
                         res, table->full_name().c_str());
        return res;
    }

    OLAP_LOG_INFO("finish to process delete data. [res=%d]", res);
    return res;
}

OLAPStatus CommandExecutor::cancel_delete(const TCancelDeleteDataReq& request) {
    OLAP_LOG_INFO("begin to process cancel delete. [table=%ld version=%ld]",
                  request.tablet_id, request.version);

    if (PaloMetrics::palo_request_count() != NULL) {
        PaloMetrics::palo_request_count()->increment(1);
    }

    OLAPStatus res = OLAP_SUCCESS;

    // 1. Get all tablets with same tablet_id
    list<SmartOLAPTable> table_list;
    res = OLAPEngine::get_instance()->get_tables_by_id(request.tablet_id, &table_list);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("can't find table. [table=%ld]", request.tablet_id);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 2. Remove delete conditions from each tablet.
    DeleteConditionHandler cond_handler;
    for (SmartOLAPTable temp_table : table_list) {
        temp_table->obtain_header_wrlock();
        res = cond_handler.delete_cond(temp_table, request.version, false);
        if (res != OLAP_SUCCESS) {
            temp_table->release_header_lock();
            OLAP_LOG_WARNING("cancel delete failed. [res=%d table=%s]",
                             res, temp_table->full_name().c_str());
            break;
        }

        res = temp_table->save_header();
        if (res != OLAP_SUCCESS) {
            temp_table->release_header_lock();
            OLAP_LOG_WARNING("fail to save header. [res=%d table=%s]",
                             res, temp_table->full_name().c_str());
            break;
        }
        temp_table->release_header_lock();
    }

    // Show delete conditions in tablet header.
    for (SmartOLAPTable table : table_list) {
        cond_handler.log_conds(table);
    }

    OLAP_LOG_INFO("finish to process cancel delete. [res=%d]", res);
    return res;
}

OLAPStatus CommandExecutor::get_all_root_path_stat(
        std::vector<OLAPRootPathStat>* root_paths_stat) {
    OLAP_LOG_INFO("begin to process get all root path stat.");
    OLAPStatus res = OLAP_SUCCESS;
    
    res = OLAPRootPath::get_instance()->get_all_root_path_stat(root_paths_stat);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process get all root path stat. [res=%d]", res);
        return res;
    }

    OLAP_LOG_INFO("success to process get all root path stat.");
    return res;
}

OLAPStatus CommandExecutor::_create_init_version(
        SmartOLAPTable olap_table, const TCreateTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;

    if (request.version < 1) {
        OLAP_LOG_WARNING("init version of tablet should at least 1.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    } else {
        Version init_base_version(0, request.version);
        res = OLAPEngine::get_instance()->create_init_version(
                request.tablet_id, request.tablet_schema.schema_hash,
                init_base_version, request.version_hash);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create init base version. [res=%d version=%ld]",
                    res, request.version);
            return res;
        }

        Version init_delta_version(request.version + 1, request.version + 1);
        res = OLAPEngine::get_instance()->create_init_version(
                request.tablet_id, request.tablet_schema.schema_hash,
                init_delta_version, 0);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create init delta version. [res=%d version=%ld]",
                    res, request.version + 1);
            return res;
        }
    }

    olap_table->obtain_header_wrlock();
    olap_table->set_cumulative_layer_point(request.version + 1);
    res = olap_table->save_header();
    olap_table->release_header_lock();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to save header. [table=%s]", olap_table->full_name().c_str());
    }

    return res;
}

}  // namespace palo
