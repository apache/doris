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

#include "olap/push_handler.h"

#include <algorithm>
#include <iostream>
#include <sstream>

#include <boost/filesystem.hpp>

#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/schema_change.h"
#include "olap/rowset/alpha_rowset_builder.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/rowset_meta_manager.h"

using std::list;
using std::map;
using std::string;
using std::vector;

namespace doris {

// Process push command, the main logical is as follows:
//    a. related tablets not exist:
//        current table isn't in schemachange state, only push for current tablet
//    b. related tablets exist
//       I.  current tablet is old table (cur.create_time < related.create_time):
//           push for current table and than convert data for related tables
//       II. current table is new table:
//           this usually means schema change is over,
//           clear schema change info in both current tablet and related tablets,
//           finally we will only push for current tablets. this is very useful in rollup action.
OLAPStatus PushHandler::process_realtime_push(
        TabletSharedPtr tablet,
        const TPushReq& request,
        PushType push_type,
        vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to realtime push. tablet=" << tablet->full_name()
              << ", transaction_id=" << request.transaction_id;

    OLAPStatus res = OLAP_SUCCESS;
    _request = request;
    vector<TabletVars> tablet_vars(1);
    tablet_vars[0].tablet = tablet;
    res = _do_realtime_push(tablet, request, push_type, &tablet_vars, tablet_info_vec);
    // if transaction existed in engine but push not finished, not report to fe
    if (res == OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
        OLAP_LOG_WARNING("find transaction existed when realtime push, not report. ",
                         "[tablet=%s partition_id=%ld transaction_id=%ld]",
                         tablet->full_name().c_str(),
                         request.partition_id, request.transaction_id);
        return res;
    }

    if (res == OLAP_SUCCESS) {
        if (tablet_info_vec != NULL) {
            _get_tablet_infos(tablet_vars, tablet_info_vec);
        }
        LOG(INFO) << "process realtime push successfully. "
                  << "tablet=" << tablet->full_name()
                  << ", partition_id=" << request.partition_id
                  << ", transaction_id=" << request.transaction_id;
    } else {

        // error happens, clear
        LOG(WARNING) << "failed to process realtime push."
                     << " table=" << tablet->full_name()
                     << "transaction_id=" << request.transaction_id;
        for (TabletVars& tablet_var : tablet_vars) {
            if (tablet_var.tablet.get() == NULL) {
                continue;
            }

            StorageEngine::get_instance()->delete_transaction(
                request.partition_id, request.transaction_id,
                tablet_var.tablet->tablet_id(), tablet_var.tablet->schema_hash());

            // actually, olap_index may has been deleted in delete_transaction()
            for (RowsetSharedPtr rowset : tablet_var.added_rowsets) {
                StorageEngine::get_instance()->add_unused_rowset(rowset);
            }
        }
    }

    return res;
}

OLAPStatus PushHandler::_do_realtime_push(
        TabletSharedPtr tablet,
        const TPushReq& request,
        PushType push_type,
        vector<TabletVars>* tablet_vars,
        std::vector<TTabletInfo>* tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    AlterTabletType alter_tablet_type;
    // add transaction in engine, then check sc status
    // lock, prevent sc handler checking transaction concurrently
    tablet->obtain_push_lock();
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    res = TxnManager::instance()->add_txn(
        request.partition_id, request.transaction_id,
        tablet->tablet_id(), tablet->schema_hash(), load_id, NULL);

    // if transaction exists, exit
    if (res == OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {

        // if push finished, report success to fe
        if (tablet->has_pending_data(request.transaction_id)) {
            LOG(WARNING) << "pending data exists in tablet, which means push finished,"
                         << "return success. tablet=" << tablet->full_name()
                         << ", transaction_id=" << request.transaction_id;
            res = OLAP_SUCCESS;
        }
        tablet->release_push_lock();
        return res;
    }

    // only when fe sends schema_change true, should consider to push related tablet
    if (_request.is_schema_changing) {
        VLOG(3) << "push req specify schema changing is true. "
                << "tablet=" << tablet->full_name()
                << ", transaction_id=" << request.transaction_id;
        TTabletId related_tablet_id;
        TSchemaHash related_schema_hash;

        tablet->obtain_header_rdlock();
        bool is_schema_changing = tablet->get_schema_change_request(
            &related_tablet_id, &related_schema_hash, NULL, &alter_tablet_type);
        tablet->release_header_lock();

        if (is_schema_changing) {
            LOG(INFO) << "find schema_change status when realtime push. "
                      << "tablet=" << tablet->full_name() 
                      << ", related_tablet_id=" << related_tablet_id
                      << ", related_schema_hash=" << related_schema_hash
                      << ", transaction_id=" << request.transaction_id;
            TabletSharedPtr related_tablet = TabletManager::instance()->get_tablet(
                related_tablet_id, related_schema_hash);

            // if related tablet not exists, only push current tablet
            if (NULL == related_tablet.get()) {
                LOG(WARNING) << "can't find related tablet, only push current tablet. "
                             << "[tablet=%s related_tablet_id=" << tablet->full_name()
                             << ", related_schema_hash=" << related_schema_hash;

            // if current tablet is new tablet, only push current tablet
            } else if (tablet->creation_time() > related_tablet->creation_time()) {
                LOG(WARNING) << "current tablet is new, only push current tablet. "
                             << "tablet=" << tablet->full_name()
                             << " related_tablet=" << related_tablet->full_name();
            } else {
                PUniqueId load_id;
                load_id.set_hi(0);
                load_id.set_lo(0);
                res = TxnManager::instance()->add_txn(
                    request.partition_id, request.transaction_id,
                    related_tablet->tablet_id(), related_tablet->schema_hash(), load_id, NULL);

                // if related tablet's transaction exists, only push current tablet
                if (res == OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
                    LOG(WARNING) << "related tablet's transaction exists in engine, "
                                 << "only push current tablet. "
                                 << "related_tablet=" << related_tablet->full_name()
                                 << " transaction_id=" << request.transaction_id;
                } else {
                    tablet_vars->push_back(TabletVars());
                    TabletVars& new_item = tablet_vars->back();
                    new_item.tablet = related_tablet;
                }
            }
        }
    }
    tablet->release_push_lock();

    if (tablet_vars->size() == 1) {
        tablet_vars->resize(2);
    }

    // not call validate request here, because realtime load does not 
    // contain version info 

    // check delete condition if push for delete
    if (push_type == PUSH_FOR_DELETE) {

        for (TabletVars& tablet_var : *tablet_vars) {
            if (tablet_var.tablet.get() == NULL) {
                continue;
            }

            if (request.delete_conditions.size() == 0) {
                OLAP_LOG_WARNING("invalid parameters for store_cond. [condition_size=0]");
                res = OLAP_ERR_DELETE_INVALID_PARAMETERS;
                return res;
            }

            DeleteConditionHandler del_cond_handler;
            tablet_var.tablet->obtain_header_rdlock();
            for (const TCondition& cond : request.delete_conditions) {
                res = del_cond_handler.check_condition_valid(tablet_var.tablet->tablet_schema(), cond);
                if (res != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("fail to check delete condition. [table=%s res=%d]",
                                     tablet_var.tablet->full_name().c_str(), res);
                    tablet_var.tablet->release_header_lock();
                    return res;
                }
            }
            tablet_var.tablet->release_header_lock();
            LOG(INFO) << "success to check delete condition when realtime push. "
                      << "tablet=" << tablet_var.tablet->full_name()
                      << ", transaction_id=" << request.transaction_id;
        }
    }

    // write
    res = _convert(tablet_vars->at(0).tablet, tablet_vars->at(1).tablet,
                   &(tablet_vars->at(0).added_rowsets), &(tablet_vars->at(1).added_rowsets),
                   alter_tablet_type);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to convert tmp file when realtime push. res=" << res;
        return res;
    }

    // add pending data to tablet
    for (TabletVars& tablet_var : *tablet_vars) {
        if (tablet_var.tablet.get() == NULL) {
            continue;
        }

        for (RowsetSharedPtr rowset : tablet_var.added_rowsets) {
            RowsetMetaManager::save(tablet_var.tablet->data_dir()->get_meta(),
                rowset->rowset_id(), rowset->rowset_meta());
        }
    }
    return OLAP_SUCCESS;
}

void PushHandler::_get_tablet_infos(
        const vector<TabletVars>& tablet_vars,
        vector<TTabletInfo>* tablet_info_vec) {
    for (const TabletVars& tablet_var : tablet_vars) {
        if (tablet_var.tablet.get() == NULL) {
            continue;
        }

        TTabletInfo tablet_info;
        tablet_info.tablet_id = tablet_var.tablet->tablet_id();
        tablet_info.schema_hash = tablet_var.tablet->schema_hash();
        TabletManager::instance()->report_tablet_info(&tablet_info);
        tablet_info_vec->push_back(tablet_info);
    }
}

OLAPStatus PushHandler::_convert(
        TabletSharedPtr curr_tablet,
        TabletSharedPtr new_tablet,
        std::vector<RowsetSharedPtr>* cur_rowsets,
        std::vector<RowsetSharedPtr>* related_rowsets,
        AlterTabletType alter_tablet_type) {
    OLAPStatus res = OLAP_SUCCESS;
    RowCursor row;
    BinaryFile raw_file;
    IBinaryReader* reader = NULL;
    ColumnDataWriter* writer = NULL;
    RowsetBuilderSharedPtr rowset_builder(new AlphaRowsetBuilder());
    if (rowset_builder == nullptr) {
        LOG(WARNING) << "new rowset builder failed.";
        return OLAP_ERR_MALLOC_ERROR;
    }
    RowsetBuilderContext context;
    uint32_t  num_rows = 0;
    RowsetId rowset_id = 0;
    res = RowsetIdGenerator::instance()->get_next_id(curr_tablet->data_dir(), &rowset_id);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "generate rowset id failed, res:" << res;
        return OLAP_ERR_ROWSET_GENERATE_ID_FAILED;
    }
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    do {
        VLOG(3) << "start to convert delta file.";
        std::vector<FieldInfo> tablet_schema = curr_tablet->tablet_schema();

        tablet_schema = curr_tablet->tablet_schema();

        // 1. Init BinaryReader to read raw file if exist,
        //    in case of empty push and delete data, this will be skipped.
        if (_request.__isset.http_file_path) {
            // open raw file
            if (OLAP_SUCCESS != (res = raw_file.init(_request.http_file_path.c_str()))) {
                OLAP_LOG_WARNING("failed to read raw file. [res=%d file='%s']",
                                 res, _request.http_file_path.c_str());
                res = OLAP_ERR_INPUT_PARAMETER_ERROR;
                break;
            }

            // create BinaryReader
            bool need_decompress = false;
            if (_request.__isset.need_decompress && _request.need_decompress) {
                need_decompress = true;
            }
            if (NULL == (reader = IBinaryReader::create(need_decompress))) {
                OLAP_LOG_WARNING("fail to create reader. [tablet='%s' file='%s']",
                                 curr_tablet->full_name().c_str(),
                                 _request.http_file_path.c_str());
                res = OLAP_ERR_MALLOC_ERROR;
                break;
            }

            // init BinaryReader
            if (OLAP_SUCCESS != (res = reader->init(curr_tablet, &raw_file))) {
                OLAP_LOG_WARNING("fail to init reader. [res=%d tablet='%s' file='%s']",
                                 res,
                                 curr_tablet->full_name().c_str(),
                                 _request.http_file_path.c_str());
                res = OLAP_ERR_PUSH_INIT_ERROR;
                break;
            }
        }

        // 2. init RowsetBuilder of cur_tablet for current push
        VLOG(3) << "init RowsetBuilder.";

        if (_request.__isset.transaction_id) {
            RowsetBuilderContextBuilder context_builder;
            context_builder.set_rowset_id(rowset_id)
                .set_tablet_id(curr_tablet->tablet_id())
                .set_partition_id(_request.partition_id)
                .set_tablet_schema_hash(curr_tablet->schema_hash())
                .set_rowset_type(ALPHA_ROWSET)
                .set_rowset_path_prefix(curr_tablet->tablet_path())
                .set_tablet_schema(curr_tablet->tablet_schema())
                .set_num_key_fields(curr_tablet->num_key_fields())
                .set_num_short_key_fields(curr_tablet->num_short_key_fields())
                .set_num_rows_per_row_block(curr_tablet->num_rows_per_row_block())
                .set_compress_kind(curr_tablet->compress_kind())
                .set_bloom_filter_fpp(curr_tablet->bloom_filter_fpp())
                .set_rowset_state(PREPARING)
                .set_txn_id(_request.transaction_id)
                .set_load_id(load_id);
            context = context_builder.build();
        } else {
            RowsetBuilderContextBuilder context_builder;
            context_builder.set_rowset_id(rowset_id)
                .set_tablet_id(curr_tablet->tablet_id())
                .set_partition_id(_request.partition_id)
                .set_tablet_schema_hash(curr_tablet->schema_hash())
                .set_rowset_type(ALPHA_ROWSET)
                .set_rowset_path_prefix(curr_tablet->tablet_path())
                .set_tablet_schema(curr_tablet->tablet_schema())
                .set_num_key_fields(curr_tablet->num_key_fields())
                .set_num_short_key_fields(curr_tablet->num_short_key_fields())
                .set_num_rows_per_row_block(curr_tablet->num_rows_per_row_block())
                .set_compress_kind(curr_tablet->compress_kind())
                .set_bloom_filter_fpp(curr_tablet->bloom_filter_fpp())
                .set_rowset_state(VISIBLE)
                .set_version(Version(_request.version, _request.version))
                .set_version_hash(_request.version_hash);
            context = context_builder.build();
        }
        rowset_builder->init(context);

        // 3. New RowsetBuilder to write data into rowset
        VLOG(3) << "init rowset builder. tablet=" << curr_tablet->full_name()
                << ", block_row_size=" << curr_tablet->num_rows_per_row_block();

        // 4. Init RowCursor
        if (OLAP_SUCCESS != (res = row.init(curr_tablet->tablet_schema()))) {
            LOG(WARNING) << "fail to init rowcursor. res=" << res;
            break;
        }

        // 5. Read data from raw file and write into SegmentGroup of curr_tablet
        if (_request.__isset.http_file_path) {
            // Convert from raw to delta
            VLOG(3) << "start to convert row file to delta.";
            while (!reader->eof()) {
                if (OLAP_SUCCESS != (res = writer->attached_by(&row))) {
                    LOG(WARNING) << "fail to attach row to writer. "
                                 << " res=" << res << ", tablet=" << curr_tablet->full_name()
                                 << " read_rows=" << num_rows;
                    break;
                }

                res = reader->next(&row, writer->mem_pool());
                if (OLAP_SUCCESS != res) {
                    LOG(WARNING) << "read next row failed."
                                 << " res=" << res
                                 << " read_rows=" << num_rows;
                    break;
                } else {
                    rowset_builder->add_row(&row);
                    num_rows++;
                }
            }

            reader->finalize();

            if (false == reader->validate_checksum()) {
                LOG(WARNING) << "pushed delta file has wrong checksum.";
                res = OLAP_ERR_PUSH_BUILD_DELTA_ERROR;
                break;
            }
        }

        RowsetSharedPtr rowset = rowset_builder->build();

        if (rowset == nullptr) {
            LOG(WARNING) << "fail to build rowset";
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        _write_bytes += rowset->data_disk_size();
        _write_rows += rowset->num_rows();

        // 7. Convert data for schema change tables
        VLOG(10) << "load to related tables of schema_change if possible.";
        if (NULL != new_tablet.get()) {
            // create related tablet's pending data dir
            string dir_path = new_tablet->construct_pending_data_dir_path();
            if (!check_dir_existed(dir_path) && (res = create_dirs(dir_path)) != OLAP_SUCCESS) {
                if (!check_dir_existed(dir_path)) {
                    OLAP_LOG_WARNING("fail to create pending dir. [res=%d tablet=%s]",
                                     res, new_tablet->full_name().c_str());
                    break;
                }
            }
            // TODO(hkp): modify schema change
            //SchemaChangeHandler schema_change;
            //res = schema_change.schema_version_convert(
            //        curr_tablet,
            //        new_tablet,
            //        curr_olap_indices,
            //        new_olap_indices);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("failed to change schema version for delta."
                                 "[res=%d new_tablet='%s']",
                                 res, new_tablet->full_name().c_str());
            }

        }
    } while (0);

    SAFE_DELETE(reader);
    OLAP_LOG_NOTICE_PUSH("processed_rows", "%d", num_rows);
    VLOG(10) << "convert delta file end. res=" << res
             << ", tablet=" << curr_tablet->full_name();
    return res;
}

OLAPStatus BinaryFile::init(const char* path) {
    // open file
    if (OLAP_SUCCESS != open(path, "rb")) {
        OLAP_LOG_WARNING("fail to open file. [file='%s']", path);
        return OLAP_ERR_IO_ERROR;
    }

    // load header
    if (OLAP_SUCCESS != _header.unserialize(this)) {
        OLAP_LOG_WARNING("fail to read file header. [file='%s']", path);
        close();
        return OLAP_ERR_PUSH_INIT_ERROR;
    }

    return OLAP_SUCCESS;
}

IBinaryReader* IBinaryReader::create(bool need_decompress) {
    IBinaryReader* reader = NULL;
    if (need_decompress) {
        reader = new(std::nothrow) LzoBinaryReader();
    } else {
        reader = new(std::nothrow) BinaryReader();
    }
    return reader;
}

BinaryReader::BinaryReader()
    : IBinaryReader(),
      _row_buf(NULL),
      _row_buf_size(0) {
}

OLAPStatus BinaryReader::init(
        TabletSharedPtr tablet,
        BinaryFile* file) {
    OLAPStatus res = OLAP_SUCCESS;

    do {
        _file = file;
        _content_len = _file->file_length() - _file->header_size();
        _row_buf_size = tablet->get_row_size();

        if (NULL == (_row_buf = new(std::nothrow) char[_row_buf_size])) {
            OLAP_LOG_WARNING("fail to malloc one row buf. [size=%zu]", _row_buf_size);
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        if (-1 == _file->seek(_file->header_size(), SEEK_SET)) {
            OLAP_LOG_WARNING("skip header, seek fail.");
            res = OLAP_ERR_IO_ERROR;
            break;
        }

        _tablet = tablet;
        _ready = true;
    } while (0);

    if (res != OLAP_SUCCESS) {
        SAFE_DELETE_ARRAY(_row_buf);
    }
    return res;
}

OLAPStatus BinaryReader::finalize() {
    _ready = false;
    SAFE_DELETE_ARRAY(_row_buf);
    return OLAP_SUCCESS;
}

OLAPStatus BinaryReader::next(RowCursor* row, MemPool* mem_pool) {
    OLAPStatus res = OLAP_SUCCESS;

    if (!_ready || NULL == row) {
        // Here i assume _ready means all states were set up correctly
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    const vector<FieldInfo>& schema = _tablet->tablet_schema();
    size_t offset = 0;
    size_t field_size = 0;
    size_t num_null_bytes = (_tablet->num_null_fields() + 7) / 8;

    if (OLAP_SUCCESS != (res = _file->read(_row_buf + offset, num_null_bytes))) {
        OLAP_LOG_WARNING("read file for one row fail. [res=%d]", res);
        return res;
    }

    size_t p  = 0;
    for (size_t i = 0; i < schema.size(); ++i) {
        row->set_not_null(i);
        if (schema[i].is_allow_null) {
            bool is_null = false;
            is_null = (_row_buf[p/8] >> ((num_null_bytes * 8 - p - 1) % 8)) & 1;
            if (is_null) {
                row->set_null(i);
            }
            p++;
        }
    }
    offset += num_null_bytes;

    for (uint32_t i = 0; i < schema.size(); i++) {
        if (row->is_null(i)) {
            continue;
        }
        if (schema[i].type == OLAP_FIELD_TYPE_VARCHAR || schema[i].type == OLAP_FIELD_TYPE_HLL) {
            // Read varchar length buffer first
            if (OLAP_SUCCESS != (res = _file->read(_row_buf + offset,
                        sizeof(StringLengthType)))) {
                OLAP_LOG_WARNING("read file for one row fail. [res=%d]", res);
                return res;
            }

            // Get varchar field size
            field_size = *reinterpret_cast<StringLengthType*>(_row_buf + offset);
            offset += sizeof(StringLengthType);
            if (field_size > schema[i].length - sizeof(StringLengthType)) {
                OLAP_LOG_WARNING("invalid data length for VARCHAR! [max_len=%d real_len=%d]",
                                 schema[i].length - sizeof(StringLengthType),
                                 field_size);
                return OLAP_ERR_PUSH_INPUT_DATA_ERROR;
            }
        } else {
            field_size = schema[i].length;
        }

        // Read field content according to field size
        if (OLAP_SUCCESS != (res = _file->read(_row_buf + offset, field_size))) {
            OLAP_LOG_WARNING("read file for one row fail. [res=%d]", res);
            return res;
        }

        if (schema[i].type == OLAP_FIELD_TYPE_CHAR
                || schema[i].type == OLAP_FIELD_TYPE_VARCHAR
                || schema[i].type == OLAP_FIELD_TYPE_HLL) {
            Slice slice(_row_buf + offset, field_size);
            row->set_field_content(i, reinterpret_cast<char*>(&slice), mem_pool);
        } else {
            row->set_field_content(i, _row_buf + offset, mem_pool);
        }
        offset += field_size;
    }
    _curr += offset;

    // Calculate checksum for validate when push finished.
    _adler_checksum = olap_adler32(_adler_checksum, _row_buf, offset);
    return res;
}

LzoBinaryReader::LzoBinaryReader()
    : IBinaryReader(),
      _row_buf(NULL),
      _row_compressed_buf(NULL),
      _row_info_buf(NULL),
      _max_row_num(0),
      _max_row_buf_size(0),
      _max_compressed_buf_size(0),
      _row_num(0),
      _next_row_start(0) {
}

OLAPStatus LzoBinaryReader::init(
        TabletSharedPtr tablet,
        BinaryFile* file) {
    OLAPStatus res = OLAP_SUCCESS;

    do {
        _file = file;
        _content_len = _file->file_length() - _file->header_size();

        size_t row_info_buf_size = sizeof(RowNumType) + sizeof(CompressedSizeType);
        if (NULL == (_row_info_buf = new(std::nothrow) char[row_info_buf_size])) {
            OLAP_LOG_WARNING("fail to malloc rows info buf. [size=%zu]", row_info_buf_size);
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        if (-1 == _file->seek(_file->header_size(), SEEK_SET)) {
            OLAP_LOG_WARNING("skip header, seek fail.");
            res = OLAP_ERR_IO_ERROR;
            break;
        }

        _tablet = tablet;
        _ready = true;
    } while (0);

    if (res != OLAP_SUCCESS) {
        SAFE_DELETE_ARRAY(_row_info_buf);
    }
    return res;
}

OLAPStatus LzoBinaryReader::finalize() {
    _ready = false;
    SAFE_DELETE_ARRAY(_row_buf);
    SAFE_DELETE_ARRAY(_row_compressed_buf);
    SAFE_DELETE_ARRAY(_row_info_buf);
    return OLAP_SUCCESS;
}

OLAPStatus LzoBinaryReader::next(RowCursor* row, MemPool* mem_pool) {
    OLAPStatus res = OLAP_SUCCESS;

    if (!_ready || NULL == row) {
        // Here i assume _ready means all states were set up correctly
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (_row_num == 0) {
        // read next block
        if (OLAP_SUCCESS != (res = _next_block())) {
            return res;
        }
    }

    const vector<FieldInfo>& schema = _tablet->tablet_schema();
    size_t offset = 0;
    size_t field_size = 0;
    size_t num_null_bytes = (_tablet->num_null_fields() + 7) / 8;

    size_t p = 0;
    for (size_t i = 0; i < schema.size(); ++i) {
        row->set_not_null(i);
        if (schema[i].is_allow_null) {
            bool is_null = false;
            is_null = (_row_buf[_next_row_start + p/8] >> ((num_null_bytes * 8 - p - 1) % 8)) & 1;
            if (is_null) {
                row->set_null(i);
            }
            p++;
        }
    }
    offset += num_null_bytes;

    for (uint32_t i = 0; i < schema.size(); i++) {
        if (row->is_null(i)) {
            continue;
        }

        if (schema[i].type == OLAP_FIELD_TYPE_VARCHAR || schema[i].type == OLAP_FIELD_TYPE_HLL) {
            // Get varchar field size
            field_size = *reinterpret_cast<StringLengthType*>(_row_buf + _next_row_start + offset);
            offset += sizeof(StringLengthType);

            if (field_size > schema[i].length - sizeof(StringLengthType)) {
                OLAP_LOG_WARNING("invalid data length for VARCHAR! [max_len=%d real_len=%d]",
                                 schema[i].length - sizeof(StringLengthType),
                                 field_size);
                return OLAP_ERR_PUSH_INPUT_DATA_ERROR;
            }
        } else {
            field_size = schema[i].length;
        }

        if (schema[i].type == OLAP_FIELD_TYPE_CHAR
                || schema[i].type == OLAP_FIELD_TYPE_VARCHAR
                || schema[i].type == OLAP_FIELD_TYPE_HLL) {
            Slice slice(_row_buf + _next_row_start + offset, field_size);
            row->set_field_content(i, reinterpret_cast<char*>(&slice), mem_pool);
        } else {
            row->set_field_content(i, _row_buf + _next_row_start + offset, mem_pool);
        }

        offset += field_size;
    }

    // Calculate checksum for validate when push finished.
    _adler_checksum = olap_adler32(_adler_checksum, _row_buf + _next_row_start, offset);

    _next_row_start += offset;
    --_row_num;
    return res;
}

OLAPStatus LzoBinaryReader::_next_block() {
    OLAPStatus res = OLAP_SUCCESS;

    // Get row num and compressed data size
    size_t row_info_buf_size = sizeof(RowNumType) + sizeof(CompressedSizeType);
    if (OLAP_SUCCESS != (res = _file->read(_row_info_buf, row_info_buf_size))) {
        OLAP_LOG_WARNING("read rows info fail. [res=%d]", res);
        return res;
    }

    RowNumType* rows_num_ptr = reinterpret_cast<RowNumType*>(_row_info_buf);
    _row_num = *rows_num_ptr;
    CompressedSizeType* compressed_size_ptr = reinterpret_cast<CompressedSizeType*>(
        _row_info_buf + sizeof(RowNumType));
    CompressedSizeType compressed_size = *compressed_size_ptr;

    if (_row_num > _max_row_num) {
        // renew rows buf
        SAFE_DELETE_ARRAY(_row_buf);

        _max_row_num = _row_num;
        _max_row_buf_size = _max_row_num * _tablet->get_row_size();
        if (NULL == (_row_buf = new(std::nothrow) char[_max_row_buf_size])) {
            OLAP_LOG_WARNING("fail to malloc rows buf. [size=%zu]", _max_row_buf_size);
            res = OLAP_ERR_MALLOC_ERROR;
            return res;
        }
    }

    if (compressed_size > _max_compressed_buf_size) {
        // renew rows compressed buf
        SAFE_DELETE_ARRAY(_row_compressed_buf);

        _max_compressed_buf_size = compressed_size;
        if (NULL == (_row_compressed_buf = new(std::nothrow) char[_max_compressed_buf_size])) {
            OLAP_LOG_WARNING("fail to malloc rows compressed buf. [size=%zu]", _max_compressed_buf_size);
            res = OLAP_ERR_MALLOC_ERROR;
            return res;
        }
    }

    if (OLAP_SUCCESS != (res = _file->read(_row_compressed_buf, compressed_size))) {
        OLAP_LOG_WARNING("read compressed rows fail. [res=%d]", res);
        return res;
    }

    // python lzo use lzo1x to compress
    // and add 5 bytes header (\xf0 + 4 bytes(uncompress data size))
    size_t written_len = 0;
    size_t block_header_size = 5;
    if (OLAP_SUCCESS != (res = olap_decompress(_row_compressed_buf + block_header_size, 
                                               compressed_size - block_header_size,
                                               _row_buf, 
                                               _max_row_buf_size, 
                                               &written_len, 
                                               OLAP_COMP_TRANSPORT))) {
        OLAP_LOG_WARNING("olap decompress fail. [res=%d]", res);
        return res;
    }

    _curr += row_info_buf_size + compressed_size;
    _next_row_start = 0;
    return res;
}

string PushHandler::_debug_version_list(const Versions& versions) const {
    std::ostringstream txt;
    txt << "Versions: ";

    for (Versions::const_iterator it = versions.begin(); it != versions.end(); ++it) {
        txt << "[" << it->first << "~" << it->second << "],";
    }

    return txt.str();
}

}  // namespace doris
