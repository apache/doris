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
#include "olap/rowset/alpha_rowset_writer.h"
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
//       I.  current tablet is old table (cur.creation_time < related.creation_time):
//           push for current table and than convert data for related tables
//       II. current table is new table:
//           this usually means schema change is over,
//           clear schema change info in both current tablet and related tablets,
//           finally we will only push for current tablets. this is very useful in rollup action.
OLAPStatus PushHandler::process_streaming_ingestion(
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
    res = _do_streaming_ingestion(tablet, request, push_type, &tablet_vars, tablet_info_vec);

    if (res == OLAP_SUCCESS) {
        if (tablet_info_vec != NULL) {
            _get_tablet_infos(tablet_vars, tablet_info_vec);
        }
        LOG(INFO) << "process realtime push successfully. "
                  << "tablet=" << tablet->full_name()
                  << ", partition_id=" << request.partition_id
                  << ", transaction_id=" << request.transaction_id;
    }

    return res;
}

OLAPStatus PushHandler::_do_streaming_ingestion(
        TabletSharedPtr tablet,
        const TPushReq& request,
        PushType push_type,
        vector<TabletVars>* tablet_vars,
        std::vector<TTabletInfo>* tablet_info_vec) {
    // add transaction in engine, then check sc status
    // lock, prevent sc handler checking transaction concurrently
    tablet->obtain_push_lock();
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    OLAPStatus res = StorageEngine::instance()->txn_manager()->prepare_txn(
            request.partition_id, request.transaction_id,
            tablet->tablet_id(), tablet->schema_hash(), load_id);

    // prepare txn will be always successful
    // if current tablet is under schema change, origin tablet is successful and
    // new tablet is not sucessful, it maybe a fatal error because new tablet has not load successfully

    // only when fe sends schema_change true, should consider to push related tablet
    if (_request.is_schema_changing) {
        VLOG(3) << "push req specify schema changing is true. "
                << "tablet=" << tablet->full_name()
                << ", transaction_id=" << request.transaction_id;

        tablet->obtain_header_rdlock();
        bool has_alter_task = tablet->has_alter_task();
        const AlterTabletTask& alter_task = tablet->alter_task();
        AlterTabletState alter_state = alter_task.alter_state();
        TTabletId related_tablet_id = alter_task.related_tablet_id();
        TSchemaHash related_schema_hash = alter_task.related_schema_hash();;
        tablet->release_header_lock();

        if (has_alter_task && alter_state != ALTER_FAILED) {
            LOG(INFO) << "find schema_change status when realtime push. "
                      << "tablet=" << tablet->full_name()
                      << ", related_tablet_id=" << related_tablet_id
                      << ", related_schema_hash=" << related_schema_hash
                      << ", transaction_id=" << request.transaction_id;
            TabletSharedPtr related_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
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
                res = StorageEngine::instance()->txn_manager()->prepare_txn(
                    request.partition_id, request.transaction_id,
                    related_tablet->tablet_id(), related_tablet->schema_hash(), load_id);
                // prepare txn will always be successful
                tablet_vars->push_back(TabletVars());
                TabletVars& new_item = tablet_vars->back();
                new_item.tablet = related_tablet;
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
    std::queue<DeletePredicatePB> del_preds;
    if (push_type == PUSH_FOR_DELETE) {
        for (TabletVars& tablet_var : *tablet_vars) {
            if (tablet_var.tablet == nullptr) {
                continue;
            }

            DeletePredicatePB del_pred;
            DeleteConditionHandler del_cond_handler;
            tablet_var.tablet->obtain_header_rdlock();
            res = del_cond_handler.generate_delete_predicate(tablet_var.tablet->tablet_schema(),
                                                             request.delete_conditions, &del_pred);
            del_preds.push(del_pred);
            tablet_var.tablet->release_header_lock();
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to generate delete condition. res=" << res
                             << ", tablet=" << tablet_var.tablet->full_name();
                return res;
            }
        }
    }

    // write
    res = _convert(tablet_vars->at(0).tablet, tablet_vars->at(1).tablet,
                   &(tablet_vars->at(0).rowset_to_add), &(tablet_vars->at(1).rowset_to_add));
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to convert tmp file when realtime push. res=" << res
                     << ", failed to process realtime push."
                     << ", table=" << tablet->full_name()
                     << ", transaction_id=" << request.transaction_id;
        for (TabletVars& tablet_var : *tablet_vars) {
            if (tablet_var.tablet == nullptr) {
                continue;
            }

            OLAPStatus rollback_status = StorageEngine::instance()->txn_manager()->rollback_txn(
                request.partition_id, request.transaction_id,
                tablet_var.tablet->tablet_id(), tablet_var.tablet->schema_hash());
            // has to check rollback status to ensure not delete a committed rowset
            if (rollback_status == OLAP_SUCCESS) {
                // actually, olap_index may has been deleted in delete_transaction()
                StorageEngine::instance()->add_unused_rowset(tablet_var.rowset_to_add);
            }
        }
        return res;
    }

    // add pending data to tablet
    for (TabletVars& tablet_var : *tablet_vars) {
        if (tablet_var.tablet == nullptr) {
            continue;
        }

        tablet_var.rowset_to_add->rowset_meta()->set_delete_predicate(del_preds.front());
        del_preds.pop();
        OLAPStatus commit_status = StorageEngine::instance()->txn_manager()->commit_txn(tablet_var.tablet->data_dir()->get_meta(),
                                                                      request.partition_id, request.transaction_id,
                                                                      tablet_var.tablet->tablet_id(),
                                                                      tablet_var.tablet->schema_hash(),
                                                                      load_id, tablet_var.rowset_to_add, false);
        if (commit_status != OLAP_SUCCESS && commit_status != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
            res = commit_status;
        }
        del_preds.pop();
    }
    return res;
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
        StorageEngine::instance()->tablet_manager()->report_tablet_info(&tablet_info);
        tablet_info_vec->push_back(tablet_info);
    }
}

OLAPStatus PushHandler::_convert(
        TabletSharedPtr curr_tablet,
        TabletSharedPtr new_tablet,
        RowsetSharedPtr* cur_rowset,
        RowsetSharedPtr* new_rowset) {
    OLAPStatus res = OLAP_SUCCESS;
    RowCursor row;
    BinaryFile raw_file;
    IBinaryReader* reader = NULL;
    RowsetWriterSharedPtr rowset_writer(new AlphaRowsetWriter());
    if (rowset_writer == nullptr) {
        LOG(WARNING) << "new rowset writer failed.";
        return OLAP_ERR_MALLOC_ERROR;
    }
    RowsetWriterContext context;
    uint32_t  num_rows = 0;
    RowsetId rowset_id = 0;
    res = curr_tablet->next_rowset_id(&rowset_id);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "generate rowset id failed, res:" << res;
        return OLAP_ERR_ROWSET_GENERATE_ID_FAILED;
    }
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    do {
        VLOG(3) << "start to convert delta file.";

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
        RowsetWriterContextBuilder context_builder;
        context_builder.set_rowset_id(rowset_id)
            .set_tablet_id(curr_tablet->tablet_id())
            .set_partition_id(_request.partition_id)
            .set_tablet_schema_hash(curr_tablet->schema_hash())
            .set_rowset_type(ALPHA_ROWSET)
            .set_rowset_path_prefix(curr_tablet->tablet_path())
            .set_tablet_schema(&(curr_tablet->tablet_schema()))
            .set_rowset_state(PREPARED)
            .set_data_dir(curr_tablet->data_dir())
            .set_txn_id(_request.transaction_id)
            .set_load_id(load_id);
        context = context_builder.build();
        rowset_writer->init(context);

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
                res = reader->next(&row, rowset_writer->mem_pool());
                if (OLAP_SUCCESS != res) {
                    LOG(WARNING) << "read next row failed."
                                 << " res=" << res
                                 << " read_rows=" << num_rows;
                    break;
                } else {
                    if (OLAP_SUCCESS != (res = rowset_writer->add_row(&row))) {
                        LOG(WARNING) << "fail to attach row to rowset_writer. "
                                << " res=" << res << ", tablet=" << curr_tablet->full_name()
                                 << " read_rows=" << num_rows;
                        break;
                    }
                    num_rows++;
                }
            }

            reader->finalize();

            if (!reader->validate_checksum()) {
                LOG(WARNING) << "pushed delta file has wrong checksum.";
                res = OLAP_ERR_PUSH_BUILD_DELTA_ERROR;
                break;
            }
        }

        if (rowset_writer->flush() != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to finalize writer.";
            break;
        }
        *cur_rowset = rowset_writer->build();

        if (*cur_rowset == nullptr) {
            LOG(WARNING) << "fail to build rowset";
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        _write_bytes += (*cur_rowset)->data_disk_size();
        _write_rows += (*cur_rowset)->num_rows();

        // 7. Convert data for schema change tables
        VLOG(10) << "load to related tables of schema_change if possible.";
        if (new_tablet != nullptr) {
            SchemaChangeHandler schema_change;
            res = schema_change.schema_version_convert(curr_tablet, new_tablet,
                                                       cur_rowset, new_rowset);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to change schema version for delta."
                             << "[res=" << res
                             <<" new_tablet='" << new_tablet->full_name() << "']";
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
        _row_buf_size = tablet->row_size();

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

    const TabletSchema& schema = _tablet->tablet_schema();
    size_t offset = 0;
    size_t field_size = 0;
    size_t num_null_bytes = (_tablet->num_null_columns() + 7) / 8;

    if (OLAP_SUCCESS != (res = _file->read(_row_buf + offset, num_null_bytes))) {
        OLAP_LOG_WARNING("read file for one row fail. [res=%d]", res);
        return res;
    }

    size_t p  = 0;
    for (size_t i = 0; i < schema.num_columns(); ++i) {
        row->set_not_null(i);
        if (schema.column(i).is_nullable()) {
            bool is_null = false;
            is_null = (_row_buf[p/8] >> ((num_null_bytes * 8 - p - 1) % 8)) & 1;
            if (is_null) {
                row->set_null(i);
            }
            p++;
        }
    }
    offset += num_null_bytes;

    for (uint32_t i = 0; i < schema.num_columns(); i++) {
        const TabletColumn& column = schema.column(i);
        if (row->is_null(i)) {
            continue;
        }
        if (column.type() == OLAP_FIELD_TYPE_VARCHAR || column.type() == OLAP_FIELD_TYPE_HLL) {
            // Read varchar length buffer first
            if (OLAP_SUCCESS != (res = _file->read(_row_buf + offset,
                        sizeof(StringLengthType)))) {
                OLAP_LOG_WARNING("read file for one row fail. [res=%d]", res);
                return res;
            }

            // Get varchar field size
            field_size = *reinterpret_cast<StringLengthType*>(_row_buf + offset);
            offset += sizeof(StringLengthType);
            if (field_size > column.length() - sizeof(StringLengthType)) {
                LOG(WARNING) << "invalid data length for VARCHAR! "
                             << "max_len=" << column.length() - sizeof(StringLengthType)
                             << ", real_len=" << field_size;
                return OLAP_ERR_PUSH_INPUT_DATA_ERROR;
            }
        } else {
            field_size = column.length();
        }

        // Read field content according to field size
        if (OLAP_SUCCESS != (res = _file->read(_row_buf + offset, field_size))) {
            OLAP_LOG_WARNING("read file for one row fail. [res=%d]", res);
            return res;
        }

        if (column.type() == OLAP_FIELD_TYPE_CHAR
                || column.type() == OLAP_FIELD_TYPE_VARCHAR
                || column.type() == OLAP_FIELD_TYPE_HLL) {
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

    const TabletSchema& schema = _tablet->tablet_schema();
    size_t offset = 0;
    size_t field_size = 0;
    size_t num_null_bytes = (_tablet->num_null_columns() + 7) / 8;

    size_t p = 0;
    for (size_t i = 0; i < schema.num_columns(); ++i) {
        row->set_not_null(i);
        if (schema.column(i).is_nullable()) {
            bool is_null = false;
            is_null = (_row_buf[_next_row_start + p/8] >> ((num_null_bytes * 8 - p - 1) % 8)) & 1;
            if (is_null) {
                row->set_null(i);
            }
            p++;
        }
    }
    offset += num_null_bytes;

    for (uint32_t i = 0; i < schema.num_columns(); i++) {
        if (row->is_null(i)) {
            continue;
        }

        const TabletColumn& column = schema.column(i);
        if (column.type() == OLAP_FIELD_TYPE_VARCHAR || column.type() == OLAP_FIELD_TYPE_HLL) {
            // Get varchar field size
            field_size = *reinterpret_cast<StringLengthType*>(_row_buf + _next_row_start + offset);
            offset += sizeof(StringLengthType);

            if (field_size > column.length() - sizeof(StringLengthType)) {
                LOG(WARNING) << "invalid data length for VARCHAR! "
                             << "max_len=" << column.length() - sizeof(StringLengthType)
                             << ", real_len=" << field_size;
                return OLAP_ERR_PUSH_INPUT_DATA_ERROR;
            }
        } else {
            field_size = column.length();
        }

        if (column.type() == OLAP_FIELD_TYPE_CHAR
                || column.type() == OLAP_FIELD_TYPE_VARCHAR
                || column.type() == OLAP_FIELD_TYPE_HLL) {
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
        _max_row_buf_size = _max_row_num * _tablet->row_size();
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
