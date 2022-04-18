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
#include <filesystem>
#include <iostream>
#include <sstream>

#include "common/status.h"
#include "exec/parquet_scanner.h"
#include "olap/row.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "runtime/exec_env.h"

using std::list;
using std::map;
using std::string;
using std::vector;

namespace doris {

// Process push command, the main logical is as follows:
//    a. related tablets not exist:
//        current table isn't in schemachange state, only push for current
//        tablet
//    b. related tablets exist
//       I.  current tablet is old table (cur.creation_time <
//       related.creation_time):
//           push for current table and than convert data for related tables
//       II. current table is new table:
//           this usually means schema change is over,
//           clear schema change info in both current tablet and related
//           tablets, finally we will only push for current tablets. this is
//           very useful in rollup action.
Status PushHandler::process_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request,
                                                PushType push_type,
                                                std::vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to realtime push. tablet=" << tablet->full_name()
              << ", transaction_id=" << request.transaction_id;

    Status res = Status::OK();
    _request = request;
    std::vector<TabletVars> tablet_vars(1);
    tablet_vars[0].tablet = tablet;
    res = _do_streaming_ingestion(tablet, request, push_type, &tablet_vars, tablet_info_vec);

    if (res.ok()) {
        if (tablet_info_vec != nullptr) {
            _get_tablet_infos(tablet_vars, tablet_info_vec);
        }
        LOG(INFO) << "process realtime push successfully. "
                  << "tablet=" << tablet->full_name() << ", partition_id=" << request.partition_id
                  << ", transaction_id=" << request.transaction_id;
    }

    return res;
}

Status PushHandler::_do_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request,
                                            PushType push_type,
                                            std::vector<TabletVars>* tablet_vars,
                                            std::vector<TTabletInfo>* tablet_info_vec) {
    // add transaction in engine, then check sc status
    // lock, prevent sc handler checking transaction concurrently
    if (tablet == nullptr) {
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    std::shared_lock base_migration_rlock(tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_RWLOCK_ERROR);
    }
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    {
        std::lock_guard<std::mutex> push_lock(tablet->get_push_lock());
        RETURN_NOT_OK(StorageEngine::instance()->txn_manager()->prepare_txn(
                request.partition_id, tablet, request.transaction_id, load_id));
    }

    if (tablet_vars->size() == 1) {
        tablet_vars->resize(2);
    }

    // not call validate request here, because realtime load does not
    // contain version info

    Status res;
    // check delete condition if push for delete
    std::queue<DeletePredicatePB> del_preds;
    if (push_type == PUSH_FOR_DELETE) {
        for (TabletVars& tablet_var : *tablet_vars) {
            if (tablet_var.tablet == nullptr) {
                continue;
            }

            DeletePredicatePB del_pred;
            DeleteConditionHandler del_cond_handler;
            {
                std::shared_lock rdlock(tablet_var.tablet->get_header_lock());
                res = del_cond_handler.generate_delete_predicate(
                        tablet_var.tablet->tablet_schema(), request.delete_conditions, &del_pred);
                del_preds.push(del_pred);
            }
            if (!res.ok()) {
                LOG(WARNING) << "fail to generate delete condition. res=" << res
                             << ", tablet=" << tablet_var.tablet->full_name();
                return res;
            }
        }
    }

    // check if version number exceed limit
    if (tablet_vars->at(0).tablet->version_count() > config::max_tablet_version_num) {
        LOG(WARNING) << "failed to push data. version count: "
                     << tablet_vars->at(0).tablet->version_count()
                     << ", exceed limit: " << config::max_tablet_version_num
                     << ". tablet: " << tablet_vars->at(0).tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_TOO_MANY_VERSION);
    }

    // write
    if (push_type == PUSH_NORMAL_V2) {
        res = _convert_v2(tablet_vars->at(0).tablet, tablet_vars->at(1).tablet,
                          &(tablet_vars->at(0).rowset_to_add), &(tablet_vars->at(1).rowset_to_add));

    } else {
        res = _convert(tablet_vars->at(0).tablet, tablet_vars->at(1).tablet,
                       &(tablet_vars->at(0).rowset_to_add), &(tablet_vars->at(1).rowset_to_add));
    }
    if (!res.ok()) {
        LOG(WARNING) << "fail to convert tmp file when realtime push. res=" << res
                     << ", failed to process realtime push."
                     << ", tablet=" << tablet->full_name()
                     << ", transaction_id=" << request.transaction_id;
        for (TabletVars& tablet_var : *tablet_vars) {
            if (tablet_var.tablet == nullptr) {
                continue;
            }

            Status rollback_status = StorageEngine::instance()->txn_manager()->rollback_txn(
                    request.partition_id, tablet_var.tablet, request.transaction_id);
            // has to check rollback status to ensure not delete a committed rowset
            if (rollback_status.ok()) {
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

        if (push_type == PUSH_FOR_DELETE) {
            tablet_var.rowset_to_add->rowset_meta()->set_delete_predicate(del_preds.front());
            del_preds.pop();
        }
        Status commit_status = StorageEngine::instance()->txn_manager()->commit_txn(
                request.partition_id, tablet_var.tablet, request.transaction_id, load_id,
                tablet_var.rowset_to_add, false);
        if (commit_status != Status::OK() &&
            commit_status != Status::OLAPInternalError(OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST)) {
            res = commit_status;
        }
    }
    return res;
}

void PushHandler::_get_tablet_infos(const std::vector<TabletVars>& tablet_vars,
                                    std::vector<TTabletInfo>* tablet_info_vec) {
    for (const TabletVars& tablet_var : tablet_vars) {
        if (tablet_var.tablet.get() == nullptr) {
            continue;
        }

        TTabletInfo tablet_info;
        tablet_info.tablet_id = tablet_var.tablet->tablet_id();
        tablet_info.schema_hash = tablet_var.tablet->schema_hash();
        StorageEngine::instance()->tablet_manager()->report_tablet_info(&tablet_info);
        tablet_info_vec->push_back(tablet_info);
    }
}

Status PushHandler::_convert_v2(TabletSharedPtr cur_tablet, TabletSharedPtr new_tablet,
                                RowsetSharedPtr* cur_rowset, RowsetSharedPtr* new_rowset) {
    Status res = Status::OK();
    uint32_t num_rows = 0;
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    do {
        VLOG_NOTICE << "start to convert delta file.";

        // 1. init RowsetBuilder of cur_tablet for current push
        VLOG_NOTICE << "init rowset builder. tablet=" << cur_tablet->full_name()
                    << ", block_row_size=" << cur_tablet->num_rows_per_row_block();
        RowsetWriterContext context;
        context.rowset_id = StorageEngine::instance()->next_rowset_id();
        context.tablet_uid = cur_tablet->tablet_uid();
        context.tablet_id = cur_tablet->tablet_id();
        context.partition_id = _request.partition_id;
        context.tablet_schema_hash = cur_tablet->schema_hash();
        context.data_dir = cur_tablet->data_dir();
        context.rowset_type = StorageEngine::instance()->default_rowset_type();
        if (cur_tablet->tablet_meta()->preferred_rowset_type() == BETA_ROWSET) {
            context.rowset_type = BETA_ROWSET;
        }
        context.path_desc = cur_tablet->tablet_path_desc();
        context.tablet_schema = &(cur_tablet->tablet_schema());
        context.rowset_state = PREPARED;
        context.txn_id = _request.transaction_id;
        context.load_id = load_id;
        // although the spark load output files are fully sorted,
        // but it depends on thirparty implementation, so we conservatively
        // set this value to OVERLAP_UNKNOWN
        context.segments_overlap = OVERLAP_UNKNOWN;

        std::unique_ptr<RowsetWriter> rowset_writer;
        res = RowsetFactory::create_rowset_writer(context, &rowset_writer);
        if (!res.ok()) {
            LOG(WARNING) << "failed to init rowset writer, tablet=" << cur_tablet->full_name()
                         << ", txn_id=" << _request.transaction_id << ", res=" << res;
            break;
        }

        // 2. Init PushBrokerReader to read broker file if exist,
        //    in case of empty push this will be skipped.
        std::string path = _request.broker_scan_range.ranges[0].path;
        LOG(INFO) << "tablet=" << cur_tablet->full_name() << ", file path=" << path
                  << ", file size=" << _request.broker_scan_range.ranges[0].file_size;

        if (!path.empty()) {
            std::unique_ptr<PushBrokerReader> reader(new (std::nothrow) PushBrokerReader());
            if (reader == nullptr) {
                LOG(WARNING) << "fail to create reader. tablet=" << cur_tablet->full_name();
                res = Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
                break;
            }

            // init schema
            std::unique_ptr<Schema> schema(new (std::nothrow) Schema(cur_tablet->tablet_schema()));
            if (schema == nullptr) {
                LOG(WARNING) << "fail to create schema. tablet=" << cur_tablet->full_name();
                res = Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
                break;
            }

            // init Reader
            if (!(res = reader->init(schema.get(), _request.broker_scan_range,
                                     _request.desc_tbl))) {
                LOG(WARNING) << "fail to init reader. res=" << res
                             << ", tablet=" << cur_tablet->full_name();
                res = Status::OLAPInternalError(OLAP_ERR_PUSH_INIT_ERROR);
                break;
            }

            // 3. Init Row
            uint8_t* tuple_buf = reader->mem_pool()->allocate(schema->schema_size());
            ContiguousRow row(schema.get(), tuple_buf);

            // 4. Read data from broker and write into SegmentGroup of cur_tablet
            // Convert from raw to delta
            VLOG_NOTICE << "start to convert etl file to delta.";
            while (!reader->eof()) {
                res = reader->next(&row);
                if (!res.ok()) {
                    LOG(WARNING) << "read next row failed."
                                 << " res=" << res << " read_rows=" << num_rows;
                    break;
                } else {
                    if (reader->eof()) {
                        break;
                    }
                    //if read row but fill tuple fails,
                    if (!reader->is_fill_tuple()) {
                        break;
                    }
                    if (!(res = rowset_writer->add_row(row))) {
                        LOG(WARNING) << "fail to attach row to rowset_writer. "
                                     << "res=" << res << ", tablet=" << cur_tablet->full_name()
                                     << ", read_rows=" << num_rows;
                        break;
                    }
                    num_rows++;
                }
            }

            reader->print_profile();
            reader->close();
        }

        if (rowset_writer->flush() != Status::OK()) {
            LOG(WARNING) << "failed to finalize writer";
            break;
        }
        *cur_rowset = rowset_writer->build();
        if (*cur_rowset == nullptr) {
            LOG(WARNING) << "fail to build rowset";
            res = Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
            break;
        }

        _write_bytes += (*cur_rowset)->data_disk_size();
        _write_rows += (*cur_rowset)->num_rows();

        // 5. Convert data for schema change tables
        VLOG_TRACE << "load to related tables of schema_change if possible.";
        if (new_tablet != nullptr) {
            auto schema_change_handler = SchemaChangeHandler::instance();
            res = schema_change_handler->schema_version_convert(cur_tablet, new_tablet, cur_rowset,
                                                                new_rowset);
            if (!res.ok()) {
                LOG(WARNING) << "failed to change schema version for delta."
                             << "[res=" << res << " new_tablet='" << new_tablet->full_name()
                             << "']";
            }
        }
    } while (0);

    VLOG_TRACE << "convert delta file end. res=" << res << ", tablet=" << cur_tablet->full_name()
               << ", processed_rows" << num_rows;
    return res;
}

Status PushHandler::_convert(TabletSharedPtr cur_tablet, TabletSharedPtr new_tablet,
                             RowsetSharedPtr* cur_rowset, RowsetSharedPtr* new_rowset) {
    Status res = Status::OK();
    RowCursor row;
    BinaryFile raw_file;
    IBinaryReader* reader = nullptr;
    uint32_t num_rows = 0;
    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);

    do {
        VLOG_NOTICE << "start to convert delta file.";

        // 1. Init BinaryReader to read raw file if exist,
        //    in case of empty push and delete data, this will be skipped.
        if (_request.__isset.http_file_path) {
            // open raw file
            if (!(res = raw_file.init(_request.http_file_path.c_str()))) {
                LOG(WARNING) << "failed to read raw file. res=" << res
                             << ", file=" << _request.http_file_path;
                res = Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
                break;
            }

            // create BinaryReader
            bool need_decompress = false;
            if (_request.__isset.need_decompress && _request.need_decompress) {
                need_decompress = true;
            }

#ifndef DORIS_WITH_LZO
            if (need_decompress) {
                // if lzo is disabled, compressed data is not allowed here
                res = Status::OLAPInternalError(OLAP_ERR_VERSION_ALREADY_MERGED);
                break;
            }
#endif

            reader = IBinaryReader::create(need_decompress);
            if (reader == nullptr) {
                LOG(WARNING) << "fail to create reader. tablet=" << cur_tablet->full_name()
                             << ", file=" << _request.http_file_path;
                res = Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
                break;
            }

            // init BinaryReader
            if (!(res = reader->init(cur_tablet, &raw_file))) {
                LOG(WARNING) << "fail to init reader. res=" << res
                             << ", tablet=" << cur_tablet->full_name()
                             << ", file=" << _request.http_file_path;
                res = Status::OLAPInternalError(OLAP_ERR_PUSH_INIT_ERROR);
                break;
            }
        }

        // 2. init RowsetBuilder of cur_tablet for current push
        VLOG_NOTICE << "init RowsetBuilder.";
        RowsetWriterContext context;
        context.rowset_id = StorageEngine::instance()->next_rowset_id();
        context.tablet_uid = cur_tablet->tablet_uid();
        context.tablet_id = cur_tablet->tablet_id();
        context.partition_id = _request.partition_id;
        context.tablet_schema_hash = cur_tablet->schema_hash();
        context.data_dir = cur_tablet->data_dir();
        context.rowset_type = StorageEngine::instance()->default_rowset_type();
        if (cur_tablet->tablet_meta()->preferred_rowset_type() == BETA_ROWSET) {
            context.rowset_type = BETA_ROWSET;
        }
        context.path_desc = cur_tablet->tablet_path_desc();
        context.tablet_schema = &(cur_tablet->tablet_schema());
        context.rowset_state = PREPARED;
        context.txn_id = _request.transaction_id;
        context.load_id = load_id;
        // although the hadoop load output files are fully sorted,
        // but it depends on thirparty implementation, so we conservatively
        // set this value to OVERLAP_UNKNOWN
        context.segments_overlap = OVERLAP_UNKNOWN;

        std::unique_ptr<RowsetWriter> rowset_writer;
        res = RowsetFactory::create_rowset_writer(context, &rowset_writer);
        if (!res.ok()) {
            LOG(WARNING) << "failed to init rowset writer, tablet=" << cur_tablet->full_name()
                         << ", txn_id=" << _request.transaction_id << ", res=" << res;
            break;
        }

        // 3. New RowsetBuilder to write data into rowset
        VLOG_NOTICE << "init rowset builder. tablet=" << cur_tablet->full_name()
                    << ", block_row_size=" << cur_tablet->num_rows_per_row_block();

        // 4. Init RowCursor
        if (!(res = row.init(cur_tablet->tablet_schema()))) {
            LOG(WARNING) << "fail to init rowcursor. res=" << res;
            break;
        }

        // 5. Read data from raw file and write into SegmentGroup of cur_tablet
        if (_request.__isset.http_file_path) {
            // Convert from raw to delta
            VLOG_NOTICE << "start to convert row file to delta.";
            while (!reader->eof()) {
                res = reader->next(&row);
                if (!res.ok()) {
                    LOG(WARNING) << "read next row failed."
                                 << " res=" << res << " read_rows=" << num_rows;
                    break;
                } else {
                    if (!(res = rowset_writer->add_row(row))) {
                        LOG(WARNING) << "fail to attach row to rowset_writer. "
                                     << " res=" << res << ", tablet=" << cur_tablet->full_name()
                                     << " read_rows=" << num_rows;
                        break;
                    }
                    num_rows++;
                }
            }

            reader->finalize();

            if (!reader->validate_checksum()) {
                LOG(WARNING) << "pushed delta file has wrong checksum.";
                res = Status::OLAPInternalError(OLAP_ERR_PUSH_BUILD_DELTA_ERROR);
                break;
            }
        }

        if (rowset_writer->flush() != Status::OK()) {
            LOG(WARNING) << "failed to finalize writer.";
            break;
        }
        *cur_rowset = rowset_writer->build();

        if (*cur_rowset == nullptr) {
            LOG(WARNING) << "fail to build rowset";
            res = Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
            break;
        }

        _write_bytes += (*cur_rowset)->data_disk_size();
        _write_rows += (*cur_rowset)->num_rows();

        // 7. Convert data for schema change tables
        VLOG_TRACE << "load to related tables of schema_change if possible.";
        if (new_tablet != nullptr) {
            auto schema_change_handler = SchemaChangeHandler::instance();
            res = schema_change_handler->schema_version_convert(cur_tablet, new_tablet, cur_rowset,
                                                                new_rowset);
            if (!res.ok()) {
                LOG(WARNING) << "failed to change schema version for delta."
                             << "[res=" << res << " new_tablet='" << new_tablet->full_name()
                             << "']";
            }
        }
    } while (0);

    SAFE_DELETE(reader);
    VLOG_TRACE << "convert delta file end. res=" << res << ", tablet=" << cur_tablet->full_name()
               << ", processed_rows" << num_rows;
    return res;
}

Status BinaryFile::init(const char* path) {
    // open file
    if (!open(path, "rb")) {
        LOG(WARNING) << "fail to open file. file=" << path;
        return Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
    }

    // load header
    if (!_header.unserialize(this)) {
        LOG(WARNING) << "fail to read file header. file=" << path;
        close();
        return Status::OLAPInternalError(OLAP_ERR_PUSH_INIT_ERROR);
    }

    return Status::OK();
}

IBinaryReader* IBinaryReader::create(bool need_decompress) {
    IBinaryReader* reader = nullptr;
    if (need_decompress) {
#ifdef DORIS_WITH_LZO
        reader = new (std::nothrow) LzoBinaryReader();
#endif
    } else {
        reader = new (std::nothrow) BinaryReader();
    }
    return reader;
}

BinaryReader::BinaryReader() : IBinaryReader(), _row_buf(nullptr), _row_buf_size(0) {}

Status BinaryReader::init(TabletSharedPtr tablet, BinaryFile* file) {
    Status res = Status::OK();

    do {
        _file = file;
        _content_len = _file->file_length() - _file->header_size();
        _row_buf_size = tablet->row_size();

        _row_buf = new (std::nothrow) char[_row_buf_size];
        if (_row_buf == nullptr) {
            LOG(WARNING) << "fail to malloc one row buf. size=" << _row_buf_size;
            res = Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
            break;
        }

        if (-1 == _file->seek(_file->header_size(), SEEK_SET)) {
            LOG(WARNING) << "skip header, seek fail.";
            res = Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
            break;
        }

        _tablet = tablet;
        _ready = true;
    } while (0);

    if (!res.ok()) {
        SAFE_DELETE_ARRAY(_row_buf);
    }
    return res;
}

Status BinaryReader::finalize() {
    _ready = false;
    SAFE_DELETE_ARRAY(_row_buf);
    return Status::OK();
}

Status BinaryReader::next(RowCursor* row) {
    Status res = Status::OK();

    if (!_ready || nullptr == row) {
        // Here i assume _ready means all states were set up correctly
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    const TabletSchema& schema = _tablet->tablet_schema();
    size_t offset = 0;
    size_t field_size = 0;
    size_t num_null_bytes = (_tablet->num_null_columns() + 7) / 8;

    if (!(res = _file->read(_row_buf + offset, num_null_bytes))) {
        LOG(WARNING) << "read file for one row fail. res=" << res;
        return res;
    }

    size_t p = 0;
    for (size_t i = 0; i < schema.num_columns(); ++i) {
        row->set_not_null(i);
        if (schema.column(i).is_nullable()) {
            bool is_null = false;
            is_null = (_row_buf[p / 8] >> ((num_null_bytes * 8 - p - 1) % 8)) & 1;
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
            if (!(res = _file->read(_row_buf + offset, sizeof(VarcharLengthType)))) {
                LOG(WARNING) << "read file for one row fail. res=" << res;
                return res;
            }

            // Get varchar field size
            field_size = *reinterpret_cast<VarcharLengthType*>(_row_buf + offset);
            offset += sizeof(VarcharLengthType);
            if (field_size > column.length() - sizeof(VarcharLengthType)) {
                LOG(WARNING) << "invalid data length for VARCHAR! "
                             << "max_len=" << column.length() - sizeof(VarcharLengthType)
                             << ", real_len=" << field_size;
                return Status::OLAPInternalError(OLAP_ERR_PUSH_INPUT_DATA_ERROR);
            }
        } else if (column.type() == OLAP_FIELD_TYPE_STRING) {
            // Read string length buffer first
            if (!(res = _file->read(_row_buf + offset, sizeof(StringLengthType)))) {
                LOG(WARNING) << "read file for one row fail. res=" << res;
                return res;
            }

            // Get string field size
            field_size = *reinterpret_cast<StringLengthType*>(_row_buf + offset);
            offset += sizeof(StringLengthType);
            if (field_size > column.length() - sizeof(StringLengthType)) {
                LOG(WARNING) << "invalid data length for string! "
                             << "max_len=" << column.length() - sizeof(StringLengthType)
                             << ", real_len=" << field_size;
                return Status::OLAPInternalError(OLAP_ERR_PUSH_INPUT_DATA_ERROR);
            }
        } else {
            field_size = column.length();
        }

        // Read field content according to field size
        if (!(res = _file->read(_row_buf + offset, field_size))) {
            LOG(WARNING) << "read file for one row fail. res=" << res;
            return res;
        }

        if (column.type() == OLAP_FIELD_TYPE_CHAR || column.type() == OLAP_FIELD_TYPE_VARCHAR ||
            column.type() == OLAP_FIELD_TYPE_HLL || column.type() == OLAP_FIELD_TYPE_STRING) {
            Slice slice(_row_buf + offset, field_size);
            row->set_field_content_shallow(i, reinterpret_cast<char*>(&slice));
        } else {
            row->set_field_content_shallow(i, _row_buf + offset);
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
          _row_buf(nullptr),
          _row_compressed_buf(nullptr),
          _row_info_buf(nullptr),
          _max_row_num(0),
          _max_row_buf_size(0),
          _max_compressed_buf_size(0),
          _row_num(0),
          _next_row_start(0) {}

Status LzoBinaryReader::init(TabletSharedPtr tablet, BinaryFile* file) {
    Status res = Status::OK();

    do {
        _file = file;
        _content_len = _file->file_length() - _file->header_size();

        size_t row_info_buf_size = sizeof(RowNumType) + sizeof(CompressedSizeType);
        _row_info_buf = new (std::nothrow) char[row_info_buf_size];
        if (_row_info_buf == nullptr) {
            LOG(WARNING) << "fail to malloc rows info buf. size=" << row_info_buf_size;
            res = Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
            break;
        }

        if (-1 == _file->seek(_file->header_size(), SEEK_SET)) {
            LOG(WARNING) << "skip header, seek fail.";
            res = Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
            break;
        }

        _tablet = tablet;
        _ready = true;
    } while (0);

    if (!res.ok()) {
        SAFE_DELETE_ARRAY(_row_info_buf);
    }
    return res;
}

Status LzoBinaryReader::finalize() {
    _ready = false;
    SAFE_DELETE_ARRAY(_row_buf);
    SAFE_DELETE_ARRAY(_row_compressed_buf);
    SAFE_DELETE_ARRAY(_row_info_buf);
    return Status::OK();
}

Status LzoBinaryReader::next(RowCursor* row) {
    Status res = Status::OK();

    if (!_ready || nullptr == row) {
        // Here i assume _ready means all states were set up correctly
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    if (_row_num == 0) {
        // read next block
        if (!(res = _next_block())) {
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
            is_null = (_row_buf[_next_row_start + p / 8] >> ((num_null_bytes * 8 - p - 1) % 8)) & 1;
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
            field_size = *reinterpret_cast<VarcharLengthType*>(_row_buf + _next_row_start + offset);
            offset += sizeof(VarcharLengthType);

            if (field_size > column.length() - sizeof(VarcharLengthType)) {
                LOG(WARNING) << "invalid data length for VARCHAR! "
                             << "max_len=" << column.length() - sizeof(VarcharLengthType)
                             << ", real_len=" << field_size;
                return Status::OLAPInternalError(OLAP_ERR_PUSH_INPUT_DATA_ERROR);
            }
        } else if (column.type() == OLAP_FIELD_TYPE_STRING) {
            // Get string field size
            field_size = *reinterpret_cast<StringLengthType*>(_row_buf + _next_row_start + offset);
            offset += sizeof(StringLengthType);

            if (field_size > column.length() - sizeof(StringLengthType)) {
                LOG(WARNING) << "invalid data length for string! "
                             << "max_len=" << column.length() - sizeof(StringLengthType)
                             << ", real_len=" << field_size;
                return Status::OLAPInternalError(OLAP_ERR_PUSH_INPUT_DATA_ERROR);
            }
        } else {
            field_size = column.length();
        }

        if (column.type() == OLAP_FIELD_TYPE_CHAR || column.type() == OLAP_FIELD_TYPE_VARCHAR ||
            column.type() == OLAP_FIELD_TYPE_HLL || column.type() == OLAP_FIELD_TYPE_STRING) {
            Slice slice(_row_buf + _next_row_start + offset, field_size);
            row->set_field_content_shallow(i, reinterpret_cast<char*>(&slice));
        } else {
            row->set_field_content_shallow(i, _row_buf + _next_row_start + offset);
        }

        offset += field_size;
    }

    // Calculate checksum for validate when push finished.
    _adler_checksum = olap_adler32(_adler_checksum, _row_buf + _next_row_start, offset);

    _next_row_start += offset;
    --_row_num;
    return res;
}

Status LzoBinaryReader::_next_block() {
    Status res = Status::OK();

    // Get row num and compressed data size
    size_t row_info_buf_size = sizeof(RowNumType) + sizeof(CompressedSizeType);
    if (!(res = _file->read(_row_info_buf, row_info_buf_size))) {
        LOG(WARNING) << "read rows info fail. res=" << res;
        return res;
    }

    RowNumType* rows_num_ptr = reinterpret_cast<RowNumType*>(_row_info_buf);
    _row_num = *rows_num_ptr;
    CompressedSizeType* compressed_size_ptr =
            reinterpret_cast<CompressedSizeType*>(_row_info_buf + sizeof(RowNumType));
    CompressedSizeType compressed_size = *compressed_size_ptr;

    if (_row_num > _max_row_num) {
        // renew rows buf
        SAFE_DELETE_ARRAY(_row_buf);

        _max_row_num = _row_num;
        _max_row_buf_size = _max_row_num * _tablet->row_size();
        _row_buf = new (std::nothrow) char[_max_row_buf_size];
        if (_row_buf == nullptr) {
            LOG(WARNING) << "fail to malloc rows buf. size=" << _max_row_buf_size;
            res = Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
            return res;
        }
    }

    if (compressed_size > _max_compressed_buf_size) {
        // renew rows compressed buf
        SAFE_DELETE_ARRAY(_row_compressed_buf);

        _max_compressed_buf_size = compressed_size;
        _row_compressed_buf = new (std::nothrow) char[_max_compressed_buf_size];
        if (_row_compressed_buf == nullptr) {
            LOG(WARNING) << "fail to malloc rows compressed buf. size=" << _max_compressed_buf_size;
            res = Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
            return res;
        }
    }

    if (!(res = _file->read(_row_compressed_buf, compressed_size))) {
        LOG(WARNING) << "read compressed rows fail. res=" << res;
        return res;
    }

    // python lzo use lzo1x to compress
    // and add 5 bytes header (\xf0 + 4 bytes(uncompress data size))
    size_t written_len = 0;
    size_t block_header_size = 5;
    if (!(res = olap_decompress(_row_compressed_buf + block_header_size,
                                compressed_size - block_header_size, _row_buf, _max_row_buf_size,
                                &written_len, OLAP_COMP_TRANSPORT))) {
        LOG(WARNING) << "olap decompress fail. res=" << res;
        return res;
    }

    _curr += row_info_buf_size + compressed_size;
    _next_row_start = 0;
    return res;
}

Status PushBrokerReader::init(const Schema* schema, const TBrokerScanRange& t_scan_range,
                              const TDescriptorTable& t_desc_tbl) {
    // init schema
    _schema = schema;

    // init runtime state, runtime profile, counter
    TUniqueId dummy_id;
    dummy_id.hi = 0;
    dummy_id.lo = 0;
    TPlanFragmentExecParams params;
    params.fragment_instance_id = dummy_id;
    params.query_id = dummy_id;
    TExecPlanFragmentParams fragment_params;
    fragment_params.params = params;
    fragment_params.protocol_version = PaloInternalServiceVersion::V1;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    _runtime_state.reset(
            new RuntimeState(params, query_options, query_globals, ExecEnv::GetInstance()));
    DescriptorTbl* desc_tbl = nullptr;
    Status status = DescriptorTbl::create(_runtime_state->obj_pool(), t_desc_tbl, &desc_tbl);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to create descriptor table, msg: " << status.get_error_msg();
        return Status::OLAPInternalError(OLAP_ERR_PUSH_INIT_ERROR);
    }
    _runtime_state->set_desc_tbl(desc_tbl);
    status = _runtime_state->init_mem_trackers(dummy_id);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to init mem trackers, msg: " << status.get_error_msg();
        return Status::OLAPInternalError(OLAP_ERR_PUSH_INIT_ERROR);
    }
    _runtime_profile = _runtime_state->runtime_profile();
    _runtime_profile->set_name("PushBrokerReader");
    _mem_pool.reset(new MemPool("PushBrokerReader"));
    _counter.reset(new ScannerCounter());

    // init scanner
    BaseScanner* scanner = nullptr;
    switch (t_scan_range.ranges[0].format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        scanner = new ParquetScanner(_runtime_state.get(), _runtime_profile, t_scan_range.params,
                                     t_scan_range.ranges, t_scan_range.broker_addresses,
                                     _pre_filter_texprs, _counter.get());
        break;
    default:
        LOG(WARNING) << "Unsupported file format type: " << t_scan_range.ranges[0].format_type;
        return Status::OLAPInternalError(OLAP_ERR_PUSH_INIT_ERROR);
    }
    _scanner.reset(scanner);
    status = _scanner->open();
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Failed to open scanner, msg: " << status.get_error_msg();
        return Status::OLAPInternalError(OLAP_ERR_PUSH_INIT_ERROR);
    }

    // init tuple
    auto tuple_id = t_scan_range.params.dest_tuple_id;
    _tuple_desc = _runtime_state->desc_tbl().get_tuple_descriptor(tuple_id);
    if (_tuple_desc == nullptr) {
        std::stringstream ss;
        LOG(WARNING) << "Failed to get tuple descriptor, tuple_id: " << tuple_id;
        return Status::OLAPInternalError(OLAP_ERR_PUSH_INIT_ERROR);
    }

    int tuple_buffer_size = _tuple_desc->byte_size();
    void* tuple_buffer = _mem_pool->allocate(tuple_buffer_size);
    if (tuple_buffer == nullptr) {
        LOG(WARNING) << "Allocate memory for tuple failed";
        return Status::OLAPInternalError(OLAP_ERR_PUSH_INIT_ERROR);
    }
    _tuple = reinterpret_cast<Tuple*>(tuple_buffer);

    _ready = true;
    return Status::OK();
}

Status PushBrokerReader::fill_field_row(RowCursorCell* dst, const char* src, bool src_null,
                                        MemPool* mem_pool, FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_DECIMAL: {
        dst->set_is_null(src_null);
        if (src_null) {
            break;
        }
        auto* decimal_value = reinterpret_cast<const DecimalV2Value*>(src);
        auto* storage_decimal_value = reinterpret_cast<decimal12_t*>(dst->mutable_cell_ptr());
        storage_decimal_value->integer = decimal_value->int_value();
        storage_decimal_value->fraction = decimal_value->frac_value();
        break;
    }
    case OLAP_FIELD_TYPE_DATETIME: {
        dst->set_is_null(src_null);
        if (src_null) {
            break;
        }

        auto* datetime_value = reinterpret_cast<const DateTimeValue*>(src);
        auto* storage_datetime_value = reinterpret_cast<uint64_t*>(dst->mutable_cell_ptr());
        *storage_datetime_value = datetime_value->to_olap_datetime();
        break;
    }

    case OLAP_FIELD_TYPE_DATE: {
        dst->set_is_null(src_null);
        if (src_null) {
            break;
        }

        auto* date_value = reinterpret_cast<const DateTimeValue*>(src);
        auto* storage_date_value = reinterpret_cast<uint24_t*>(dst->mutable_cell_ptr());
        *storage_date_value = static_cast<int64_t>(date_value->to_olap_date());
        break;
    }
    case OLAP_FIELD_TYPE_BOOL:
    case OLAP_FIELD_TYPE_TINYINT:
    case OLAP_FIELD_TYPE_SMALLINT:
    case OLAP_FIELD_TYPE_INT:
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
    case OLAP_FIELD_TYPE_BIGINT:
    case OLAP_FIELD_TYPE_LARGEINT:
    case OLAP_FIELD_TYPE_FLOAT:
    case OLAP_FIELD_TYPE_DOUBLE:
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_HLL:
    case OLAP_FIELD_TYPE_OBJECT: {
        dst->set_is_null(src_null);
        if (src_null) {
            break;
        }
        const auto* type_info = get_scalar_type_info(type);
        type_info->deep_copy(dst->mutable_cell_ptr(), src, mem_pool);
        break;
    }
    default:
        return Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA);
    }

    return Status::OK();
}

Status PushBrokerReader::next(ContiguousRow* row) {
    if (!_ready || row == nullptr) {
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    memset(_tuple, 0, _tuple_desc->num_null_bytes());
    // Get from scanner
    Status status = _scanner->get_next(_tuple, _mem_pool.get(), &_eof, &_fill_tuple);
    if (UNLIKELY(!status.ok())) {
        LOG(WARNING) << "Scanner get next tuple failed";
        return Status::OLAPInternalError(OLAP_ERR_PUSH_INPUT_DATA_ERROR);
    }
    if (_eof || !_fill_tuple) {
        return Status::OK();
    }

    auto slot_descs = _tuple_desc->slots();
    // finalize row
    for (size_t i = 0; i < slot_descs.size(); ++i) {
        auto cell = row->cell(i);
        const SlotDescriptor* slot = slot_descs[i];
        bool is_null = _tuple->is_null(slot->null_indicator_offset());
        const void* value = _tuple->get_slot(slot->tuple_offset());

        FieldType type = _schema->column(i)->type();
        Status field_status =
                fill_field_row(&cell, (const char*)value, is_null, _mem_pool.get(), type);
        if (field_status != Status::OK()) {
            LOG(WARNING) << "fill field row failed in spark load, slot index: " << i
                         << ", type: " << type;
            return Status::OLAPInternalError(OLAP_ERR_SCHEMA_SCHEMA_FIELD_INVALID);
        }
    }

    return Status::OK();
}

void PushBrokerReader::print_profile() {
    std::stringstream ss;
    _runtime_profile->pretty_print(&ss);
    LOG(INFO) << ss.str();
}

std::string PushHandler::_debug_version_list(const Versions& versions) const {
    std::ostringstream txt;
    txt << "Versions: ";

    for (Versions::const_iterator it = versions.begin(); it != versions.end(); ++it) {
        txt << "[" << it->first << "~" << it->second << "],";
    }

    return txt.str();
}

} // namespace doris
