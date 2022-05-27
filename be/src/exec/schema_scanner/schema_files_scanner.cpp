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

#include "exec/schema_scanner/schema_files_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace doris {

SchemaScanner::ColumnDesc SchemaFilesScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"FILE_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"FILE_NAME", TYPE_STRING, sizeof(StringValue), true},
        {"FILE_TYPE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLESPACE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_CATALOG", TYPE_CHAR, sizeof(StringValue), false},
        {"TABLE_SCHEMA", TYPE_STRING, sizeof(StringValue), true},
        {"TABLE_NAME", TYPE_STRING, sizeof(StringValue), true},
        {"LOGFILE_GROUP_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"LOGFILE_GROUP_NUMBER", TYPE_BIGINT, sizeof(int64_t), true},
        {"ENGINE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"FULLTEXT_KEYS", TYPE_STRING, sizeof(StringValue), true},
        {"DELETED_ROWS", TYPE_STRING, sizeof(StringValue), true},
        {"UPDATE_COUNT", TYPE_STRING, sizeof(StringValue), true},
        {"FREE_EXTENTS", TYPE_BIGINT, sizeof(int64_t), true},
        {"TOTAL_EXTENTS", TYPE_BIGINT, sizeof(int64_t), true},
        {"EXTENT_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"INITIAL_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAXIMUM_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"AUTOEXTEND_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATION_TIME", TYPE_STRING, sizeof(StringValue), true},
        {"LAST_UPDATE_TIME", TYPE_STRING, sizeof(StringValue), true},
        {"LAST_ACCESS_TIME", TYPE_STRING, sizeof(StringValue), true},
        {"RECOVER_TIME", TYPE_STRING, sizeof(StringValue), true},
        {"TRANSACTION_COUNTER", TYPE_STRING, sizeof(StringValue), true},
        {"VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROW_FORMAT", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_ROWS", TYPE_STRING, sizeof(StringValue), true},
        {"AVG_ROW_LENGTH", TYPE_STRING, sizeof(StringValue), true},
        {"DATA_LENGTH", TYPE_STRING, sizeof(StringValue), true},
        {"MAX_DATA_LENGTH", TYPE_STRING, sizeof(StringValue), true},
        {"INDEX_LENGTH", TYPE_STRING, sizeof(StringValue), true},
        {"DATA_FREE", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_TIME", TYPE_STRING, sizeof(StringValue), true},
        {"UPDATE_TIME", TYPE_STRING, sizeof(StringValue), true},
        {"CHECK_TIME", TYPE_STRING, sizeof(StringValue), true},
        {"CHECKSUM", TYPE_STRING, sizeof(StringValue), true},
        {"STATUS", TYPE_VARCHAR, sizeof(StringValue), true},
        {"EXTRA", TYPE_VARCHAR, sizeof(StringValue), true},
};

SchemaFilesScanner::SchemaFilesScanner()
        : SchemaScanner(_s_tbls_columns,
                        sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _db_index(0),
          _table_index(0) {}

SchemaFilesScanner::~SchemaFilesScanner() {}

Status SchemaFilesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TGetDbsParams db_params;
    if (NULL != _param->db) {
        db_params.__set_pattern(*(_param->db));
    }
    if (NULL != _param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (NULL != _param->user) {
            db_params.__set_user(*(_param->user));
        }
        if (NULL != _param->user_ip) {
            db_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (NULL != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(
                SchemaHelper::get_db_names(*(_param->ip), _param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaFilesScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == tuple || nullptr == pool || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    *eos = true;
    return Status::OK();
}

} // namespace doris
