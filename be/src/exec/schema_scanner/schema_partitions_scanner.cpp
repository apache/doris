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

#include "exec/schema_scanner/schema_partitions_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/datetime_value.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace doris {

SchemaScanner::ColumnDesc SchemaPartitionsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PARTITION_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"SUBPARTITION_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"PARTITION_ORDINAL_POSITION", TYPE_INT, sizeof(int32_t), true},
        {"SUBPARTITION_ORDINAL_POSITION", TYPE_INT, sizeof(int32_t), true},
        {"PARTITION_METHOD", TYPE_VARCHAR, sizeof(StringValue), true},
        {"SUBPARTITION_METHOD", TYPE_VARCHAR, sizeof(StringValue), true},
        {"PARTITION_EXPRESSION", TYPE_VARCHAR, sizeof(StringValue), true},
        {"SUBPARTITION_EXPRESSION", TYPE_VARCHAR, sizeof(StringValue), true},
        {"PARTITION_DESCRIPTION", TYPE_STRING, sizeof(StringValue), true},
        {"TABLE_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"AVG_ROW_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAX_DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"INDEX_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_FREE", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_TIME", TYPE_BIGINT, sizeof(int64_t), false},
        {"UPDATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"CHECK_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"CHECKSUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"PARTITION_COMMENT", TYPE_STRING, sizeof(StringValue), false},
        {"NODEGROUP", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLESPACE_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
};

SchemaPartitionsScanner::SchemaPartitionsScanner()
        : SchemaScanner(_s_tbls_columns,
                        sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _db_index(0),
          _table_index(0) {}

SchemaPartitionsScanner::~SchemaPartitionsScanner() {}

Status SchemaPartitionsScanner::start(RuntimeState* state) {
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

Status SchemaPartitionsScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
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
