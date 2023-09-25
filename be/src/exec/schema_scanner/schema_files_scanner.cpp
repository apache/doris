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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <stdint.h>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/define_primitive_type.h"
#include "util/runtime_profile.h"
#include "vec/common/string_ref.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaFilesScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"FILE_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"FILE_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"FILE_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLESPACE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TABLE_CATALOG", TYPE_CHAR, sizeof(StringRef), false},
        {"TABLE_SCHEMA", TYPE_STRING, sizeof(StringRef), true},
        {"TABLE_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"LOGFILE_GROUP_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"LOGFILE_GROUP_NUMBER", TYPE_BIGINT, sizeof(int64_t), true},
        {"ENGINE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"FULLTEXT_KEYS", TYPE_STRING, sizeof(StringRef), true},
        {"DELETED_ROWS", TYPE_STRING, sizeof(StringRef), true},
        {"UPDATE_COUNT", TYPE_STRING, sizeof(StringRef), true},
        {"FREE_EXTENTS", TYPE_BIGINT, sizeof(int64_t), true},
        {"TOTAL_EXTENTS", TYPE_BIGINT, sizeof(int64_t), true},
        {"EXTENT_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"INITIAL_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAXIMUM_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"AUTOEXTEND_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATION_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"LAST_UPDATE_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"LAST_ACCESS_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"RECOVER_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"TRANSACTION_COUNTER", TYPE_STRING, sizeof(StringRef), true},
        {"VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROW_FORMAT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_ROWS", TYPE_STRING, sizeof(StringRef), true},
        {"AVG_ROW_LENGTH", TYPE_STRING, sizeof(StringRef), true},
        {"DATA_LENGTH", TYPE_STRING, sizeof(StringRef), true},
        {"MAX_DATA_LENGTH", TYPE_STRING, sizeof(StringRef), true},
        {"INDEX_LENGTH", TYPE_STRING, sizeof(StringRef), true},
        {"DATA_FREE", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"UPDATE_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"CHECK_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"CHECKSUM", TYPE_STRING, sizeof(StringRef), true},
        {"STATUS", TYPE_VARCHAR, sizeof(StringRef), true},
        {"EXTRA", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaFilesScanner::SchemaFilesScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_FILES),
          _db_index(0),
          _table_index(0) {}

SchemaFilesScanner::~SchemaFilesScanner() {}

Status SchemaFilesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    SCOPED_TIMER(_get_db_timer);
    TGetDbsParams db_params;
    if (nullptr != _param->common_param->db) {
        db_params.__set_pattern(*(_param->common_param->db));
    }
    if (nullptr != _param->common_param->catalog) {
        db_params.__set_catalog(*(_param->common_param->catalog));
    }
    if (nullptr != _param->common_param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->common_param->current_user_ident));
    } else {
        if (nullptr != _param->common_param->user) {
            db_params.__set_user(*(_param->common_param->user));
        }
        if (nullptr != _param->common_param->user_ip) {
            db_params.__set_user_ip(*(_param->common_param->user_ip));
        }
    }

    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_names(
                *(_param->common_param->ip), _param->common_param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaFilesScanner::get_next_block(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    *eos = true;
    return Status::OK();
}

} // namespace doris
