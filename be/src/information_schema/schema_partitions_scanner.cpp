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

#include "information_schema/schema_partitions_scanner.h"

#include <stdint.h>

#include "core/string_ref.h"
#include "core/value/decimalv2_value.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaPartitionsScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"PARTITION_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"PARTITION_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SUBPARTITION_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PARTITION_ORDINAL_POSITION", TYPE_INT, sizeof(int32_t), true},
        {"SUBPARTITION_ORDINAL_POSITION", TYPE_INT, sizeof(int32_t), true},
        {"PARTITION_METHOD", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SUBPARTITION_METHOD", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PARTITION_EXPRESSION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SUBPARTITION_EXPRESSION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PARTITION_DESCRIPTION", TYPE_STRING, sizeof(StringRef), true},
        {"TABLE_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"AVG_ROW_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAX_DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"INDEX_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_FREE", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_TIME", TYPE_BIGINT, sizeof(int64_t), false},
        {"UPDATE_TIME", TYPE_DATETIME, sizeof(int128_t), true},
        {"CHECK_TIME", TYPE_DATETIME, sizeof(int128_t), true},
        {"CHECKSUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"PARTITION_COMMENT", TYPE_STRING, sizeof(StringRef), false},
        {"NODEGROUP", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLESPACE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"LOCAL_DATA_SIZE", TYPE_STRING, sizeof(StringRef), true},
        {"REMOTE_DATA_SIZE", TYPE_STRING, sizeof(StringRef), true},
        {"STATE", TYPE_STRING, sizeof(StringRef), true},
        {"REPLICA_ALLOCATION", TYPE_STRING, sizeof(StringRef), true},
        {"REPLICA_NUM", TYPE_INT, sizeof(int32_t), true},
        {"STORAGE_POLICY", TYPE_STRING, sizeof(StringRef), true},
        {"STORAGE_MEDIUM", TYPE_STRING, sizeof(StringRef), true},
        {"COOLDOWN_TIME_MS", TYPE_STRING, sizeof(StringRef), true},
        {"LAST_CONSISTENCY_CHECK_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"BUCKET_NUM", TYPE_INT, sizeof(int32_t), true},
        {"COMMITTED_VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"VISIBLE_VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"PARTITION_KEY", TYPE_STRING, sizeof(StringRef), true},
        {"RANGE", TYPE_STRING, sizeof(StringRef), true},
        {"DISTRIBUTION", TYPE_STRING, sizeof(StringRef), true},
};

SchemaPartitionsScanner::SchemaPartitionsScanner()
        : SchemaPerDbScanner(_s_tbls_columns, TSchemaTableType::SCH_PARTITIONS,
                             TSchemaTableName::PARTITIONS, "partitions") {}

void SchemaPartitionsScanner::add_extra_db_params(TGetDbsParams* db_params) {
    // `SHOW PARTITIONS FROM <db>` reaches here as a db pattern, so only that db is listed.
    if (_param->common_param->db) {
        db_params->__set_pattern(*(_param->common_param->db));
    }
}

void SchemaPartitionsScanner::add_extra_request_params(TSchemaTableRequestParams* params) {
    if (_param->common_param->thread_id > 0) {
        params->__set_thread_id(_param->common_param->thread_id);
    }
    // The FE renders partition timestamps, so it has to know which time zone to render in.
    params->__set_time_zone(_timezone);
}

} // namespace doris
