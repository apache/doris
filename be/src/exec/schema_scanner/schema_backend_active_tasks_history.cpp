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

#include "exec/schema_scanner/schema_backend_active_tasks_history.h"

#include "exec/schema_scanner/materialized_schema_table_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaBackendActiveTasksScannerHistory::_s_tbls_columns = {
        //   name,       type,          size
        {"TIME_SLICE", TYPE_DATETIME, sizeof(int128_t), false},
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"FE_HOST", TYPE_VARCHAR, sizeof(StringRef), false},
        {"WORKLOAD_GROUP_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"QUERY_ID", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TASK_TIME_MS", TYPE_BIGINT, sizeof(int64_t), false},
        {"TASK_CPU_TIME_MS", TYPE_BIGINT, sizeof(int64_t), false},
        {"SCAN_ROWS", TYPE_BIGINT, sizeof(int64_t), false},
        {"SCAN_BYTES", TYPE_BIGINT, sizeof(int64_t), false},
        {"BE_PEAK_MEMORY_BYTES", TYPE_BIGINT, sizeof(int64_t), false},
        {"CURRENT_USED_MEMORY_BYTES", TYPE_BIGINT, sizeof(int64_t), false},
        {"SHUFFLE_SEND_BYTES", TYPE_BIGINT, sizeof(int64_t), false},
        {"SHUFFLE_SEND_ROWS", TYPE_BIGINT, sizeof(int64_t), false},
        {"QUERY_TYPE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"SPILL_WRITE_BYTES_TO_LOCAL_STORAGE", TYPE_BIGINT, sizeof(int64_t), false},
        {"SPILL_READ_BYTES_FROM_LOCAL_STORAGE", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaBackendActiveTasksScannerHistory::SchemaBackendActiveTasksScannerHistory()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKEND_ACTIVE_TASKS_HISTORY) {}

SchemaBackendActiveTasksScannerHistory::~SchemaBackendActiveTasksScannerHistory() = default;

Status SchemaBackendActiveTasksScannerHistory::start(RuntimeState* state) {
    RETURN_IF_ERROR(MaterializedSchemaTableMgr::instance()->get_reader(
            TSchemaTableType::SCH_BACKEND_ACTIVE_TASKS_HISTORY, {}, state->batch_size(),
            _timezone_obj, &materialized_reader_));
    return Status::OK();
}

Status SchemaBackendActiveTasksScannerHistory::get_next_block_internal(vectorized::Block* block,
                                                                       bool* eos) {
    RETURN_IF_ERROR(materialized_reader_->get_batch(block, eos));
    return Status::OK();
}

} // namespace doris