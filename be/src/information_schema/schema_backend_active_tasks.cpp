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

#include "information_schema/schema_backend_active_tasks.h"

#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/string_ref.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/runtime_state.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaBackendActiveTasksScanner::_s_tbls_columns = {
        //   name,       type,          size
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

SchemaBackendActiveTasksScanner::SchemaBackendActiveTasksScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKEND_ACTIVE_TASKS) {}

SchemaBackendActiveTasksScanner::~SchemaBackendActiveTasksScanner() {}

Status SchemaBackendActiveTasksScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    return Status::OK();
}

Status SchemaBackendActiveTasksScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_task_stats_block == nullptr) {
        _task_stats_block = Block::create_unique();

        for (int i = 0; i < _s_tbls_columns.size(); ++i) {
            auto data_type =
                    DataTypeFactory::instance().create_data_type(_s_tbls_columns[i].type, true);
            _task_stats_block->insert(ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                            _s_tbls_columns[i].name));
        }

        _task_stats_block->reserve(_block_rows_limit);

        ExecEnv::GetInstance()->runtime_query_statistics_mgr()->get_active_be_tasks_block(
                _task_stats_block.get());
        _total_rows = (int)_task_stats_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    MutableBlock mblock = MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_task_stats_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris