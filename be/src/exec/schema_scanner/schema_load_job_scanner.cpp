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

#include "exec/schema_scanner/schema_load_job_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>

#include <string>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaLoadJobScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"JOB_ID", TYPE_STRING, sizeof(StringRef), true},
        {"LABEL", TYPE_STRING, sizeof(StringRef), true},
        {"STATE", TYPE_STRING, sizeof(StringRef), true},
        {"PROGRESS", TYPE_STRING, sizeof(StringRef), true},
        {"TYPE", TYPE_STRING, sizeof(StringRef), true},
        {"ETL_INFO", TYPE_STRING, sizeof(StringRef), true},
        {"TASK_INFO", TYPE_STRING, sizeof(StringRef), true},
        {"ERROR_MSG", TYPE_STRING, sizeof(StringRef), true},
        {"CREATE_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"ETL_START_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"ETL_FINISH_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"LOAD_START_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"LOAD_FINISH_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"URL", TYPE_STRING, sizeof(StringRef), true},
        {"JOB_DETAILS", TYPE_STRING, sizeof(StringRef), true},
        {"TRANSACTION_ID", TYPE_STRING, sizeof(StringRef), true},
        {"ERROR_TABLETS", TYPE_STRING, sizeof(StringRef), true},
        {"USER", TYPE_STRING, sizeof(StringRef), true},
        {"COMMENT", TYPE_STRING, sizeof(StringRef), true},
        {"FIRST_ERROR_MSG", TYPE_STRING, sizeof(StringRef), true},
};

SchemaLoadJobScanner::SchemaLoadJobScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_LOAD_JOBS) {}

SchemaLoadJobScanner::~SchemaLoadJobScanner() {}

Status SchemaLoadJobScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TFetchLoadJobRequest request;
    RETURN_IF_ERROR(SchemaHelper::fetch_load_job(*(_param->common_param->ip),
                                                 _param->common_param->port, request, &_result));
    return Status::OK();
}

Status SchemaLoadJobScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (block == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    if (_result.loadJobs.empty()) {
        return Status::OK();
    }

    return _fill_block_impl(block);
}

Status SchemaLoadJobScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    const auto& jobs_info = _result.loadJobs;
    size_t row_num = jobs_info.size();
    if (row_num == 0) {
        return Status::OK();
    }

    for (size_t col_idx = 0; col_idx < _s_tbls_columns.size(); ++col_idx) {
        const auto& col_desc = _s_tbls_columns[col_idx];

        std::vector<StringRef> str_refs(row_num);
        std::vector<void*> datas(row_num);
        std::vector<std::string> column_values(row_num);

        for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
            const auto& job_info = jobs_info[row_idx];
            std::string& column_value = column_values[row_idx];

            if (col_desc.type == TYPE_STRING) {
                switch (col_idx) {
                case 0: // JOB_ID
                    column_value = job_info.__isset.job_id ? job_info.job_id : "";
                    break;
                case 1: // LABEL
                    column_value = job_info.__isset.label ? job_info.label : "";
                    break;
                case 2: // STATE
                    column_value = job_info.__isset.state ? job_info.state : "";
                    break;
                case 3: // PROGRESS
                    column_value = job_info.__isset.progress ? job_info.progress : "";
                    break;
                case 4: // TYPE
                    column_value = job_info.__isset.type ? job_info.type : "";
                    break;
                case 5: // ETL_INFO
                    column_value = job_info.__isset.etl_info ? job_info.etl_info : "";
                    break;
                case 6: // TASK_INFO
                    column_value = job_info.__isset.task_info ? job_info.task_info : "";
                    break;
                case 7: // ERROR_MSG
                    column_value = job_info.__isset.error_msg ? job_info.error_msg : "";
                    break;
                case 8: // CREATE_TIME
                    column_value = job_info.__isset.create_time ? job_info.create_time : "";
                    break;
                case 9: // ETL_START_TIME
                    column_value = job_info.__isset.etl_start_time ? job_info.etl_start_time : "";
                    break;
                case 10: // ETL_FINISH_TIME
                    column_value = job_info.__isset.etl_finish_time ? job_info.etl_finish_time : "";
                    break;
                case 11: // LOAD_START_TIME
                    column_value = job_info.__isset.load_start_time ? job_info.load_start_time : "";
                    break;
                case 12: // LOAD_FINISH_TIME
                    column_value =
                            job_info.__isset.load_finish_time ? job_info.load_finish_time : "";
                    break;
                case 13: // URL
                    column_value = job_info.__isset.url ? job_info.url : "";
                    break;
                case 14: // JOB_DETAILS
                    column_value = job_info.__isset.job_details ? job_info.job_details : "";
                    break;
                case 15: // TRANSACTION_ID
                    column_value = job_info.__isset.transaction_id ? job_info.transaction_id : "";
                    break;
                case 16: // ERROR_TABLETS
                    column_value = job_info.__isset.error_tablets ? job_info.error_tablets : "";
                    break;
                case 17: // USER
                    column_value = job_info.__isset.user ? job_info.user : "";
                    break;
                case 18: // COMMENT
                    column_value = job_info.__isset.comment ? job_info.comment : "";
                    break;
                case 19: // FIRST_ERROR_MSG
                    column_value = job_info.__isset.first_error_msg ? job_info.first_error_msg : "";
                    break;
                }

                str_refs[row_idx] =
                        StringRef(column_values[row_idx].data(), column_values[row_idx].size());
                datas[row_idx] = &str_refs[row_idx];
            }
        }

        RETURN_IF_ERROR(fill_dest_column_for_range(block, col_idx, datas));
    }

    return Status::OK();
}

} // namespace doris
