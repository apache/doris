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

#include "exec/schema_scanner/schema_routine_load_job_scanner.h"

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

std::vector<SchemaScanner::ColumnDesc> SchemaRoutineLoadJobScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"JOB_ID", TYPE_STRING, sizeof(StringRef), true},
        {"JOB_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"CREATE_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"PAUSE_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"END_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"DB_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"TABLE_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"STATE", TYPE_STRING, sizeof(StringRef), true},
        {"CURRENT_TASK_NUM", TYPE_STRING, sizeof(StringRef), true},
        {"JOB_PROPERTIES", TYPE_STRING, sizeof(StringRef), true},
        {"DATA_SOURCE_PROPERTIES", TYPE_STRING, sizeof(StringRef), true},
        {"CUSTOM_PROPERTIES", TYPE_STRING, sizeof(StringRef), true},
        {"STATISTIC", TYPE_STRING, sizeof(StringRef), true},
        {"PROGRESS", TYPE_STRING, sizeof(StringRef), true},
        {"LAG", TYPE_STRING, sizeof(StringRef), true},
        {"REASON_OF_STATE_CHANGED", TYPE_STRING, sizeof(StringRef), true},
        {"ERROR_LOG_URLS", TYPE_STRING, sizeof(StringRef), true},
        {"USER_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"CURRENT_ABORT_TASK_NUM", TYPE_INT, sizeof(int32_t), true},
        {"IS_ABNORMAL_PAUSE", TYPE_BOOLEAN, sizeof(int8_t), true},
};

SchemaRoutineLoadJobScanner::SchemaRoutineLoadJobScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_ROUTINE_LOAD_JOBS) {}

SchemaRoutineLoadJobScanner::~SchemaRoutineLoadJobScanner() {}

Status SchemaRoutineLoadJobScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TFetchRoutineLoadJobRequest request;
    RETURN_IF_ERROR(SchemaHelper::fetch_routine_load_job(
            *(_param->common_param->ip), _param->common_param->port, request, &_result));
    return Status::OK();
}

Status SchemaRoutineLoadJobScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (block == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    if (_result.routineLoadJobs.empty()) {
        return Status::OK();
    }

    return _fill_block_impl(block);
}

Status SchemaRoutineLoadJobScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    const auto& jobs_info = _result.routineLoadJobs;
    size_t row_num = jobs_info.size();
    if (row_num == 0) {
        return Status::OK();
    }

    for (size_t col_idx = 0; col_idx < _s_tbls_columns.size(); ++col_idx) {
        const auto& col_desc = _s_tbls_columns[col_idx];

        std::vector<StringRef> str_refs(row_num);
        std::vector<int32_t> int_vals(row_num);
        std::vector<int8_t> bool_vals(row_num);
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
                case 1: // JOB_NAME
                    column_value = job_info.__isset.job_name ? job_info.job_name : "";
                    break;
                case 2: // CREATE_TIME
                    column_value = job_info.__isset.create_time ? job_info.create_time : "";
                    break;
                case 3: // PAUSE_TIME
                    column_value = job_info.__isset.pause_time ? job_info.pause_time : "";
                    break;
                case 4: // END_TIME
                    column_value = job_info.__isset.end_time ? job_info.end_time : "";
                    break;
                case 5: // DB_NAME
                    column_value = job_info.__isset.db_name ? job_info.db_name : "";
                    break;
                case 6: // TABLE_NAME
                    column_value = job_info.__isset.table_name ? job_info.table_name : "";
                    break;
                case 7: // STATE
                    column_value = job_info.__isset.state ? job_info.state : "";
                    break;
                case 8: // CURRENT_TASK_NUM
                    column_value =
                            job_info.__isset.current_task_num ? job_info.current_task_num : "";
                    break;
                case 9: // JOB_PROPERTIES
                    column_value = job_info.__isset.job_properties ? job_info.job_properties : "";
                    break;
                case 10: // DATA_SOURCE_PROPERTIES
                    column_value = job_info.__isset.data_source_properties
                                           ? job_info.data_source_properties
                                           : "";
                    break;
                case 11: // CUSTOM_PROPERTIES
                    column_value =
                            job_info.__isset.custom_properties ? job_info.custom_properties : "";
                    break;
                case 12: // STATISTIC
                    column_value = job_info.__isset.statistic ? job_info.statistic : "";
                    break;
                case 13: // PROGRESS
                    column_value = job_info.__isset.progress ? job_info.progress : "";
                    break;
                case 14: // LAG
                    column_value = job_info.__isset.lag ? job_info.lag : "";
                    break;
                case 15: // REASON_OF_STATE_CHANGED
                    column_value = job_info.__isset.reason_of_state_changed
                                           ? job_info.reason_of_state_changed
                                           : "";
                    break;
                case 16: // ERROR_LOG_URLS
                    column_value = job_info.__isset.error_log_urls ? job_info.error_log_urls : "";
                    break;
                case 17: // USER_NAME
                    column_value = job_info.__isset.user_name ? job_info.user_name : "";
                    break;
                }

                str_refs[row_idx] =
                        StringRef(column_values[row_idx].data(), column_values[row_idx].size());
                datas[row_idx] = &str_refs[row_idx];
            } else if (col_desc.type == TYPE_INT) {
                int_vals[row_idx] = job_info.__isset.current_abort_task_num
                                            ? job_info.current_abort_task_num
                                            : 0;
                datas[row_idx] = &int_vals[row_idx];
            } else if (col_desc.type == TYPE_BOOLEAN) {
                bool_vals[row_idx] =
                        job_info.__isset.is_abnormal_pause ? job_info.is_abnormal_pause : false;
                datas[row_idx] = &bool_vals[row_idx];
            }
        }

        RETURN_IF_ERROR(fill_dest_column_for_range(block, col_idx, datas));
    }

    return Status::OK();
}

} // namespace doris