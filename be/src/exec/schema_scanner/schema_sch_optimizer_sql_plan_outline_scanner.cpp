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

#include "exec/schema_scanner/schema_sch_optimizer_sql_plan_outline_scanner.h"

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

std::vector<SchemaScanner::ColumnDesc> SchemaOptimizerSqlPlanOutlineScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"OUTLINE_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"VISIBLE_SIGNATURE", TYPE_STRING, sizeof(StringRef), true},
        {"SQL_ID", TYPE_STRING, sizeof(StringRef), true},
        {"SQL_TEXT", TYPE_STRING, sizeof(StringRef), true},
        {"OUTLINE_TARGET", TYPE_STRING, sizeof(StringRef), true},
        {"OUTLINE_DATA", TYPE_STRING, sizeof(StringRef), true},
};

SchemaOptimizerSqlPlanOutlineScanner::SchemaOptimizerSqlPlanOutlineScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_OPTIMIZER_SQL_PLAN_OUTLINE) {}

SchemaOptimizerSqlPlanOutlineScanner::~SchemaOptimizerSqlPlanOutlineScanner() {}

Status SchemaOptimizerSqlPlanOutlineScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TFetchOutlineInfoRequest request;
    RETURN_IF_ERROR(SchemaHelper::fetch_outline_info(
            *(_param->common_param->ip), _param->common_param->port, request, &_result));
    return Status::OK();
}

Status SchemaOptimizerSqlPlanOutlineScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (block == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    if (_result.outlineInfos.empty()) {
        return Status::OK();
    }

    return _fill_block_impl(block);
}

Status SchemaOptimizerSqlPlanOutlineScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    const auto& outline_infos = _result.outlineInfos;
    size_t row_num = outline_infos.size();
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
            const auto& outline_info = outline_infos[row_idx];
            std::string& column_value = column_values[row_idx];

            if (col_desc.type == TYPE_STRING) {
                switch (col_idx) {
                case 0: // OUTLINE_NAME
                    column_value = outline_info.__isset.outline_name ? outline_info.outline_name : "";
                    break;
                case 1: // VISIBLE_SIGNATURE
                    column_value = outline_info.__isset.visible_signature ? outline_info.visible_signature : "";
                    break;
                case 2: // SQL_ID
                    column_value = outline_info.__isset.sql_id ? outline_info.sql_id : "";
                    break;
                case 3: // SQL_TEXT
                    column_value = outline_info.__isset.sql_text ? outline_info.sql_text : "";
                    break;
                case 4: // OUTLINE_TARGET
                    column_value = outline_info.__isset.outline_target ? outline_info.outline_target : "";
                    break;
                case 5: // OUTLINE_DATA
                    column_value = outline_info.__isset.outline_data ? outline_info.outline_data : "";
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
