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

#include "exec/schema_scanner/schema_processlist_scanner.h"

#include <gen_cpp/FrontendService_types.h>

#include <exception>
#include <vector>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/define_primitive_type.h"
#include "runtime/runtime_state.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaProcessListScanner::_s_processlist_columns = {
        {"CURRENT_CONNECTED", TYPE_VARCHAR, sizeof(StringRef), false},
        {"ID", TYPE_LARGEINT, sizeof(int128_t), false},
        {"USER", TYPE_VARCHAR, sizeof(StringRef), false},
        {"HOST", TYPE_VARCHAR, sizeof(StringRef), false},
        {"LOGIN_TIME", TYPE_DATETIMEV2, sizeof(DateTimeV2ValueType), false},
        {"CATALOG", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DB", TYPE_VARCHAR, sizeof(StringRef), false},
        {"COMMAND", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TIME", TYPE_INT, sizeof(int32_t), false},
        {"STATE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"QUERY_ID", TYPE_VARCHAR, sizeof(StringRef), false},
        {"INFO", TYPE_VARCHAR, sizeof(StringRef), false},
        {"FE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"CLOUD_CLUSTER", TYPE_VARCHAR, sizeof(StringRef), false}};

SchemaProcessListScanner::SchemaProcessListScanner()
        : SchemaScanner(_s_processlist_columns, TSchemaTableType::SCH_PROCESSLIST) {}

SchemaProcessListScanner::~SchemaProcessListScanner() = default;

Status SchemaProcessListScanner::start(RuntimeState* state) {
    TShowProcessListRequest request;
    request.__set_show_full_sql(true);

    RETURN_IF_ERROR(SchemaHelper::show_process_list(*(_param->common_param->ip),
                                                    _param->common_param->port, request,
                                                    &_process_list_result));

    return Status::OK();
}

Status SchemaProcessListScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (block == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    if (_process_list_result.process_list.empty()) {
        return Status::OK();
    }

    return _fill_block_impl(block);
}

Status SchemaProcessListScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    const auto& process_list = _process_list_result.process_list;
    size_t row_num = process_list.size();
    if (row_num == 0) {
        return Status::OK();
    }

    for (size_t col_idx = 0; col_idx < _s_processlist_columns.size(); ++col_idx) {
        std::vector<StringRef> str_refs(row_num);
        std::vector<int128_t> int_vals(row_num);
        std::vector<void*> datas(row_num);
        std::vector<std::string> column_values(
                row_num); // Store the strings to ensure their lifetime

        for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
            const auto& row = process_list[row_idx];
            if (row.size() != _s_processlist_columns.size()) {
                return Status::InternalError(
                        "process list meet invalid schema, schema_size={}, input_data_size={}",
                        _s_processlist_columns.size(), row.size());
            }

            // Fetch and store the column value based on its index
            std::string& column_value =
                    column_values[row_idx]; // Reference to the actual string in the vector
            column_value = row[col_idx];

            if (_s_processlist_columns[col_idx].type == TYPE_LARGEINT ||
                _s_processlist_columns[col_idx].type == TYPE_INT) {
                try {
                    int128_t val = !column_value.empty() ? std::stoll(column_value) : 0;
                    int_vals[row_idx] = val;
                } catch (const std::exception& e) {
                    return Status::InternalError(
                            "process list meet invalid data, column={}, data={}, reason={}",
                            _s_processlist_columns[col_idx].name, column_value, e.what());
                }
                datas[row_idx] = &int_vals[row_idx];
            } else if (_s_processlist_columns[col_idx].type == TYPE_DATETIMEV2) {
                auto* dv = reinterpret_cast<DateV2Value<DateTimeV2ValueType>*>(&int_vals[row_idx]);
                if (!dv->from_date_str(column_value.data(), column_value.size(), -1,
                                       config::allow_zero_date)) {
                    return Status::InternalError(
                            "process list meet invalid data, column={}, data={}, reason={}",
                            _s_processlist_columns[col_idx].name, column_value);
                }
                datas[row_idx] = &int_vals[row_idx];
            } else {
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
