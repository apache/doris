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

#include "information_schema/schema_processlist_scanner.h"

#include <gen_cpp/FrontendService_types.h>

#include <exception>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "information_schema/schema_helper.h"
#include "runtime/runtime_state.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaProcessListScanner::_s_processlist_columns = {
        {"CurrentConnected", TYPE_VARCHAR, sizeof(StringRef), false},       // 0
        {"Id", TYPE_LARGEINT, sizeof(int128_t), false},                     // 1
        {"User", TYPE_VARCHAR, sizeof(StringRef), false},                   // 2
        {"Host", TYPE_VARCHAR, sizeof(StringRef), false},                   // 3
        {"LoginTime", TYPE_DATETIMEV2, sizeof(DateTimeV2ValueType), false}, // 4
        {"Catalog", TYPE_VARCHAR, sizeof(StringRef), false},                // 5
        {"Db", TYPE_VARCHAR, sizeof(StringRef), false},                     // 6
        {"Command", TYPE_VARCHAR, sizeof(StringRef), false},                // 7
        {"Time", TYPE_INT, sizeof(int32_t), false},                         // 8
        {"State", TYPE_VARCHAR, sizeof(StringRef), false},                  // 9
        {"QueryId", TYPE_VARCHAR, sizeof(StringRef), false},                // 10
        {"TraceId", TYPE_VARCHAR, sizeof(StringRef), false},                // 11
        {"Info", TYPE_VARCHAR, sizeof(StringRef), false},                   // 12
        {"FE", TYPE_VARCHAR, sizeof(StringRef), false},                     // 13
        {"CloudCluster", TYPE_VARCHAR, sizeof(StringRef), false},           // 14
        {"Protocol", TYPE_VARCHAR, sizeof(StringRef), false}};              // 15

SchemaProcessListScanner::SchemaProcessListScanner()
        : SchemaScanner(_s_processlist_columns, TSchemaTableType::SCH_PROCESSLIST) {}

SchemaProcessListScanner::~SchemaProcessListScanner() = default;

// A row is the FE's SHOW PROCESSLIST row, read by position; an FE of another version sends
// another set of columns. Fit the row to this scanner's columns so _fill_block_impl can read it:
// a row from before the TraceId column (#51400) gets an empty TraceId at position 11; a row
// that ends before the columns added since (Protocol, position 15) is padded with empty
// strings; a row of a newer FE with columns this scanner does not know is cut to them.
void SchemaProcessListScanner::_fit_row_to_columns(std::vector<std::string>& row) {
    if (row.size() == 14) {
        row.insert(row.begin() + 11, "");
    }
    row.resize(_s_processlist_columns.size());
}

Status SchemaProcessListScanner::start(RuntimeState* state) {
    TShowProcessListRequest request;
    request.__set_show_full_sql(true);
    request.__set_time_zone(state->timezone());
    request.__set_current_user_ident(*_param->common_param->current_user_ident);

    for (const auto& fe_addr : _param->common_param->fe_addr_list) {
        TShowProcessListResult tmp_ret;
        RETURN_IF_ERROR(
                SchemaHelper::show_process_list(fe_addr.hostname, fe_addr.port, request, &tmp_ret));

        for (auto& row : tmp_ret.process_list) {
            _fit_row_to_columns(row);
        }

        _process_list_result.process_list.insert(_process_list_result.process_list.end(),
                                                 tmp_ret.process_list.begin(),
                                                 tmp_ret.process_list.end());
    }

    return Status::OK();
}

Status SchemaProcessListScanner::get_next_block_internal(Block* block, bool* eos) {
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

Status SchemaProcessListScanner::_fill_block_impl(Block* block) {
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
                CastParameters params;
                if (!CastToDatetimeV2::from_string_non_strict_mode(
                            {column_value.data(), column_value.size()}, *dv, nullptr, -1, params)) {
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
