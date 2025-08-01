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

#include "exec/schema_scanner/schema_frontend_metrics_scanner.h"

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
#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaFrontendMetricsScanner::_s_frontend_metric_columns = {
        //   name,       type,          size,        is_null
        {"FE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"METRIC_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"METRIC_TYPE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"METRIC_VALUE", TYPE_DOUBLE, sizeof(double), false},
        {"TAG", TYPE_VARCHAR, sizeof(StringRef), true}
};

SchemaFrontendMetricsScanner::SchemaFrontendMetricsScanner()
        : SchemaScanner(_s_frontend_metric_columns, TSchemaTableType::SCH_FE_METRICS) {}

SchemaFrontendMetricsScanner::~SchemaFrontendMetricsScanner() = default;

Status SchemaFrontendMetricsScanner::start(RuntimeState* state) {
    TFetchFeMetricsRequest request;

    for (const auto& fe_addr : _param->common_param->fe_addr_list) {
        TFetchFeMetricsResult tmp_ret;
        RETURN_IF_ERROR(
                SchemaHelper::fetch_frontend_metrics(fe_addr.hostname, fe_addr.port, request, &tmp_ret));

        _metrics_list_result.metrics_list.insert(_metrics_list_result.metrics_list.end(),
                                                 tmp_ret.metrics_list.begin(),
                                                 tmp_ret.metrics_list.end());
    }

    return Status::OK();
}

Status SchemaFrontendMetricsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (block == nullptr || eos == nullptr) {
        return Status::InternalError("invalid parameter.");
    }

    *eos = true;
    if (_metrics_list_result.metrics_list.empty()) {
        return Status::OK();
    }

    return _fill_block_impl(block);
}

Status SchemaFrontendMetricsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    const auto& metrics_list = _metrics_list_result.metrics_list;
    size_t row_num = metrics_list.size();
    if (row_num == 0) {
        return Status::OK();
    }

    for (size_t col_idx = 0; col_idx < _s_frontend_metric_columns.size(); ++col_idx) {
        std::vector<StringRef> str_refs(row_num);
        std::vector<double> double_vals(row_num);
        std::vector<void*> datas(row_num);
        std::vector<std::string> column_values(
                row_num); // Store the strings to ensure their lifetime

        for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
            const auto& row = metrics_list[row_idx];
            if (row.size() != _s_frontend_metric_columns.size()) {
                return Status::InternalError(
                        "process list meet invalid schema, schema_size={}, input_data_size={}",
                        _s_frontend_metric_columns.size(), row.size());
            }

            // Fetch and store the column value based on its index
            std::string& column_value =
                    column_values[row_idx]; // Reference to the actual string in the vector
            column_value = row[col_idx];

            if (_s_frontend_metric_columns[col_idx].type == TYPE_DOUBLE) {
                try {
                    double val = !column_value.empty() ? std::stod(column_value) : 0;
                    double_vals[row_idx] = val;
                } catch (const std::exception& e) {
                    return Status::InternalError(
                            "process list meet invalid data, column={}, data={}, reason={}",
                            _s_frontend_metric_columns[col_idx].name, column_value, e.what());
                }
                datas[row_idx] = &double_vals[row_idx];
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
