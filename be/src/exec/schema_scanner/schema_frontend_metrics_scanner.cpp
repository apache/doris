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

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaFrontendMetricsScanner::_s_metric_columns = {
        {"METRIC", TYPE_STRING, sizeof(StringRef), true},
        {"VALUE", TYPE_STRING, sizeof(StringRef), true},
        {"DESCRIPTION", TYPE_STRING, sizeof(StringRef), true},
};

SchemaFrontendMetricsScanner::SchemaFrontendMetricsScanner()
        : SchemaScanner(_s_metric_columns, TSchemaTableType::SCH_TABLE_OPTIONS) {}

Status SchemaFrontendMetricsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }

    TGetFrontendMetricsRequest request;
    RETURN_IF_ERROR(SchemaHelper::get_frontend_metrics(
            *(_param->common_param->ip), _param->common_param->port, request, &_metrics_result));
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
    if (_metrics_result.metrics.empty()) {
        return Status::OK();
    }

    return _fill_block_impl(block);
}

Status SchemaFrontendMetricsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);

    const auto& metrics = _metrics_result.metrics;
    size_t row_num = metrics.size();
    if (row_num == 0) {
        return Status::OK();
    }

    for (size_t col_idx = 0; col_idx < _s_metric_columns.size(); ++col_idx) {
        std::vector<StringRef> str_refs(row_num);
        std::vector<int64_t> int_vals(row_num);
        std::vector<void*> datas(row_num);
        std::vector<std::string> column_values(row_num);

        for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
            const auto& metric_info = metrics[row_idx];
            std::string& column_value = column_values[row_idx];

            switch (col_idx) {
            case 0: // METRICS
                column_value = metric_info.__isset.metric ? metric_info.metric : "";
                break;
            case 1: // VALUE
                column_value = metric_info.__isset.value ? metric_info.value : "";
                break;
            case 2: // DESCRIPTION
                column_value = metric_info.__isset.description ? metric_info.description : "";
                break;
            }

            str_refs[row_idx] =
                    StringRef(column_values[row_idx].data(), column_values[row_idx].size());
            datas[row_idx] = &str_refs[row_idx];
        }

        RETURN_IF_ERROR(fill_dest_column_for_range(block, col_idx, datas));
    }

    return Status::OK();
}

} // namespace doris
