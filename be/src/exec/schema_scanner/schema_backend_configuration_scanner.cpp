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

#include "exec/schema_scanner/schema_backend_configuration_scanner.h"

#include <gen_cpp/Descriptors_types.h>

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaBackendConfigurationScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"CONFIGURATION", TYPE_STRING, sizeof(StringRef), true},
        {"CONFIGURATION_TYPE", TYPE_STRING, sizeof(StringRef), true},
        {"CONFIGURATION_VALUE", TYPE_STRING, sizeof(StringRef), true},
        {"IS_MUTABLE", TYPE_BOOLEAN, sizeof(bool), true}};

SchemaBackendConfigurationScanner::SchemaBackendConfigurationScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKEND_CONFIGURATION) {}

SchemaBackendConfigurationScanner::~SchemaBackendConfigurationScanner() {}

Status SchemaBackendConfigurationScanner::start(doris::RuntimeState* state) {
    _config_infos = config::get_config_info();
    return Status::OK();
}

Status SchemaBackendConfigurationScanner::get_next_block_internal(vectorized::Block* block,
                                                                  bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    if (_config_infos.empty()) {
        return Status::OK();
    }

    for (size_t col_idx = 0; col_idx < _s_tbls_columns.size(); ++col_idx) {
        size_t row_num = _config_infos.size();
        std::vector<StringRef> str_refs(row_num);
        std::vector<int8_t> bool_vals(row_num);
        std::vector<void*> datas(row_num);
        std::vector<std::string> column_values(row_num);

        for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
            const auto& row = _config_infos[row_idx];
            if (row.size() != _s_tbls_columns.size()) {
                return Status::InternalError(
                        "backend configs info meet invalid schema, schema_size={}, "
                        "input_data_size={}",
                        _config_infos.size(), row.size());
            }

            std::string& column_value =
                    column_values[row_idx]; // Reference to the actual string in the vector
            column_value = row[col_idx];
            if (_s_tbls_columns[col_idx].type == TYPE_BOOLEAN) {
                bool_vals[row_idx] = column_value == "true" ? 1 : 0;
                datas[row_idx] = &bool_vals[row_idx];
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