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

#include "information_schema/schema_backend_ms_rpc_table_throttlers_scanner.h"

#include <gen_cpp/Descriptors_types.h>

#include <string>

#include "cloud/cloud_throttle_state_machine.h"
#include "core/block/block.h"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaBackendMsRpcTableThrottlersScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"TABLE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"RPC_TYPE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"QPS_LIMIT", TYPE_DOUBLE, sizeof(double), false},
        {"CURRENT_QPS", TYPE_DOUBLE, sizeof(double), false},
};

SchemaBackendMsRpcTableThrottlersScanner::SchemaBackendMsRpcTableThrottlersScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_BACKEND_MS_RPC_TABLE_THROTTLERS),
          _backend_id(ExecEnv::GetInstance()->cluster_info()->backend_id) {}

SchemaBackendMsRpcTableThrottlersScanner::~SchemaBackendMsRpcTableThrottlersScanner() = default;

Status SchemaBackendMsRpcTableThrottlersScanner::start(RuntimeState* state) {
    auto* throttler = ExecEnv::GetInstance()->table_rpc_throttler();
    if (throttler) {
        _entries = throttler->get_all_throttled_entries();
    }
    return Status::OK();
}

Status SchemaBackendMsRpcTableThrottlersScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    if (_entries.empty()) {
        return Status::OK();
    }

    auto* registry = ExecEnv::GetInstance()->table_rpc_qps_registry();

    size_t row_num = _entries.size();
    for (size_t col_idx = 0; col_idx < _s_tbls_columns.size(); ++col_idx) {
        std::vector<StringRef> str_refs(row_num);
        std::vector<double> double_vals(row_num);
        std::vector<void*> datas(row_num);
        std::vector<std::string> rpc_names(row_num);

        for (size_t row_idx = 0; row_idx < row_num; ++row_idx) {
            const auto& entry = _entries[row_idx];
            switch (col_idx) {
            case 0: // BE_ID
                datas[row_idx] = &_backend_id;
                break;
            case 1: // TABLE_ID
                datas[row_idx] = const_cast<int64_t*>(&entry.table_id);
                break;
            case 2: { // RPC_TYPE
                rpc_names[row_idx] = std::string(cloud::load_related_rpc_name(entry.rpc_type));
                str_refs[row_idx] = StringRef(rpc_names[row_idx].data(), rpc_names[row_idx].size());
                datas[row_idx] = &str_refs[row_idx];
                break;
            }
            case 3: // QPS_LIMIT
                double_vals[row_idx] = entry.qps_limit;
                datas[row_idx] = &double_vals[row_idx];
                break;
            case 4: // CURRENT_QPS
                double_vals[row_idx] =
                        registry ? registry->get_qps(entry.rpc_type, entry.table_id) : 0;
                datas[row_idx] = &double_vals[row_idx];
                break;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, col_idx, datas));
    }
    return Status::OK();
}

} // namespace doris
