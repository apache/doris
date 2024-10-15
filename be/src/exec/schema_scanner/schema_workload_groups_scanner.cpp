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

#include "exec/schema_scanner/schema_workload_groups_scanner.h"

#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
std::vector<SchemaScanner::ColumnDesc> SchemaWorkloadGroupsScanner::_s_tbls_columns = {
        {"ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CPU_SHARE", TYPE_BIGINT, sizeof(int64_t), true},
        {"MEMORY_LIMIT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ENABLE_MEMORY_OVERCOMMIT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"MAX_CONCURRENCY", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAX_QUEUE_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"QUEUE_TIMEOUT", TYPE_BIGINT, sizeof(int64_t), true},
        {"CPU_HARD_LIMIT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SCAN_THREAD_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAX_REMOTE_SCAN_THREAD_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"MIN_REMOTE_SCAN_THREAD_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"SPILL_THRESHOLD_LOW_WATERMARK", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SPILL_THRESHOLD_HIGH_WATERMARK", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TAG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"READ_BYTES_PER_SECOND", TYPE_BIGINT, sizeof(int64_t), true},
        {"REMOTE_READ_BYTES_PER_SECOND", TYPE_BIGINT, sizeof(int64_t), true},
};

SchemaWorkloadGroupsScanner::SchemaWorkloadGroupsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_WORKLOAD_GROUPS) {}

SchemaWorkloadGroupsScanner::~SchemaWorkloadGroupsScanner() {}

Status SchemaWorkloadGroupsScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    _rpc_timeout = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaWorkloadGroupsScanner::_get_workload_groups_block_from_fe() {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;

    TSchemaTableRequestParams schema_table_request_params;
    for (int i = 0; i < _s_tbls_columns.size(); i++) {
        schema_table_request_params.__isset.columns_name = true;
        schema_table_request_params.columns_name.emplace_back(_s_tbls_columns[i].name);
    }
    schema_table_request_params.__set_current_user_ident(*_param->common_param->current_user_ident);

    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::WORKLOAD_GROUPS);
    request.__set_schema_table_params(schema_table_request_params);

    TFetchSchemaTableDataResult result;

    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchSchemaTableData(result, request);
            },
            _rpc_timeout));

    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "fetch workload groups from FE failed, errmsg=" << status;
        return status;
    }
    std::vector<TRow> result_data = result.data_batch;

    _workload_groups_block = vectorized::Block::create_unique();
    for (int i = 0; i < _s_tbls_columns.size(); ++i) {
        TypeDescriptor descriptor(_s_tbls_columns[i].type);
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(descriptor, true);
        _workload_groups_block->insert(vectorized::ColumnWithTypeAndName(
                data_type->create_column(), data_type, _s_tbls_columns[i].name));
    }

    _workload_groups_block->reserve(_block_rows_limit);

    if (result_data.size() > 0) {
        int col_size = result_data[0].column_value.size();
        if (col_size != _s_tbls_columns.size()) {
            return Status::InternalError<false>(
                    "workload groups schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];

        for (int j = 0; j < _s_tbls_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(
                    row.column_value[j], j, _workload_groups_block.get(), _s_tbls_columns[j].type));
        }
    }
    return Status::OK();
}

Status SchemaWorkloadGroupsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_workload_groups_block == nullptr) {
        RETURN_IF_ERROR(_get_workload_groups_block_from_fe());
        _total_rows = _workload_groups_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_workload_groups_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris