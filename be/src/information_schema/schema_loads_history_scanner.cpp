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

#include "information_schema/schema_loads_history_scanner.h"

#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/string_ref.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

// Must stay aligned with information_schema.loads_history (SchemaTable.java) and the loads table:
// same 20 columns, same order, all strings.
std::vector<SchemaScanner::ColumnDesc> SchemaLoadsHistoryScanner::_s_tbls_columns = {
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

SchemaLoadsHistoryScanner::SchemaLoadsHistoryScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_LOADS_HISTORY) {}

SchemaLoadsHistoryScanner::~SchemaLoadsHistoryScanner() {}

Status SchemaLoadsHistoryScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    _rpc_timeout = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaLoadsHistoryScanner::_get_loads_history_block_from_fe() {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;

    TSchemaTableRequestParams schema_table_params;
    for (int i = 0; i < _s_tbls_columns.size(); i++) {
        schema_table_params.__isset.columns_name = true;
        schema_table_params.columns_name.emplace_back(_s_tbls_columns[i].name);
    }
    schema_table_params.__set_time_zone(_timezone);

    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::LOADS_HISTORY);
    request.__set_schema_table_params(schema_table_params);

    TFetchSchemaTableDataResult result;

    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchSchemaTableData(result, request);
            },
            _rpc_timeout));

    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "fetch loads history from FE failed, errmsg=" << status;
        return status;
    }
    std::vector<TRow> result_data = result.data_batch;

    _loads_history_block = Block::create_unique();
    for (int i = 0; i < _s_tbls_columns.size(); ++i) {
        auto data_type =
                DataTypeFactory::instance().create_data_type(_s_tbls_columns[i].type, true);
        _loads_history_block->insert(ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                           _s_tbls_columns[i].name));
    }

    _loads_history_block->reserve(_block_rows_limit);

    if (result_data.size() > 0) {
        auto col_size = result_data[0].column_value.size();
        if (col_size != _s_tbls_columns.size()) {
            return Status::InternalError<false>("loads history schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];
        for (int j = 0; j < _s_tbls_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j, _loads_history_block.get(),
                                                _s_tbls_columns[j].type));
        }
    }
    return Status::OK();
}

Status SchemaLoadsHistoryScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_loads_history_block == nullptr) {
        RETURN_IF_ERROR(_get_loads_history_block_from_fe());
        _total_rows = (int)_loads_history_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    ScopedMutableBlock scoped_mblock(block);
    auto& mblock = scoped_mblock.mutable_block();
    RETURN_IF_ERROR(mblock.add_rows(_loads_history_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris
