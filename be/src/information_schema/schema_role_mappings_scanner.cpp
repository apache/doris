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

#include "information_schema/schema_role_mappings_scanner.h"

#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/string_ref.h"
#include "information_schema/schema_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace doris {
#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaRoleMappingsScanner::_s_tbls_columns = {
        {"NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"INTEGRATION_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"RULES", TYPE_STRING, sizeof(StringRef), true},
        {"COMMENT", TYPE_STRING, sizeof(StringRef), true},
        {"CREATE_USER", TYPE_STRING, sizeof(StringRef), true},
        {"CREATE_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"ALTER_USER", TYPE_STRING, sizeof(StringRef), true},
        {"MODIFY_TIME", TYPE_STRING, sizeof(StringRef), true},
};

SchemaRoleMappingsScanner::SchemaRoleMappingsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_ROLE_MAPPINGS) {}

Status SchemaRoleMappingsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    _block_rows_limit = state->batch_size();
    _rpc_timeout_ms = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaRoleMappingsScanner::_get_role_mappings_block_from_fe() {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;

    TSchemaTableRequestParams schema_table_request_params;
    for (int i = 0; i < _s_tbls_columns.size(); i++) {
        schema_table_request_params.__isset.columns_name = true;
        schema_table_request_params.columns_name.emplace_back(_s_tbls_columns[i].name);
    }
    schema_table_request_params.__set_current_user_ident(*_param->common_param->current_user_ident);
    if (_param->common_param->frontend_conjuncts) {
        schema_table_request_params.__set_frontend_conjuncts(
                *_param->common_param->frontend_conjuncts);
    }

    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::ROLE_MAPPINGS);
    request.__set_schema_table_params(schema_table_request_params);

    TFetchSchemaTableDataResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchSchemaTableData(result, request);
            },
            _rpc_timeout_ms));

    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "fetch role mappings from FE failed, errmsg=" << status;
        return status;
    }

    _role_mappings_block = Block::create_unique();
    for (int i = 0; i < _s_tbls_columns.size(); ++i) {
        auto data_type =
                DataTypeFactory::instance().create_data_type(_s_tbls_columns[i].type, true);
        _role_mappings_block->insert(ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                           _s_tbls_columns[i].name));
    }
    _role_mappings_block->reserve(_block_rows_limit);

    std::vector<TRow> result_data = std::move(result.data_batch);
    if (!result_data.empty()) {
        auto col_size = result_data[0].column_value.size();
        if (col_size != _s_tbls_columns.size()) {
            return Status::InternalError<false>("role mappings schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        const TRow& row = result_data[i];
        for (int j = 0; j < _s_tbls_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j, _role_mappings_block.get(),
                                                _s_tbls_columns[j].type));
        }
    }
    return Status::OK();
}

Status SchemaRoleMappingsScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_role_mappings_block == nullptr) {
        RETURN_IF_ERROR(_get_role_mappings_block_from_fe());
        _total_rows = static_cast<int>(_role_mappings_block->rows());
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    MutableBlock mblock = MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_role_mappings_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
