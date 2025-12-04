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

#include "exec/schema_scanner/schema_view_dependency_scanner.h"

#include <gen_cpp/FrontendService_types.h>

#include <vector>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaViewDependencyScanner::_s_view_dependency_columns = {
        {"VIEW_CATALOG", TYPE_VARCHAR, sizeof(StringRef), false},
        {"VIEW_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), false},
        {"VIEW_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"VIEW_TYPE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"REF_CATALOG", TYPE_VARCHAR, sizeof(StringRef), false},
        {"REF_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), false},
        {"REF_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"REF_TYPE", TYPE_VARCHAR, sizeof(StringRef), false}};

SchemaViewDependencyScanner::SchemaViewDependencyScanner()
        : SchemaScanner(_s_view_dependency_columns, TSchemaTableType::SCH_VIEW_DEPENDENCY) {}

SchemaViewDependencyScanner::~SchemaViewDependencyScanner() = default;

Status SchemaViewDependencyScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    _rpc_timeout = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaViewDependencyScanner::_get_view_dependency_block_from_fe() {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;

    TSchemaTableRequestParams schema_table_request_params;
    for (int i = 0; i < _s_view_dependency_columns.size(); i++) {
        schema_table_request_params.__isset.columns_name = true;
        schema_table_request_params.columns_name.emplace_back(_s_view_dependency_columns[i].name);
    }
    schema_table_request_params.__set_current_user_ident(*_param->common_param->current_user_ident);
    schema_table_request_params.__set_frontend_conjuncts(*_param->common_param->frontend_conjuncts);
    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::VIEW_DEPENDENCY);
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
        LOG(WARNING) << "fetch transactions from FE failed, errmsg=" << status;
        return status;
    }
    std::vector<TRow> result_data = result.data_batch;

    _view_dependency_block = vectorized::Block::create_unique();
    for (int i = 0; i < _s_view_dependency_columns.size(); ++i) {
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                _s_view_dependency_columns[i].type, true);
        _view_dependency_block->insert(vectorized::ColumnWithTypeAndName(
                data_type->create_column(), data_type, _s_view_dependency_columns[i].name));
    }

    _view_dependency_block->reserve(_block_rows_limit);

    if (result_data.size() > 0) {
        auto col_size = result_data[0].column_value.size();
        if (col_size != _s_view_dependency_columns.size()) {
            return Status::InternalError<false>("transactions schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];
        for (int j = 0; j < _s_view_dependency_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j,
                                                _view_dependency_block.get(),
                                                _s_view_dependency_columns[j].type));
        }
    }
    return Status::OK();
}

Status SchemaViewDependencyScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_view_dependency_block == nullptr) {
        RETURN_IF_ERROR(_get_view_dependency_block_from_fe());
        _total_rows = (int)_view_dependency_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_view_dependency_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris
