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

#include "information_schema/schema_plugins_scanner.h"

#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/string_ref.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaPluginsScanner::_s_tbls_columns = {
        {"PLUGIN_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PLUGIN_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PLUGIN_VERSION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SOURCE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"DESCRIPTION", TYPE_STRING, sizeof(StringRef), true},
};

SchemaPluginsScanner::SchemaPluginsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_PLUGINS) {}

Status SchemaPluginsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    _block_rows_limit = state->batch_size();
    _rpc_timeout_ms = state->execution_timeout() * 1000;
    // Plugins are per-FE local state: ask the FE this session is connected to.
    _fe_addr = state->get_query_ctx()->current_connect_fe;
    return Status::OK();
}

Status SchemaPluginsScanner::_get_plugins_block_from_fe() {
    TSchemaTableRequestParams schema_table_request_params;
    for (int i = 0; i < _s_tbls_columns.size(); i++) {
        schema_table_request_params.__isset.columns_name = true;
        schema_table_request_params.columns_name.emplace_back(_s_tbls_columns[i].name);
    }
    schema_table_request_params.__set_current_user_ident(*_param->common_param->current_user_ident);

    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::PLUGINS);
    request.__set_schema_table_params(schema_table_request_params);

    TFetchSchemaTableDataResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            _fe_addr.hostname, _fe_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchSchemaTableData(result, request);
            },
            _rpc_timeout_ms));

    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "fetch plugins from FE(" << _fe_addr.hostname
                     << ") failed, errmsg=" << status;
        return status;
    }

    _plugins_block = Block::create_unique();
    for (int i = 0; i < _s_tbls_columns.size(); ++i) {
        auto data_type =
                DataTypeFactory::instance().create_data_type(_s_tbls_columns[i].type, true);
        _plugins_block->insert(ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                     _s_tbls_columns[i].name));
    }
    _plugins_block->reserve(_block_rows_limit);

    std::vector<TRow> result_data = std::move(result.data_batch);
    if (!result_data.empty()) {
        auto col_size = result_data[0].column_value.size();
        if (col_size != _s_tbls_columns.size()) {
            return Status::InternalError<false>("plugins schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        const TRow& row = result_data[i];
        for (int j = 0; j < _s_tbls_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j, _plugins_block.get(),
                                                _s_tbls_columns[j].type));
        }
    }
    return Status::OK();
}

Status SchemaPluginsScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_plugins_block == nullptr) {
        RETURN_IF_ERROR(_get_plugins_block_from_fe());
        _total_rows = static_cast<int>(_plugins_block->rows());
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    ScopedMutableBlock scoped_mblock(block);
    auto& mblock = scoped_mblock.mutable_block();
    RETURN_IF_ERROR(mblock.add_rows(_plugins_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris
