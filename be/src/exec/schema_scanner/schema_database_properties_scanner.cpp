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

#include "exec/schema_scanner/schema_database_properties_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaDatabasePropertiesScanner::_s_tbls_columns = {
        {"CATALOG_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SCHEMA_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PROPERTY_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"PROPERTY_VALUE", TYPE_STRING, sizeof(StringRef), true},
};

SchemaDatabasePropertiesScanner::SchemaDatabasePropertiesScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_DATABASE_PROPERTIES) {}

Status SchemaDatabasePropertiesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    SCOPED_TIMER(_get_db_timer);
    TGetDbsParams db_params;
    if (_param->common_param->catalog) {
        db_params.__set_catalog(*(_param->common_param->catalog));
    }
    if (_param->common_param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->common_param->current_user_ident));
    }
    if (_param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_names(
                *(_param->common_param->ip), _param->common_param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _block_rows_limit = state->batch_size();
    _rpc_timeout_ms = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaDatabasePropertiesScanner::get_onedb_info_from_fe(int64_t dbId) {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;

    TSchemaTableRequestParams schema_table_request_params;
    for (int i = 0; i < _s_tbls_columns.size(); i++) {
        schema_table_request_params.__isset.columns_name = true;
        schema_table_request_params.columns_name.emplace_back(_s_tbls_columns[i].name);
    }
    schema_table_request_params.__set_current_user_ident(*_param->common_param->current_user_ident);
    schema_table_request_params.__set_catalog(*_param->common_param->catalog);
    schema_table_request_params.__set_dbId(dbId);

    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::DATABASE_PROPERTIES);
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
        return status;
    }
    std::vector<TRow> result_data = result.data_batch;

    _dbproperties_block = vectorized::Block::create_unique();
    for (int i = 0; i < _s_tbls_columns.size(); ++i) {
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                _s_tbls_columns[i].type, true);
        _dbproperties_block->insert(vectorized::ColumnWithTypeAndName(
                data_type->create_column(), data_type, _s_tbls_columns[i].name));
    }
    _dbproperties_block->reserve(_block_rows_limit);
    if (result_data.size() > 0) {
        auto col_size = result_data[0].column_value.size();
        if (col_size != _s_tbls_columns.size()) {
            return Status::InternalError<false>(
                    "database properties schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];
        for (int j = 0; j < _s_tbls_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j, _dbproperties_block.get(),
                                                _s_tbls_columns[j].type));
        }
    }
    return Status::OK();
}

bool SchemaDatabasePropertiesScanner::check_and_mark_eos(bool* eos) const {
    if (_row_idx == _total_rows) {
        *eos = true;
        if (_db_index < _db_result.db_ids.size()) {
            *eos = false;
        }
        return true;
    }
    return false;
}

Status SchemaDatabasePropertiesScanner::get_next_block_internal(vectorized::Block* block,
                                                                bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if ((_dbproperties_block == nullptr) || (_row_idx == _total_rows)) {
        if (_db_index < _db_result.db_ids.size()) {
            RETURN_IF_ERROR(get_onedb_info_from_fe(_db_result.db_ids[_db_index]));
            _row_idx = 0;
            _total_rows = (int)_dbproperties_block->rows();
            _db_index++;
        }
    }
    if (check_and_mark_eos(eos)) {
        return Status::OK();
    }
    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_dbproperties_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;
    if (!check_and_mark_eos(eos)) {
        *eos = false;
    }
    return Status::OK();
}

} // namespace doris
