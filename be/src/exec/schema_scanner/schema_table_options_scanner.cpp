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

#include "exec/schema_scanner/schema_table_options_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
std::vector<SchemaScanner::ColumnDesc> SchemaTableOptionsScanner::_s_tbls_columns = {
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_MODEL", TYPE_STRING, sizeof(StringRef), true},
        {"TABLE_MODEL_KEY", TYPE_STRING, sizeof(StringRef), true},
        {"DISTRIBUTE_KEY", TYPE_STRING, sizeof(StringRef), true},
        {"DISTRIBUTE_TYPE", TYPE_STRING, sizeof(StringRef), true},
        {"BUCKETS_NUM", TYPE_INT, sizeof(int32_t), true},
        {"PARTITION_NUM", TYPE_INT, sizeof(int32_t), true},
};

SchemaTableOptionsScanner::SchemaTableOptionsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_TABLE_OPTIONS) {}

Status SchemaTableOptionsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }

    // first get the all the database specific to current catalog
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

Status SchemaTableOptionsScanner::get_onedb_info_from_fe(int64_t dbId) {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;

    TSchemaTableRequestParams schema_table_request_params;
    for (int i = 0; i < _s_tbls_columns.size(); i++) {
        schema_table_request_params.__isset.columns_name = true;
        schema_table_request_params.columns_name.emplace_back(_s_tbls_columns[i].name);
    }
    schema_table_request_params.__set_current_user_ident(*_param->common_param->current_user_ident);
    schema_table_request_params.__set_catalog(*_param->common_param->catalog);
    schema_table_request_params.__set_dbId(dbId);

    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::TABLE_OPTIONS);
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
        LOG(WARNING) << "fetch table options from FE failed, errmsg=" << status;
        return status;
    }
    std::vector<TRow> result_data = result.data_batch;

    _tableoptions_block = vectorized::Block::create_unique();
    for (int i = 0; i < _s_tbls_columns.size(); ++i) {
        TypeDescriptor descriptor(_s_tbls_columns[i].type);
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(descriptor, true);
        _tableoptions_block->insert(vectorized::ColumnWithTypeAndName(
                data_type->create_column(), data_type, _s_tbls_columns[i].name));
    }
    _tableoptions_block->reserve(_block_rows_limit);
    if (result_data.size() > 0) {
        int col_size = result_data[0].column_value.size();
        if (col_size != _s_tbls_columns.size()) {
            return Status::InternalError<false>("table options schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];
        for (int j = 0; j < _s_tbls_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j, _tableoptions_block.get(),
                                                _s_tbls_columns[j].type));
        }
    }
    return Status::OK();
}

bool SchemaTableOptionsScanner::check_and_mark_eos(bool* eos) const {
    if (_row_idx == _total_rows) {
        *eos = true;
        if (_db_index < _db_result.db_ids.size()) {
            *eos = false;
        }
        return true;
    }
    return false;
}

Status SchemaTableOptionsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if ((_tableoptions_block == nullptr) || (_row_idx == _total_rows)) {
        if (_db_index < _db_result.db_ids.size()) {
            RETURN_IF_ERROR(get_onedb_info_from_fe(_db_result.db_ids[_db_index]));
            _row_idx = 0; // reset row index so that it start filling for next block.
            _total_rows = _tableoptions_block->rows();
            _db_index++;
        }
    }

    if (check_and_mark_eos(eos)) {
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_tableoptions_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    if (!check_and_mark_eos(eos)) {
        *eos = false;
    }
    return Status::OK();
}

} // namespace doris
