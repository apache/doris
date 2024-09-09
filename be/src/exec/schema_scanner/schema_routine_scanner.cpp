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

#include "exec/schema_scanner/schema_routine_scanner.h"

#include "exec/schema_scanner/schema_scanner_helper.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
std::vector<SchemaScanner::ColumnDesc> SchemaRoutinesScanner::_s_tbls_columns = {
        {"SPECIFIC_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"DTD_IDENTIFIER", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_BODY", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_DEFINITION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"EXTERNAL_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"EXTERNAL_LANGUAGE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PARAMETER_STYLE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"IS_DETERMINISTIC", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SQL_DATA_ACCESS", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SQL_PATH", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SECURITY_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CREATED", TYPE_DATETIME, sizeof(int64_t), true},
        {"LAST_ALTERED", TYPE_DATETIME, sizeof(int64_t), true},
        {"SQL_MODE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ROUTINE_COMMENT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"DEFINER", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CHARACTER_SET_CLIENT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COLLATION_CONNECTION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"DATABASE_COLLATION", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaRoutinesScanner::SchemaRoutinesScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_PROCEDURES) {}

Status SchemaRoutinesScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    _rpc_timeout = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaRoutinesScanner::get_block_from_fe() {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    TSchemaTableRequestParams schema_table_request_params;
    for (int i = 0; i < _s_tbls_columns.size(); i++) {
        schema_table_request_params.__isset.columns_name = true;
        schema_table_request_params.columns_name.emplace_back(_s_tbls_columns[i].name);
    }
    schema_table_request_params.__set_current_user_ident(*_param->common_param->current_user_ident);
    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::ROUTINES_INFO);
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
        LOG(WARNING) << "fetch routines from FE failed, errmsg=" << status;
        return status;
    }
    std::vector<TRow> result_data = result.data_batch;
    _routines_block = vectorized::Block::create_unique();
    for (int i = 0; i < _s_tbls_columns.size(); ++i) {
        TypeDescriptor descriptor(_s_tbls_columns[i].type);
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(descriptor, true);
        _routines_block->insert(vectorized::ColumnWithTypeAndName(
                data_type->create_column(), data_type, _s_tbls_columns[i].name));
    }
    _routines_block->reserve(_block_rows_limit);
    if (result_data.size() > 0) {
        int col_size = result_data[0].column_value.size();
        if (col_size != _s_tbls_columns.size()) {
            return Status::InternalError<false>("routine table schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];

        for (int j = 0; j < _s_tbls_columns.size(); j++) {
            if (_s_tbls_columns[j].type == TYPE_DATETIME) {
                std::vector<void*> datas(1);
                VecDateTimeValue src[1];
                src[0].from_date_str(row.column_value[j].stringVal.data(),
                                     row.column_value[j].stringVal.size());
                datas[0] = src;
                SchemaScannerHelper::insert_datetime_value(j, datas, _routines_block.get());
            } else {
                SchemaScannerHelper::insert_string_value(j, row.column_value[j].stringVal,
                                                         _routines_block.get());
            }
        }
    }
    return Status::OK();
}

Status SchemaRoutinesScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_routines_block == nullptr) {
        RETURN_IF_ERROR(get_block_from_fe());
        _total_rows = _routines_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_routines_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris