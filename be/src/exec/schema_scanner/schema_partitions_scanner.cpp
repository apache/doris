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

#include "exec/schema_scanner/schema_partitions_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <stdint.h>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaPartitionsScanner::_s_tbls_columns = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"PARTITION_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SUBPARTITION_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PARTITION_ORDINAL_POSITION", TYPE_INT, sizeof(int32_t), true},
        {"SUBPARTITION_ORDINAL_POSITION", TYPE_INT, sizeof(int32_t), true},
        {"PARTITION_METHOD", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SUBPARTITION_METHOD", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PARTITION_EXPRESSION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SUBPARTITION_EXPRESSION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"PARTITION_DESCRIPTION", TYPE_STRING, sizeof(StringRef), true},
        {"TABLE_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"AVG_ROW_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAX_DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"INDEX_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_FREE", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_TIME", TYPE_BIGINT, sizeof(int64_t), false},
        {"UPDATE_TIME", TYPE_DATETIME, sizeof(int128_t), true},
        {"CHECK_TIME", TYPE_DATETIME, sizeof(int128_t), true},
        {"CHECKSUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"PARTITION_COMMENT", TYPE_STRING, sizeof(StringRef), false},
        {"NODEGROUP", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLESPACE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
};

SchemaPartitionsScanner::SchemaPartitionsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_PARTITIONS) {}

SchemaPartitionsScanner::~SchemaPartitionsScanner() {}

Status SchemaPartitionsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    SCOPED_TIMER(_get_db_timer);
    TGetDbsParams db_params;
    if (_param->common_param->db) {
        db_params.__set_pattern(*(_param->common_param->db));
    }
    if (_param->common_param->catalog) {
        db_params.__set_catalog(*(_param->common_param->catalog));
    }
    if (_param->common_param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->common_param->current_user_ident));
    }

    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_names(
                *(_param->common_param->ip), _param->common_param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _block_rows_limit = state->batch_size();
    _rpc_timeout_ms = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaPartitionsScanner::get_onedb_info_from_fe(int64_t dbId) {
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
    request.__set_schema_table_name(TSchemaTableName::PARTITIONS);
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

    _partitions_block = vectorized::Block::create_unique();
    for (int i = 0; i < _s_tbls_columns.size(); ++i) {
        TypeDescriptor descriptor(_s_tbls_columns[i].type);
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(descriptor, true);
        _partitions_block->insert(vectorized::ColumnWithTypeAndName(
                data_type->create_column(), data_type, _s_tbls_columns[i].name));
    }
    _partitions_block->reserve(_block_rows_limit);
    if (result_data.size() > 0) {
        int col_size = result_data[0].column_value.size();
        if (col_size != _s_tbls_columns.size()) {
            return Status::InternalError<false>("table options schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];
        for (int j = 0; j < _s_tbls_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j, _partitions_block.get(),
                                                _s_tbls_columns[j].type));
        }
    }
    return Status::OK();
}

bool SchemaPartitionsScanner::check_and_mark_eos(bool* eos) const {
    if (_row_idx == _total_rows) {
        *eos = true;
        if (_db_index < _db_result.db_ids.size()) {
            *eos = false;
        }
        return true;
    }
    return false;
}

Status SchemaPartitionsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if ((_partitions_block == nullptr) || (_row_idx == _total_rows)) {
        if (_db_index < _db_result.db_ids.size()) {
            RETURN_IF_ERROR(get_onedb_info_from_fe(_db_result.db_ids[_db_index]));
            _row_idx = 0; // reset row index so that it start filling for next block.
            _total_rows = _partitions_block->rows();
            _db_index++;
        }
    }

    if (check_and_mark_eos(eos)) {
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_partitions_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    if (!check_and_mark_eos(eos)) {
        *eos = false;
    }
    return Status::OK();
}

} // namespace doris
