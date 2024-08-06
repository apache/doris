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

#include "exec/schema_scanner/schema_active_queries_scanner.h"

#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
std::vector<SchemaScanner::ColumnDesc> SchemaActiveQueriesScanner::_s_tbls_columns = {
        //   name,       type,          size
        {"QUERY_ID", TYPE_VARCHAR, sizeof(StringRef), true},
        {"QUERY_START_TIME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"QUERY_TIME_MS", TYPE_BIGINT, sizeof(int64_t), true},
        {"WORKLOAD_GROUP_ID", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATABASE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"FRONTEND_INSTANCE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"QUEUE_START_TIME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"QUEUE_END_TIME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"QUERY_STATUS", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SQL", TYPE_STRING, sizeof(StringRef), true}};

SchemaActiveQueriesScanner::SchemaActiveQueriesScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_ACTIVE_QUERIES) {}

SchemaActiveQueriesScanner::~SchemaActiveQueriesScanner() {}

Status SchemaActiveQueriesScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    _rpc_timeout = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaActiveQueriesScanner::_get_active_queries_block_from_fe() {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;

    TSchemaTableRequestParams schema_table_params;
    for (int i = 0; i < _s_tbls_columns.size(); i++) {
        schema_table_params.__isset.columns_name = true;
        schema_table_params.columns_name.emplace_back(_s_tbls_columns[i].name);
    }
    schema_table_params.replay_to_other_fe = true;
    schema_table_params.__isset.replay_to_other_fe = true;

    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::ACTIVE_QUERIES);
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
        LOG(WARNING) << "fetch active queries from FE failed, errmsg=" << status;
        return status;
    }
    std::vector<TRow> result_data = result.data_batch;

    _active_query_block = vectorized::Block::create_unique();
    for (int i = 0; i < _s_tbls_columns.size(); ++i) {
        TypeDescriptor descriptor(_s_tbls_columns[i].type);
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(descriptor, true);
        _active_query_block->insert(vectorized::ColumnWithTypeAndName(
                data_type->create_column(), data_type, _s_tbls_columns[i].name));
    }

    _active_query_block->reserve(_block_rows_limit);

    if (result_data.size() > 0) {
        int col_size = result_data[0].column_value.size();
        if (col_size != _s_tbls_columns.size()) {
            return Status::InternalError<false>("active queries schema is not match for FE and BE");
        }
    }

    // todo(wb) reuse this callback function
    auto insert_string_value = [&](int col_index, std::string str_val, vectorized::Block* block) {
        vectorized::MutableColumnPtr mutable_col_ptr;
        mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
        auto* nullable_column =
                reinterpret_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
        vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
        reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(str_val.data(),
                                                                          str_val.size());
        nullable_column->get_null_map_data().emplace_back(0);
    };
    auto insert_int_value = [&](int col_index, int64_t int_val, vectorized::Block* block) {
        vectorized::MutableColumnPtr mutable_col_ptr;
        mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
        auto* nullable_column =
                reinterpret_cast<vectorized::ColumnNullable*>(mutable_col_ptr.get());
        vectorized::IColumn* col_ptr = &nullable_column->get_nested_column();
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(
                int_val);
        nullable_column->get_null_map_data().emplace_back(0);
    };

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];

        insert_string_value(0, row.column_value[0].stringVal, _active_query_block.get());
        insert_string_value(1, row.column_value[1].stringVal, _active_query_block.get());
        insert_int_value(2, row.column_value[2].longVal, _active_query_block.get());
        insert_int_value(3, row.column_value[3].longVal, _active_query_block.get());
        insert_string_value(4, row.column_value[4].stringVal, _active_query_block.get());
        insert_string_value(5, row.column_value[5].stringVal, _active_query_block.get());
        insert_string_value(6, row.column_value[6].stringVal, _active_query_block.get());
        insert_string_value(7, row.column_value[7].stringVal, _active_query_block.get());
        insert_string_value(8, row.column_value[8].stringVal, _active_query_block.get());
        insert_string_value(9, row.column_value[9].stringVal, _active_query_block.get());
    }
    return Status::OK();
}

Status SchemaActiveQueriesScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_active_query_block == nullptr) {
        RETURN_IF_ERROR(_get_active_queries_block_from_fe());
        _total_rows = _active_query_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_active_query_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris