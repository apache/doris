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

#include "information_schema/schema_per_db_scanner.h"

#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/string_ref.h"
#include "information_schema/schema_helper.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace doris {

SchemaPerDbScanner::SchemaPerDbScanner(const std::vector<SchemaScanner::ColumnDesc>& columns,
                                       TSchemaTableType::type type,
                                       TSchemaTableName::type request_name,
                                       std::string display_name)
        : SchemaScanner(columns, type),
          _request_name(request_name),
          _display_name(std::move(display_name)) {}

SchemaPerDbScanner::~SchemaPerDbScanner() = default;

Status SchemaPerDbScanner::start(RuntimeState* state) {
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
    add_extra_db_params(&db_params);

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

Status SchemaPerDbScanner::get_onedb_info_from_fe(int64_t db_id) {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;

    TSchemaTableRequestParams schema_table_request_params;
    const std::vector<SchemaScanner::ColumnDesc>& columns = get_column_desc();
    for (const auto& column : columns) {
        schema_table_request_params.__isset.columns_name = true;
        schema_table_request_params.columns_name.emplace_back(column.name);
    }
    schema_table_request_params.__set_current_user_ident(*_param->common_param->current_user_ident);
    schema_table_request_params.__set_catalog(*_param->common_param->catalog);
    schema_table_request_params.__set_dbId(db_id);
    add_extra_request_params(&schema_table_request_params);

    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(_request_name);
    request.__set_schema_table_params(schema_table_request_params);

    TFetchSchemaTableDataResult result;
    RETURN_IF_ERROR(SchemaHelper::fetch_schema_table_data(master_addr.hostname, master_addr.port,
                                                          request, &result, _rpc_timeout_ms));
    return fill_block_from_result(result);
}

Status SchemaPerDbScanner::fill_block_from_result(TFetchSchemaTableDataResult& result) {
    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "fetch " << _display_name << " from FE failed, errmsg=" << status;
        return status;
    }
    const std::vector<SchemaScanner::ColumnDesc>& columns = get_column_desc();
    std::vector<TRow> result_data = result.data_batch;

    _fetched_block = Block::create_unique();
    for (const auto& column : columns) {
        auto data_type = DataTypeFactory::instance().create_data_type(column.type, true);
        _fetched_block->insert(
                ColumnWithTypeAndName(data_type->create_column(), data_type, column.name));
    }
    _fetched_block->reserve(_block_rows_limit);
    if (!result_data.empty() && result_data[0].column_value.size() != columns.size()) {
        return Status::InternalError<false>("{} schema is not match for FE and BE", _display_name);
    }

    for (auto& row : result_data) {
        for (int j = 0; j < (int)columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j, _fetched_block.get(),
                                                columns[j].type));
        }
    }
    return Status::OK();
}

bool SchemaPerDbScanner::check_and_mark_eos(bool* eos) const {
    if (_row_idx == _total_rows) {
        *eos = true;
        if (_db_index < _db_result.db_ids.size()) {
            *eos = false;
        }
        return true;
    }
    return false;
}

Status SchemaPerDbScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    SCOPED_TIMER(_fill_block_timer);

    if ((_fetched_block == nullptr) || (_row_idx == _total_rows)) {
        if (_db_index < _db_result.db_ids.size()) {
            RETURN_IF_ERROR(get_onedb_info_from_fe(_db_result.db_ids[_db_index]));
            _row_idx = 0; // reset row index so that it starts filling the next block.
            _total_rows = (int)_fetched_block->rows();
            _db_index++;
        }
    }

    if (check_and_mark_eos(eos)) {
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    ScopedMutableBlock scoped_mblock(block);
    auto& mblock = scoped_mblock.mutable_block();
    RETURN_IF_ERROR(mblock.add_rows(_fetched_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    if (!check_and_mark_eos(eos)) {
        *eos = false;
    }
    return Status::OK();
}

} // namespace doris
