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

#include "information_schema/schema_tso_status_scanner.h"

#include <gen_cpp/FrontendService_types.h>

#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaTsoStatusScanner::_s_tso_status_columns = {
        {"WINDOW_END_PHYSICAL_TIME", TYPE_BIGINT, sizeof(int64_t), true},
        {"CURRENT_TSO", TYPE_BIGINT, sizeof(int64_t), true},
        {"CURRENT_TSO_PHYSICAL_TIME", TYPE_BIGINT, sizeof(int64_t), true},
        {"CURRENT_TSO_LOGICAL_COUNTER", TYPE_BIGINT, sizeof(int64_t), true},
};

SchemaTsoStatusScanner::SchemaTsoStatusScanner()
        : SchemaScanner(_s_tso_status_columns, TSchemaTableType::SCH_TSO_STATUS) {}

SchemaTsoStatusScanner::~SchemaTsoStatusScanner() = default;

Status SchemaTsoStatusScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    _rpc_timeout_ms = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaTsoStatusScanner::_get_tso_status_block_from_fe() {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;

    TSchemaTableRequestParams schema_table_request_params;
    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::TSO_STATUS);
    request.__set_schema_table_params(schema_table_request_params);

    TFetchSchemaTableDataResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchSchemaTableData(result, request);
            },
            _rpc_timeout_ms));

    return _process_tso_status_result(result);
}

Status SchemaTsoStatusScanner::_process_tso_status_result(
        const TFetchSchemaTableDataResult& result) {
    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "fetch TSO status from FE failed, errmsg=" << status;
        return status;
    }

    _tso_status_block = Block::create_unique();
    for (int i = 0; i < _s_tso_status_columns.size(); ++i) {
        auto data_type =
                DataTypeFactory::instance().create_data_type(_s_tso_status_columns[i].type, true);
        _tso_status_block->insert(ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                        _s_tso_status_columns[i].name));
    }

    _tso_status_block->reserve(result.data_batch.size());
    for (const TRow& row : result.data_batch) {
        if (row.column_value.size() != _s_tso_status_columns.size()) {
            return Status::InternalError<false>(
                    "TSO status schema does not match between FE and BE");
        }
        for (int i = 0; i < _s_tso_status_columns.size(); ++i) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[i], i, _tso_status_block.get(),
                                                _s_tso_status_columns[i].type));
        }
    }
    _total_rows = static_cast<int>(_tso_status_block->rows());
    return Status::OK();
}

Status SchemaTsoStatusScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_tso_status_block == nullptr) {
        RETURN_IF_ERROR(_get_tso_status_block_from_fe());
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    ScopedMutableBlock scoped_mblock(block);
    auto& mblock = scoped_mblock.mutable_block();
    RETURN_IF_ERROR(mblock.add_rows(_tso_status_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris
