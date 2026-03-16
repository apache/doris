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

#include "information_schema/schema_streams_scanner.h"

#include <cstddef>

#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/define_primitive_type.h"
#include "core/string_ref.h"
#include "gen_cpp/FrontendService_types.h"
#include "information_schema/schema_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaStreamsScanner::_s_streams_columns = {
        {"STREAM_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"STREAM_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CONSUME_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"STREAM_COMMENT", TYPE_STRING, sizeof(StringRef), true},
        {"DB_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"BASE_TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"BASE_TABLE_DB", TYPE_VARCHAR, sizeof(StringRef), true},
        {"BASE_TABLE_CTL", TYPE_VARCHAR, sizeof(StringRef), true},
        {"BASE_TABLE_TYPE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"ENABLED", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"IS_STALE", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"STALE_REASON", TYPE_STRING, sizeof(StringRef), true},
};

SchemaStreamsScanner::SchemaStreamsScanner()
        : SchemaScanner(_s_streams_columns, TSchemaTableType::SCH_STREAMS) {}

SchemaStreamsScanner::~SchemaStreamsScanner() = default;

Status SchemaStreamsScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    _rpc_timeout_ms = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaStreamsScanner::_get_streams_block_from_fe() {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;

    TSchemaTableRequestParams schema_table_request_params;
    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::STREAMS);
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
        LOG(WARNING) << "fetch transactions from FE failed, errmsg=" << status;
        return status;
    }
    std::vector<TRow> result_data = result.data_batch;

    _streams_block = Block::create_unique();
    for (int i = 0; i < _s_streams_columns.size(); ++i) {
        auto data_type =
                DataTypeFactory::instance().create_data_type(_s_streams_columns[i].type, true);
        _streams_block->insert(ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                     _s_streams_columns[i].name));
    }

    _streams_block->reserve(_block_rows_limit);

    if (result_data.size() > 0) {
        auto col_size = result_data[0].column_value.size();
        if (col_size != _s_streams_columns.size()) {
            return Status::InternalError<false>("streams schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];
        for (int j = 0; j < _s_streams_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j, _streams_block.get(),
                                                _s_streams_columns[j].type));
        }
    }
    return Status::OK();
}

Status SchemaStreamsScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_streams_block == nullptr) {
        RETURN_IF_ERROR(_get_streams_block_from_fe());
        _total_rows = (int)_streams_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    MutableBlock mblock = MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_streams_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}


} // namespace doris