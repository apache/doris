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

#include "exec/schema_scanner/schema_backends_scanner.h"

#include <gen_cpp/FrontendService_types.h>

#include <string>
#include <vector>

#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
#include "common/compile_check_begin.h"

class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized

std::vector<SchemaScanner::ColumnDesc> SchemaBackendsScanner::_s_backends_columns = {
        //   name,       type,          size,     is_null
        {"BACKENDID", TYPE_BIGINT, sizeof(int64_t), true},
        {"HOST", TYPE_STRING, sizeof(StringRef), true},
        {"HEARTBEATPORT", TYPE_INT, sizeof(int32_t), true},
        {"BEPORT", TYPE_INT, sizeof(int32_t), true},
        {"HTTPPORT", TYPE_INT, sizeof(int32_t), true},
        {"BRPCPORT", TYPE_INT, sizeof(int32_t), true},
        {"ARROWFLIGHTSQLPORT", TYPE_INT, sizeof(int32_t), true},
        {"LASTSTARTTIME", TYPE_STRING, sizeof(StringRef), true},
        {"LASTHEARTBEAT", TYPE_STRING, sizeof(StringRef), true},
        {"ALIVE", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"SYSTEMDECOMMISSIONED", TYPE_BOOLEAN, sizeof(int8_t), true},
        {"TABLETNUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATAUSEDCAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"TRASHUSEDCAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"AVAILCAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"TOTALCAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"USEDPCT", TYPE_DOUBLE, sizeof(double), true},
        {"MAXDISKUSEDPCT", TYPE_DOUBLE, sizeof(double), true},
        {"REMOTEUSEDCAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"TAG", TYPE_STRING, sizeof(StringRef), true},
        {"ERRMSG", TYPE_STRING, sizeof(StringRef), true},
        {"VERSION", TYPE_STRING, sizeof(StringRef), true},
        {"STATUS", TYPE_STRING, sizeof(StringRef), true},
        {"HEARTBEATFAILURECOUNTER", TYPE_INT, sizeof(int32_t), true},
        {"NODEROLE", TYPE_STRING, sizeof(StringRef), true},
};

SchemaBackendsScanner::SchemaBackendsScanner()
        : SchemaScanner(_s_backends_columns, TSchemaTableType::SCH_BACKENDS) {}

SchemaBackendsScanner::~SchemaBackendsScanner() {}

Status SchemaBackendsScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    _rpc_timeout = state->execution_timeout() * 1000;
    return Status::OK();
}

Status SchemaBackendsScanner::get_next_block_internal(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_backends_block == nullptr) {
        RETURN_IF_ERROR(_get_backends_from_fe());
        _total_rows = (int)_backends_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    vectorized::MutableBlock mblock = vectorized::MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_backends_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

Status SchemaBackendsScanner::_get_backends_from_fe() {
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;

    TFetchSchemaTableDataRequest request;
    request.__set_cluster_name("");
    request.__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::BACKENDS);

    TBackendsMetadataParams backends_metadata_params;
    backends_metadata_params.__set_cluster_name("");
    metadata_table_params.__set_backends_metadata_params(backends_metadata_params);

    request.__set_metada_table_params(metadata_table_params);

    std::vector<std::string> filter_columns;
    for (int i = 0; i < _s_backends_columns.size(); i++) {
        filter_columns.emplace_back(_s_backends_columns[i].name);
    }
    request.metada_table_params.__set_columns_name(filter_columns);

    TFetchSchemaTableDataResult result;

    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchSchemaTableData(result, request);
            },
            _rpc_timeout));

    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "fetch backends info from FE failed, errmsg=" << status;
        return status;
    }
    std::vector<TRow> result_data = result.data_batch;

    _backends_block = vectorized::Block::create_unique();
    for (int i = 0; i < _s_backends_columns.size(); ++i) {
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                _s_backends_columns[i].type, true);
        _backends_block->insert(vectorized::ColumnWithTypeAndName(
                data_type->create_column(), data_type, _s_backends_columns[i].name));
    }

    _backends_block->reserve(_block_rows_limit);

    if (result_data.size() > 0) {
        auto col_size = result_data[0].column_value.size();
        if (col_size != _s_backends_columns.size()) {
            return Status::InternalError<false>("transactions schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];
        for (int j = 0; j < _s_backends_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j, _backends_block.get(),
                                                _s_backends_columns[j].type));
        }
    }
    return Status::OK();
}
#include "common/compile_check_end.h"

} // namespace doris
