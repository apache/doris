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

#include "information_schema/schema_catalog_meta_cache_stats_scanner.h"

#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/string_ref.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/client_cache.h"
#include "util/thrift_rpc_helper.h"

namespace doris {
#include "common/compile_check_begin.h"

std::vector<SchemaScanner::ColumnDesc> SchemaCatalogMetaCacheStatsScanner::_s_tbls_columns = {
        {"FE_HOST", TYPE_STRING, sizeof(StringRef), true},
        {"CATALOG_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"ENGINE_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"ENTRY_NAME", TYPE_STRING, sizeof(StringRef), true},
        {"EFFECTIVE_ENABLED", TYPE_BOOLEAN, sizeof(bool), true},
        {"CONFIG_ENABLED", TYPE_BOOLEAN, sizeof(bool), true},
        {"AUTO_REFRESH", TYPE_BOOLEAN, sizeof(bool), true},
        {"TTL_SECOND", TYPE_BIGINT, sizeof(int64_t), true},
        {"CAPACITY", TYPE_BIGINT, sizeof(int64_t), true},
        {"ESTIMATED_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"REQUEST_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"HIT_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"MISS_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"HIT_RATE", TYPE_DOUBLE, sizeof(double), true},
        {"LOAD_SUCCESS_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"LOAD_FAILURE_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"TOTAL_LOAD_TIME_MS", TYPE_BIGINT, sizeof(int64_t), true},
        {"AVG_LOAD_PENALTY_MS", TYPE_DOUBLE, sizeof(double), true},
        {"EVICTION_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"INVALIDATE_COUNT", TYPE_BIGINT, sizeof(int64_t), true},
        {"LAST_LOAD_SUCCESS_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"LAST_LOAD_FAILURE_TIME", TYPE_STRING, sizeof(StringRef), true},
        {"LAST_ERROR", TYPE_STRING, sizeof(StringRef), true},
};

SchemaCatalogMetaCacheStatsScanner::SchemaCatalogMetaCacheStatsScanner()
        : SchemaScanner(_s_tbls_columns, TSchemaTableType::SCH_CATALOG_META_CACHE_STATISTICS) {}

SchemaCatalogMetaCacheStatsScanner::~SchemaCatalogMetaCacheStatsScanner() {}

Status SchemaCatalogMetaCacheStatsScanner::start(RuntimeState* state) {
    _block_rows_limit = state->batch_size();
    _rpc_timeout = state->execution_timeout() * 1000;
    _fe_addr = state->get_query_ctx()->current_connect_fe;
    return Status::OK();
}

Status SchemaCatalogMetaCacheStatsScanner::_get_meta_cache_from_fe() {
    TSchemaTableRequestParams schema_table_request_params;
    for (int i = 0; i < _s_tbls_columns.size(); i++) {
        schema_table_request_params.__isset.columns_name = true;
        schema_table_request_params.columns_name.emplace_back(_s_tbls_columns[i].name);
    }
    schema_table_request_params.__set_current_user_ident(*_param->common_param->current_user_ident);

    TFetchSchemaTableDataRequest request;
    request.__set_schema_table_name(TSchemaTableName::CATALOG_META_CACHE_STATS);
    request.__set_schema_table_params(schema_table_request_params);

    TFetchSchemaTableDataResult result;

    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            _fe_addr.hostname, _fe_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchSchemaTableData(result, request);
            },
            _rpc_timeout));

    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "fetch catalog meta cache stats from FE(" << _fe_addr.hostname
                     << ") failed, errmsg=" << status;
        return status;
    }
    std::vector<TRow> result_data = result.data_batch;

    _block = Block::create_unique();
    for (int i = 0; i < _s_tbls_columns.size(); ++i) {
        auto data_type =
                DataTypeFactory::instance().create_data_type(_s_tbls_columns[i].type, true);
        _block->insert(ColumnWithTypeAndName(data_type->create_column(), data_type,
                                             _s_tbls_columns[i].name));
    }

    _block->reserve(_block_rows_limit);

    if (result_data.size() > 0) {
        auto col_size = result_data[0].column_value.size();
        if (col_size != _s_tbls_columns.size()) {
            return Status::InternalError<false>(
                    "catalog meta cache stats schema is not match for FE and BE");
        }
    }

    for (int i = 0; i < result_data.size(); i++) {
        TRow row = result_data[i];
        for (int j = 0; j < _s_tbls_columns.size(); j++) {
            RETURN_IF_ERROR(insert_block_column(row.column_value[j], j, _block.get(),
                                                _s_tbls_columns[j].type));
        }
    }
    return Status::OK();
}

Status SchemaCatalogMetaCacheStatsScanner::get_next_block_internal(Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (_block == nullptr) {
        RETURN_IF_ERROR(_get_meta_cache_from_fe());
        _total_rows = (int)_block->rows();
    }

    if (_row_idx == _total_rows) {
        *eos = true;
        return Status::OK();
    }

    int current_batch_rows = std::min(_block_rows_limit, _total_rows - _row_idx);
    MutableBlock mblock = MutableBlock::build_mutable_block(block);
    RETURN_IF_ERROR(mblock.add_rows(_block.get(), _row_idx, current_batch_rows));
    _row_idx += current_batch_rows;

    *eos = _row_idx == _total_rows;
    return Status::OK();
}

} // namespace doris
