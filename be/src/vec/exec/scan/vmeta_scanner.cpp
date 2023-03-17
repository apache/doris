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

#include "vmeta_scanner.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>

#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"
#include "util/thrift_rpc_helper.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

VMetaScanner::VMetaScanner(RuntimeState* state, VMetaScanNode* parent, int64_t tuple_id,
                           const TScanRangeParams& scan_range, int64_t limit,
                           RuntimeProfile* profile)
        : VScanner(state, static_cast<VScanNode*>(parent), limit, profile),
          _meta_eos(false),
          _tuple_id(tuple_id),
          _scan_range(scan_range.scan_range) {}

Status VMetaScanner::open(RuntimeState* state) {
    VLOG_CRITICAL << "VMetaScanner::open";
    RETURN_IF_ERROR(VScanner::open(state));
    return Status::OK();
}

Status VMetaScanner::prepare(RuntimeState* state, VExprContext** vconjunct_ctx_ptr) {
    VLOG_CRITICAL << "VMetaScanner::prepare";
    RETURN_IF_ERROR(VScanner::prepare(_state, vconjunct_ctx_ptr));
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    RETURN_IF_ERROR(_fetch_metadata(_scan_range.meta_scan_range));
    return Status::OK();
}

Status VMetaScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    VLOG_CRITICAL << "VMetaScanner::_get_block_impl";
    if (nullptr == state || nullptr == block || nullptr == eof) {
        return Status::InternalError("input is NULL pointer");
    }
    if (_meta_eos == true) {
        *eof = true;
        return Status::OK();
    }

    auto column_size = _tuple_desc->slots().size();
    std::vector<MutableColumnPtr> columns(column_size);
    bool mem_reuse = block->mem_reuse();
    do {
        RETURN_IF_CANCELLED(state);

        columns.resize(column_size);
        for (auto i = 0; i < column_size; i++) {
            if (mem_reuse) {
                columns[i] = std::move(*block->get_by_position(i).column).mutate();
            } else {
                columns[i] = _tuple_desc->slots()[i]->get_empty_mutable_column();
            }
        }
        // fill block
        _fill_block_with_remote_data(columns);
        if (_meta_eos == true) {
            if (block->rows() == 0) {
                *eof = true;
            }
            break;
        }
        // Before really use the Block, must clear other ptr of column in block
        // So here need do std::move and clear in `columns`
        if (!mem_reuse) {
            int column_index = 0;
            for (const auto slot_desc : _tuple_desc->slots()) {
                block->insert(ColumnWithTypeAndName(std::move(columns[column_index++]),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
            }
        } else {
            columns.clear();
        }
        VLOG_ROW << "VMetaScanNode output rows: " << block->rows();
    } while (block->rows() == 0 && !(*eof));
    return Status::OK();
}

Status VMetaScanner::_fill_block_with_remote_data(const std::vector<MutableColumnPtr>& columns) {
    VLOG_CRITICAL << "VMetaScanner::_fill_block_with_remote_data";
    for (int col_idx = 0; col_idx < columns.size(); col_idx++) {
        auto slot_desc = _tuple_desc->slots()[col_idx];
        // because the fe planner filter the non_materialize column
        if (!slot_desc->is_materialized()) {
            continue;
        }

        for (int _row_idx = 0; _row_idx < _batch_data.size(); _row_idx++) {
            vectorized::IColumn* col_ptr = columns[col_idx].get();
            if (slot_desc->is_nullable() == true) {
                auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(col_ptr);
                col_ptr = &nullable_column->get_nested_column();
            }
            switch (slot_desc->type().type) {
            case TYPE_INT: {
                int64_t data = _batch_data[_row_idx].column_value[col_idx].intVal;
                reinterpret_cast<vectorized::ColumnVector<vectorized::Int32>*>(col_ptr)
                        ->insert_value(data);
                break;
            }
            case TYPE_BIGINT: {
                int64_t data = _batch_data[_row_idx].column_value[col_idx].longVal;
                reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)
                        ->insert_value(data);
                break;
            }
            case TYPE_DATETIMEV2: {
                uint64_t data = _batch_data[_row_idx].column_value[col_idx].longVal;
                reinterpret_cast<vectorized::ColumnVector<vectorized::UInt64>*>(col_ptr)
                        ->insert_value(data);
                break;
            }
            case TYPE_STRING:
            case TYPE_CHAR:
            case TYPE_VARCHAR: {
                std::string data = _batch_data[_row_idx].column_value[col_idx].stringVal;
                reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(data.c_str(),
                                                                                  data.length());
                break;
            }
            default: {
                std::string error_msg =
                        fmt::format("Invalid column type {} on column: {}.",
                                    slot_desc->type().debug_string(), slot_desc->col_name());
                return Status::InternalError(std::string(error_msg));
            }
            }
        }
    }
    _meta_eos = true;
    return Status::OK();
}

Status VMetaScanner::_fetch_metadata(const TMetaScanRange& meta_scan_range) {
    VLOG_CRITICAL << "VMetaScanner::_fetch_metadata";
    TFetchSchemaTableDataRequest request;
    switch (meta_scan_range.metadata_type) {
    case TMetadataType::ICEBERG:
        RETURN_IF_ERROR(_build_iceberg_metadata_request(meta_scan_range, &request));
        break;
    default:
        _meta_eos = true;
        return Status::OK();
    }

    // _state->execution_timeout() is seconds, change to milliseconds
    int time_out = _state->execution_timeout() * 1000;
    TNetworkAddress master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    TFetchSchemaTableDataResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchSchemaTableData(result, request);
            },
            time_out));

    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "fetch schema table data from master failed, errmsg=" << status;
        return status;
    }
    _batch_data = std::move(result.data_batch);
    return Status::OK();
}

Status VMetaScanner::_build_iceberg_metadata_request(const TMetaScanRange& meta_scan_range,
                                                     TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "VMetaScanner::_build_iceberg_metadata_request";
    if (!meta_scan_range.__isset.iceberg_params) {
        return Status::InternalError("Can not find TIcebergMetadataParams from meta_scan_range.");
    }

    // create request
    request->__set_cluster_name("");
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::ICEBERG);
    metadata_table_params.__set_iceberg_metadata_params(meta_scan_range.iceberg_params);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status VMetaScanner::close(RuntimeState* state) {
    VLOG_CRITICAL << "VMetaScanner::close";
    RETURN_IF_ERROR(VScanner::close(state));
    return Status::OK();
}
} // namespace doris::vectorized
