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

#include "meta_scanner.h"

#include <fmt/format.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>

#include <ostream>
#include <string>
#include <unordered_map>

#include "common/logging.h"
#include "runtime/client_cache.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/thrift_rpc_helper.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/exec/format/table/iceberg_sys_table_jni_reader.h"
#include "vec/exec/format/table/paimon_sys_table_jni_reader.h"

namespace doris {
class RuntimeProfile;
namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

MetaScanner::MetaScanner(RuntimeState* state, pipeline::ScanLocalStateBase* local_state,
                         int64_t tuple_id, const TScanRangeParams& scan_range, int64_t limit,
                         RuntimeProfile* profile, TUserIdentity user_identity)
        : Scanner(state, local_state, limit, profile),
          _meta_eos(false),
          _tuple_id(tuple_id),
          _user_identity(user_identity),
          _scan_range(scan_range.scan_range) {}

Status MetaScanner::open(RuntimeState* state) {
    VLOG_CRITICAL << "MetaScanner::open";
    RETURN_IF_ERROR(Scanner::open(state));
    if (_scan_range.meta_scan_range.metadata_type == TMetadataType::ICEBERG) {
        // TODO: refactor this code
        auto reader = IcebergSysTableJniReader::create_unique(_tuple_desc->slots(), state, _profile,
                                                              _scan_range.meta_scan_range);
        const std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
        RETURN_IF_ERROR(reader->init_reader(&colname_to_value_range));
        _reader = std::move(reader);
    } else if (_scan_range.meta_scan_range.metadata_type == TMetadataType::PAIMON) {
        auto reader = PaimonSysTableJniReader::create_unique(_tuple_desc->slots(), state, _profile,
                                                             _scan_range.meta_scan_range);
        const std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
        RETURN_IF_ERROR(reader->init_reader(&colname_to_value_range));
        _reader = std::move(reader);
    } else {
        RETURN_IF_ERROR(_fetch_metadata(_scan_range.meta_scan_range));
    }
    return Status::OK();
}

Status MetaScanner::init(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    VLOG_CRITICAL << "MetaScanner::init";
    RETURN_IF_ERROR(Scanner::init(_state, conjuncts));
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    return Status::OK();
}

Status MetaScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    VLOG_CRITICAL << "MetaScanner::_get_block_impl";
    if (nullptr == state || nullptr == block || nullptr == eof) {
        return Status::InternalError("input is NULL pointer");
    }
    if (_reader) {
        // TODO: This is a temporary workaround; the code is planned to be refactored later.
        size_t read_rows = 0;
        return _reader->get_next_block(block, &read_rows, eof);
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
                columns[i] = block->get_by_position(i).column->assume_mutable();
            } else {
                columns[i] = _tuple_desc->slots()[i]->get_empty_mutable_column();
            }
        }
        // fill block
        RETURN_IF_ERROR(_fill_block_with_remote_data(columns));
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

Status MetaScanner::_fill_block_with_remote_data(const std::vector<MutableColumnPtr>& columns) {
    VLOG_CRITICAL << "MetaScanner::_fill_block_with_remote_data";
    for (int col_idx = 0; col_idx < columns.size(); col_idx++) {
        auto slot_desc = _tuple_desc->slots()[col_idx];
        // because the fe planner filter the non_materialize column
        if (!slot_desc->is_materialized()) {
            continue;
        }

        for (int _row_idx = 0; _row_idx < _batch_data.size(); _row_idx++) {
            vectorized::IColumn* col_ptr = columns[col_idx].get();
            TCell& cell = _batch_data[_row_idx].column_value[col_idx];
            if (cell.__isset.isNull && cell.isNull) {
                DCHECK(slot_desc->is_nullable())
                        << "cell is null but column is not nullable: " << slot_desc->col_name();
                auto& null_col = reinterpret_cast<ColumnNullable&>(*col_ptr);
                null_col.get_nested_column().insert_default();
                null_col.get_null_map_data().push_back(1);
            } else {
                if (slot_desc->is_nullable()) {
                    auto& null_col = reinterpret_cast<ColumnNullable&>(*col_ptr);
                    null_col.get_null_map_data().push_back(0);
                    col_ptr = null_col.get_nested_column_ptr().get();
                }
                switch (slot_desc->type()->get_primitive_type()) {
                case TYPE_BOOLEAN: {
                    bool data = cell.boolVal;
                    assert_cast<vectorized::ColumnBool*>(col_ptr)->insert_value((uint8_t)data);
                    break;
                }
                case TYPE_TINYINT: {
                    int8_t data = (int8_t)cell.intVal;
                    assert_cast<vectorized::ColumnInt8*>(col_ptr)->insert_value(data);
                    break;
                }
                case TYPE_SMALLINT: {
                    int16_t data = (int16_t)cell.intVal;
                    assert_cast<vectorized::ColumnInt16*>(col_ptr)->insert_value(data);
                    break;
                }
                case TYPE_INT: {
                    int32_t data = cell.intVal;
                    assert_cast<vectorized::ColumnInt32*>(col_ptr)->insert_value(data);
                    break;
                }
                case TYPE_BIGINT: {
                    int64_t data = cell.longVal;
                    assert_cast<vectorized::ColumnInt64*>(col_ptr)->insert_value(data);
                    break;
                }
                case TYPE_FLOAT: {
                    double data = cell.doubleVal;
                    assert_cast<vectorized::ColumnFloat32*>(col_ptr)->insert_value(data);
                    break;
                }
                case TYPE_DOUBLE: {
                    double data = cell.doubleVal;
                    assert_cast<vectorized::ColumnFloat64*>(col_ptr)->insert_value(data);
                    break;
                }
                case TYPE_DATEV2: {
                    uint32_t data = (uint32_t)cell.longVal;
                    assert_cast<vectorized::ColumnDateV2*>(col_ptr)->insert_value(data);
                    break;
                }
                case TYPE_DATETIMEV2: {
                    uint64_t data = cell.longVal;
                    assert_cast<vectorized::ColumnDateTimeV2*>(col_ptr)->insert_value(data);
                    break;
                }
                case TYPE_STRING:
                case TYPE_CHAR:
                case TYPE_VARCHAR: {
                    std::string data = cell.stringVal;
                    assert_cast<vectorized::ColumnString*>(col_ptr)->insert_data(data.c_str(),
                                                                                 data.length());
                    break;
                }
                default: {
                    std::string error_msg =
                            fmt::format("Invalid column type {} on column: {}.",
                                        slot_desc->type()->get_name(), slot_desc->col_name());
                    return Status::InternalError(std::string(error_msg));
                }
                }
            }
        }
    }
    _meta_eos = true;
    return Status::OK();
}

Status MetaScanner::_fetch_metadata(const TMetaScanRange& meta_scan_range) {
    VLOG_CRITICAL << "MetaScanner::_fetch_metadata";
    TFetchSchemaTableDataRequest request;
    switch (meta_scan_range.metadata_type) {
    case TMetadataType::HUDI:
        RETURN_IF_ERROR(_build_hudi_metadata_request(meta_scan_range, &request));
        break;
    case TMetadataType::BACKENDS:
        RETURN_IF_ERROR(_build_backends_metadata_request(meta_scan_range, &request));
        break;
    case TMetadataType::FRONTENDS:
        RETURN_IF_ERROR(_build_frontends_metadata_request(meta_scan_range, &request));
        break;
    case TMetadataType::FRONTENDS_DISKS:
        RETURN_IF_ERROR(_build_frontends_disks_metadata_request(meta_scan_range, &request));
        break;
    case TMetadataType::WORKLOAD_SCHED_POLICY:
        RETURN_IF_ERROR(_build_workload_sched_policy_metadata_request(meta_scan_range, &request));
        break;
    case TMetadataType::CATALOGS:
        RETURN_IF_ERROR(_build_catalogs_metadata_request(meta_scan_range, &request));
        break;
    case TMetadataType::MATERIALIZED_VIEWS:
        RETURN_IF_ERROR(_build_materialized_views_metadata_request(meta_scan_range, &request));
        break;
    case TMetadataType::PARTITIONS:
        RETURN_IF_ERROR(_build_partitions_metadata_request(meta_scan_range, &request));
        break;
    case TMetadataType::JOBS:
        RETURN_IF_ERROR(_build_jobs_metadata_request(meta_scan_range, &request));
        break;
    case TMetadataType::TASKS:
        RETURN_IF_ERROR(_build_tasks_metadata_request(meta_scan_range, &request));
        break;
    case TMetadataType::PARTITION_VALUES:
        RETURN_IF_ERROR(_build_partition_values_metadata_request(meta_scan_range, &request));
        break;
    default:
        _meta_eos = true;
        return Status::OK();
    }

    // set filter columns
    std::vector<std::string> filter_columns;
    for (const auto& slot : _tuple_desc->slots()) {
        filter_columns.emplace_back(slot->col_name_lower_case());
    }
    request.metada_table_params.__set_columns_name(filter_columns);

    // _state->execution_timeout() is seconds, change to milliseconds
    int time_out = _state->execution_timeout() * 1000;
    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;
    TFetchSchemaTableDataResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->fetchSchemaTableData(result, request);
            },
            time_out));

    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "fetch schema table data from master failed, errmsg=" << status;
        return status;
    }
    _batch_data = std::move(result.data_batch);
    return Status::OK();
}

Status MetaScanner::_build_hudi_metadata_request(const TMetaScanRange& meta_scan_range,
                                                 TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_hudi_metadata_request";
    if (!meta_scan_range.__isset.hudi_params) {
        return Status::InternalError("Can not find THudiMetadataParams from meta_scan_range.");
    }

    // create request
    request->__set_cluster_name("");
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::HUDI);
    metadata_table_params.__set_hudi_metadata_params(meta_scan_range.hudi_params);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::_build_backends_metadata_request(const TMetaScanRange& meta_scan_range,
                                                     TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_backends_metadata_request";
    if (!meta_scan_range.__isset.backends_params) {
        return Status::InternalError("Can not find TBackendsMetadataParams from meta_scan_range.");
    }
    // create request
    request->__set_cluster_name("");
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::BACKENDS);
    metadata_table_params.__set_backends_metadata_params(meta_scan_range.backends_params);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::_build_frontends_metadata_request(const TMetaScanRange& meta_scan_range,
                                                      TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_frontends_metadata_request";
    if (!meta_scan_range.__isset.frontends_params) {
        return Status::InternalError("Can not find TFrontendsMetadataParams from meta_scan_range.");
    }
    // create request
    request->__set_cluster_name("");
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::FRONTENDS);
    metadata_table_params.__set_frontends_metadata_params(meta_scan_range.frontends_params);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::_build_frontends_disks_metadata_request(const TMetaScanRange& meta_scan_range,
                                                            TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_frontends_metadata_request";
    if (!meta_scan_range.__isset.frontends_params) {
        return Status::InternalError("Can not find TFrontendsMetadataParams from meta_scan_range.");
    }
    // create request
    request->__set_cluster_name("");
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::FRONTENDS_DISKS);
    metadata_table_params.__set_frontends_metadata_params(meta_scan_range.frontends_params);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::_build_workload_sched_policy_metadata_request(
        const TMetaScanRange& meta_scan_range, TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_workload_sched_policy_metadata_request";

    // create request
    request->__set_cluster_name("");
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::WORKLOAD_SCHED_POLICY);
    metadata_table_params.__set_current_user_ident(_user_identity);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::_build_catalogs_metadata_request(const TMetaScanRange& meta_scan_range,
                                                     TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_catalogs_metadata_request";

    // create request
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::CATALOGS);
    metadata_table_params.__set_current_user_ident(_user_identity);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::_build_materialized_views_metadata_request(
        const TMetaScanRange& meta_scan_range, TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_materialized_views_metadata_request";
    if (!meta_scan_range.__isset.materialized_views_params) {
        return Status::InternalError(
                "Can not find TMaterializedViewsMetadataParams from meta_scan_range.");
    }

    // create request
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::MATERIALIZED_VIEWS);
    metadata_table_params.__set_materialized_views_metadata_params(
            meta_scan_range.materialized_views_params);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::_build_partitions_metadata_request(const TMetaScanRange& meta_scan_range,
                                                       TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_partitions_metadata_request";
    if (!meta_scan_range.__isset.partitions_params) {
        return Status::InternalError(
                "Can not find TPartitionsMetadataParams from meta_scan_range.");
    }

    // create request
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::PARTITIONS);
    metadata_table_params.__set_partitions_metadata_params(meta_scan_range.partitions_params);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::_build_jobs_metadata_request(const TMetaScanRange& meta_scan_range,
                                                 TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_jobs_metadata_request";
    if (!meta_scan_range.__isset.jobs_params) {
        return Status::InternalError("Can not find TJobsMetadataParams from meta_scan_range.");
    }

    // create request
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::JOBS);
    metadata_table_params.__set_jobs_metadata_params(meta_scan_range.jobs_params);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::_build_tasks_metadata_request(const TMetaScanRange& meta_scan_range,
                                                  TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_tasks_metadata_request";
    if (!meta_scan_range.__isset.tasks_params) {
        return Status::InternalError("Can not find TTasksMetadataParams from meta_scan_range.");
    }

    // create request
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::TASKS);
    metadata_table_params.__set_tasks_metadata_params(meta_scan_range.tasks_params);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::_build_partition_values_metadata_request(
        const TMetaScanRange& meta_scan_range, TFetchSchemaTableDataRequest* request) {
    VLOG_CRITICAL << "MetaScanner::_build_partition_values_metadata_request";
    if (!meta_scan_range.__isset.partition_values_params) {
        return Status::InternalError(
                "Can not find TPartitionValuesMetadataParams from meta_scan_range.");
    }

    // create request
    request->__set_schema_table_name(TSchemaTableName::METADATA_TABLE);

    // create TMetadataTableRequestParams
    TMetadataTableRequestParams metadata_table_params;
    metadata_table_params.__set_metadata_type(TMetadataType::PARTITION_VALUES);
    metadata_table_params.__set_partition_values_metadata_params(
            meta_scan_range.partition_values_params);

    request->__set_metada_table_params(metadata_table_params);
    return Status::OK();
}

Status MetaScanner::close(RuntimeState* state) {
    VLOG_CRITICAL << "MetaScanner::close";
    if (_reader) {
        RETURN_IF_ERROR(_reader->close());
    }
    RETURN_IF_ERROR(Scanner::close(state));
    return Status::OK();
}

} // namespace doris::vectorized
