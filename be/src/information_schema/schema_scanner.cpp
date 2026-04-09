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

#include "information_schema/schema_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <string.h>

#include <new>
#include <ostream>
#include <utility>

#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/packed_int128.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/value/hll.h"
#include "exec/pipeline/dependency.h"
#include "exprs/function/cast/cast_to_date_or_datetime_impl.hpp"
#include "information_schema/schema_active_queries_scanner.h"
#include "information_schema/schema_authentication_integrations_scanner.h"
#include "information_schema/schema_backend_active_tasks.h"
#include "information_schema/schema_backend_configuration_scanner.h"
#include "information_schema/schema_backend_kerberos_ticket_cache.h"
#include "information_schema/schema_catalog_meta_cache_stats_scanner.h"
#include "information_schema/schema_charsets_scanner.h"
#include "information_schema/schema_cluster_snapshot_properties_scanner.h"
#include "information_schema/schema_cluster_snapshots_scanner.h"
#include "information_schema/schema_collations_scanner.h"
#include "information_schema/schema_column_data_sizes_scanner.h"
#include "information_schema/schema_columns_scanner.h"
#include "information_schema/schema_compaction_tasks_scanner.h"
#include "information_schema/schema_database_properties_scanner.h"
#include "information_schema/schema_dummy_scanner.h"
#include "information_schema/schema_encryption_keys_scanner.h"
#include "information_schema/schema_file_cache_info_scanner.h"
#include "information_schema/schema_file_cache_statistics.h"
#include "information_schema/schema_files_scanner.h"
#include "information_schema/schema_load_job_scanner.h"
#include "information_schema/schema_metadata_name_ids_scanner.h"
#include "information_schema/schema_partitions_scanner.h"
#include "information_schema/schema_processlist_scanner.h"
#include "information_schema/schema_profiling_scanner.h"
#include "information_schema/schema_routine_load_job_scanner.h"
#include "information_schema/schema_rowsets_scanner.h"
#include "information_schema/schema_schema_privileges_scanner.h"
#include "information_schema/schema_schemata_scanner.h"
#include "information_schema/schema_sql_block_rule_status_scanner.h"
#include "information_schema/schema_table_options_scanner.h"
#include "information_schema/schema_table_privileges_scanner.h"
#include "information_schema/schema_table_properties_scanner.h"
#include "information_schema/schema_table_stream_consumption_scanner.h"
#include "information_schema/schema_table_streams_scanner.h"
#include "information_schema/schema_tables_scanner.h"
#include "information_schema/schema_tablets_scanner.h"
#include "information_schema/schema_user_privileges_scanner.h"
#include "information_schema/schema_user_scanner.h"
#include "information_schema/schema_variables_scanner.h"
#include "information_schema/schema_view_dependency_scanner.h"
#include "information_schema/schema_views_scanner.h"
#include "information_schema/schema_workload_group_privileges.h"
#include "information_schema/schema_workload_group_resource_usage_scanner.h"
#include "information_schema/schema_workload_groups_scanner.h"
#include "information_schema/schema_workload_sched_policy_scanner.h"
#include "runtime/fragment_mgr.h"
#include "util/string_util.h"

namespace doris {
class ObjectPool;

SchemaScanner::SchemaScanner(const std::vector<ColumnDesc>& columns, TSchemaTableType::type type)
        : _is_init(false), _columns(columns), _schema_table_type(type) {}

SchemaScanner::~SchemaScanner() = default;

Status SchemaScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("call Start before Init.");
    }

    return Status::OK();
}

Status SchemaScanner::get_next_block(RuntimeState* state, Block* block, bool* eos) {
    if (_data_block == nullptr) {
        return Status::InternalError("No data left!");
    }
    DCHECK(_async_thread_running == false);
    RETURN_IF_ERROR(_scanner_status.status());
    for (size_t i = 0; i < block->columns(); i++) {
        std::move(*block->get_by_position(i).column)
                .mutate()
                ->insert_range_from(*_data_block->get_by_position(i).column, 0,
                                    _data_block->rows());
    }
    _data_block->clear_column_data();
    *eos = _eos;
    if (!*eos) {
        RETURN_IF_ERROR(get_next_block_async(state));
    }
    return Status::OK();
}

Status SchemaScanner::get_next_block_async(RuntimeState* state) {
    _dependency->block();
    auto task_ctx = state->get_task_execution_context();
    RETURN_IF_ERROR(ExecEnv::GetInstance()->fragment_mgr()->get_thread_pool()->submit_func(
            [this, task_ctx, state]() {
                auto task_lock = task_ctx.lock();
                if (task_lock == nullptr) {
                    return;
                }
                DCHECK(_async_thread_running == false);
                SCOPED_ATTACH_TASK(state);
                _async_thread_running = true;
                if (!_opened) {
                    _data_block = Block::create_unique();
                    _init_block(_data_block.get());
                    _scanner_status.update(start(state));
                    _opened = true;
                }
                bool eos = false;
                auto call_next_block_internal = [&]() -> Status {
                    RETURN_IF_CATCH_EXCEPTION(
                            { return get_next_block_internal(_data_block.get(), &eos); });
                };
                _scanner_status.update(call_next_block_internal());
                _eos = eos;
                _async_thread_running = false;
                _dependency->set_ready();
            }));
    return Status::OK();
}

Status SchemaScanner::init(RuntimeState* state, SchemaScannerParam* param, ObjectPool* pool) {
    if (_is_init) {
        return Status::OK();
    }
    if (nullptr == param || nullptr == pool) {
        return Status::InternalError("invalid parameter");
    }

    _param = param;
    _timezone = state->timezone();
    _timezone_obj = state->timezone_obj();
    _is_init = true;

    if (_param->profile) {
        _get_db_timer = ADD_TIMER(_param->profile, "GetDbTime");
        _get_table_timer = ADD_TIMER(_param->profile, "GetTableTime");
        _get_describe_timer = ADD_TIMER(_param->profile, "GetDescribeTime");
        _fill_block_timer = ADD_TIMER(_param->profile, "FillBlockTime");
    }

    return Status::OK();
}

std::unique_ptr<SchemaScanner> SchemaScanner::create(TSchemaTableType::type type) {
    switch (type) {
    case TSchemaTableType::SCH_TABLES:
        return SchemaTablesScanner::create_unique();
    case TSchemaTableType::SCH_SCHEMATA:
        return SchemaSchemataScanner::create_unique();
    case TSchemaTableType::SCH_COLUMNS:
        return SchemaColumnsScanner::create_unique();
    case TSchemaTableType::SCH_CHARSETS:
        return SchemaCharsetsScanner::create_unique();
    case TSchemaTableType::SCH_COLLATIONS:
        return SchemaCollationsScanner::create_unique();
    case TSchemaTableType::SCH_GLOBAL_VARIABLES:
        return SchemaVariablesScanner::create_unique(TVarType::GLOBAL);
    case TSchemaTableType::SCH_SESSION_VARIABLES:
    case TSchemaTableType::SCH_VARIABLES:
        return SchemaVariablesScanner::create_unique(TVarType::SESSION);
    case TSchemaTableType::SCH_VIEWS:
        return SchemaViewsScanner::create_unique();
    case TSchemaTableType::SCH_TABLE_PRIVILEGES:
        return SchemaTablePrivilegesScanner::create_unique();
    case TSchemaTableType::SCH_SCHEMA_PRIVILEGES:
        return SchemaSchemaPrivilegesScanner::create_unique();
    case TSchemaTableType::SCH_USER_PRIVILEGES:
        return SchemaUserPrivilegesScanner::create_unique();
    case TSchemaTableType::SCH_FILES:
        return SchemaFilesScanner::create_unique();
    case TSchemaTableType::SCH_PARTITIONS:
        return SchemaPartitionsScanner::create_unique();
    case TSchemaTableType::SCH_BACKEND_CONFIGURATION:
        return SchemaBackendConfigurationScanner::create_unique();
    case TSchemaTableType::SCH_ROWSETS:
        return SchemaRowsetsScanner::create_unique();
    case TSchemaTableType::SCH_METADATA_NAME_IDS:
        return SchemaMetadataNameIdsScanner::create_unique();
    case TSchemaTableType::SCH_PROFILING:
        return SchemaProfilingScanner::create_unique();
    case TSchemaTableType::SCH_BACKEND_ACTIVE_TASKS:
        return SchemaBackendActiveTasksScanner::create_unique();
    case TSchemaTableType::SCH_ACTIVE_QUERIES:
        return SchemaActiveQueriesScanner::create_unique();
    case TSchemaTableType::SCH_WORKLOAD_GROUPS:
        return SchemaWorkloadGroupsScanner::create_unique();
    case TSchemaTableType::SCH_PROCESSLIST:
        return SchemaProcessListScanner::create_unique();
    case TSchemaTableType::SCH_USER:
        return SchemaUserScanner::create_unique();
    case TSchemaTableType::SCH_WORKLOAD_POLICY:
        return SchemaWorkloadSchedulePolicyScanner::create_unique();
    case TSchemaTableType::SCH_TABLE_OPTIONS:
        return SchemaTableOptionsScanner::create_unique();
    case TSchemaTableType::SCH_WORKLOAD_GROUP_PRIVILEGES:
        return SchemaWorkloadGroupPrivilegesScanner::create_unique();
    case TSchemaTableType::SCH_WORKLOAD_GROUP_RESOURCE_USAGE:
        return SchemaBackendWorkloadGroupResourceUsage::create_unique();
    case TSchemaTableType::SCH_TABLE_PROPERTIES:
        return SchemaTablePropertiesScanner::create_unique();
    case TSchemaTableType::SCH_DATABASE_PROPERTIES:
        return SchemaDatabasePropertiesScanner::create_unique();
    case TSchemaTableType::SCH_FILE_CACHE_STATISTICS:
        return SchemaFileCacheStatisticsScanner::create_unique();
    case TSchemaTableType::SCH_CATALOG_META_CACHE_STATISTICS:
        return SchemaCatalogMetaCacheStatsScanner::create_unique();
    case TSchemaTableType::SCH_BACKEND_KERBEROS_TICKET_CACHE:
        return SchemaBackendKerberosTicketCacheScanner::create_unique();
    case TSchemaTableType::SCH_ROUTINE_LOAD_JOBS:
        return SchemaRoutineLoadJobScanner::create_unique();
    case TSchemaTableType::SCH_LOAD_JOBS:
        return SchemaLoadJobScanner::create_unique();
    case TSchemaTableType::SCH_BACKEND_TABLETS:
        return SchemaTabletsScanner::create_unique();
    case TSchemaTableType::SCH_VIEW_DEPENDENCY:
        return SchemaViewDependencyScanner::create_unique();
    case TSchemaTableType::SCH_SQL_BLOCK_RULE_STATUS:
        return SchemaSqlBlockRuleStatusScanner::create_unique();
    case TSchemaTableType::SCH_ENCRYPTION_KEYS:
        return SchemaEncryptionKeysScanner::create_unique();
    case TSchemaTableType::SCH_CLUSTER_SNAPSHOTS:
        return SchemaClusterSnapshotsScanner::create_unique();
    case TSchemaTableType::SCH_CLUSTER_SNAPSHOT_PROPERTIES:
        return SchemaClusterSnapshotPropertiesScanner::create_unique();
    case TSchemaTableType::SCH_COLUMN_DATA_SIZES:
        return SchemaColumnDataSizesScanner::create_unique();
    case TSchemaTableType::SCH_FILE_CACHE_INFO:
        return SchemaFileCacheInfoScanner::create_unique();
    case TSchemaTableType::SCH_AUTHENTICATION_INTEGRATIONS:
        return SchemaAuthenticationIntegrationsScanner::create_unique();
    case TSchemaTableType::SCH_TABLE_STREAMS:
        return SchemaTableStreamsScanner::create_unique();
    case TSchemaTableType::SCH_TABLE_STREAM_CONSUMPTION:
        return SchemaTableStreamConsumptionScanner::create_unique();
    case TSchemaTableType::SCH_BE_COMPACTION_TASKS:
        return SchemaCompactionTasksScanner::create_unique();
    default:
        return SchemaDummyScanner::create_unique();
        break;
    }
}

void SchemaScanner::_init_block(Block* src_block) {
    const std::vector<SchemaScanner::ColumnDesc>& columns_desc(get_column_desc());
    for (int i = 0; i < columns_desc.size(); ++i) {
        auto data_type = DataTypeFactory::instance().create_data_type(columns_desc[i].type, true);
        src_block->insert(
                ColumnWithTypeAndName(data_type->create_column(), data_type, columns_desc[i].name));
    }
}

Status SchemaScanner::fill_dest_column_for_range(Block* block, size_t pos,
                                                 const std::vector<void*>& datas) {
    const ColumnDesc& col_desc = _columns[pos];
    MutableColumnPtr column_ptr;
    column_ptr = std::move(*block->get_by_position(pos).column).assume_mutable();
    IColumn* col_ptr = column_ptr.get();

    auto* nullable_column = reinterpret_cast<ColumnNullable*>(col_ptr);

    // Resize in advance to improve insertion efficiency.
    size_t fill_num = datas.size();
    col_ptr = &nullable_column->get_nested_column();
    for (int i = 0; i < fill_num; ++i) {
        auto* data = datas[i];
        if (data == nullptr) {
            // For nested column need not insert default.
            nullable_column->insert_data(nullptr, 0);
            continue;
        } else {
            nullable_column->push_false_to_nullmap(1);
        }
        switch (col_desc.type) {
        case TYPE_HLL: {
            auto* hll_slot = reinterpret_cast<HyperLogLog*>(data);
            assert_cast<ColumnHLL*>(col_ptr)->get_data().emplace_back(*hll_slot);
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING: {
            auto* str_slot = reinterpret_cast<StringRef*>(data);
            assert_cast<ColumnString*>(col_ptr)->insert_data(str_slot->data, str_slot->size);
            break;
        }

        case TYPE_BOOLEAN: {
            uint8_t num = *reinterpret_cast<bool*>(data);
            assert_cast<ColumnBool*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_TINYINT: {
            int8_t num = *reinterpret_cast<int8_t*>(data);
            assert_cast<ColumnInt8*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_SMALLINT: {
            int16_t num = *reinterpret_cast<int16_t*>(data);
            assert_cast<ColumnInt16*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_INT: {
            int32_t num = *reinterpret_cast<int32_t*>(data);
            assert_cast<ColumnInt32*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_BIGINT: {
            int64_t num = *reinterpret_cast<int64_t*>(data);
            assert_cast<ColumnInt64*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_LARGEINT: {
            __int128 num;
            memcpy(&num, data, sizeof(__int128));
            assert_cast<ColumnInt128*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_FLOAT: {
            float num = *reinterpret_cast<float*>(data);
            assert_cast<ColumnFloat32*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_DOUBLE: {
            double num = *reinterpret_cast<double*>(data);
            assert_cast<ColumnFloat64*>(col_ptr)->insert_value(num);
            break;
        }

        case TYPE_DATE: {
            assert_cast<ColumnDate*>(col_ptr)->insert_data(reinterpret_cast<char*>(data), 0);
            break;
        }

        case TYPE_DATEV2: {
            assert_cast<ColumnDateV2*>(col_ptr)->insert_value(
                    *reinterpret_cast<DateV2Value<DateV2ValueType>*>(data));
            break;
        }

        case TYPE_DATETIME: {
            assert_cast<ColumnDateTime*>(col_ptr)->insert_data(reinterpret_cast<char*>(data), 0);
            break;
        }

        case TYPE_DATETIMEV2: {
            assert_cast<ColumnDateTimeV2*>(col_ptr)->insert_value(
                    *reinterpret_cast<DateV2Value<DateTimeV2ValueType>*>(data));
            break;
        }

        case TYPE_TIMESTAMPTZ: {
            assert_cast<ColumnTimeStampTz*>(col_ptr)->insert_value(
                    *reinterpret_cast<TimestampTzValue*>(data));
            break;
        }

        case TYPE_DECIMALV2: {
            const Int128 num = (reinterpret_cast<PackedInt128*>(data))->value;
            assert_cast<ColumnDecimal128V2*>(col_ptr)->insert_data(
                    reinterpret_cast<const char*>(&num), 0);
            break;
        }
        case TYPE_DECIMAL128I: {
            const Int128 num = (reinterpret_cast<PackedInt128*>(data))->value;
            assert_cast<ColumnDecimal128V3*>(col_ptr)->insert_data(
                    reinterpret_cast<const char*>(&num), 0);
            break;
        }

        case TYPE_DECIMAL32: {
            const int32_t num = *reinterpret_cast<int32_t*>(data);
            assert_cast<ColumnDecimal32*>(col_ptr)->insert_data(reinterpret_cast<const char*>(&num),
                                                                0);
            break;
        }

        case TYPE_DECIMAL64: {
            const int64_t num = *reinterpret_cast<int64_t*>(data);
            assert_cast<ColumnDecimal64*>(col_ptr)->insert_data(reinterpret_cast<const char*>(&num),
                                                                0);
            break;
        }

        default: {
            DCHECK(false) << "bad slot type: " << col_desc.type;
            std::stringstream ss;
            ss << "Fail to convert schema type:'" << col_desc.type << " on column:`"
               << std::string(col_desc.name) + "`";
            return Status::InternalError(ss.str());
        }
        }
    }
    return Status::OK();
}

std::string SchemaScanner::get_db_from_full_name(const std::string& full_name) {
    std::vector<std::string> part = split(full_name, ".");
    if (part.size() == 2) {
        return part[1];
    }
    return full_name;
}

Status SchemaScanner::insert_block_column(TCell cell, int col_index, Block* block,
                                          PrimitiveType type) {
    MutableColumnPtr mutable_col_ptr;
    mutable_col_ptr = std::move(*block->get_by_position(col_index).column).assume_mutable();
    auto* nullable_column = reinterpret_cast<ColumnNullable*>(mutable_col_ptr.get());
    IColumn* col_ptr = &nullable_column->get_nested_column();

    switch (type) {
    case TYPE_BIGINT: {
        reinterpret_cast<ColumnInt64*>(col_ptr)->insert_value(cell.longVal);
        break;
    }

    case TYPE_INT: {
        reinterpret_cast<ColumnInt32*>(col_ptr)->insert_value(cell.intVal);
        break;
    }

    case TYPE_FLOAT: {
        assert_cast<ColumnFloat32*>(col_ptr)->insert_value(cell.doubleVal);
        break;
    }

    case TYPE_DOUBLE: {
        assert_cast<ColumnFloat64*>(col_ptr)->insert_value(cell.doubleVal);
        break;
    }

    case TYPE_BOOLEAN: {
        reinterpret_cast<ColumnUInt8*>(col_ptr)->insert_value(cell.boolVal);
        break;
    }

    case TYPE_STRING:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        reinterpret_cast<ColumnString*>(col_ptr)->insert_data(cell.stringVal.data(),
                                                              cell.stringVal.size());
        break;
    }

    case TYPE_DATETIME: {
        std::vector<void*> datas(1);
        VecDateTimeValue src[1];
        CastParameters params;
        CastToDateOrDatetime::from_string_non_strict_mode<DatelikeTargetType::DATE_TIME>(
                {cell.stringVal.data(), cell.stringVal.size()}, src[0], nullptr, params);
        datas[0] = src;
        auto data = datas[0];
        reinterpret_cast<ColumnDateTime*>(col_ptr)->insert_data(reinterpret_cast<char*>(data), 0);
        break;
    }
    default: {
        std::stringstream ss;
        ss << "unsupported column type:" << type;
        return Status::InternalError(ss.str());
    }
    }
    nullable_column->push_false_to_nullmap(1);
    return Status::OK();
}

} // namespace doris
