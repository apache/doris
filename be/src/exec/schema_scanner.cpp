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

#include "exec/schema_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <string.h>

#include <new>
#include <ostream>
#include <utility>

#include "exec/schema_scanner/schema_charsets_scanner.h"
#include "exec/schema_scanner/schema_collations_scanner.h"
#include "exec/schema_scanner/schema_columns_scanner.h"
#include "exec/schema_scanner/schema_dummy_scanner.h"
#include "exec/schema_scanner/schema_files_scanner.h"
#include "exec/schema_scanner/schema_metadata_name_ids_scanner.h"
#include "exec/schema_scanner/schema_partitions_scanner.h"
#include "exec/schema_scanner/schema_profiling_scanner.h"
#include "exec/schema_scanner/schema_rowsets_scanner.h"
#include "exec/schema_scanner/schema_schema_privileges_scanner.h"
#include "exec/schema_scanner/schema_schemata_scanner.h"
#include "exec/schema_scanner/schema_table_privileges_scanner.h"
#include "exec/schema_scanner/schema_tables_scanner.h"
#include "exec/schema_scanner/schema_user_privileges_scanner.h"
#include "exec/schema_scanner/schema_variables_scanner.h"
#include "exec/schema_scanner/schema_views_scanner.h"
#include "olap/hll.h"
#include "runtime/define_primitive_type.h"
#include "util/types.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris {
class ObjectPool;

SchemaScanner::SchemaScanner(const std::vector<ColumnDesc>& columns)
        : _is_init(false),
          _param(nullptr),
          _columns(columns),
          _schema_table_type(TSchemaTableType::SCH_INVALID) {}

SchemaScanner::SchemaScanner(const std::vector<ColumnDesc>& columns, TSchemaTableType::type type)
        : _is_init(false), _param(nullptr), _columns(columns), _schema_table_type(type) {}

SchemaScanner::~SchemaScanner() {}

Status SchemaScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("call Start before Init.");
    }

    return Status::OK();
}

Status SchemaScanner::get_next_block(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }

    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    return Status::OK();
}

Status SchemaScanner::init(SchemaScannerParam* param, ObjectPool* pool) {
    if (_is_init) {
        return Status::OK();
    }
    if (nullptr == param || nullptr == pool) {
        return Status::InternalError("invalid parameter");
    }

    _param = param;
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
    case TSchemaTableType::SCH_ROWSETS:
        return SchemaRowsetsScanner::create_unique();
    case TSchemaTableType::SCH_METADATA_NAME_IDS:
        return SchemaMetadataNameIdsScanner::create_unique();
    case TSchemaTableType::SCH_PROFILING:
        return SchemaProfilingScanner::create_unique();
    default:
        return SchemaDummyScanner::create_unique();
        break;
    }
}

Status SchemaScanner::fill_dest_column_for_range(vectorized::Block* block, size_t pos,
                                                 const std::vector<void*>& datas) {
    const ColumnDesc& col_desc = _columns[pos];
    vectorized::MutableColumnPtr column_ptr;
    column_ptr = std::move(*block->get_by_position(pos).column).assume_mutable();
    vectorized::IColumn* col_ptr = column_ptr.get();

    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(col_ptr);

    // Resize in advance to improve insertion efficiency.
    size_t fill_num = datas.size();
    col_ptr = &nullable_column->get_nested_column();
    for (int i = 0; i < fill_num; ++i) {
        auto data = datas[i];
        if (data == nullptr) {
            // For nested column need not insert default.
            nullable_column->insert_data(nullptr, 0);
            continue;
        } else {
            nullable_column->get_null_map_data().emplace_back(0);
        }
        switch (col_desc.type) {
        case TYPE_HLL: {
            HyperLogLog* hll_slot = reinterpret_cast<HyperLogLog*>(data);
            reinterpret_cast<vectorized::ColumnHLL*>(col_ptr)->get_data().emplace_back(*hll_slot);
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING: {
            StringRef* str_slot = reinterpret_cast<StringRef*>(data);
            reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(str_slot->data,
                                                                              str_slot->size);
            break;
        }

        case TYPE_BOOLEAN: {
            uint8_t num = *reinterpret_cast<bool*>(data);
            reinterpret_cast<vectorized::ColumnVector<vectorized::UInt8>*>(col_ptr)->insert_value(
                    num);
            break;
        }

        case TYPE_TINYINT: {
            int8_t num = *reinterpret_cast<int8_t*>(data);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int8>*>(col_ptr)->insert_value(
                    num);
            break;
        }

        case TYPE_SMALLINT: {
            int16_t num = *reinterpret_cast<int16_t*>(data);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int16>*>(col_ptr)->insert_value(
                    num);
            break;
        }

        case TYPE_INT: {
            int32_t num = *reinterpret_cast<int32_t*>(data);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int32>*>(col_ptr)->insert_value(
                    num);
            break;
        }

        case TYPE_BIGINT: {
            int64_t num = *reinterpret_cast<int64_t*>(data);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(
                    num);
            break;
        }

        case TYPE_LARGEINT: {
            __int128 num;
            memcpy(&num, data, sizeof(__int128));
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)->insert_value(
                    num);
            break;
        }

        case TYPE_FLOAT: {
            float num = *reinterpret_cast<float*>(data);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Float32>*>(col_ptr)->insert_value(
                    num);
            break;
        }

        case TYPE_DOUBLE: {
            double num = *reinterpret_cast<double*>(data);
            reinterpret_cast<vectorized::ColumnVector<vectorized::Float64>*>(col_ptr)->insert_value(
                    num);
            break;
        }

        case TYPE_DATE: {
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                    reinterpret_cast<char*>(data), 0);
            break;
        }

        case TYPE_DATEV2: {
            uint32_t num = *reinterpret_cast<uint32_t*>(data);
            reinterpret_cast<vectorized::ColumnVector<vectorized::UInt32>*>(col_ptr)->insert_value(
                    num);
            break;
        }

        case TYPE_DATETIME: {
            reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                    reinterpret_cast<char*>(data), 0);
            break;
        }

        case TYPE_DATETIMEV2: {
            uint32_t num = *reinterpret_cast<uint64_t*>(data);
            reinterpret_cast<vectorized::ColumnVector<vectorized::UInt64>*>(col_ptr)->insert_value(
                    num);
            break;
        }

        case TYPE_DECIMALV2: {
            const vectorized::Int128 num = (reinterpret_cast<PackedInt128*>(data))->value;
            reinterpret_cast<vectorized::ColumnDecimal128V2*>(col_ptr)->insert_data(
                    reinterpret_cast<const char*>(&num), 0);
            break;
        }
        case TYPE_DECIMAL128I: {
            const vectorized::Int128 num = (reinterpret_cast<PackedInt128*>(data))->value;
            reinterpret_cast<vectorized::ColumnDecimal128V3*>(col_ptr)->insert_data(
                    reinterpret_cast<const char*>(&num), 0);
            break;
        }

        case TYPE_DECIMAL32: {
            const int32_t num = *reinterpret_cast<int32_t*>(data);
            reinterpret_cast<vectorized::ColumnDecimal32*>(col_ptr)->insert_data(
                    reinterpret_cast<const char*>(&num), 0);
            break;
        }

        case TYPE_DECIMAL64: {
            const int64_t num = *reinterpret_cast<int64_t*>(data);
            reinterpret_cast<vectorized::ColumnDecimal64*>(col_ptr)->insert_data(
                    reinterpret_cast<const char*>(&num), 0);
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

} // namespace doris
