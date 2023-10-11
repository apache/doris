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

#include "vec/common/schema_util.h"

#include <assert.h>
#include <fmt/format.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/types.h"
#include "udf/udf.h"
#include "util/string_util.h"
#include "util/thrift_rpc_helper.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_object.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/json/path_in_data.h"

namespace doris::vectorized::schema_util {

size_t get_number_of_dimensions(const IDataType& type) {
    if (const auto* type_array = typeid_cast<const DataTypeArray*>(&type)) {
        return type_array->get_number_of_dimensions();
    }
    return 0;
}
size_t get_number_of_dimensions(const IColumn& column) {
    if (const auto* column_array = check_and_get_column<ColumnArray>(column)) {
        return column_array->get_number_of_dimensions();
    }
    return 0;
}

DataTypePtr get_base_type_of_array(const DataTypePtr& type) {
    /// Get raw pointers to avoid extra copying of type pointers.
    const DataTypeArray* last_array = nullptr;
    const auto* current_type = type.get();
    while (const auto* type_array = typeid_cast<const DataTypeArray*>(current_type)) {
        current_type = type_array->get_nested_type().get();
        last_array = type_array;
    }
    return last_array ? last_array->get_nested_type() : type;
}

Array create_empty_array_field(size_t num_dimensions) {
    DCHECK(num_dimensions > 0);
    Array array;
    Array* current_array = &array;
    for (size_t i = 1; i < num_dimensions; ++i) {
        current_array->push_back(Array());
        current_array = &current_array->back().get<Array&>();
    }
    return array;
}

bool is_conversion_required_between_integers(const IDataType& lhs, const IDataType& rhs) {
    WhichDataType which_lhs(lhs);
    WhichDataType which_rhs(rhs);
    bool is_native_int = which_lhs.is_native_int() && which_rhs.is_native_int();
    bool is_native_uint = which_lhs.is_native_uint() && which_rhs.is_native_uint();
    return (is_native_int || is_native_uint) &&
           lhs.get_size_of_value_in_memory() <= rhs.get_size_of_value_in_memory();
}

bool is_conversion_required_between_integers(FieldType lhs, FieldType rhs) {
    // We only support signed integers for semi-structure data at present
    // TODO add unsigned integers
    if (lhs == FieldType::OLAP_FIELD_TYPE_BIGINT) {
        return !(rhs == FieldType::OLAP_FIELD_TYPE_TINYINT ||
                 rhs == FieldType::OLAP_FIELD_TYPE_SMALLINT ||
                 rhs == FieldType::OLAP_FIELD_TYPE_INT || rhs == FieldType::OLAP_FIELD_TYPE_BIGINT);
    }
    if (lhs == FieldType::OLAP_FIELD_TYPE_INT) {
        return !(rhs == FieldType::OLAP_FIELD_TYPE_TINYINT ||
                 rhs == FieldType::OLAP_FIELD_TYPE_SMALLINT ||
                 rhs == FieldType::OLAP_FIELD_TYPE_INT);
    }
    if (lhs == FieldType::OLAP_FIELD_TYPE_SMALLINT) {
        return !(rhs == FieldType::OLAP_FIELD_TYPE_TINYINT ||
                 rhs == FieldType::OLAP_FIELD_TYPE_SMALLINT);
    }
    if (lhs == FieldType::OLAP_FIELD_TYPE_TINYINT) {
        return !(rhs == FieldType::OLAP_FIELD_TYPE_TINYINT);
    }
    return true;
}

Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result,
                   RuntimeState* state) {
    ColumnsWithTypeAndName arguments;
    if (WhichDataType(type->get_type_id()).is_string()) {
        // Special handle ColumnString, since the original cast logic use ColumnString's first item
        // as the name of the dest type
        arguments = {arg, {type->create_column_const(1, type->get_name()), type, ""}};
    } else {
        arguments = {arg, {type->create_column_const_with_default_value(1), type, ""}};
    }
    auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments, type);
    Block tmp_block {arguments};
    // the 0 position is input argument, the 1 position is to type argument, the 2 position is result argument
    vectorized::ColumnNumbers argnum;
    argnum.emplace_back(0);
    argnum.emplace_back(1);
    size_t result_column = tmp_block.columns();
    tmp_block.insert({nullptr, type, arg.name});
    auto need_state_only = FunctionContext::create_context(state, {}, {});
    RETURN_IF_ERROR(function->execute(need_state_only.get(), tmp_block, argnum, result_column,
                                      arg.column->size()));
    *result = std::move(tmp_block.get_by_position(result_column).column);
    return Status::OK();
}

// send an empty add columns rpc, the rpc response will fill with base schema info
// maybe we could seperate this rpc from add columns rpc
Status send_fetch_full_base_schema_view_rpc(FullBaseSchemaView* schema_view) {
    TAddColumnsRequest req;
    TAddColumnsResult res;
    TTabletInfo tablet_info;
    req.__set_table_name(schema_view->table_name);
    req.__set_db_name(schema_view->db_name);
    req.__set_table_id(schema_view->table_id);
    // Set empty columns
    req.__set_addColumns({});
    auto master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    Status rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&req, &res](FrontendServiceConnection& client) { client->addColumns(res, req); },
            config::txn_commit_rpc_timeout_ms);
    if (!rpc_st.ok()) {
        return Status::InternalError("Failed to fetch schema info, encounter rpc failure");
    }
    // TODO(lhy) handle more status code
    if (res.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << "failed to fetch schema info, code:" << res.status.status_code
                     << ", msg:" << res.status.error_msgs[0];
        return Status::InvalidArgument(
                fmt::format("Failed to fetch schema info, {}", res.status.error_msgs[0]));
    }
    for (const auto& column : res.allColumns) {
        schema_view->column_name_to_column[column.column_name] = column;
    }
    schema_view->schema_version = res.schema_version;
    return Status::OK();
}

static const std::regex COLUMN_NAME_REGEX(
        "^[_a-zA-Z@0-9\\s<>/][.a-zA-Z0-9_+-/><?@#$%^&*\"\\s,:]{0,255}$");

Status unfold_object(size_t dynamic_col_position, Block& block, bool cast_to_original_type,
                     RuntimeState* state) {
    auto dynamic_col = block.get_by_position(dynamic_col_position).column->assume_mutable();
    auto* column_object_ptr = assert_cast<ColumnObject*>(dynamic_col.get());
    if (column_object_ptr->empty()) {
        return Status::OK();
    }
    size_t num_rows = column_object_ptr->size();
    CHECK(block.rows() <= num_rows);
    CHECK(column_object_ptr->is_finalized());
    Columns subcolumns;
    DataTypes types;
    std::vector<std::string> names;
    std::unordered_set<std::string> static_column_names;

    // extract columns from dynamic column
    for (auto& subcolumn : column_object_ptr->get_subcolumns()) {
        subcolumns.push_back(subcolumn->data.get_finalized_column().get_ptr());
        types.push_back(subcolumn->data.get_least_common_type());
        names.push_back(subcolumn->path.get_path());
    }
    for (size_t i = 0; i < subcolumns.size(); ++i) {
        // Block may already contains this column, eg. key columns, we should ignore
        // or replcace the same column from object subcolumn
        ColumnWithTypeAndName* column_type_name = block.try_get_by_name(names[i]);
        if (column_type_name) {
            ColumnPtr column = subcolumns[i];
            DataTypePtr dst_type = column_type_name->type;
            // Make it nullable when src is nullable but dst is not
            // since we should filter some data when slot type is not null
            // but column contains nulls
            if (!dst_type->is_nullable() && column->is_nullable()) {
                dst_type = make_nullable(dst_type);
            }
            if (cast_to_original_type && !dst_type->equals(*types[i])) {
                // Cast static columns to original slot type
                RETURN_IF_ERROR(schema_util::cast_column({subcolumns[i], types[i], ""}, dst_type,
                                                         &column, state));
            }
            // replace original column
            column_type_name->column = column;
            column_type_name->type = dst_type;
            static_column_names.emplace(names[i]);
        }
    }

    // Remove static ones remain extra dynamic columns
    column_object_ptr->remove_subcolumns(static_column_names);

    // Fill default value
    for (auto& entry : block) {
        if (entry.column->size() < num_rows) {
            entry.column->assume_mutable()->insert_many_defaults(num_rows - entry.column->size());
        }
    }
#ifndef NDEBUG
    for (const auto& column_type_name : block) {
        if (column_type_name.column->size() != num_rows) {
            LOG(FATAL) << "unmatched column:" << column_type_name.name
                       << ", expeted rows:" << num_rows
                       << ", but meet:" << column_type_name.column->size();
        }
    }
#endif
    return Status::OK();
}

void LocalSchemaChangeRecorder::add_extended_columns(const TabletColumn& new_column,
                                                     int32_t schema_version) {
    std::lock_guard<std::mutex> lock(_lock);
    _schema_version = std::max(_schema_version, schema_version);
    auto it = _extended_columns.find(new_column.name());
    if (it != _extended_columns.end()) {
        return;
    }
    _extended_columns.emplace_hint(it, new_column.name(), new_column);
}

bool LocalSchemaChangeRecorder::has_extended_columns() {
    std::lock_guard<std::mutex> lock(_lock);
    return !_extended_columns.empty();
}

std::map<std::string, TabletColumn> LocalSchemaChangeRecorder::copy_extended_columns() {
    std::lock_guard<std::mutex> lock(_lock);
    return _extended_columns;
}

const TabletColumn& LocalSchemaChangeRecorder::column(const std::string& col_name) {
    std::lock_guard<std::mutex> lock(_lock);
    assert(_extended_columns.find(col_name) != _extended_columns.end());
    return _extended_columns[col_name];
}

int32_t LocalSchemaChangeRecorder::schema_version() {
    std::lock_guard<std::mutex> lock(_lock);
    return _schema_version;
}

} // namespace doris::vectorized::schema_util
