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
#include "olap/tablet_schema.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
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
#include "vec/core/names.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_object.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/json/parse2column.h"
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

Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result) {
    ColumnsWithTypeAndName arguments {
            arg, {type->create_column_const_with_default_value(1), type, type->get_name()}};
    auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments, type);
    Block tmp_block {arguments};
    // the 0 position is input argument, the 1 position is to type argument, the 2 position is result argument
    vectorized::ColumnNumbers argnum;
    argnum.emplace_back(0);
    argnum.emplace_back(1);
    size_t result_column = tmp_block.columns();
    tmp_block.insert({nullptr, type, arg.name});
    RETURN_IF_ERROR(
            function->execute(nullptr, tmp_block, argnum, result_column, arg.column->size()));
    *result = std::move(tmp_block.get_by_position(result_column).column);
    return Status::OK();
}

void get_column_by_type(const vectorized::DataTypePtr& data_type, const std::string& name,
                        TabletColumn& column) {
    column.set_name(name);
    column.set_type(data_type->get_type_as_field_type());
    // -1 indicates it's not a Frontend generated column
    column.set_unique_id(-1);
    if (data_type->is_nullable()) {
        const auto& real_type = static_cast<const DataTypeNullable&>(*data_type);
        column.set_is_nullable(true);
        get_column_by_type(real_type.get_nested_type(), name, column);
        return;
    }
    if (data_type->get_type_id() == TypeIndex::Array) {
        TabletColumn child;
        get_column_by_type(assert_cast<const DataTypeArray*>(data_type.get())->get_nested_type(),
                           "", child);
        column.set_length(TabletColumn::get_field_length_by_type(
                data_type->get_type_as_tprimitive_type(), 0));
        column.set_default_value("[]");
        column.add_sub_column(child);
        return;
    }
    if (WhichDataType(*data_type).is_string()) {
        return;
    }
    if (WhichDataType(*data_type).is_simple()) {
        column.set_length(data_type->get_size_of_value_in_memory());
        return;
    }
    // TODO handle more types like struct/date/datetime/decimal...
    __builtin_unreachable();
}

void update_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                                TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id) {
    // Types of subcolumns by path from all tuples.
    std::unordered_map<PathInData, DataTypes, PathInData::Hash> subcolumns_types;
    for (const TabletSchemaSPtr& schema : schemas) {
        for (const TabletColumn& col : schema->columns()) {
            // Get subcolumns of this variant
            if (!col.path_info().empty() && col.parent_unique_d() > 0 &&
                col.parent_unique_d() == variant_col_unique_id) {
                subcolumns_types[col.path_info()].push_back(
                        DataTypeFactory::instance().create_data_type(col, col.is_nullable()));
            }
        }
    }
    PathsInData tuple_paths;
    DataTypes tuple_types;
    // Get the least common type for all paths.
    for (const auto& [key, subtypes] : subcolumns_types) {
        assert(!subtypes.empty());
        if (key.get_path() == ColumnObject::COLUMN_NAME_DUMMY) {
            continue;
        }
        size_t first_dim = get_number_of_dimensions(*subtypes[0]);
        tuple_paths.emplace_back(key);
        for (size_t i = 1; i < subtypes.size(); ++i) {
            if (first_dim != get_number_of_dimensions(*subtypes[i])) {
                tuple_types.emplace_back(make_nullable(std::make_shared<DataTypeJsonb>()));
                LOG(INFO) << fmt::format(
                        "Uncompatible types of subcolumn '{}': {} and {}, cast to JSONB",
                        key.get_path(), subtypes[0]->get_name(), subtypes[i]->get_name());
                break;
            }
        }
        if (tuple_paths.size() == tuple_types.size()) {
            continue;
        }
        DataTypePtr common_type;
        get_least_supertype<LeastSupertypeOnError::Jsonb>(subtypes, &common_type);
        // Array Column is not nullable at present, but other types should all be nullable
        if (common_type->get_type_id() != TypeIndex::Array && !common_type->is_nullable()) {
            common_type = make_nullable(common_type);
        }
        tuple_types.emplace_back(common_type);
    }
    CHECK_EQ(tuple_paths.size(), tuple_types.size());

    // TODO handle ambiguos path and deduce to JSONB type

    std::string variant_col_name = common_schema->column_by_uid(variant_col_unique_id).name();
    // Append all common type columns of this variant
    for (int i = 0; i < tuple_paths.size(); ++i) {
        TabletColumn common_column;
        const std::string& column_name = variant_col_name + "." + tuple_paths[i].get_path();
        get_column_by_type(tuple_types[i], column_name, common_column);
        common_column.set_parent_unique_id(variant_col_unique_id);
        common_column.set_path_info(tuple_paths[i]);
        common_schema->append_column(common_column);
    }
}

void get_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                             TabletSchemaSPtr& common_schema) {
    // Pick tablet schema with max schema version
    const TabletSchemaSPtr base_schema =
            *std::max_element(schemas.cbegin(), schemas.cend(),
                              [](const TabletSchemaSPtr a, const TabletSchemaSPtr b) {
                                  return a->schema_version() < b->schema_version();
                              });
    CHECK(base_schema);
    CHECK(common_schema);
    std::vector<int32_t> variant_column_unique_id;
    // Get all columns without extracted columns and collect variant col unique id
    for (const TabletColumn& col : base_schema->columns()) {
        if (col.is_variant_type()) {
            variant_column_unique_id.push_back(col.unique_id());
        }
        if (!col.is_extracted_column()) {
            common_schema->append_column(col);
        }
    }
    for (int32_t unique_id : variant_column_unique_id) {
        update_least_common_schema(schemas, common_schema, unique_id);
    }
}

void parse_variant_columns(Block& block, const std::vector<int>& variant_pos) {
    for (int i = 0; i < variant_pos.size(); ++i) {
        auto& column = block.get_by_position(variant_pos[i]).column;
        const ColumnString& raw_json_column =
                column->is_nullable() ? assert_cast<const ColumnString&>(
                                                assert_cast<const ColumnNullable&>(*column.get())
                                                        .get_nested_column())
                                      : assert_cast<const ColumnString&>(*column.get());
        MutableColumnPtr variant_column = ColumnObject::create(true);
        parse_json_to_variant(*variant_column.get(), raw_json_column);
        block.get_by_position(variant_pos[i]).column = variant_column->get_ptr();
        block.get_by_position(variant_pos[i]).type = std::make_shared<DataTypeObject>("json", true);
    }
}

void finalize_variant_columns(Block& block, const std::vector<int>& variant_pos) {
    for (int i = 0; i < variant_pos.size(); ++i) {
        auto& column = assert_cast<ColumnObject&>(
                block.get_by_position(variant_pos[i]).column->assume_mutable_ref());
        column.finalize();
    }
}

static TColumnDef get_columndef(const TabletColumn& column) {
    TColumnDesc t_column_desc;
    t_column_desc.__set_columnName(column.name());
    t_column_desc.__set_columnLength(column.length());
    t_column_desc.__set_colUniqueId(column.unique_id());
    t_column_desc.__set_parentColUniqueId(column.parent_unique_d());
    t_column_desc.__set_columnType(
            DataTypeFactory::instance().create_data_type(column)->get_type_as_tprimitive_type());
    t_column_desc.__set_isAllowNull(column.is_nullable());

    if (!column.get_sub_columns().empty()) {
        t_column_desc.__isset.children = true;
    }
    for (const TabletColumn& subcolumn : column.get_sub_columns()) {
        TColumnDef t_child_column_def = get_columndef(subcolumn);
        t_column_desc.children.push_back(std::move(t_child_column_def.columnDesc));
    }
    TColumnDef t_column_def;
    t_column_def.__set_columnDesc(std::move(t_column_desc));
    return t_column_def;
}

Status update_front_end_schema(UpdateSchemaRequest& request) {
    TModifyColumnsRequest modify_request;
    TModifyColumnsResult modify_res;
    modify_request.__set_index_id(request.index_id);
    modify_request.__set_table_id(request.tablet_id);
    modify_request.__set_schema_version(request.schema_version);
    if (request.new_columns_pos.empty() && request.modifying_columns.empty()) {
        return Status::OK();
    }
    modify_request.__isset.newColumns = true;
    for (int pos : request.new_columns_pos) {
        modify_request.newColumns.push_back(get_columndef(request.from_schema->columns()[pos]));
    }
    modify_request.__isset.modifyColumns = true;
    for (int pos : request.modifying_columns) {
        CHECK(request.from_schema->columns()[pos].unique_id() > 0);
        modify_request.modifyColumns.push_back(get_columndef(request.from_schema->columns()[pos]));
    }
    auto master_addr = ExecEnv::GetInstance()->master_info()->network_address;
    Status rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&modify_request, &modify_res](FrontendServiceConnection& client) {
                client->modifyColumns(modify_res, modify_request);
            },
            config::txn_commit_rpc_timeout_ms);
    if (!rpc_st.ok()) {
        return Status::InternalError("Failed to do schema change, rpc error {}",
                                     rpc_st.to_string());
    }
    if (modify_res.status.status_code != TStatusCode::CONFLICT_SCHEMA_VERSION &&
        modify_res.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << fmt::format("Encouter unkown modifyColumns RPC error, status: {}, msg: {}",
                                    modify_res.status.status_code, modify_res.status.error_msgs[0]);
        return Status::InternalError(modify_res.status.error_msgs[0]);
    }
    CHECK(!modify_res.allColumns.empty());

    // column_name->column
    phmap::flat_hash_map<std::string, const TColumn*> column_name_map;
    for (const TColumn& c : modify_res.allColumns) {
        column_name_map[c.column_name] = &c;
        // flatten it's subcolumns
        if (c.column_type.type == TPrimitiveType::VARIANT) {
            for (const TColumn& subcol : c.children_column) {
                column_name_map[subcol.column_name] = &subcol;
            }
        }
    }

    // Schema version missmatch, CAS failed retry
    if (modify_res.status.status_code == TStatusCode::CONFLICT_SCHEMA_VERSION) {
        LOG(INFO) << fmt::format(
                "Index {} meet conflict schema version, retry. Expected {} but meet {}",
                modify_request.index_id, modify_request.schema_version, modify_res.schema_version);
        if (--request.max_try <= 0) {
            return Status::InternalError("Reach max retry, meet too many conflict schema version");
        }
        if (request.need_backoff) {
            // TODO
        }
        // Get least common schema, and refresh new columns, and try again
        std::vector<int> refreshed_new_columns_pos;
        for (int pos : request.new_columns_pos) {
            TabletColumn& new_column = request.from_schema->mutable_columns()[pos];
            auto it = column_name_map.find(new_column.name());
            if (it == column_name_map.end()) {
                refreshed_new_columns_pos.push_back(pos);
            }
        }
        std::vector<int> refreshed_modify_columns_pos;
        // for (int pos : request.modifying_columns) {
        //     // TODO update to least common schema
        // }
        // retry
        request.modifying_columns = refreshed_modify_columns_pos;
        request.new_columns_pos = refreshed_new_columns_pos;
        RETURN_IF_ERROR(update_front_end_schema(request));
        return Status::OK();
    }
    // Set unique id for new columns
    for (int pos : request.new_columns_pos) {
        TabletColumn& new_column = request.from_schema->mutable_columns()[pos];
        auto it = column_name_map.find(new_column.name());
        CHECK(it != column_name_map.end());
        CHECK(it->second->__isset.col_unique_id);
        new_column.set_unique_id(it->second->col_unique_id);
    }
    return Status::OK();
}

} // namespace doris::vectorized::schema_util
