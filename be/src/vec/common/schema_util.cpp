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
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exprs/json_functions.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/segment_v2/variant_column_writer_impl.h"
#include "olap/segment_loader.h"
#include "olap/tablet.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_schema.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "util/defer_op.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/field_visitors.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_object.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/json/json_parser.h"
#include "vec/json/parse2column.h"
#include "vec/json/path_in_data.h"

namespace doris::vectorized::schema_util {
#include "common/compile_check_begin.h"

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

size_t get_size_of_interger(TypeIndex type) {
    switch (type) {
    case TypeIndex::Int8:
        return sizeof(int8_t);
    case TypeIndex::Int16:
        return sizeof(int16_t);
    case TypeIndex::Int32:
        return sizeof(int32_t);
    case TypeIndex::Int64:
        return sizeof(int64_t);
    case TypeIndex::Int128:
        return sizeof(int128_t);
    case TypeIndex::UInt8:
        return sizeof(uint8_t);
    case TypeIndex::UInt16:
        return sizeof(uint16_t);
    case TypeIndex::UInt32:
        return sizeof(uint32_t);
    case TypeIndex::UInt64:
        return sizeof(uint64_t);
    case TypeIndex::UInt128:
        return sizeof(uint128_t);
    default:
        throw Exception(Status::FatalError("Unknown integer type: {}", getTypeName(type)));
        return 0;
    }
}

bool is_conversion_required_between_integers(const TypeIndex& lhs, const TypeIndex& rhs) {
    WhichDataType which_lhs(lhs);
    WhichDataType which_rhs(rhs);
    bool is_native_int = which_lhs.is_native_int() && which_rhs.is_native_int();
    bool is_native_uint = which_lhs.is_native_uint() && which_rhs.is_native_uint();
    return (!is_native_int && !is_native_uint) ||
           get_size_of_interger(lhs) > get_size_of_interger(rhs);
}

Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result) {
    ColumnsWithTypeAndName arguments {arg, {nullptr, type, type->get_name()}};

    // To prevent from null info lost, we should not call function since the function framework will wrap
    // nullable to Variant instead of the root of Variant
    // correct output: Nullable(Array(int)) -> Nullable(Variant(Nullable(Array(int))))
    // incorrect output: Nullable(Array(int)) -> Nullable(Variant(Array(int)))
    if (WhichDataType(remove_nullable(type)).is_variant_type()) {
        // If source column is variant, so the nullable info is different from dst column
        if (WhichDataType(remove_nullable(arg.type)).is_variant_type()) {
            *result = type->is_nullable() ? make_nullable(arg.column) : remove_nullable(arg.column);
            return Status::OK();
        }
        // set variant root column/type to from column/type
        CHECK(arg.column->is_nullable());
        auto to_type = remove_nullable(type);
        const auto& data_type_object = assert_cast<const DataTypeObject&>(*to_type);
        auto variant = ColumnObject::create(data_type_object.variant_max_subcolumns_count());

        variant->create_root(arg.type, arg.column->assume_mutable());
        ColumnPtr nullable = ColumnNullable::create(
                variant->get_ptr(),
                check_and_get_column<ColumnNullable>(arg.column.get())->get_null_map_column_ptr());
        *result = type->is_nullable() ? nullable : variant->get_ptr();
        return Status::OK();
    }

    auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments, type);
    if (!function) {
        return Status::InternalError("Not found cast function {} to {}", arg.type->get_name(),
                                     type->get_name());
    }
    Block tmp_block {arguments};
    uint32_t result_column = cast_set<uint32_t>(tmp_block.columns());
    RuntimeState state;
    auto ctx = FunctionContext::create_context(&state, {}, {});

    if (WhichDataType(arg.type).is_nothing()) {
        // cast from nothing to any type should result in nulls
        *result = type->create_column_const_with_default_value(arg.column->size())
                          ->convert_to_full_column_if_const();
        return Status::OK();
    }

    // We convert column string to jsonb type just add a string jsonb field to dst column instead of parse
    // each line in original string column.
    ctx->set_string_as_jsonb_string(true);
    ctx->set_jsonb_string_as_string(true);
    tmp_block.insert({nullptr, type, arg.name});
    if (!function->execute(ctx.get(), tmp_block, {0}, result_column, arg.column->size())) {
        LOG_EVERY_N(WARNING, 100) << fmt::format("cast from {} to {}", arg.type->get_name(),
                                                 type->get_name());
        *result = type->create_column_const_with_default_value(arg.column->size())
                          ->convert_to_full_column_if_const();
        return Status::OK();
    }
    *result = tmp_block.get_by_position(result_column).column->convert_to_full_column_if_const();
    VLOG_DEBUG << fmt::format("{} before convert {}, after convert {}", arg.name,
                              arg.column->get_name(), (*result)->get_name());
    return Status::OK();
}

void get_column_by_type(const vectorized::DataTypePtr& data_type, const std::string& name,
                        TabletColumn& column, const ExtraInfo& ext_info) {
    column.set_name(name);
    column.set_type(data_type->get_storage_field_type());
    if (ext_info.unique_id >= 0) {
        column.set_unique_id(ext_info.unique_id);
    }
    if (ext_info.parent_unique_id >= 0) {
        column.set_parent_unique_id(ext_info.parent_unique_id);
    }
    if (!ext_info.path_info.empty()) {
        column.set_path_info(ext_info.path_info);
    }
    if (data_type->is_nullable()) {
        const auto& real_type = static_cast<const DataTypeNullable&>(*data_type);
        column.set_is_nullable(true);
        get_column_by_type(real_type.get_nested_type(), name, column, {});
        return;
    }
    if (data_type->get_type_id() == TypeIndex::Array) {
        TabletColumn child;
        get_column_by_type(assert_cast<const DataTypeArray*>(data_type.get())->get_nested_type(),
                           "", child, {});
        column.set_length(TabletColumn::get_field_length_by_type(TPrimitiveType::ARRAY, 0));
        column.add_sub_column(child);
        return;
    }
    // size is not fixed when type is string or json
    if (WhichDataType(*data_type).is_string() || WhichDataType(*data_type).is_json()) {
        column.set_length(INT_MAX);
        return;
    }
    if (WhichDataType(*data_type).is_simple()) {
        column.set_length(data_type->get_size_of_value_in_memory());
        return;
    }
    // none json type
    if (WhichDataType(*data_type).is_decimal()) {
        column.set_precision_frac(data_type->get_precision(), data_type->get_scale());
        column.set_is_decimal(true);
        return;
    }
    if (WhichDataType(*data_type).is_date_time_v2()) {
        column.set_precision_frac(-1, data_type->get_scale());
        return;
    }

    // TODO handle more types like struct/date/datetime/decimal...
    throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                           "unexcepted data column type: {}, column name is: {}",
                           data_type->get_name(), name);
}

TabletColumn get_column_by_type(const vectorized::DataTypePtr& data_type, const std::string& name,
                                const ExtraInfo& ext_info) {
    TabletColumn result;
    get_column_by_type(data_type, name, result, ext_info);
    return result;
}

void update_least_schema_internal(const std::map<PathInData, DataTypes>& subcolumns_types,
                                  TabletSchemaSPtr& common_schema, bool update_sparse_column,
                                  int32_t variant_col_unique_id,
                                  const std::map<std::string, TabletColumnPtr>& typed_columns,
                                  std::set<PathInData>* path_set = nullptr) {
    PathsInData tuple_paths;
    DataTypes tuple_types;
    CHECK(common_schema.use_count() == 1);
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
        get_least_supertype_jsonb(subtypes, &common_type);
        if (!common_type->is_nullable()) {
            common_type = make_nullable(common_type);
        }
        tuple_types.emplace_back(common_type);
    }
    CHECK_EQ(tuple_paths.size(), tuple_types.size());

    // Append all common type columns of this variant
    for (int i = 0; i < tuple_paths.size(); ++i) {
        TabletColumn common_column;
        // typed path not contains root part
        auto path_without_root = tuple_paths[i].copy_pop_front().get_path();
        if (typed_columns.contains(path_without_root) && !tuple_paths[i].has_nested_part()) {
            common_column = *typed_columns.at(path_without_root);
            // parent unique id and path may not be init in write path
            common_column.set_parent_unique_id(variant_col_unique_id);
            common_column.set_path_info(tuple_paths[i]);
            common_column.set_name(tuple_paths[i].get_path());
        } else {
            // const std::string& column_name = variant_col_name + "." + tuple_paths[i].get_path();
            get_column_by_type(tuple_types[i], tuple_paths[i].get_path(), common_column,
                               ExtraInfo {.unique_id = -1,
                                          .parent_unique_id = variant_col_unique_id,
                                          .path_info = tuple_paths[i]});
        }
        if (update_sparse_column) {
            common_schema->mutable_column_by_uid(variant_col_unique_id)
                    .append_sparse_column(common_column);
        } else {
            common_schema->append_column(common_column);
        }
        if (path_set != nullptr) {
            path_set->insert(tuple_paths[i]);
        }
    }
}

void update_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                                TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id,
                                std::set<PathInData>* path_set) {
    std::map<std::string, TabletColumnPtr> typed_columns;
    for (const TabletColumnPtr& col :
         common_schema->column_by_uid(variant_col_unique_id).get_sub_columns()) {
        typed_columns[col->name()] = col;
    }
    // Types of subcolumns by path from all tuples.
    std::map<PathInData, DataTypes> subcolumns_types;
    for (const TabletSchemaSPtr& schema : schemas) {
        for (const TabletColumnPtr& col : schema->columns()) {
            // Get subcolumns of this variant
            if (col->has_path_info() && col->parent_unique_id() > 0 &&
                col->parent_unique_id() == variant_col_unique_id) {
                subcolumns_types[*col->path_info_ptr()].emplace_back(
                        DataTypeFactory::instance().create_data_type(*col, col->is_nullable()));
            }
        }
    }
    for (const TabletSchemaSPtr& schema : schemas) {
        if (schema->field_index(variant_col_unique_id) == -1) {
            // maybe dropped
            continue;
        }
        for (const TabletColumnPtr& col :
             schema->column_by_uid(variant_col_unique_id).sparse_columns()) {
            // Get subcolumns of this variant
            if (col->has_path_info() && col->parent_unique_id() > 0 &&
                col->parent_unique_id() == variant_col_unique_id &&
                // this column have been found in origin columns
                subcolumns_types.find(*col->path_info_ptr()) != subcolumns_types.end()) {
                subcolumns_types[*col->path_info_ptr()].emplace_back(
                        DataTypeFactory::instance().create_data_type(*col, col->is_nullable()));
            }
        }
    }
    update_least_schema_internal(subcolumns_types, common_schema, false, variant_col_unique_id,
                                 typed_columns, path_set);
}

void update_least_sparse_column(const std::vector<TabletSchemaSPtr>& schemas,
                                TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id,
                                const std::set<PathInData>& path_set) {
    std::map<std::string, TabletColumnPtr> typed_columns;
    for (const TabletColumnPtr& col :
         common_schema->column_by_uid(variant_col_unique_id).get_sub_columns()) {
        typed_columns[col->name()] = col;
    }
    // Types of subcolumns by path from all tuples.
    std::map<PathInData, DataTypes> subcolumns_types;
    for (const TabletSchemaSPtr& schema : schemas) {
        if (schema->field_index(variant_col_unique_id) == -1) {
            // maybe dropped
            continue;
        }
        for (const TabletColumnPtr& col :
             schema->column_by_uid(variant_col_unique_id).sparse_columns()) {
            // Get subcolumns of this variant
            if (col->has_path_info() && col->parent_unique_id() > 0 &&
                col->parent_unique_id() == variant_col_unique_id &&
                path_set.find(*col->path_info_ptr()) == path_set.end()) {
                subcolumns_types[*col->path_info_ptr()].emplace_back(
                        DataTypeFactory::instance().create_data_type(*col, col->is_nullable()));
            }
        }
    }
    update_least_schema_internal(subcolumns_types, common_schema, true, variant_col_unique_id,
                                 typed_columns);
}

void inherit_column_attributes(const TabletColumn& source, TabletColumn& target,
                               TabletSchemaSPtr* target_schema) {
    DCHECK(target.is_extracted_column());
    target.set_aggregation_method(source.aggregation());

    // 1. bloom filter
    if (target.type() != FieldType::OLAP_FIELD_TYPE_TINYINT &&
        target.type() != FieldType::OLAP_FIELD_TYPE_ARRAY &&
        target.type() != FieldType::OLAP_FIELD_TYPE_DOUBLE &&
        target.type() != FieldType::OLAP_FIELD_TYPE_FLOAT) {
        // above types are not supported in bf
        target.set_is_bf_column(source.is_bf_column());
    }

    if (!target_schema) {
        return;
    }

    // 2. inverted index
    const auto* source_index_meta = (*target_schema)->inverted_index(source.unique_id());
    if (source_index_meta != nullptr) {
        // add index meta
        TabletIndex index_info = *source_index_meta;
        index_info.set_escaped_escaped_index_suffix_path(target.path_info_ptr()->get_path());
        const auto* target_index_meta =
                (*target_schema)
                        ->inverted_index(target.parent_unique_id(),
                                         target.path_info_ptr()->get_path());
        if (target_index_meta != nullptr) {
            // already exist
            (*target_schema)->update_index(target, IndexType::INVERTED, std::move(index_info));
        } else {
            (*target_schema)->append_index(std::move(index_info));
        }
    }

    // 3. TODO: gnragm bf index
}

void inherit_column_attributes(TabletSchemaSPtr& schema) {
    // Add index meta if extracted column is missing index meta
    for (size_t i = 0; i < schema->num_columns(); ++i) {
        TabletColumn& col = schema->mutable_column(i);
        if (!col.is_extracted_column()) {
            continue;
        }
        if (schema->field_index(col.parent_unique_id()) == -1) {
            // parent column is missing, maybe dropped
            continue;
        }
        inherit_column_attributes(schema->column_by_uid(col.parent_unique_id()), col, &schema);
    }
}

Status get_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                               const TabletSchemaSPtr& base_schema, TabletSchemaSPtr& output_schema,
                               bool check_schema_size) {
    std::vector<int32_t> variant_column_unique_id;

    // Construct a schema excluding the extracted columns and gather unique identifiers for variants.
    // Ensure that the output schema also excludes these extracted columns. This approach prevents
    // duplicated paths following the update_least_common_schema process.
    auto build_schema_without_extracted_columns = [&](const TabletSchemaSPtr& base_schema) {
        output_schema = std::make_shared<TabletSchema>();
        // not copy columns but only shadow copy other attributes
        output_schema->shawdow_copy_without_columns(*base_schema);
        // Get all columns without extracted columns and collect variant col unique id
        for (const TabletColumnPtr& col : base_schema->columns()) {
            if (col->is_variant_type()) {
                variant_column_unique_id.push_back(col->unique_id());
            }
            if (!col->is_extracted_column()) {
                output_schema->append_column(*col);
            }
        }
    };
    if (base_schema == nullptr) {
        // Pick tablet schema with max schema version
        auto max_version_schema =
                *std::max_element(schemas.cbegin(), schemas.cend(),
                                  [](const TabletSchemaSPtr a, const TabletSchemaSPtr b) {
                                      return a->schema_version() < b->schema_version();
                                  });
        CHECK(max_version_schema);
        build_schema_without_extracted_columns(max_version_schema);
    } else {
        // use input base_schema schema as base schema
        build_schema_without_extracted_columns(base_schema);
    }

    // schema consists of two parts, static part of columns, variant columns (include extracted columns and sparse columns)
    //     static     extracted       sparse
    // | --------- | ----------- | ------------|
    // If a sparse column in one schema's is found in another schema's extracted columns
    // move it out of the sparse column and merge it into the extracted column.
    //                     static                extracted                         sparse
    //                 | --------- | ----------- ------------ ------- ---------| ------------|
    //    schema 1:       k (int)     v:a (float)       v:c (string)               v:b (int)
    //    schema 2:       k (int)     v:a (float)       v:b (bigint)               v:d (string)
    //    schema 3:       k (int)     v:a (double)      v:b (smallint)
    //    result :        k (int)     v:a (double)  v:b (bigint) v:c (string)      v:d (string)
    for (int32_t unique_id : variant_column_unique_id) {
        std::set<PathInData> path_set;
        // 1. cast extracted column to common type
        // path set is used to record the paths of those sparse columns that have been merged into the extracted columns, eg: v:b
        update_least_common_schema(schemas, output_schema, unique_id, &path_set);
        // 2. cast sparse column to common type, exclude the columns from the path set
        update_least_sparse_column(schemas, output_schema, unique_id, path_set);
    }

    inherit_column_attributes(output_schema);
    if (check_schema_size &&
        output_schema->columns().size() > config::variant_max_merged_tablet_schema_size) {
        return Status::DataQualityError("Reached max column size limit {}",
                                        config::variant_max_merged_tablet_schema_size);
    }

    return Status::OK();
}

Status _parse_variant_columns(Block& block, const std::vector<int>& variant_pos,
                              const ParseConfig& config) {
    for (int i = 0; i < variant_pos.size(); ++i) {
        auto column_ref = block.get_by_position(variant_pos[i]).column;
        bool is_nullable = column_ref->is_nullable();
        const auto& column = remove_nullable(column_ref);
        const auto& var = assert_cast<const ColumnObject&>(*column.get());
        var.assume_mutable_ref().finalize();

        MutableColumnPtr variant_column;
        if (!var.is_scalar_variant()) {
            variant_column = var.assume_mutable();
            // already parsed
            continue;
        }
        ColumnPtr scalar_root_column;
        if (WhichDataType(remove_nullable(var.get_root_type())).is_json()) {
            // TODO more efficient way to parse jsonb type, currently we just convert jsonb to
            // json str and parse them into variant
            RETURN_IF_ERROR(cast_column({var.get_root(), var.get_root_type(), ""},
                                        var.get_root()->is_nullable()
                                                ? make_nullable(std::make_shared<DataTypeString>())
                                                : std::make_shared<DataTypeString>(),
                                        &scalar_root_column));
            if (scalar_root_column->is_nullable()) {
                scalar_root_column = assert_cast<const ColumnNullable*>(scalar_root_column.get())
                                             ->get_nested_column_ptr();
            }
        } else {
            const auto& root = *var.get_root();
            scalar_root_column =
                    root.is_nullable()
                            ? assert_cast<const ColumnNullable&>(root).get_nested_column_ptr()
                            : var.get_root();
        }

        if (scalar_root_column->is_column_string()) {
            // now, subcolumns have not been set, so we set it to 0
            variant_column = ColumnObject::create(0);
            parse_json_to_variant(*variant_column.get(),
                                  assert_cast<const ColumnString&>(*scalar_root_column), config);
        } else {
            // Root maybe other types rather than string like ColumnObject(Int32).
            // In this case, we should finlize the root and cast to JSON type
            auto expected_root_type =
                    make_nullable(std::make_shared<ColumnObject::MostCommonType>());
            const_cast<ColumnObject&>(var).ensure_root_node_type(expected_root_type);
            variant_column = var.assume_mutable();
        }

        // Wrap variant with nullmap if it is nullable
        ColumnPtr result = variant_column->get_ptr();
        if (is_nullable) {
            const auto& null_map =
                    assert_cast<const ColumnNullable&>(*column_ref).get_null_map_column_ptr();
            result = ColumnNullable::create(result, null_map);
        }
        block.get_by_position(variant_pos[i]).column = result;
    }
    return Status::OK();
}

Status parse_variant_columns(Block& block, const std::vector<int>& variant_pos,
                             const ParseConfig& config) {
    // Parse each variant column from raw string column
    RETURN_IF_CATCH_EXCEPTION({
        return vectorized::schema_util::_parse_variant_columns(block, variant_pos, config);
    });
}

// sort by paths in lexicographical order
vectorized::ColumnObject::Subcolumns get_sorted_subcolumns(
        const vectorized::ColumnObject::Subcolumns& subcolumns) {
    // sort by paths in lexicographical order
    vectorized::ColumnObject::Subcolumns sorted = subcolumns;
    std::sort(sorted.begin(), sorted.end(), [](const auto& lhsItem, const auto& rhsItem) {
        return lhsItem->path < rhsItem->path;
    });
    return sorted;
}

bool has_schema_index_diff(const TabletSchema* new_schema, const TabletSchema* old_schema,
                           int32_t new_col_idx, int32_t old_col_idx) {
    const auto& column_new = new_schema->column(new_col_idx);
    const auto& column_old = old_schema->column(old_col_idx);

    if (column_new.is_bf_column() != column_old.is_bf_column() ||
        column_new.has_bitmap_index() != column_old.has_bitmap_index()) {
        return true;
    }

    bool new_schema_has_inverted_index = new_schema->inverted_index(column_new);
    bool old_schema_has_inverted_index = old_schema->inverted_index(column_old);

    return new_schema_has_inverted_index != old_schema_has_inverted_index;
}

TabletColumn create_sparse_column(const TabletColumn& variant) {
    TabletColumn res;
    res.set_name(variant.name_lower_case() + "." + SPARSE_COLUMN_PATH);
    res.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
    res.set_aggregation_method(variant.aggregation());
    res.set_path_info(PathInData {variant.name_lower_case() + "." + SPARSE_COLUMN_PATH});
    res.set_parent_unique_id(variant.unique_id());
    // set default value to "NULL" DefaultColumnIterator will call insert_many_defaults
    res.set_default_value("NULL");
    TabletColumn child_tcolumn;
    child_tcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    res.add_sub_column(child_tcolumn);
    res.add_sub_column(child_tcolumn);
    return res;
}

Status collect_path_stats(const RowsetSharedPtr& rs,
                          std::unordered_map<int32_t, PathToNoneNullValues>& uid_to_path_stats) {
    SegmentCacheHandle segment_cache;
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(rs), &segment_cache));

    for (const auto& column : rs->tablet_schema()->columns()) {
        if (!column->is_variant_type()) {
            continue;
        }

        for (const auto& segment : segment_cache.get_segments()) {
            auto column_reader_or = segment->get_column_reader(column->unique_id());
            if (!column_reader_or.has_value()) {
                continue;
            }
            auto* column_reader = column_reader_or.value();
            if (!column_reader) {
                continue;
            }

            CHECK(column_reader->get_meta_type() == FieldType::OLAP_FIELD_TYPE_VARIANT);
            const auto* source_stats =
                    static_cast<const segment_v2::VariantColumnReader*>(column_reader)->get_stats();
            CHECK(source_stats);

            // 合并子列统计信息
            for (const auto& [path, size] : source_stats->subcolumns_non_null_size) {
                uid_to_path_stats[column->unique_id()][path] += size;
            }

            // 合并稀疏列统计信息
            for (const auto& [path, size] : source_stats->sparse_column_non_null_size) {
                CHECK(!path.empty());
                uid_to_path_stats[column->unique_id()][path] += size;
            }
        }
    }
    return Status::OK();
}

// get the subpaths and sparse paths for the variant column
void get_subpaths(const TabletColumn& variant,
                  const std::unordered_map<int32_t, PathToNoneNullValues>& path_stats,
                  std::unordered_map<int32_t, TabletSchema::PathsSetInfo>& uid_to_paths_set_info) {
    if (path_stats.find(variant.unique_id()) == path_stats.end()) {
        return;
    }
    // get the stats for the variant column
    const auto& stats = path_stats.at(variant.unique_id());
    int32_t uid = variant.unique_id();
    if (stats.size() > variant.variant_max_subcolumns_count()) {
        // 按非空值数量排序
        std::vector<std::pair<size_t, std::string_view>> paths_with_sizes;
        paths_with_sizes.reserve(stats.size());
        for (const auto& [path, size] : stats) {
            paths_with_sizes.emplace_back(size, path);
        }
        std::sort(paths_with_sizes.begin(), paths_with_sizes.end(), std::greater());

        // Select top N paths as subcolumns, remaining paths as sparse columns
        for (const auto& [size, path] : paths_with_sizes) {
            if (uid_to_paths_set_info[uid].sub_path_set.size() <
                variant.variant_max_subcolumns_count()) {
                uid_to_paths_set_info[uid].sub_path_set.emplace(path);
            } else {
                uid_to_paths_set_info[uid].sparse_path_set.emplace(path);
            }
        }
        LOG(INFO) << "subpaths " << uid_to_paths_set_info[uid].sub_path_set.size()
                  << " sparse paths " << uid_to_paths_set_info[uid].sparse_path_set.size()
                  << " variant max subcolumns count " << variant.variant_max_subcolumns_count()
                  << " stats size " << paths_with_sizes.size();
    } else {
        // Apply all paths as subcolumns
        for (const auto& [path, _] : stats) {
            uid_to_paths_set_info[uid].sub_path_set.emplace(path);
        }
    }
}

Status check_path_stats(const std::vector<RowsetSharedPtr>& intputs, RowsetSharedPtr output,
                        BaseTabletSPtr tablet) {
    // only check path stats for dup_keys since the rows may be merged in other models
    if (tablet->keys_type() != KeysType::DUP_KEYS) {
        return Status::OK();
    }
    // if there is a delete predicate in the input rowsets, we skip the path stats check
    for (auto& rowset : intputs) {
        if (rowset->rowset_meta()->has_delete_predicate()) {
            return Status::OK();
        }
    }
    std::unordered_map<int32_t, PathToNoneNullValues> original_uid_to_path_stats;
    for (const auto& rs : intputs) {
        RETURN_IF_ERROR(collect_path_stats(rs, original_uid_to_path_stats));
    }
    std::unordered_map<int32_t, PathToNoneNullValues> output_uid_to_path_stats;
    RETURN_IF_ERROR(collect_path_stats(output, output_uid_to_path_stats));
    for (const auto& [uid, stats] : output_uid_to_path_stats) {
        if (original_uid_to_path_stats.find(uid) == original_uid_to_path_stats.end()) {
            return Status::InternalError("Path stats not found for uid {}, tablet_id {}", uid,
                                         tablet->tablet_id());
        }

        // In input rowsets, some rowsets may have statistics values exceeding the maximum limit,
        // which leads to inaccurate statistics
        if (stats.size() > config::variant_max_sparse_column_statistics_size) {
            // When there is only one segment, we can ensure that the size of each path in output stats is accurate
            if (output->num_segments() == 1) {
                for (const auto& [path, size] : stats) {
                    if (original_uid_to_path_stats.at(uid).find(path) ==
                        original_uid_to_path_stats.at(uid).end()) {
                        continue;
                    }
                    if (original_uid_to_path_stats.at(uid).at(path) > size) {
                        return Status::InternalError(
                                "Path stats not smaller for uid {} with path `{}`, input size {}, "
                                "output "
                                "size {}, "
                                "tablet_id {}",
                                uid, path, original_uid_to_path_stats.at(uid).at(path), size,
                                tablet->tablet_id());
                    }
                }
            }
        }
        // in this case, input stats is accurate, so we check the stats size and stats value
        else {
            if (stats.size() != original_uid_to_path_stats.at(uid).size()) {
                return Status::InternalError(
                        "Path stats size not match for uid {}, tablet_id {}, input size {}, output "
                        "size {}",
                        uid, tablet->tablet_id(), original_uid_to_path_stats.at(uid).size(),
                        stats.size());
            }
            for (const auto& [path, size] : stats) {
                if (original_uid_to_path_stats.at(uid).at(path) != size) {
                    return Status::InternalError(
                            "Path stats not match for uid {} with path `{}`, input size {}, output "
                            "size {}, "
                            "tablet_id {}",
                            uid, path, original_uid_to_path_stats.at(uid).at(path), size,
                            tablet->tablet_id());
                }
            }
        }
    }
    return Status::OK();
}

// Build the temporary schema for compaction
// 1. collect path stats from all rowsets
// 2. get the subpaths and sparse paths for each unique id
// 3. build the output schema with the subpaths and sparse paths
// 4. set the path set info for each unique id
// 5. append the subpaths and sparse paths to the output schema
// 6. return the output schema
Status get_compaction_schema(const std::vector<RowsetSharedPtr>& rowsets,
                             TabletSchemaSPtr& target) {
    std::unordered_map<int32_t, PathToNoneNullValues> uid_to_path_stats;

    // collect path stats from all rowsets and segments
    for (const auto& rs : rowsets) {
        RETURN_IF_ERROR(collect_path_stats(rs, uid_to_path_stats));
    }

    // build the output schema
    TabletSchemaSPtr output_schema = std::make_shared<TabletSchema>();
    output_schema->shawdow_copy_without_columns(*target);
    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> uid_to_paths_set_info;
    for (const TabletColumnPtr& column : target->columns()) {
        output_schema->append_column(*column);
        if (!column->is_variant_type()) {
            continue;
        }
        VLOG_DEBUG << "column " << column->name() << " unique id " << column->unique_id();

        // get the subpaths
        get_subpaths(*column, uid_to_path_stats, uid_to_paths_set_info);
        std::vector<StringRef> sorted_subpaths(
                uid_to_paths_set_info[column->unique_id()].sub_path_set.begin(),
                uid_to_paths_set_info[column->unique_id()].sub_path_set.end());
        std::sort(sorted_subpaths.begin(), sorted_subpaths.end());
        // append subcolumns
        for (const auto& subpath : sorted_subpaths) {
            TabletColumn subcolumn;
            subcolumn.set_name(column->name_lower_case() + "." + subpath.to_string());
            subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
            subcolumn.set_parent_unique_id(column->unique_id());
            subcolumn.set_path_info(
                    PathInData(column->name_lower_case() + "." + subpath.to_string()));
            subcolumn.set_aggregation_method(column->aggregation());
            subcolumn.set_variant_max_subcolumns_count(column->variant_max_subcolumns_count());
            subcolumn.set_is_nullable(true);
            output_schema->append_column(subcolumn);
        }
        // append sparse column
        TabletColumn sparse_column = create_sparse_column(*column);
        output_schema->append_column(sparse_column);
    }

    target = output_schema;
    // used to merge & filter path to sparse column during reading in compaction
    target->set_path_set_info(uid_to_paths_set_info);
    VLOG_DEBUG << "dump schema " << target->dump_full_schema();
    return Status::OK();
}

// Calculate statistics about variant data paths from the encoded sparse column
void calculate_variant_stats(const IColumn& encoded_sparse_column,
                             segment_v2::VariantStatisticsPB* stats, size_t row_pos,
                             size_t num_rows) {
    // Cast input column to ColumnMap type since sparse column is stored as a map
    const auto& map_column = assert_cast<const ColumnMap&>(encoded_sparse_column);

    // Get the keys column which contains the paths as strings
    const auto& sparse_data_paths =
            assert_cast<const ColumnString*>(map_column.get_keys_ptr().get());
    const auto& serialized_sparse_column_offsets =
            assert_cast<const ColumnArray::Offsets64&>(map_column.get_offsets());
    auto& count_map = *stats->mutable_sparse_column_non_null_size();
    // Iterate through all paths in the sparse column
    for (size_t i = row_pos; i != row_pos + num_rows; ++i) {
        size_t offset = serialized_sparse_column_offsets[i - 1];
        size_t end = serialized_sparse_column_offsets[i];
        for (size_t j = offset; j != end; ++j) {
            auto path = sparse_data_paths->get_data_at(j);

            const auto& sparse_path = path.to_string();
            // If path already exists in statistics, increment its count
            if (auto it = count_map.find(sparse_path); it != count_map.end()) {
                ++it->second;
            }
            // If path doesn't exist and we haven't hit the max statistics size limit,
            // add it with count 1
            else if (count_map.size() < config::variant_max_sparse_column_statistics_size) {
                count_map.emplace(sparse_path, 1);
            }
        }
    }

    if (stats->sparse_column_non_null_size().size() >
        config::variant_max_sparse_column_statistics_size) {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "Sparse column non null size: {} is greater than max statistics size: {}",
                stats->sparse_column_non_null_size().size(),
                config::variant_max_sparse_column_statistics_size);
    }
}

/// Calculates number of dimensions in array field.
/// Returns 0 for scalar fields.
class FieldVisitorToNumberOfDimensions : public StaticVisitor<size_t> {
public:
    size_t operator()(const Array& x) const {
        const size_t size = x.size();
        size_t dimensions = 0;
        for (size_t i = 0; i < size; ++i) {
            size_t element_dimensions = apply_visitor(*this, x[i]);
            dimensions = std::max(dimensions, element_dimensions);
        }
        return 1 + dimensions;
    }
    size_t operator()(const VariantField& x) { return apply_visitor(*this, x.get_field()); }
    template <typename T>
    size_t operator()(const T&) const {
        return 0;
    }
};

// Visitor that allows to get type of scalar field
// but exclude fields contain complex field.This is a faster version
// for FieldVisitorToScalarType which does not support complex field.
class SimpleFieldVisitorToScalarType : public StaticVisitor<size_t> {
public:
    size_t operator()(const Array& x) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Array type is not supported");
    }
    size_t operator()(const UInt64& x) {
        if (x <= std::numeric_limits<Int8>::max()) {
            type = TypeIndex::Int8;
        } else if (x <= std::numeric_limits<Int16>::max()) {
            type = TypeIndex::Int16;
        } else if (x <= std::numeric_limits<Int32>::max()) {
            type = TypeIndex::Int32;
        } else {
            type = TypeIndex::Int64;
        }
        return 1;
    }
    size_t operator()(const Int64& x) {
        if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min()) {
            type = TypeIndex::Int8;
        } else if (x <= std::numeric_limits<Int16>::max() &&
                   x >= std::numeric_limits<Int16>::min()) {
            type = TypeIndex::Int16;
        } else if (x <= std::numeric_limits<Int32>::max() &&
                   x >= std::numeric_limits<Int32>::min()) {
            type = TypeIndex::Int32;
        } else {
            type = TypeIndex::Int64;
        }
        return 1;
    }
    size_t operator()(const JsonbField& x) {
        type = TypeIndex::JSONB;
        return 1;
    }
    size_t operator()(const Null&) {
        have_nulls = true;
        return 1;
    }
    size_t operator()(const VariantMap&) {
        type = TypeIndex::VARIANT;
        return 1;
    }
    size_t operator()(const VariantField& x) {
        typed_field_info =
                FieldInfo {x.get_type_id(), true, false, 0, x.get_scale(), x.get_precision()};
        return 1;
    }
    template <typename T>
    size_t operator()(const T&) {
        type = TypeId<NearestFieldType<T>>::value;
        return 1;
    }
    void get_scalar_type(TypeIndex* data_type, int* precision, int* scale) const {
        if (typed_field_info.has_value()) {
            *data_type = typed_field_info->scalar_type_id;
            *precision = typed_field_info->precision;
            *scale = typed_field_info->scale;
            return;
        }
        *data_type = type;
    }
    bool contain_nulls() const { return have_nulls; }

    bool need_convert_field() const { return false; }

private:
    // initialized when operator()(const VariantField& x)
    std::optional<FieldInfo> typed_field_info;
    TypeIndex type = TypeIndex::Nothing;
    bool have_nulls = false;
};

/// Visitor that allows to get type of scalar field
/// or least common type of scalars in array.
/// More optimized version of FieldToDataType.
class FieldVisitorToScalarType : public StaticVisitor<size_t> {
public:
    using FieldType = Field::Types::Which;
    size_t operator()(const Array& x) {
        size_t size = x.size();
        for (size_t i = 0; i < size; ++i) {
            apply_visitor(*this, x[i]);
        }
        return 0;
    }
    // TODO doris not support unsigned integers for now
    // treat as signed integers
    size_t operator()(const UInt64& x) {
        field_types.insert(FieldType::UInt64);
        if (x <= std::numeric_limits<Int8>::max()) {
            type_indexes.insert(TypeIndex::Int8);
        } else if (x <= std::numeric_limits<Int16>::max()) {
            type_indexes.insert(TypeIndex::Int16);
        } else if (x <= std::numeric_limits<Int32>::max()) {
            type_indexes.insert(TypeIndex::Int32);
        } else {
            type_indexes.insert(TypeIndex::Int64);
        }
        return 0;
    }
    size_t operator()(const Int64& x) {
        field_types.insert(FieldType::Int64);
        if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min()) {
            type_indexes.insert(TypeIndex::Int8);
        } else if (x <= std::numeric_limits<Int16>::max() &&
                   x >= std::numeric_limits<Int16>::min()) {
            type_indexes.insert(TypeIndex::Int16);
        } else if (x <= std::numeric_limits<Int32>::max() &&
                   x >= std::numeric_limits<Int32>::min()) {
            type_indexes.insert(TypeIndex::Int32);
        } else {
            type_indexes.insert(TypeIndex::Int64);
        }
        return 0;
    }
    size_t operator()(const JsonbField& x) {
        field_types.insert(FieldType::JSONB);
        type_indexes.insert(TypeIndex::JSONB);
        return 0;
    }
    size_t operator()(const VariantMap&) {
        field_types.insert(FieldType::VariantMap);
        type_indexes.insert(TypeIndex::VARIANT);
        return 0;
    }
    size_t operator()(const VariantField& x) {
        if (x.get_type_id() == TypeIndex::Array) {
            apply_visitor(*this, x.get_field());
        } else {
            typed_field_info =
                    FieldInfo {x.get_type_id(), true, false, 0, x.get_scale(), x.get_precision()};
        }
        return 0;
    }
    size_t operator()(const Null&) {
        have_nulls = true;
        return 0;
    }
    template <typename T>
    size_t operator()(const T&) {
        Field::EnumToType<Field::Types::Array>::Type a;
        field_types.insert(Field::TypeToEnum<NearestFieldType<T>>::value);
        type_indexes.insert(TypeId<NearestFieldType<T>>::value);
        return 0;
    }
    void get_scalar_type(TypeIndex* type, int* precision, int* scale) const {
        if (typed_field_info.has_value()) {
            // fast path
            *type = typed_field_info->scalar_type_id;
            *precision = typed_field_info->precision;
            *scale = typed_field_info->scale;
            return;
        }
        DataTypePtr data_type;
        get_least_supertype_jsonb(type_indexes, &data_type);
        *type = data_type->get_type_id();
    }
    bool contain_nulls() const { return have_nulls; }
    bool need_convert_field() const { return field_types.size() > 1; }

private:
    // initialized when operator()(const VariantField& x)
    std::optional<FieldInfo> typed_field_info;
    phmap::flat_hash_set<TypeIndex> type_indexes;
    phmap::flat_hash_set<FieldType> field_types;
    bool have_nulls = false;
};

template <typename Visitor>
void get_field_info_impl(const Field& field, FieldInfo* info) {
    Visitor to_scalar_type_visitor;
    apply_visitor(to_scalar_type_visitor, field);
    TypeIndex type_id;
    int precision = 0;
    int scale = 0;
    to_scalar_type_visitor.get_scalar_type(&type_id, &precision, &scale);
    // array item's dimension may missmatch, eg. [1, 2, [1, 2, 3]]
    *info = {
            type_id,
            to_scalar_type_visitor.contain_nulls(),
            to_scalar_type_visitor.need_convert_field(),
            apply_visitor(FieldVisitorToNumberOfDimensions(), field),
            scale,
            precision,
    };
}

bool is_complex_field(const Field& field) {
    return field.is_complex_field() ||
           (field.is_variant_field() &&
            field.get<const VariantField&>().get_field().is_complex_field());
}

void get_field_info(const Field& field, FieldInfo* info) {
    if (is_complex_field(field)) {
        get_field_info_impl<FieldVisitorToScalarType>(field, info);
    } else {
        get_field_info_impl<SimpleFieldVisitorToScalarType>(field, info);
    }
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized::schema_util
