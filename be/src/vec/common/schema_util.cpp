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
#include <memory>
#include <ostream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exprs/json_functions.h"
#include "olap/olap_common.h"
#include "olap/tablet_schema.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "udf/udf.h"
#include "util/defer_op.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
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
    if (!function) {
        return Status::InternalError("Not found cast function {} to {}", arg.type->get_name(),
                                     type->get_name());
    }
    Block tmp_block {arguments};
    size_t result_column = tmp_block.columns();
    auto ctx = FunctionContext::create_context(nullptr, {}, {});

    // To prevent from null info lost, we should not call function since the function framework will wrap
    // nullable to Variant instead of the root of Variant
    // correct output: Nullable(Array(int)) -> Nullable(Variant(Nullable(Array(int))))
    // incorrect output: Nullable(Array(int)) -> Nullable(Variant(Array(int)))
    if (WhichDataType(remove_nullable(type)).is_variant_type()) {
        // set variant root column/type to from column/type
        auto variant = ColumnObject::create(true /*always nullable*/);
        CHECK(arg.column->is_nullable());
        variant->create_root(arg.type, arg.column->assume_mutable());
        ColumnPtr nullable = ColumnNullable::create(
                variant->get_ptr(),
                check_and_get_column<ColumnNullable>(arg.column.get())->get_null_map_column_ptr());
        *result = type->is_nullable() ? nullable : variant->get_ptr();
        return Status::OK();
    }

    // We convert column string to jsonb type just add a string jsonb field to dst column instead of parse
    // each line in original string column.
    ctx->set_string_as_jsonb_string(true);
    tmp_block.insert({nullptr, type, arg.name});
    RETURN_IF_ERROR(
            function->execute(ctx.get(), tmp_block, {0}, result_column, arg.column->size()));
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
        column.set_default_value("[]");
        return;
    }
    // size is not fixed when type is string or json
    if (WhichDataType(*data_type).is_string() || WhichDataType(*data_type).is_json()) {
        return;
    }
    if (WhichDataType(*data_type).is_simple()) {
        column.set_length(data_type->get_size_of_value_in_memory());
        return;
    }
    // TODO handle more types like struct/date/datetime/decimal...
    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}

TabletColumn get_column_by_type(const vectorized::DataTypePtr& data_type, const std::string& name,
                                const ExtraInfo& ext_info) {
    TabletColumn result;
    get_column_by_type(data_type, name, result, ext_info);
    return result;
}

TabletColumn get_least_type_column(const TabletColumn& original, const DataTypePtr& new_type,
                                   const ExtraInfo& ext_info, bool* changed) {
    TabletColumn result_column;
    vectorized::DataTypePtr original_type = original.get_vec_type();
    vectorized::DataTypePtr common_type;
    vectorized::get_least_supertype<vectorized::LeastSupertypeOnError::Jsonb>(
            vectorized::DataTypes {original_type, new_type}, &common_type);
    if (!original_type->equals(*common_type)) {
        // update to common type
        *changed = true;
        vectorized::schema_util::get_column_by_type(common_type, original.name(), result_column,
                                                    ext_info);
    } else {
        *changed = false;
        result_column = original;
        result_column.set_parent_unique_id(ext_info.parent_unique_id);
        result_column.set_unique_id(ext_info.unique_id);
        result_column.set_path_info(ext_info.path_info);
    }
    return result_column;
}

void update_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                                TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id) {
    // Types of subcolumns by path from all tuples.
    std::unordered_map<PathInData, DataTypes, PathInData::Hash> subcolumns_types;
    for (const TabletSchemaSPtr& schema : schemas) {
        for (const TabletColumn& col : schema->columns()) {
            // Get subcolumns of this variant
            if (!col.path_info().empty() && col.parent_unique_id() > 0 &&
                col.parent_unique_id() == variant_col_unique_id) {
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
        if (!common_type->is_nullable()) {
            common_type = make_nullable(common_type);
        }
        tuple_types.emplace_back(common_type);
    }
    CHECK_EQ(tuple_paths.size(), tuple_types.size());

    std::string variant_col_name = common_schema->column_by_uid(variant_col_unique_id).name();
    // Append all common type columns of this variant
    for (int i = 0; i < tuple_paths.size(); ++i) {
        TabletColumn common_column;
        // const std::string& column_name = variant_col_name + "." + tuple_paths[i].get_path();
        get_column_by_type(tuple_types[i], tuple_paths[i].get_path(), common_column,
                           ExtraInfo {.unique_id = -1,
                                      .parent_unique_id = variant_col_unique_id,
                                      .path_info = tuple_paths[i]});
        // set ColumnType::VARIANT to occupy _field_path_to_index
        common_schema->append_column(common_column, TabletSchema::ColumnType::VARIANT);
    }
}

void inherit_tablet_index(TabletSchemaSPtr& schema) {
    std::unordered_map<int32_t, TabletIndex> variants_index_meta;
    // Get all variants tablet index metas if exist
    for (const auto& col : schema->columns()) {
        auto index_meta = schema->get_inverted_index(col.unique_id(), "");
        if (col.is_variant_type() && index_meta != nullptr) {
            variants_index_meta.emplace(col.unique_id(), *index_meta);
        }
    }

    // Add index meta if extracted column is missing index meta
    for (const auto& col : schema->columns()) {
        if (!col.is_extracted_column()) {
            continue;
        }
        auto it = variants_index_meta.find(col.parent_unique_id());
        // variant has no index meta, ignore
        if (it == variants_index_meta.end()) {
            continue;
        }
        auto index_meta = schema->get_inverted_index(col);
        // add index meta
        TabletIndex index_info = it->second;
        index_info.set_escaped_escaped_index_suffix_path(col.path_info().get_path());
        if (index_meta != nullptr) {
            // already exist
            schema->update_index(col, index_info);
        } else {
            schema->append_index(index_info);
        }
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
        output_schema->copy_from(*base_schema);
        // Merge columns from other schemas
        output_schema->clear_columns();
        // Get all columns without extracted columns and collect variant col unique id
        for (const TabletColumn& col : base_schema->columns()) {
            if (col.is_variant_type()) {
                variant_column_unique_id.push_back(col.unique_id());
            }
            if (!col.is_extracted_column()) {
                output_schema->append_column(col);
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

    for (int32_t unique_id : variant_column_unique_id) {
        update_least_common_schema(schemas, output_schema, unique_id);
    }

    inherit_tablet_index(output_schema);
    if (check_schema_size &&
        output_schema->columns().size() > config::variant_max_merged_tablet_schema_size) {
        return Status::DataQualityError("Reached max column size limit {}",
                                        config::variant_max_merged_tablet_schema_size);
    }
    return Status::OK();
}

Status parse_and_encode_variant_columns(Block& block, const std::vector<int>& variant_pos,
                                        const ParseContext& ctx) {
    try {
        // Parse each variant column from raw string column
        RETURN_IF_ERROR(vectorized::schema_util::parse_variant_columns(block, variant_pos, ctx));
        vectorized::schema_util::finalize_variant_columns(block, variant_pos,
                                                          false /*not ingore sparse*/);
        vectorized::schema_util::encode_variant_sparse_subcolumns(block, variant_pos);
    } catch (const doris::Exception& e) {
        // TODO more graceful, max_filter_ratio
        LOG(WARNING) << "encounter execption " << e.to_string();
        return Status::InternalError(e.to_string());
    }
    return Status::OK();
}

Status parse_variant_columns(Block& block, const std::vector<int>& variant_pos,
                             const ParseContext& ctx) {
    for (int i = 0; i < variant_pos.size(); ++i) {
        auto column_ref = block.get_by_position(variant_pos[i]).column;
        bool is_nullable = column_ref->is_nullable();
        const auto& column = remove_nullable(column_ref);
        const auto& var = assert_cast<const ColumnObject&>(*column.get());
        var.assume_mutable_ref().finalize();

        MutableColumnPtr variant_column;
        bool record_raw_string_with_serialization = false;
        // set
        auto __defer = Defer([&]() {
            if (!ctx.record_raw_json_column) {
                return;
            }
            auto* var = static_cast<vectorized::ColumnObject*>(variant_column.get());
            if (record_raw_string_with_serialization) {
                // encode to raw json column
                auto raw_column = vectorized::ColumnString::create();
                for (size_t i = 0; i < var->rows(); ++i) {
                    std::string raw_str;
                    var->serialize_one_row_to_string(i, &raw_str);
                    raw_column->insert_data(raw_str.c_str(), raw_str.size());
                }
                var->set_rowstore_column(raw_column->get_ptr());
            } else {
                // use original input json column
                auto original_var_root = vectorized::check_and_get_column<vectorized::ColumnObject>(
                                                 remove_nullable(column_ref).get())
                                                 ->get_root();
                var->set_rowstore_column(original_var_root);
            }
        });

        if (!var.is_scalar_variant()) {
            variant_column = var.assume_mutable();
            record_raw_string_with_serialization = true;
            // already parsed
            continue;
        }
        ColumnPtr raw_json_column;
        if (WhichDataType(remove_nullable(var.get_root_type())).is_json()) {
            // TODO more efficient way to parse jsonb type, currently we just convert jsonb to
            // json str and parse them into variant
            RETURN_IF_ERROR(cast_column({var.get_root(), var.get_root_type(), ""},
                                        var.get_root()->is_nullable()
                                                ? make_nullable(std::make_shared<DataTypeString>())
                                                : std::make_shared<DataTypeString>(),
                                        &raw_json_column));
            if (raw_json_column->is_nullable()) {
                raw_json_column = assert_cast<const ColumnNullable*>(raw_json_column.get())
                                          ->get_nested_column_ptr();
            }
        } else {
            const auto& root = *var.get_root();
            raw_json_column =
                    root.is_nullable()
                            ? assert_cast<const ColumnNullable&>(root).get_nested_column_ptr()
                            : var.get_root();
        }

        variant_column = ColumnObject::create(true);
        parse_json_to_variant(*variant_column.get(),
                              assert_cast<const ColumnString&>(*raw_json_column));

        // Wrap variant with nullmap if it is nullable
        ColumnPtr result = variant_column->get_ptr();
        if (is_nullable) {
            const auto& null_map =
                    assert_cast<const ColumnNullable&>(*column_ref).get_null_map_column_ptr();
            result = ColumnNullable::create(result, null_map);
        }
        block.get_by_position(variant_pos[i]).column = result;
        // block.get_by_position(variant_pos[i]).type = std::make_shared<DataTypeObject>("json", true);
    }
    return Status::OK();
}

void finalize_variant_columns(Block& block, const std::vector<int>& variant_pos,
                              bool ignore_sparse) {
    for (int i = 0; i < variant_pos.size(); ++i) {
        auto& column_ref = block.get_by_position(variant_pos[i]).column->assume_mutable_ref();
        auto& column =
                column_ref.is_nullable()
                        ? assert_cast<ColumnObject&>(
                                  assert_cast<ColumnNullable&>(column_ref).get_nested_column())
                        : assert_cast<ColumnObject&>(column_ref);
        column.finalize(ignore_sparse);
    }
}

void encode_variant_sparse_subcolumns(Block& block, const std::vector<int>& variant_pos) {
    for (int i = 0; i < variant_pos.size(); ++i) {
        auto& column_ref = block.get_by_position(variant_pos[i]).column->assume_mutable_ref();
        auto& column =
                column_ref.is_nullable()
                        ? assert_cast<ColumnObject&>(
                                  assert_cast<ColumnNullable&>(column_ref).get_nested_column())
                        : assert_cast<ColumnObject&>(column_ref);
        // Make sure the root node is jsonb storage type
        auto expected_root_type = make_nullable(std::make_shared<ColumnObject::MostCommonType>());
        column.ensure_root_node_type(expected_root_type);
        column.merge_sparse_to_root_column();
    }
}

// ---------------------------
Status extract(ColumnPtr source, const PathInData& path, MutableColumnPtr& dst) {
    auto type_string = std::make_shared<DataTypeString>();
    std::string jsonpath = path.to_jsonpath();
    bool is_nullable = source->is_nullable();
    auto json_type = is_nullable ? make_nullable(std::make_shared<DataTypeJsonb>())
                                 : std::make_shared<DataTypeJsonb>();
    ColumnsWithTypeAndName arguments {
            {source, json_type, ""},
            {type_string->create_column_const(1, Field(jsonpath.data(), jsonpath.size())),
             type_string, ""}};
    auto function =
            SimpleFunctionFactory::instance().get_function("jsonb_extract", arguments, json_type);
    if (!function) {
        return Status::InternalError("Not found function jsonb_extract");
    }
    Block tmp_block {arguments};
    vectorized::ColumnNumbers argnum;
    argnum.emplace_back(0);
    argnum.emplace_back(1);
    size_t result_column = tmp_block.columns();
    tmp_block.insert({nullptr, json_type, ""});
    RETURN_IF_ERROR(function->execute(nullptr, tmp_block, argnum, result_column, source->size()));
    dst = tmp_block.get_by_position(result_column)
                  .column->convert_to_full_column_if_const()
                  ->assume_mutable();
    return Status::OK();
}
// ---------------------------

std::string dump_column(DataTypePtr type, const ColumnPtr& col) {
    Block tmp;
    tmp.insert(ColumnWithTypeAndName {col, type, col->get_name()});
    return tmp.dump_data(0, tmp.rows());
}
// ---------------------------

} // namespace doris::vectorized::schema_util
