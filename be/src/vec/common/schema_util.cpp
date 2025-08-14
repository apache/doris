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
#include <fnmatch.h>
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
#include <unicode/uchar.h>

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
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#include "olap/rowset/segment_v2/variant/variant_column_writer_impl.h"
#include "olap/segment_loader.h"
#include "olap/tablet.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_schema.h"
#include "runtime/client_cache.h"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "util/defer_op.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_variant.h"
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
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_variant.h"
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

size_t get_size_of_interger(PrimitiveType type) {
    switch (type) {
    case PrimitiveType::TYPE_TINYINT:
        return sizeof(int8_t);
    case PrimitiveType::TYPE_SMALLINT:
        return sizeof(int16_t);
    case PrimitiveType::TYPE_INT:
        return sizeof(int32_t);
    case PrimitiveType::TYPE_BIGINT:
        return sizeof(int64_t);
    case PrimitiveType::TYPE_LARGEINT:
        return sizeof(int128_t);
    case PrimitiveType::TYPE_BOOLEAN:
        return sizeof(uint8_t);
    default:
        throw Exception(Status::FatalError("Unknown integer type: {}", type_to_string(type)));
        return 0;
    }
}

bool is_conversion_required_between_integers(const PrimitiveType& lhs, const PrimitiveType& rhs) {
    bool is_native_int = is_int_or_bool(lhs) && is_int_or_bool(rhs);
    return !is_native_int || get_size_of_interger(lhs) > get_size_of_interger(rhs);
}

Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result) {
    ColumnsWithTypeAndName arguments {arg, {nullptr, type, type->get_name()}};

    // To prevent from null info lost, we should not call function since the function framework will wrap
    // nullable to Variant instead of the root of Variant
    // correct output: Nullable(Array(int)) -> Nullable(Variant(Nullable(Array(int))))
    // incorrect output: Nullable(Array(int)) -> Nullable(Variant(Array(int)))
    if (type->get_primitive_type() == TYPE_VARIANT) {
        // If source column is variant, so the nullable info is different from dst column
        if (arg.type->get_primitive_type() == TYPE_VARIANT) {
            *result = type->is_nullable() ? make_nullable(arg.column) : remove_nullable(arg.column);
            return Status::OK();
        }
        // set variant root column/type to from column/type
        CHECK(arg.column->is_nullable());
        auto to_type = remove_nullable(type);
        const auto& data_type_object = assert_cast<const DataTypeVariant&>(*to_type);
        auto variant = ColumnVariant::create(data_type_object.variant_max_subcolumns_count());

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

    if (arg.type->get_primitive_type() == INVALID_TYPE) {
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
    // TODO(lihangyu): we should handle this error in strict mode
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
    if (data_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY) {
        TabletColumn child;
        get_column_by_type(assert_cast<const DataTypeArray*>(data_type.get())->get_nested_type(),
                           "", child, {});
        column.set_length(TabletColumn::get_field_length_by_type(TPrimitiveType::ARRAY, 0));
        column.add_sub_column(child);
        return;
    }
    // size is not fixed when type is string or json
    if (is_string_type(data_type->get_primitive_type()) ||
        data_type->get_primitive_type() == TYPE_JSONB) {
        column.set_length(INT_MAX);
        return;
    }

    PrimitiveType type = data_type->get_primitive_type();
    if (is_int_or_bool(type) || is_string_type(type) || is_float_or_double(type) || is_ip(type) ||
        is_date_or_datetime(type) || type == PrimitiveType::TYPE_DATEV2) {
        column.set_length(cast_set<int32_t>(data_type->get_size_of_value_in_memory()));
        return;
    }
    if (is_decimal(type)) {
        column.set_precision(data_type->get_precision());
        column.set_frac(data_type->get_scale());
        return;
    }
    // datetimev2 needs scale
    if (type == PrimitiveType::TYPE_DATETIMEV2) {
        column.set_precision(-1);
        column.set_frac(data_type->get_scale());
        return;
    }

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

// check if two paths which same prefix have different structure
static bool has_different_structure_in_same_path(const PathInData::Parts& lhs,
                                                 const PathInData::Parts& rhs) {
    if (lhs.size() != rhs.size()) {
        return false; // different size means different structure
    }
    // Since we group by path string, lhs and rhs must have the same size and keys
    // We only need to check if they have different nested structure
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (lhs[i] != rhs[i]) {
            VLOG_DEBUG << fmt::format(
                    "Check different structure: {} vs {}, lhs[i].is_nested: {}, rhs[i].is_nested: "
                    "{}",
                    lhs[i].key, rhs[i].key, lhs[i].is_nested, rhs[i].is_nested);
            return true;
        }
    }
    return false;
}

Status check_variant_has_no_ambiguous_paths(const PathsInData& tuple_paths) {
    // Group paths by their string representation to reduce comparisons
    std::unordered_map<std::string, std::vector<size_t>> path_groups;

    for (size_t i = 0; i < tuple_paths.size(); ++i) {
        // same path should have same structure, so we group them by path
        path_groups[tuple_paths[i].get_path()].push_back(i);
        // print part of tuple_paths[i]
        VLOG_DEBUG << "tuple_paths[i]: " << tuple_paths[i].get_path();
    }

    // Only compare paths within the same group
    for (const auto& [path_str, indices] : path_groups) {
        if (indices.size() <= 1) {
            continue; // No conflicts possible
        }

        // Compare all pairs within this group
        for (size_t i = 0; i < indices.size(); ++i) {
            for (size_t j = 0; j < i; ++j) {
                if (has_different_structure_in_same_path(tuple_paths[indices[i]].get_parts(),
                                                         tuple_paths[indices[j]].get_parts())) {
                    return Status::DataQualityError(
                            "Ambiguous paths: {} vs {} with different nested part {} vs {}",
                            tuple_paths[indices[i]].get_path(), tuple_paths[indices[j]].get_path(),
                            tuple_paths[indices[i]].has_nested_part(),
                            tuple_paths[indices[j]].has_nested_part());
                }
            }
        }
    }
    return Status::OK();
}

Status update_least_schema_internal(const std::map<PathInData, DataTypes>& subcolumns_types,
                                    TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id,
                                    const std::map<std::string, TabletColumnPtr>& typed_columns,
                                    std::set<PathInData>* path_set) {
    PathsInData tuple_paths;
    DataTypes tuple_types;
    CHECK(common_schema.use_count() == 1);
    // Get the least common type for all paths.
    for (const auto& [key, subtypes] : subcolumns_types) {
        assert(!subtypes.empty());
        if (key.get_path() == ColumnVariant::COLUMN_NAME_DUMMY) {
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
        common_schema->append_column(common_column);
        if (path_set != nullptr) {
            path_set->insert(tuple_paths[i]);
        }
    }
    return Status::OK();
}

Status update_least_common_schema(const std::vector<TabletSchemaSPtr>& schemas,
                                  TabletSchemaSPtr& common_schema, int32_t variant_col_unique_id,
                                  std::set<PathInData>* path_set) {
    std::map<std::string, TabletColumnPtr> typed_columns;
    for (const TabletColumnPtr& col :
         common_schema->column_by_uid(variant_col_unique_id).get_sub_columns()) {
        typed_columns[col->name()] = col;
    }
    // Types of subcolumns by path from all tuples.
    std::map<PathInData, DataTypes> subcolumns_types;

    // Collect all paths first to enable batch checking
    std::vector<PathInData> all_paths;

    for (const TabletSchemaSPtr& schema : schemas) {
        for (const TabletColumnPtr& col : schema->columns()) {
            // Get subcolumns of this variant
            if (col->has_path_info() && col->parent_unique_id() > 0 &&
                col->parent_unique_id() == variant_col_unique_id) {
                subcolumns_types[*col->path_info_ptr()].emplace_back(
                        DataTypeFactory::instance().create_data_type(*col, col->is_nullable()));
                all_paths.push_back(*col->path_info_ptr());
            }
        }
    }

    // Batch check for conflicts
    RETURN_IF_ERROR(check_variant_has_no_ambiguous_paths(all_paths));

    return update_least_schema_internal(subcolumns_types, common_schema, variant_col_unique_id,
                                        typed_columns, path_set);
}

void inherit_column_attributes(const TabletColumn& source, TabletColumn& target,
                               TabletSchemaSPtr* target_schema) {
    if (!target.is_extracted_column()) {
        return;
    }
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
    std::vector<TabletIndex> indexes_to_update;
    auto source_indexes = (*target_schema)->inverted_indexs(source.unique_id());
    for (const auto& source_index_meta : source_indexes) {
        TabletIndex index_info = *source_index_meta;
        index_info.set_escaped_escaped_index_suffix_path(target.path_info_ptr()->get_path());
        indexes_to_update.emplace_back(std::move(index_info));
    }
    auto target_indexes = (*target_schema)
                                  ->inverted_indexs(target.parent_unique_id(),
                                                    target.path_info_ptr()->get_path());
    if (!target_indexes.empty()) {
        (*target_schema)->update_index(target, IndexType::INVERTED, std::move(indexes_to_update));
    } else {
        for (auto& index_info : indexes_to_update) {
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

    for (int32_t unique_id : variant_column_unique_id) {
        std::set<PathInData> path_set;
        RETURN_IF_ERROR(update_least_common_schema(schemas, output_schema, unique_id, &path_set));
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
        const auto& var = assert_cast<const ColumnVariant&>(*column.get());
        var.assume_mutable_ref().finalize();

        MutableColumnPtr variant_column;
        if (!var.is_scalar_variant()) {
            variant_column = var.assume_mutable();
            // already parsed
            continue;
        }
        ColumnPtr scalar_root_column;
        if (var.get_root_type()->get_primitive_type() == TYPE_JSONB) {
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
            variant_column = ColumnVariant::create(0);
            parse_json_to_variant(*variant_column.get(),
                                  assert_cast<const ColumnString&>(*scalar_root_column), config);
        } else {
            // Root maybe other types rather than string like ColumnVariant(Int32).
            // In this case, we should finlize the root and cast to JSON type
            auto expected_root_type =
                    make_nullable(std::make_shared<ColumnVariant::MostCommonType>());
            const_cast<ColumnVariant&>(var).ensure_root_node_type(expected_root_type);
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
vectorized::ColumnVariant::Subcolumns get_sorted_subcolumns(
        const vectorized::ColumnVariant::Subcolumns& subcolumns) {
    // sort by paths in lexicographical order
    vectorized::ColumnVariant::Subcolumns sorted = subcolumns;
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

    auto new_schema_inverted_indexs = new_schema->inverted_indexs(column_new);
    auto old_schema_inverted_indexs = old_schema->inverted_indexs(column_old);

    if (new_schema_inverted_indexs.size() != old_schema_inverted_indexs.size()) {
        return true;
    }

    for (size_t i = 0; i < new_schema_inverted_indexs.size(); ++i) {
        if (!new_schema_inverted_indexs[i]->is_same_except_id(old_schema_inverted_indexs[i])) {
            return true;
        }
    }

    return false;
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

Status VariantCompactionUtil::aggregate_path_to_stats(
        const RowsetSharedPtr& rs,
        std::unordered_map<int32_t, PathToNoneNullValues>* uid_to_path_stats) {
    SegmentCacheHandle segment_cache;
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(rs), &segment_cache));

    for (const auto& column : rs->tablet_schema()->columns()) {
        if (!column->is_variant_type()) {
            continue;
        }

        for (const auto& segment : segment_cache.get_segments()) {
            segment_v2::ColumnReader* column_reader = nullptr;
            RETURN_IF_ERROR(segment->get_column_reader(column->unique_id(), &column_reader));
            if (!column_reader) {
                continue;
            }

            CHECK(column_reader->get_meta_type() == FieldType::OLAP_FIELD_TYPE_VARIANT);
            const auto* variant_column_reader =
                    assert_cast<const segment_v2::VariantColumnReader*>(column_reader);
            const auto* source_stats = variant_column_reader->get_stats();
            CHECK(source_stats);

            // agg path -> stats
            for (const auto& [path, size] : source_stats->sparse_column_non_null_size) {
                (*uid_to_path_stats)[column->unique_id()][path] += size;
            }

            for (const auto& [path, size] : source_stats->subcolumns_non_null_size) {
                (*uid_to_path_stats)[column->unique_id()][path] += size;
            }
        }
    }
    return Status::OK();
}

Status VariantCompactionUtil::aggregate_variant_extended_info(
        const RowsetSharedPtr& rs,
        std::unordered_map<int32_t, VariantExtendedInfo>* uid_to_variant_extended_info) {
    SegmentCacheHandle segment_cache;
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(rs), &segment_cache));

    for (const auto& column : rs->tablet_schema()->columns()) {
        if (!column->is_variant_type()) {
            continue;
        }

        for (const auto& segment : segment_cache.get_segments()) {
            segment_v2::ColumnReader* column_reader = nullptr;
            RETURN_IF_ERROR(segment->get_column_reader(column->unique_id(), &column_reader));
            if (!column_reader) {
                continue;
            }

            CHECK(column_reader->get_meta_type() == FieldType::OLAP_FIELD_TYPE_VARIANT);
            const auto* variant_column_reader =
                    assert_cast<const segment_v2::VariantColumnReader*>(column_reader);
            const auto* source_stats = variant_column_reader->get_stats();
            CHECK(source_stats);

            // 1. agg path -> stats
            for (const auto& [path, size] : source_stats->sparse_column_non_null_size) {
                (*uid_to_variant_extended_info)[column->unique_id()]
                        .path_to_none_null_values[path] += size;
                (*uid_to_variant_extended_info)[column->unique_id()].sparse_paths.emplace(path);
            }

            for (const auto& [path, size] : source_stats->subcolumns_non_null_size) {
                (*uid_to_variant_extended_info)[column->unique_id()]
                        .path_to_none_null_values[path] += size;
            }

            //2. agg path -> schema
            auto& paths_types =
                    (*uid_to_variant_extended_info)[column->unique_id()].path_to_data_types;
            variant_column_reader->get_subcolumns_types(&paths_types);

            // 3. extract typed paths
            auto& typed_paths = (*uid_to_variant_extended_info)[column->unique_id()].typed_paths;
            variant_column_reader->get_typed_paths(&typed_paths);

            // 4. extract nested paths
            auto& nested_paths = (*uid_to_variant_extended_info)[column->unique_id()].nested_paths;
            variant_column_reader->get_nested_paths(&nested_paths);
        }
    }
    return Status::OK();
}

// get the subpaths and sparse paths for the variant column
void VariantCompactionUtil::get_subpaths(int32_t max_subcolumns_count,
                                         const PathToNoneNullValues& stats,
                                         TabletSchema::PathsSetInfo& paths_set_info) {
    // max_subcolumns_count is 0 means no limit
    if (max_subcolumns_count > 0 && stats.size() > max_subcolumns_count) {
        std::vector<std::pair<size_t, std::string_view>> paths_with_sizes;
        paths_with_sizes.reserve(stats.size());
        for (const auto& [path, size] : stats) {
            paths_with_sizes.emplace_back(size, path);
        }
        std::sort(paths_with_sizes.begin(), paths_with_sizes.end(), std::greater());

        // Select top N paths as subcolumns, remaining paths as sparse columns
        for (const auto& [size, path] : paths_with_sizes) {
            if (paths_set_info.sub_path_set.size() < max_subcolumns_count) {
                paths_set_info.sub_path_set.emplace(path);
            } else {
                paths_set_info.sparse_path_set.emplace(path);
            }
        }
        LOG(INFO) << "subpaths " << paths_set_info.sub_path_set.size() << " sparse paths "
                  << paths_set_info.sparse_path_set.size() << " variant max subcolumns count "
                  << max_subcolumns_count << " stats size " << paths_with_sizes.size();
    } else {
        // Apply all paths as subcolumns
        for (const auto& [path, _] : stats) {
            paths_set_info.sub_path_set.emplace(path);
        }
    }
}

Status VariantCompactionUtil::check_path_stats(const std::vector<RowsetSharedPtr>& intputs,
                                               RowsetSharedPtr output, BaseTabletSPtr tablet) {
    if (output->tablet_schema()->num_variant_columns() == 0) {
        return Status::OK();
    }
    // check no extended schema in output rowset
    for (const auto& column : output->tablet_schema()->columns()) {
        if (column->is_extracted_column()) {
            return Status::InternalError("Unexpected extracted column {} in output rowset",
                                         column->name());
        }
    }
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
        RETURN_IF_ERROR(aggregate_path_to_stats(rs, &original_uid_to_path_stats));
    }
    std::unordered_map<int32_t, PathToNoneNullValues> output_uid_to_path_stats;
    RETURN_IF_ERROR(aggregate_path_to_stats(output, &output_uid_to_path_stats));
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

Status VariantCompactionUtil::get_compaction_typed_columns(
        const TabletSchemaSPtr& target, const std::unordered_set<std::string>& typed_paths,
        const TabletColumnPtr parent_column, TabletSchemaSPtr& output_schema,
        TabletSchema::PathsSetInfo& paths_set_info) {
    if (parent_column->variant_enable_typed_paths_to_sparse()) {
        return Status::OK();
    }
    for (const auto& path : typed_paths) {
        TabletSchema::SubColumnInfo sub_column_info;
        if (generate_sub_column_info(*target, parent_column->unique_id(), path, &sub_column_info)) {
            vectorized::schema_util::inherit_column_attributes(*parent_column,
                                                               sub_column_info.column);
            output_schema->append_column(sub_column_info.column);
            paths_set_info.typed_path_set.insert({path, std::move(sub_column_info)});
            VLOG_DEBUG << "append typed column " << path;
        } else {
            return Status::InternalError("Failed to generate sub column info for path {}", path);
        }
    }
    return Status::OK();
}

Status VariantCompactionUtil::get_compaction_nested_columns(
        const std::unordered_set<vectorized::PathInData, vectorized::PathInData::Hash>&
                nested_paths,
        const PathToDataTypes& path_to_data_types, const TabletColumnPtr parent_column,
        TabletSchemaSPtr& output_schema, TabletSchema::PathsSetInfo& paths_set_info) {
    const auto& parent_indexes = output_schema->inverted_indexs(parent_column->unique_id());
    for (const auto& path : nested_paths) {
        const auto& find_data_types = path_to_data_types.find(path);
        if (find_data_types == path_to_data_types.end() || find_data_types->second.empty()) {
            return Status::InternalError("Nested path {} has no data type", path.get_path());
        }
        DataTypePtr data_type;
        get_least_supertype_jsonb(find_data_types->second, &data_type);

        const std::string& column_name = parent_column->name_lower_case() + "." + path.get_path();
        vectorized::PathInDataBuilder full_path_builder;
        auto full_path = full_path_builder.append(parent_column->name_lower_case(), false)
                                 .append(path.get_parts(), false)
                                 .build();
        TabletColumn nested_column = get_column_by_type(
                data_type, column_name,
                vectorized::schema_util::ExtraInfo {.unique_id = -1,
                                                    .parent_unique_id = parent_column->unique_id(),
                                                    .path_info = full_path});
        vectorized::schema_util::inherit_column_attributes(*parent_column, nested_column);
        TabletIndexes sub_column_indexes;
        vectorized::schema_util::inherit_index(parent_indexes, sub_column_indexes, nested_column);
        paths_set_info.subcolumn_indexes.emplace(path.get_path(), std::move(sub_column_indexes));
        output_schema->append_column(nested_column);
        VLOG_DEBUG << "append nested column " << path.get_path();
    }
    return Status::OK();
}

void VariantCompactionUtil::get_compaction_subcolumns(
        TabletSchema::PathsSetInfo& paths_set_info, const TabletColumnPtr parent_column,
        const TabletSchemaSPtr& target, const PathToDataTypes& path_to_data_types,
        const std::unordered_set<std::string>& sparse_paths, TabletSchemaSPtr& output_schema) {
    auto& path_set = paths_set_info.sub_path_set;
    std::vector<StringRef> sorted_subpaths(path_set.begin(), path_set.end());
    std::sort(sorted_subpaths.begin(), sorted_subpaths.end());
    const auto& parent_indexes = target->inverted_indexs(parent_column->unique_id());
    // append subcolumns
    for (const auto& subpath : sorted_subpaths) {
        auto column_name = parent_column->name_lower_case() + "." + subpath.to_string();
        auto column_path = PathInData(column_name);

        const auto& find_data_types = path_to_data_types.find(PathInData(subpath));

        // some cases: the subcolumn type is variant
        // 1. this path has no data type in segments
        // 2. this path is in sparse paths
        // 3. the sparse paths are too much
        TabletSchema::SubColumnInfo sub_column_info;
        if (parent_column->variant_enable_typed_paths_to_sparse() &&
            generate_sub_column_info(*target, parent_column->unique_id(), std::string(subpath),
                                     &sub_column_info)) {
            vectorized::schema_util::inherit_column_attributes(*parent_column,
                                                               sub_column_info.column);
            output_schema->append_column(sub_column_info.column);
            paths_set_info.subcolumn_indexes.emplace(subpath, std::move(sub_column_info.indexes));
            VLOG_DEBUG << "append typed column " << subpath;
        } else if (find_data_types == path_to_data_types.end() || find_data_types->second.empty() ||
                   sparse_paths.find(std::string(subpath)) != sparse_paths.end() ||
                   sparse_paths.size() >= config::variant_max_sparse_column_statistics_size) {
            TabletColumn subcolumn;
            subcolumn.set_name(column_name);
            subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
            subcolumn.set_parent_unique_id(parent_column->unique_id());
            subcolumn.set_path_info(column_path);
            subcolumn.set_aggregation_method(parent_column->aggregation());
            subcolumn.set_variant_max_subcolumns_count(
                    parent_column->variant_max_subcolumns_count());
            subcolumn.set_variant_enable_typed_paths_to_sparse(
                    parent_column->variant_enable_typed_paths_to_sparse());
            subcolumn.set_is_nullable(true);
            output_schema->append_column(subcolumn);
            VLOG_DEBUG << "append sub column " << subpath << " data type "
                       << "VARIANT";
        }
        // normal case: the subcolumn type can be calculated from the data types in segments
        else {
            DataTypePtr data_type;
            get_least_supertype_jsonb(find_data_types->second, &data_type);
            TabletColumn sub_column =
                    get_column_by_type(data_type, column_name,
                                       vectorized::schema_util::ExtraInfo {
                                               .unique_id = -1,
                                               .parent_unique_id = parent_column->unique_id(),
                                               .path_info = column_path});
            vectorized::schema_util::inherit_column_attributes(*parent_column, sub_column);
            TabletIndexes sub_column_indexes;
            vectorized::schema_util::inherit_index(parent_indexes, sub_column_indexes, sub_column);
            paths_set_info.subcolumn_indexes.emplace(subpath, std::move(sub_column_indexes));
            output_schema->append_column(sub_column);
            VLOG_DEBUG << "append sub column " << subpath << " data type " << data_type->get_name();
        }
    }
}

// Build the temporary schema for compaction
// 1. aggregate path stats and data types from all rowsets
// 2. append typed columns and nested columns to the output schema
// 3. sort the subpaths and sparse paths for each unique id
// 4. append the subpaths and sparse paths to the output schema
// 5. set the path set info for each unique id
// 6. return the output schema
Status VariantCompactionUtil::get_extended_compaction_schema(
        const std::vector<RowsetSharedPtr>& rowsets, TabletSchemaSPtr& target) {
    std::unordered_map<int32_t, VariantExtendedInfo> uid_to_variant_extended_info;
    // collect path stats from all rowsets and segments
    for (const auto& rs : rowsets) {
        RETURN_IF_ERROR(aggregate_variant_extended_info(rs, &uid_to_variant_extended_info));
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

        // 1. append typed columns
        RETURN_IF_ERROR(get_compaction_typed_columns(
                target, uid_to_variant_extended_info[column->unique_id()].typed_paths, column,
                output_schema, uid_to_paths_set_info[column->unique_id()]));
        // 2. append nested columns
        RETURN_IF_ERROR(get_compaction_nested_columns(
                uid_to_variant_extended_info[column->unique_id()].nested_paths,
                uid_to_variant_extended_info[column->unique_id()].path_to_data_types, column,
                output_schema, uid_to_paths_set_info[column->unique_id()]));

        // 3. get the subpaths
        get_subpaths(column->variant_max_subcolumns_count(),
                     uid_to_variant_extended_info[column->unique_id()].path_to_none_null_values,
                     uid_to_paths_set_info[column->unique_id()]);

        // 4. append subcolumns
        get_compaction_subcolumns(
                uid_to_paths_set_info[column->unique_id()], column, target,
                uid_to_variant_extended_info[column->unique_id()].path_to_data_types,
                uid_to_variant_extended_info[column->unique_id()].sparse_paths, output_schema);

        // append sparse column
        TabletColumn sparse_column = create_sparse_column(*column);
        output_schema->append_column(sparse_column);
    }

    target = output_schema;
    // used to merge & filter path to sparse column during reading in compaction
    target->set_path_set_info(std::move(uid_to_paths_set_info));
    VLOG_DEBUG << "dump schema " << target->dump_full_schema();
    return Status::OK();
}

// Calculate statistics about variant data paths from the encoded sparse column
void VariantCompactionUtil::calculate_variant_stats(const IColumn& encoded_sparse_column,
                                                    segment_v2::VariantStatisticsPB* stats,
                                                    size_t row_pos, size_t num_rows) {
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
    FieldVisitorToNumberOfDimensions() = default;
    template <PrimitiveType T>
    size_t apply(const typename PrimitiveTypeTraits<T>::NearestFieldType& x) {
        if constexpr (T == TYPE_ARRAY) {
            const size_t size = x.size();
            size_t dimensions = 0;
            for (size_t i = 0; i < size; ++i) {
                size_t element_dimensions = apply_visitor(*this, x[i]);
                dimensions = std::max(dimensions, element_dimensions);
            }
            return 1 + dimensions;
        } else {
            return 0;
        }
    }
};

// Visitor that allows to get type of scalar field
// but exclude fields contain complex field.This is a faster version
// for FieldVisitorToScalarType which does not support complex field.
class SimpleFieldVisitorToScalarType : public StaticVisitor<size_t> {
public:
    template <PrimitiveType T>
    size_t apply(const typename PrimitiveTypeTraits<T>::NearestFieldType& x) {
        if constexpr (T == TYPE_ARRAY) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Array type is not supported");
        } else if constexpr (T == TYPE_BIGINT) {
            if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min()) {
                type = PrimitiveType::TYPE_TINYINT;
            } else if (x <= std::numeric_limits<Int16>::max() &&
                       x >= std::numeric_limits<Int16>::min()) {
                type = PrimitiveType::TYPE_SMALLINT;
            } else if (x <= std::numeric_limits<Int32>::max() &&
                       x >= std::numeric_limits<Int32>::min()) {
                type = PrimitiveType::TYPE_INT;
            } else {
                type = PrimitiveType::TYPE_BIGINT;
            }
            return 1;
        } else if constexpr (T == TYPE_NULL) {
            have_nulls = true;
            return 1;
        } else {
            type = T;
            return 1;
        }
    }
    void get_scalar_type(PrimitiveType* data_type) const { *data_type = type; }
    bool contain_nulls() const { return have_nulls; }

    bool need_convert_field() const { return false; }

private:
    PrimitiveType type = PrimitiveType::INVALID_TYPE;
    bool have_nulls = false;
};

/// Visitor that allows to get type of scalar field
/// or least common type of scalars in array.
/// More optimized version of FieldToDataType.
class FieldVisitorToScalarType : public StaticVisitor<size_t> {
public:
    template <PrimitiveType T>
    size_t apply(const typename PrimitiveTypeTraits<T>::NearestFieldType& x) {
        if constexpr (T == TYPE_ARRAY) {
            size_t size = x.size();
            for (size_t i = 0; i < size; ++i) {
                apply_visitor(*this, x[i]);
            }
            return 0;
        } else if constexpr (T == TYPE_BIGINT) {
            field_types.insert(PrimitiveType::TYPE_BIGINT);
            if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min()) {
                type_indexes.insert(PrimitiveType::TYPE_TINYINT);
            } else if (x <= std::numeric_limits<Int16>::max() &&
                       x >= std::numeric_limits<Int16>::min()) {
                type_indexes.insert(PrimitiveType::TYPE_SMALLINT);
            } else if (x <= std::numeric_limits<Int32>::max() &&
                       x >= std::numeric_limits<Int32>::min()) {
                type_indexes.insert(PrimitiveType::TYPE_INT);
            } else {
                type_indexes.insert(PrimitiveType::TYPE_BIGINT);
            }
            return 0;
        } else if constexpr (T == TYPE_NULL) {
            have_nulls = true;
            return 0;
        } else {
            PrimitiveTypeTraits<PrimitiveType::TYPE_ARRAY>::CppType a;
            field_types.insert(T);
            type_indexes.insert(T);
            return 0;
        }
    }
    void get_scalar_type(PrimitiveType* type) const {
        if (type_indexes.size() == 1) {
            // Most cases will have only one type
            *type = *type_indexes.begin();
            return;
        }
        DataTypePtr data_type;
        get_least_supertype_jsonb(type_indexes, &data_type);
        *type = data_type->get_primitive_type();
    }
    bool contain_nulls() const { return have_nulls; }
    bool need_convert_field() const { return field_types.size() > 1; }

private:
    phmap::flat_hash_set<PrimitiveType> type_indexes;
    phmap::flat_hash_set<PrimitiveType> field_types;
    bool have_nulls = false;
};

template <typename Visitor>
void get_field_info_impl(const Field& field, FieldInfo* info) {
    Visitor to_scalar_type_visitor;
    apply_visitor(to_scalar_type_visitor, field);
    PrimitiveType type_id;
    to_scalar_type_visitor.get_scalar_type(&type_id);
    // array item's dimension may missmatch, eg. [1, 2, [1, 2, 3]]
    *info = {type_id, to_scalar_type_visitor.contain_nulls(),
             to_scalar_type_visitor.need_convert_field(),
             apply_visitor(FieldVisitorToNumberOfDimensions(), field)};
}

void get_field_info(const Field& field, FieldInfo* info) {
    if (field.is_complex_field()) {
        get_field_info_impl<FieldVisitorToScalarType>(field, info);
    } else {
        get_field_info_impl<SimpleFieldVisitorToScalarType>(field, info);
    }
}

bool generate_sub_column_info(const TabletSchema& schema, int32_t col_unique_id,
                              const std::string& path,
                              TabletSchema::SubColumnInfo* sub_column_info) {
    const auto& parent_column = schema.column_by_uid(col_unique_id);
    std::function<void(const TabletColumn&, TabletColumn*)> generate_result_column =
            [&](const TabletColumn& from_column, TabletColumn* to_column) {
                to_column->set_name(parent_column.name_lower_case() + "." + path);
                to_column->set_type(from_column.type());
                to_column->set_parent_unique_id(parent_column.unique_id());
                bool is_typed = !parent_column.variant_enable_typed_paths_to_sparse();
                to_column->set_path_info(
                        PathInData(parent_column.name_lower_case() + "." + path, is_typed));
                to_column->set_aggregation_method(parent_column.aggregation());
                to_column->set_is_nullable(true);
                to_column->set_parent_unique_id(parent_column.unique_id());
                if (from_column.is_decimal()) {
                    to_column->set_precision(from_column.precision());
                }
                to_column->set_frac(from_column.frac());

                if (from_column.is_array_type()) {
                    TabletColumn nested_column;
                    generate_result_column(*from_column.get_sub_columns()[0], &nested_column);
                    to_column->add_sub_column(nested_column);
                }
            };

    auto generate_index = [&](const std::string& pattern) {
        // 1. find subcolumn's index
        if (const auto& indexes = schema.inverted_index_by_field_pattern(col_unique_id, pattern);
            !indexes.empty()) {
            for (const auto& index : indexes) {
                auto index_ptr = std::make_shared<TabletIndex>(*index);
                index_ptr->set_escaped_escaped_index_suffix_path(
                        sub_column_info->column.path_info_ptr()->get_path());
                sub_column_info->indexes.emplace_back(std::move(index_ptr));
            }
        }
        // 2. find parent column's index
        else if (const auto parent_index = schema.inverted_indexs(col_unique_id);
                 !parent_index.empty()) {
            inherit_index(parent_index, sub_column_info->indexes, sub_column_info->column);
        } else {
            sub_column_info->indexes.clear();
        }
    };

    const auto& sub_columns = parent_column.get_sub_columns();
    for (const auto& sub_column : sub_columns) {
        const char* pattern = sub_column->name().c_str();
        switch (sub_column->pattern_type()) {
        case PatternTypePB::MATCH_NAME: {
            if (strcmp(pattern, path.c_str()) == 0) {
                generate_result_column(*sub_column, &sub_column_info->column);
                generate_index(sub_column->name());
                return true;
            }
            break;
        }
        case PatternTypePB::MATCH_NAME_GLOB: {
            int result = fnmatch(pattern, path.c_str(), FNM_PATHNAME);
            if (result == 0) {
                generate_result_column(*sub_column, &sub_column_info->column);
                generate_index(sub_column->name());
                return true;
            }
            break;
        }
        default:
            break;
        }
    }
    return false;
}

TabletSchemaSPtr calculate_variant_extended_schema(const std::vector<RowsetSharedPtr>& rowsets,
                                                   const TabletSchemaSPtr& base_schema) {
    if (rowsets.empty()) {
        return nullptr;
    }

    std::vector<TabletSchemaSPtr> schemas;
    for (const auto& rs : rowsets) {
        if (rs->num_segments() == 0) {
            continue;
        }
        const auto& tablet_schema = rs->tablet_schema();
        SegmentCacheHandle segment_cache;
        auto st = SegmentLoader::instance()->load_segments(std::static_pointer_cast<BetaRowset>(rs),
                                                           &segment_cache);
        if (!st.ok()) {
            return base_schema;
        }
        for (const auto& segment : segment_cache.get_segments()) {
            TabletSchemaSPtr schema = tablet_schema->copy_without_variant_extracted_columns();
            for (const auto& column : tablet_schema->columns()) {
                if (!column->is_variant_type()) {
                    continue;
                }
                segment_v2::ColumnReader* column_reader = nullptr;
                auto status = segment->get_column_reader(column->unique_id(), &column_reader);
                if (!status.ok() || !column_reader) {
                    continue;
                }

                CHECK(column_reader->get_meta_type() == FieldType::OLAP_FIELD_TYPE_VARIANT);
                const auto* subcolumn_readers =
                        assert_cast<VariantColumnReader*>(column_reader)->get_subcolumn_readers();
                for (const auto& entry : *subcolumn_readers) {
                    if (entry->path.empty()) {
                        continue;
                    }
                    const std::string& column_name =
                            column->name_lower_case() + "." + entry->path.get_path();
                    const vectorized::DataTypePtr& data_type = entry->data.file_column_type;
                    vectorized::PathInDataBuilder full_path_builder;
                    auto full_path = full_path_builder.append(column->name_lower_case(), false)
                                             .append(entry->path.get_parts(), false)
                                             .build();
                    TabletColumn subcolumn =
                            get_column_by_type(data_type, column_name,
                                               vectorized::schema_util::ExtraInfo {
                                                       .unique_id = -1,
                                                       .parent_unique_id = column->unique_id(),
                                                       .path_info = full_path});
                    schema->append_column(subcolumn);
                }
            }
            schemas.emplace_back(schema);
        }
    }
    TabletSchemaSPtr least_common_schema;
    auto st = get_least_common_schema(schemas, base_schema, least_common_schema, false);
    if (!st.ok()) {
        return base_schema;
    }
    return least_common_schema;
}

bool inherit_index(const std::vector<const TabletIndex*>& parent_indexes,
                   TabletIndexes& subcolumns_indexes, FieldType column_type,
                   const std::string& suffix_path, bool is_array_nested_type) {
    if (parent_indexes.empty()) {
        return false;
    }
    subcolumns_indexes.clear();
    // bkd index or array index only need to inherit one index
    if (field_is_numeric_type(column_type) ||
        (is_array_nested_type &&
         (field_is_numeric_type(column_type) || field_is_slice_type(column_type)))) {
        auto index_ptr = std::make_shared<TabletIndex>(*parent_indexes[0]);
        index_ptr->set_escaped_escaped_index_suffix_path(suffix_path);
        // no need parse for bkd index or array index
        index_ptr->remove_parser_and_analyzer();
        subcolumns_indexes.emplace_back(std::move(index_ptr));
        return true;
    }
    // string type need to inherit all indexes
    else if (field_is_slice_type(column_type) && !is_array_nested_type) {
        for (const auto& index : parent_indexes) {
            auto index_ptr = std::make_shared<TabletIndex>(*index);
            index_ptr->set_escaped_escaped_index_suffix_path(suffix_path);
            subcolumns_indexes.emplace_back(std::move(index_ptr));
        }
        return true;
    }
    return false;
}

bool inherit_index(const std::vector<const TabletIndex*>& parent_indexes,
                   TabletIndexes& subcolumns_indexes, const TabletColumn& column) {
    if (!column.is_extracted_column()) {
        return false;
    }
    if (column.is_array_type()) {
        if (column.get_sub_columns().empty()) {
            return false;
        }
        return inherit_index(parent_indexes, subcolumns_indexes,
                             column.get_sub_columns()[0]->type(),
                             column.path_info_ptr()->get_path(), true);
    }
    return inherit_index(parent_indexes, subcolumns_indexes, column.type(),
                         column.path_info_ptr()->get_path());
}

bool inherit_index(const std::vector<const TabletIndex*>& parent_indexes,
                   TabletIndexes& subcolumns_indexes, const ColumnMetaPB& column_pb) {
    if (!column_pb.has_column_path_info()) {
        return false;
    }
    if (column_pb.type() == (int)FieldType::OLAP_FIELD_TYPE_ARRAY) {
        if (column_pb.children_columns_size() == 0) {
            return false;
        }
        return inherit_index(parent_indexes, subcolumns_indexes,
                             (FieldType)column_pb.children_columns(0).type(),
                             column_pb.column_path_info().path(), true);
    }
    return inherit_index(parent_indexes, subcolumns_indexes, (FieldType)column_pb.type(),
                         column_pb.column_path_info().path());
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized::schema_util
