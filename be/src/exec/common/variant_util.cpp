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

#include "exec/common/variant_util.h"

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
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/get_least_supertype.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type/storage_field_type.h"
#include "core/field.h"
#include "core/typeid_cast.h"
#include "core/types.h"
#include "exec/common/field_visitors.h"
#include "exec/common/sip_hash.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "exprs/json_functions.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/segment/variant/variant_column_writer_impl.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/tablet/tablet_schema.h"
#include "util/client_cache.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/json/path_in_data.h"
#include "util/json/simd_json_parser.h"
#include "util/jsonb_utils.h"

namespace doris::variant_util {

namespace {

PathInData make_full_subcolumn_path(const TabletColumnPtr& parent_column, std::string_view path) {
    if (!path.empty()) {
        return PathInData(parent_column->name_lower_case() + "." + std::string(path));
    }

    // Keep the empty JSON key as a real path part. The variant root is `parts.empty()`;
    // an empty key is `parts.size() == 1 && parts[0].key.empty()` after popping root.
    PathInDataBuilder builder;
    return builder.append(parent_column->name_lower_case(), false).append("", false).build();
}

void append_empty_key_subcolumn_from_stats(VariantCompactionPaths& paths_set_info,
                                           const TabletColumnPtr& parent_column,
                                           TabletSchemaSPtr& output_schema) {
    if (!paths_set_info.sub_path_set.contains("") || paths_set_info.sparse_path_set.contains("") ||
        paths_set_info.subcolumn_indexes.contains("")) {
        return;
    }

    auto column_name = parent_column->name_lower_case() + ".";
    auto column_path = make_full_subcolumn_path(parent_column, "");

    TabletColumn subcolumn;
    subcolumn.set_name(column_name);
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    subcolumn.set_parent_unique_id(parent_column->unique_id());
    subcolumn.set_path_info(column_path);
    subcolumn.set_aggregation_method(parent_column->aggregation());
    subcolumn.set_variant_max_subcolumns_count(parent_column->variant_max_subcolumns_count());
    subcolumn.set_variant_enable_doc_mode(parent_column->variant_enable_doc_mode());
    subcolumn.set_is_nullable(true);
    output_schema->append_column(subcolumn);
}

} // namespace

// NestedGroup's physical children and offsets are produced by NestedGroupWriteProvider, not by
// appending TabletSchema extracted columns here. This predicate keeps only ordinary Variant paths
// that are outside the NG tree, for example `v.owner` beside `v.items[*]`.
bool is_regular_path_outside_nested_group(const PathInData& path) {
    const std::string& relative_path = path.get_path();
    return !relative_path.empty() && !path.get_is_typed() && !path.has_nested_part() &&
           !segment_v2::contains_nested_group_marker(relative_path) &&
           !segment_v2::is_root_nested_group_path(relative_path) &&
           relative_path != SPARSE_COLUMN_PATH &&
           relative_path.find(DOC_VALUE_COLUMN_PATH) == std::string::npos;
}

bool should_materialize_nested_group_regular_subcolumns(
        const TabletColumnPtr& column,
        const std::unordered_map<int32_t, VariantExtendedInfo>& uid_to_variant_extended_info) {
    const auto info_it = uid_to_variant_extended_info.find(column->unique_id());
    return column->variant_enable_nested_group() ||
           (info_it != uid_to_variant_extended_info.end() && info_it->second.has_nested_group);
}

bool is_ordinary_template_variant(const TabletColumn& column) {
    return column.is_variant_type() && !column.get_sub_columns().empty() &&
           !column.variant_enable_doc_mode() && !column.variant_enable_nested_group() &&
           !column.variant_enable_typed_paths_to_sparse();
}

std::unordered_set<int32_t> collect_nested_group_compaction_root_uids(
        const TabletSchema& target,
        const std::unordered_map<int32_t, VariantExtendedInfo>& uid_to_variant_extended_info) {
    std::unordered_set<int32_t> root_uids;
    for (const TabletColumnPtr& column : target.columns()) {
        if (column->is_variant_type() && should_materialize_nested_group_regular_subcolumns(
                                                 column, uid_to_variant_extended_info)) {
            root_uids.insert(column->unique_id());
        }
    }
    return root_uids;
}

PathToDataTypes collect_regular_types_outside_nested_group(
        const VariantExtendedInfo& extended_info) {
    PathToDataTypes regular_path_to_data_types;
    for (const auto& [path, data_types] : extended_info.path_to_data_types) {
        if (!is_regular_path_outside_nested_group(path)) {
            continue;
        }
        regular_path_to_data_types.emplace(path, data_types);
    }
    return regular_path_to_data_types;
}

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
    if (const auto* nullable = typeid_cast<const DataTypeNullable*>(current_type)) {
        current_type = nullable->get_nested_type().get();
    }
    while (const auto* type_array = typeid_cast<const DataTypeArray*>(current_type)) {
        current_type = type_array->get_nested_type().get();
        last_array = type_array;
        if (const auto* nullable = typeid_cast<const DataTypeNullable*>(current_type)) {
            current_type = nullable->get_nested_type().get();
        }
    }
    return last_array ? last_array->get_nested_type() : type;
}

Status cast_column(const ColumnWithTypeAndName& arg, const DataTypePtr& type, ColumnPtr* result) {
    ColumnsWithTypeAndName arguments {arg, {nullptr, type, type->get_name()}};

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
    Status cast_status =
            function->execute(ctx.get(), tmp_block, {0}, result_column, arg.column->size());
    if (!cast_status.ok()) {
        return cast_status;
    }
    *result = tmp_block.get_by_position(result_column).column->convert_to_full_column_if_const();
    VLOG_DEBUG << fmt::format("{} before convert {}, after convert {}", arg.name,
                              arg.column->get_name(), (*result)->get_name());
    return Status::OK();
}

ColumnPtr jsonb_root_to_json_string_column(const IColumn& root) {
    auto root_column = root.convert_to_full_column_if_const();
    const IColumn* jsonb_column = root_column.get();
    const NullMap* null_map = nullptr;
    if (root_column->is_nullable()) {
        const auto& nullable = assert_cast<const ColumnNullable&>(*root_column);
        jsonb_column = &nullable.get_nested_column();
        null_map = &nullable.get_null_map_data();
    }

    const auto& column = assert_cast<const ColumnString&>(*jsonb_column);
    auto result = ColumnString::create();
    result->reserve(column.size());
    for (size_t i = 0; i < column.size(); ++i) {
        if (null_map != nullptr && (*null_map)[i]) {
            result->insert_default();
            continue;
        }

        const auto jsonb = column.get_data_at(i);
        if (jsonb.size == 0) {
            result->insert_default();
            continue;
        }

        const auto json = JsonbToJson::jsonb_to_json_string(jsonb.data, jsonb.size);
        result->insert_data(json.data(), json.size());
    }
    return result->get_ptr();
}

void get_column_by_type(const DataTypePtr& data_type, const std::string& name, TabletColumn& column,
                        const ExtraInfo& ext_info) {
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
    if (data_type->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
        const auto* variant_v2 = typeid_cast<const DataTypeVariantV2*>(data_type.get());
        DORIS_CHECK(variant_v2 != nullptr);
        column.set_variant_max_subcolumns_count(variant_v2->variant_max_subcolumns_count());
        column.set_variant_enable_doc_mode(variant_v2->enable_doc_mode());
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
        is_date_or_datetime(type) || type == PrimitiveType::TYPE_DATEV2 ||
        type == PrimitiveType::TYPE_TIMESTAMP_NS || type == PrimitiveType::TYPE_UUID) {
        column.set_length(cast_set<int32_t>(data_type->get_size_of_value_in_memory()));
        return;
    }
    if (is_decimal(type)) {
        column.set_precision(data_type->get_precision());
        column.set_frac(data_type->get_scale());
        return;
    }
    // datetimev2 needs scale
    if (type == PrimitiveType::TYPE_DATETIMEV2 || type == PrimitiveType::TYPE_TIMESTAMPTZ) {
        column.set_precision(-1);
        column.set_frac(data_type->get_scale());
        return;
    }

    throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                           "unexcepted data column type: {}, column name is: {}",
                           data_type->get_name(), name);
}

TabletColumn get_column_by_type(const DataTypePtr& data_type, const std::string& name,
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
        static constexpr std::string_view column_name_dummy = "_dummy";
        if (key.get_path() == column_name_dummy) {
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
            if (col->has_path_info() && col->parent_unique_id() >= 0 &&
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

// Keep variant subcolumn BF support aligned with FE DDL checks.
bool is_bf_supported_by_fe_for_variant_subcolumn(FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
    case FieldType::OLAP_FIELD_TYPE_INT:
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
    case FieldType::OLAP_FIELD_TYPE_CHAR:
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING:
    case FieldType::OLAP_FIELD_TYPE_DATE:
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS:
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
    case FieldType::OLAP_FIELD_TYPE_IPV4:
    case FieldType::OLAP_FIELD_TYPE_IPV6:
    case FieldType::OLAP_FIELD_TYPE_UUID:
        return true;
    default:
        return false;
    }
}

void inherit_column_attributes(const TabletColumn& source, TabletColumn& target,
                               TabletSchemaSPtr* target_schema) {
    if (!target.is_extracted_column()) {
        return;
    }
    target.set_aggregation_method(source.aggregation());

    // 1. bloom filter
    if (is_bf_supported_by_fe_for_variant_subcolumn(target.type())) {
        target.set_is_bf_column(source.is_bf_column());
    }

    if (!target_schema) {
        return;
    }

    // 2. inverted index
    TabletIndexes indexes_to_add;
    auto source_indexes = (*target_schema)->inverted_indexs(source.unique_id());
    // if target is variant type, we need to inherit all indexes
    // because this schema is a read schema from fe
    if (target.is_variant_type()) {
        for (auto& index : source_indexes) {
            auto index_info = std::make_shared<TabletIndex>(*index);
            index_info->set_escaped_escaped_index_suffix_path(target.path_info_ptr()->get_path());
            indexes_to_add.emplace_back(std::move(index_info));
        }
    } else {
        inherit_index(source_indexes, indexes_to_add, target);
    }
    auto target_indexes = (*target_schema)
                                  ->inverted_indexs(target.parent_unique_id(),
                                                    target.path_info_ptr()->get_path());
    if (target_indexes.empty()) {
        for (auto& index_info : indexes_to_add) {
            (*target_schema)->append_index(std::move(*index_info));
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

bool has_schema_index_diff(const TabletSchema* new_schema, const TabletSchema* old_schema,
                           int32_t new_col_idx, int32_t old_col_idx) {
    const auto& column_new = new_schema->column(new_col_idx);
    const auto& column_old = old_schema->column(old_col_idx);

    if (column_new.is_bf_column() != column_old.is_bf_column()) {
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

MutableColumnPtr create_variant_binary_column() {
    return ColumnMap::create(ColumnString::create(), ColumnString::create(),
                             ColumnArray::ColumnOffsets::create());
}

TabletColumn create_sparse_column(const TabletColumn& variant) {
    TabletColumn res;
    res.set_name(variant.name_lower_case() + "." + SPARSE_COLUMN_PATH);
    res.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
    res.set_aggregation_method(variant.aggregation());
    res.set_path_info(PathInData {variant.name_lower_case() + "." + SPARSE_COLUMN_PATH});
    res.set_parent_unique_id(variant.unique_id());
    res.set_default_value("{}");
    TabletColumn child_tcolumn;
    child_tcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    res.add_sub_column(child_tcolumn);
    res.add_sub_column(child_tcolumn);
    return res;
}

TabletColumn create_sparse_shard_column(const TabletColumn& variant, int bucket_index) {
    TabletColumn res;
    std::string name = variant.name_lower_case() + "." + SPARSE_COLUMN_PATH + ".b" +
                       std::to_string(bucket_index);
    res.set_name(name);
    res.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
    res.set_aggregation_method(variant.aggregation());
    res.set_parent_unique_id(variant.unique_id());
    res.set_default_value("{}");
    PathInData path(name);
    res.set_path_info(path);
    TabletColumn child_tcolumn;
    child_tcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    res.add_sub_column(child_tcolumn);
    res.add_sub_column(child_tcolumn);
    return res;
}

TabletColumn create_doc_value_column(const TabletColumn& variant, int bucket_index) {
    TabletColumn res;
    std::string name = variant.name_lower_case() + "." + DOC_VALUE_COLUMN_PATH + ".b" +
                       std::to_string(bucket_index);
    res.set_name(name);
    res.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
    res.set_aggregation_method(variant.aggregation());
    res.set_parent_unique_id(variant.unique_id());
    res.set_default_value("{}");
    res.set_path_info(PathInData {name});

    TabletColumn child_tcolumn;
    child_tcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    res.add_sub_column(child_tcolumn);
    res.add_sub_column(child_tcolumn);
    return res;
}

TabletColumn create_doc_value_compaction_column(const TabletColumn& variant, int bucket_index) {
    TabletColumn res;
    const std::string name = variant.name_lower_case() + "." + DOC_VALUE_COLUMN_PATH + ".b" +
                             std::to_string(bucket_index);
    res.set_name(name);
    res.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    res.set_aggregation_method(variant.aggregation());
    res.set_parent_unique_id(variant.unique_id());
    res.set_path_info(PathInData {name});
    res.set_is_nullable(true);
    res.set_variant_enable_doc_mode(true);
    // A segment predating the root column supplies SQL NULL. The doc compact writer accepts
    // the null map and emits no doc fields for these rows, preserving their row positions.
    res.set_default_value("NULL");
    return res;
}

uint32_t variant_binary_shard_of(const StringRef& path, uint32_t bucket_num) {
    if (bucket_num <= 1) return 0;
    SipHash hash;
    hash.update(path.data, path.size);
    uint64_t h = hash.get64();
    return static_cast<uint32_t>(h % bucket_num);
}

Status VariantCompactionUtil::aggregate_path_to_stats(
        const RowsetSharedPtr& rs,
        std::unordered_map<int32_t, PathToNoneNullValues>* uid_to_path_stats) {
    SegmentCacheHandle segment_cache;
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(rs), &segment_cache));

    for (const auto& column : rs->tablet_schema()->columns()) {
        if (!column->is_variant_type() || column->unique_id() < 0) {
            continue;
        }
        if (!should_check_variant_path_stats(*column)) {
            continue;
        }
        for (const auto& segment : segment_cache.get_segments()) {
            std::shared_ptr<segment_v2::VariantColumnReader> variant_column_reader;
            OlapReaderStatistics stats;
            StorageReadOptions read_options(stats);
            Status st =
                    segment->get_variant_root_reader(*column, read_options, &variant_column_reader);
            if (st.is<ErrorCode::NOT_FOUND>()) {
                // An ALTER-added nullable/defaulted VARIANT has no physical path statistics in old
                // segments; its logical default is handled only by the value-read path.
                continue;
            }
            RETURN_IF_ERROR(st);
            DORIS_CHECK(variant_column_reader != nullptr);
            // load external meta before getting stats
            RETURN_IF_ERROR(variant_column_reader->load_external_meta_once());
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
        auto& extended_info = (*uid_to_variant_extended_info)[column->unique_id()];
        if (column->variant_enable_nested_group()) {
            extended_info.has_nested_group = true;
        }
        for (const auto& segment : segment_cache.get_segments()) {
            std::shared_ptr<segment_v2::VariantColumnReader> variant_column_reader;
            OlapReaderStatistics stats;
            StorageReadOptions read_options(stats);
            Status st =
                    segment->get_variant_root_reader(*column, read_options, &variant_column_reader);
            if (st.is<ErrorCode::NOT_FOUND>()) {
                // A schema default contributes logical rows but no sparse paths, typed leaves, or
                // nested-group metadata to this physical-metadata aggregation.
                continue;
            }
            RETURN_IF_ERROR(st);
            DORIS_CHECK(variant_column_reader != nullptr);
            // load external meta before getting stats
            RETURN_IF_ERROR(variant_column_reader->load_external_meta_once());
            const auto* source_stats = variant_column_reader->get_stats();
            CHECK(source_stats);

            if (!column->variant_enable_nested_group()) {
                // NG roots still need type metadata for regular subpaths such as `v.owner`,
                // but their compaction schema should not be driven by flat path stats.
                for (const auto& [path, size] : source_stats->sparse_column_non_null_size) {
                    extended_info.path_to_none_null_values[path] += size;
                    extended_info.sparse_paths.emplace(path);
                }

                for (const auto& [path, size] : source_stats->subcolumns_non_null_size) {
                    extended_info.path_to_none_null_values[path] += size;
                }
            }

            //2. agg path -> schema
            variant_column_reader->get_subcolumns_types(&extended_info.path_to_data_types);

            // 3. extract typed paths
            variant_column_reader->get_typed_paths(&extended_info.typed_paths);

            // 4. extract nested paths
            if (!column->variant_enable_nested_group()) {
                variant_column_reader->get_nested_paths(&extended_info.nested_paths);
            }
        }
    }
    return Status::OK();
}

// get the subpaths and sparse paths for the variant column
void VariantCompactionUtil::get_subpaths(int32_t max_subcolumns_count,
                                         const PathToNoneNullValues& stats,
                                         VariantCompactionPaths& paths_set_info) {
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

namespace {

// Typed paths historically omit none_null_size. ADD carriers also contain newly typed rows;
// include those actual cells in validation without adding typed counts to budget stats.
Status add_missing_typed_path_counts(
        const RowsetSharedPtr& rowset, const VariantCompactionPathsMap& carriers,
        const std::unordered_map<int32_t, PathToNoneNullValues>& output_stats,
        std::unordered_map<int32_t, PathToNoneNullValues>& input_stats) {
    // Output path counts already come from the normal stats check. Only the compaction's planned
    // carriers are counted in output while some inputs hold them typed, including new typed
    // rowsets whose template already equals the target but which contribute to a carrier made
    // from older inputs. Unaltered typed paths have no output count and are skipped.
    std::unordered_map<int32_t, std::unordered_set<std::string>> counted_template_paths;
    for (const auto& column : rowset->tablet_schema()->columns()) {
        const auto output = output_stats.find(column->unique_id());
        const auto layout = carriers.find(column->unique_id());
        if (layout == carriers.end() || layout->second.carrier_path_set.empty() ||
            output == output_stats.end() || !should_check_variant_path_stats(*column)) {
            continue;
        }
        for (const auto& path : layout->second.carrier_path_set) {
            if (output->second.contains(path)) {
                counted_template_paths[column->unique_id()].insert(path);
            }
        }
    }
    if (counted_template_paths.empty()) {
        return Status::OK();
    }
    SegmentCacheHandle segments;
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(rowset), &segments));
    for (const auto& column : rowset->tablet_schema()->columns()) {
        const auto paths = counted_template_paths.find(column->unique_id());
        if (paths == counted_template_paths.end()) {
            continue;
        }
        for (const auto& segment : segments.get_segments()) {
            OlapReaderStatistics stats;
            StorageReadOptions read_options(stats);
            std::shared_ptr<segment_v2::VariantColumnReader> root;
            auto status = segment->get_variant_root_reader(*column, read_options, &root);
            if (status.is<ErrorCode::NOT_FOUND>()) {
                continue; // An absent physical root contributes no non-null cells.
            }
            RETURN_IF_ERROR(status);
            RETURN_IF_ERROR(root->load_external_meta_once());
            std::unordered_set<std::string> needed;
            root->get_typed_paths(&needed);
            std::erase_if(needed, [&](const auto& path) {
                return !paths->second.contains(path) ||
                       root->get_stats()->subcolumns_non_null_size.contains(path);
            });
            // Unaltered typed paths are also uncounted in output, so do not traverse metadata
            // or read their pages. This fallback is only for typed -> counted-carrier transitions.
            if (needed.empty()) {
                continue;
            }
            std::vector<segment_v2::ColumnMetaPB> metas;
            RETURN_IF_ERROR(segment->traverse_column_meta_pbs([&](const auto& meta) {
                if (!meta.has_column_path_info() ||
                    meta.column_path_info().parrent_column_unique_id() != column->unique_id()) {
                    return;
                }
                PathInData path;
                path.from_protobuf(meta.column_path_info());
                const auto relative = path.copy_pop_front();
                if (relative.get_is_typed() && needed.contains(relative.get_path())) {
                    metas.push_back(meta);
                }
            }));
            for (const auto& meta : metas) {
                PathInData full_path;
                full_path.from_protobuf(meta.column_path_info());
                const auto path = full_path.copy_pop_front().get_path();
                uint64_t non_null = 0;
                if (meta.has_none_null_size()) {
                    non_null = meta.none_null_size();
                } else if (!meta.is_nullable()) {
                    non_null = meta.num_rows();
                } else if (meta.num_rows() != 0) {
                    // Legacy nullable metadata has no exact non-null total. Read bounded native
                    // batches, with no query/template cast, rather than treating every row as set.
                    segment_v2::ColumnReaderOptions options;
                    options.tablet_schema = rowset->tablet_schema();
                    std::shared_ptr<segment_v2::ColumnReader> reader;
                    RETURN_IF_ERROR(segment_v2::ColumnReader::create(
                            options, meta, meta.num_rows(), segment->file_reader(), &reader));
                    segment_v2::ColumnIteratorUPtr iterator;
                    RETURN_IF_ERROR(reader->new_iterator(&iterator, nullptr));
                    segment_v2::ColumnIteratorOptions iterator_options;
                    iterator_options.file_reader = segment->file_reader().get();
                    iterator_options.stats = &stats;
                    RETURN_IF_ERROR(iterator->init(iterator_options));
                    RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
                    const auto type = DataTypeFactory::instance().create_data_type(meta);
                    uint64_t remaining = meta.num_rows();
                    while (remaining > 0) {
                        MutableColumnPtr values = type->create_column();
                        size_t count = std::min<uint64_t>(remaining, 4096);
                        RETURN_IF_ERROR(iterator->next_batch(&count, values));
                        if (count == 0) {
                            return Status::Corruption(
                                    "Incomplete typed path in segment {} uid {} path {}: {} rows "
                                    "remaining",
                                    segment->id(), column->unique_id(), path, remaining);
                        }
                        const auto& nulls =
                                assert_cast<const ColumnNullable&>(*values).get_null_map_data();
                        non_null += std::count(nulls.begin(), nulls.end(), UInt8 {0});
                        remaining -= count;
                    }
                }
                input_stats[column->unique_id()][path] += non_null;
            }
        }
    }
    return Status::OK();
}

} // namespace

Status VariantCompactionUtil::check_path_stats(const std::vector<RowsetSharedPtr>& inputs,
                                               RowsetSharedPtr output, BaseTabletSPtr tablet,
                                               const VariantCompactionPathsMap* paths) {
    if (output->tablet_schema()->num_variant_columns() == 0) {
        return Status::OK();
    }
    for (const auto& rowset : inputs) {
        for (const auto& column : rowset->tablet_schema()->columns()) {
            if (column->is_variant_type() && !should_check_variant_path_stats(*column)) {
                return Status::OK();
            }
        }
    }
    // check no extended schema in input rowsets
    for (const auto& rowset : inputs) {
        for (const auto& column : rowset->tablet_schema()->columns()) {
            if (column->is_extracted_column()) {
                return Status::OK();
            }
        }
    }
#ifndef BE_TEST
    // check no extended schema in output rowset
    for (const auto& column : output->tablet_schema()->columns()) {
        if (column->is_extracted_column()) {
            const auto& name = column->name();
            if (name.find("." + DOC_VALUE_COLUMN_PATH + ".") != std::string::npos ||
                name.find("." + SPARSE_COLUMN_PATH + ".") != std::string::npos ||
                name.ends_with("." + SPARSE_COLUMN_PATH)) {
                continue;
            }
            return Status::InternalError("Unexpected extracted column {} in output rowset",
                                         column->name());
        }
    }
#endif
    // only check path stats for dup_keys since the rows may be merged in other models
    if (tablet->keys_type() != KeysType::DUP_KEYS) {
        return Status::OK();
    }
    // if there is a delete predicate in the input rowsets, we skip the path stats check
    for (auto& rowset : inputs) {
        if (rowset->rowset_meta()->has_delete_predicate()) {
            return Status::OK();
        }
    }
    for (const auto& column : output->tablet_schema()->columns()) {
        if (column->is_variant_type() && !should_check_variant_path_stats(*column)) {
            return Status::OK();
        }
    }
    std::unordered_map<int32_t, PathToNoneNullValues> output_uid_to_path_stats;
    RETURN_IF_ERROR(aggregate_path_to_stats(output, &output_uid_to_path_stats));
    std::unordered_map<int32_t, PathToNoneNullValues> original_uid_to_path_stats;
    for (const auto& rs : inputs) {
        RETURN_IF_ERROR(aggregate_path_to_stats(rs, &original_uid_to_path_stats));
        if (paths != nullptr) {
            RETURN_IF_ERROR(add_missing_typed_path_counts(rs, *paths, output_uid_to_path_stats,
                                                          original_uid_to_path_stats));
        }
    }
    for (const auto& [uid, stats] : output_uid_to_path_stats) {
        if (output->tablet_schema()->column_by_uid(uid).is_variant_type() &&
            output->tablet_schema()->column_by_uid(uid).variant_enable_doc_mode()) {
            continue;
        }
        if (original_uid_to_path_stats.find(uid) == original_uid_to_path_stats.end()) {
            return Status::InternalError("Path stats not found for uid {}, tablet_id {}", uid,
                                         tablet->tablet_id());
        }

        // In input rowsets, some rowsets may have statistics values exceeding the maximum limit,
        // which leads to inaccurate statistics
        if (stats.size() > output->tablet_schema()
                                   ->column_by_uid(uid)
                                   .variant_max_sparse_column_statistics_size()) {
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
                if (original_uid_to_path_stats.at(uid).find(path) ==
                    original_uid_to_path_stats.at(uid).end()) {
                    return Status::InternalError(
                            "Path stats not found for uid {}, path {}, tablet_id {}", uid, path,
                            tablet->tablet_id());
                }
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

namespace {

Result<TabletSchema::SubColumnInfo> reconcile_typed_path(const TabletSchema& target,
                                                         const TabletColumn& parent_column,
                                                         const std::string& path) {
    TabletSchema::SubColumnInfo info;
    if (!generate_sub_column_info(target, parent_column.unique_id(), path, &info)) {
        return ResultError(
                Status::InternalError("Failed to generate sub column info for path {}", path));
    }
    inherit_column_attributes(parent_column, info.column);
    return info;
}

Result<TabletSchema::SubColumnInfo> reconcile_nested_path(
        const PathInData& path, const PathToDataTypes& path_to_data_types,
        const TabletColumn& parent_column, const std::vector<const TabletIndex*>& parent_indexes) {
    const auto& find_data_types = path_to_data_types.find(path);
    if (find_data_types == path_to_data_types.end() || find_data_types->second.empty()) {
        return ResultError(
                Status::InternalError("Nested path {} has no data type", path.get_path()));
    }
    DataTypePtr data_type;
    get_least_supertype_jsonb(find_data_types->second, &data_type);

    const std::string& column_name = parent_column.name_lower_case() + "." + path.get_path();
    PathInDataBuilder full_path_builder;
    auto full_path = full_path_builder.append(parent_column.name_lower_case(), false)
                             .append(path.get_parts(), false)
                             .build();
    TabletSchema::SubColumnInfo info;
    info.column = get_column_by_type(data_type, column_name,
                                     ExtraInfo {.unique_id = -1,
                                                .parent_unique_id = parent_column.unique_id(),
                                                .path_info = full_path});
    inherit_column_attributes(parent_column, info.column);
    inherit_index(parent_indexes, info.indexes, info.column);
    return info;
}

void build_typed_column(const std::string& path, TabletSchema::SubColumnInfo info,
                        TabletSchemaSPtr& output_schema, VariantCompactionPaths& paths) {
    output_schema->append_column(info.column);
    paths.typed_path_set.insert({path, std::move(info)});
    VLOG_DEBUG << "append typed column " << path;
}

void build_nested_column(const std::string& path, TabletSchema::SubColumnInfo info,
                         TabletSchemaSPtr& output_schema, VariantCompactionPaths& paths) {
    paths.subcolumn_indexes.emplace(path, std::move(info.indexes));
    output_schema->append_column(info.column);
    VLOG_DEBUG << "append nested column " << path;
}

} // namespace

Status VariantCompactionUtil::get_compaction_typed_columns(
        const TabletSchemaSPtr& target, const std::unordered_set<std::string>& input_typed_paths,
        const TabletColumnPtr parent_column, TabletSchemaSPtr& output_schema,
        VariantCompactionPaths& paths_set_info) {
    if (parent_column->variant_enable_typed_paths_to_sparse()) {
        return Status::OK();
    }
    for (const auto& path : input_typed_paths) {
        auto info = reconcile_typed_path(*target, *parent_column, path);
        if (!info.has_value()) {
            return info.error();
        }
        build_typed_column(path, std::move(info).value(), output_schema, paths_set_info);
    }
    return Status::OK();
}

Status VariantCompactionUtil::get_compaction_nested_columns(
        const std::unordered_set<PathInData, PathInData::Hash>& nested_paths,
        const PathToDataTypes& path_to_data_types, const TabletColumnPtr parent_column,
        TabletSchemaSPtr& output_schema, VariantCompactionPaths& paths_set_info) {
    const auto& parent_indexes = output_schema->inverted_indexs(parent_column->unique_id());
    for (const auto& path : nested_paths) {
        auto info = reconcile_nested_path(path, path_to_data_types, *parent_column, parent_indexes);
        if (!info.has_value()) {
            return info.error();
        }
        build_nested_column(path.get_path(), std::move(info).value(), output_schema,
                            paths_set_info);
    }
    return Status::OK();
}

void VariantCompactionUtil::get_compaction_subcolumns_from_subpaths(
        VariantCompactionPaths& paths_set_info, const TabletColumnPtr parent_column,
        const TabletSchemaSPtr& target, const PathToDataTypes& path_to_data_types,
        const std::unordered_set<std::string>& sparse_paths, TabletSchemaSPtr& output_schema,
        const PathSet& carrier_paths) {
    auto& path_set = paths_set_info.sub_path_set;
    std::vector<StringRef> sorted_subpaths(path_set.begin(), path_set.end());
    std::sort(sorted_subpaths.begin(), sorted_subpaths.end());
    const auto& parent_indexes = target->inverted_indexs(parent_column->unique_id());
    // append subcolumns
    for (const auto& subpath : sorted_subpaths) {
        auto column_name = parent_column->name_lower_case() + "." + subpath.to_string();
        auto column_path = make_full_subcolumn_path(parent_column,
                                                    std::string_view(subpath.data, subpath.size));

        const auto& find_data_types = path_to_data_types.find(PathInData(subpath));

        // some cases: the subcolumn type is variant
        // 1. this path has no data type in segments
        // 2. this path is in sparse paths
        // 3. the sparse paths are too much
        // 4. a pinned template carrier must bypass inference (old STRING plus new INT).
        TabletSchema::SubColumnInfo sub_column_info;
        if (parent_column->variant_enable_typed_paths_to_sparse() &&
            generate_sub_column_info(*target, parent_column->unique_id(), std::string(subpath),
                                     &sub_column_info)) {
            inherit_column_attributes(*parent_column, sub_column_info.column);
            output_schema->append_column(sub_column_info.column);
            paths_set_info.subcolumn_indexes.emplace(subpath, std::move(sub_column_info.indexes));
            VLOG_DEBUG << "append typed column " << subpath;
        } else if (carrier_paths.contains(subpath) || find_data_types == path_to_data_types.end() ||
                   find_data_types->second.empty() ||
                   sparse_paths.find(std::string(subpath)) != sparse_paths.end() ||
                   sparse_paths.size() >=
                           parent_column->variant_max_sparse_column_statistics_size()) {
            TabletColumn subcolumn;
            subcolumn.set_name(column_name);
            subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
            subcolumn.set_parent_unique_id(parent_column->unique_id());
            subcolumn.set_path_info(column_path);
            subcolumn.set_aggregation_method(parent_column->aggregation());
            subcolumn.set_variant_max_subcolumns_count(
                    parent_column->variant_max_subcolumns_count());
            subcolumn.set_variant_enable_doc_mode(parent_column->variant_enable_doc_mode());
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
                                       ExtraInfo {.unique_id = -1,
                                                  .parent_unique_id = parent_column->unique_id(),
                                                  .path_info = column_path});
            inherit_column_attributes(*parent_column, sub_column);
            TabletIndexes sub_column_indexes;
            inherit_index(parent_indexes, sub_column_indexes, sub_column);
            paths_set_info.subcolumn_indexes.emplace(subpath, std::move(sub_column_indexes));
            output_schema->append_column(sub_column);
            VLOG_DEBUG << "append sub column " << subpath << " data type " << data_type->get_name();
        }
    }
}

void VariantCompactionUtil::get_compaction_subcolumns_from_data_types(
        VariantCompactionPaths& paths_set_info, const TabletColumnPtr parent_column,
        const TabletSchemaSPtr& target, const PathToDataTypes& path_to_data_types,
        TabletSchemaSPtr& output_schema) {
    const auto& parent_indexes = target->inverted_indexs(parent_column->unique_id());
    for (const auto& [path, data_types] : path_to_data_types) {
        // Typed paths are materialized by get_compaction_typed_columns(); this helper only
        // materializes regular subcolumns inferred from rowset data types.
        if (data_types.empty() || path.empty() || path.get_is_typed() || path.has_nested_part()) {
            continue;
        }
        DataTypePtr data_type;
        get_least_supertype_jsonb(data_types, &data_type);
        auto column_name = parent_column->name_lower_case() + "." + path.get_path();
        auto column_path = make_full_subcolumn_path(parent_column, path.get_path());
        TabletColumn sub_column =
                get_column_by_type(data_type, column_name,
                                   ExtraInfo {.unique_id = -1,
                                              .parent_unique_id = parent_column->unique_id(),
                                              .path_info = column_path});
        inherit_column_attributes(*parent_column, sub_column);
        TabletIndexes sub_column_indexes;
        inherit_index(parent_indexes, sub_column_indexes, sub_column);
        paths_set_info.sub_path_set.emplace(path.get_path());
        paths_set_info.subcolumn_indexes.emplace(path.get_path(), std::move(sub_column_indexes));
        output_schema->append_column(sub_column);
        VLOG_DEBUG << "append sub column " << path.get_path() << " data type "
                   << data_type->get_name();
    }
    // The data-type map can contain the variant root as PathInData(), while an empty JSON key
    // only appears in path stats as "". If stats selected it as a materialized path, append it
    // with an explicit empty path part so it does not collide with root.
    append_empty_key_subcolumn_from_stats(paths_set_info, parent_column, output_schema);
}

namespace {

using CollectedCompactionSchema = std::unordered_map<int32_t, VariantExtendedInfo>;

// Bounded statistics cannot enumerate every old sparse path. Read only the flattened keys,
// not their values or row offsets, so ADD/glob templates also claim paths omitted from stats.
// A rowset schema written without pattern_type (field 26) decodes as MATCH_NAME_GLOB. Without
// glob metacharacters both pattern types match exactly the same paths, so the entry is unchanged
// and must not trigger the ADD-only sparse key scan. The metacharacters are those of BE matching
// (fnmatch with FNM_PATHNAME in generate_sub_column_info): FE index binding uses Java
// PathMatcher("glob:"), which also treats {a,b} and ** specially, but BE matches '{' literally
// under either pattern type, so this BE-side equivalence holds (H2).
bool same_template_entry(const TabletColumn& source, const TabletColumn& target) {
    if (source.name() != target.name()) {
        return false;
    }
    return source.pattern_type() == target.pattern_type() ||
           target.name().find_first_of("*?[\\") == std::string::npos;
}

Status collect_sparse_keys(const segment_v2::SegmentSharedPtr& segment,
                           const TabletSchemaSPtr& schema, const TabletSchema& target,
                           int32_t parent_uid, std::unordered_set<std::string>& sparse_paths) {
    std::vector<std::pair<std::string, segment_v2::ColumnMetaPB>> sparse_metas;
    auto collect_sparse_meta = [&](const auto& meta) {
        if (!meta.has_column_path_info() ||
            meta.column_path_info().parrent_column_unique_id() != parent_uid) {
            return;
        }
        PathInData path;
        path.from_protobuf(meta.column_path_info());
        const auto relative_path = path.copy_pop_front().get_path();
        if (relative_path == SPARSE_COLUMN_PATH ||
            relative_path.starts_with(SPARSE_COLUMN_PATH + ".b")) {
            sparse_metas.emplace_back(path.get_path(), meta);
        }
    };
    RETURN_IF_ERROR(segment->traverse_column_meta_pbs([&](const auto& meta) {
        collect_sparse_meta(meta);
        // Like VariantColumnReader::init, inspect root children as well as top-level metas.
        // External column-meta traversal returns the root with its sparse maps embedded in it.
        // This mirrors the reader's traversal and the writer's sparse map layout (children(0) is
        // keys, SPARSE_COLUMN_PATH.b<N> shards); keep them in sync (H6).
        if (meta.unique_id() == parent_uid) {
            for (const auto& child : meta.children_columns()) {
                collect_sparse_meta(child);
            }
        }
    }));
    for (const auto& [sparse_path, sparse_meta] : sparse_metas) {
        // These are persisted bytes, not writer invariants: malformed maps must fail compaction
        // with a diagnostic rather than aborting the BE or indexing a missing keys child.
        if (sparse_meta.type() != static_cast<int>(FieldType::OLAP_FIELD_TYPE_MAP) ||
            sparse_meta.children_columns_size() < 3) {
            return Status::Corruption(
                    "Invalid sparse map metadata in segment {} uid {} path {}: "
                    "type {} children {}",
                    segment->id(), parent_uid, sparse_path, sparse_meta.type(),
                    sparse_meta.children_columns_size());
        }
        const auto& meta = sparse_meta.children_columns(0);
        // The scan decodes flattened keys into ColumnString. A nullable or non-string keys
        // child cannot use that destination, even if its corrupt map wrapper looks valid.
        const auto key_type = static_cast<FieldType>(meta.type());
        if (meta.is_nullable() || (key_type != FieldType::OLAP_FIELD_TYPE_STRING &&
                                   key_type != FieldType::OLAP_FIELD_TYPE_VARCHAR &&
                                   key_type != FieldType::OLAP_FIELD_TYPE_CHAR)) {
            return Status::Corruption(
                    "Invalid sparse keys metadata in segment {} uid {} path {}: "
                    "type {} nullable {}",
                    segment->id(), parent_uid, sparse_path, meta.type(), meta.is_nullable());
        }
        if (meta.num_rows() == 0) {
            continue; // An empty sparse shard has no path names to discover.
        }
        segment_v2::ColumnReaderOptions reader_options;
        reader_options.tablet_schema = schema;
        std::shared_ptr<segment_v2::ColumnReader> reader;
        RETURN_IF_ERROR(segment_v2::ColumnReader::create(reader_options, meta, meta.num_rows(),
                                                         segment->file_reader(), &reader));
        segment_v2::ColumnIteratorUPtr iterator;
        RETURN_IF_ERROR(reader->new_iterator(&iterator, nullptr));
        OlapReaderStatistics stats;
        segment_v2::ColumnIteratorOptions iterator_options;
        iterator_options.file_reader = segment->file_reader().get();
        iterator_options.stats = &stats;
        RETURN_IF_ERROR(iterator->init(iterator_options));
        RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
        uint64_t remaining = meta.num_rows();
        while (remaining > 0) {
            MutableColumnPtr keys = ColumnString::create();
            size_t count = std::min<uint64_t>(remaining, 4096);
            RETURN_IF_ERROR(iterator->next_batch(&count, keys));
            // A truncated keys stream must not spin forever against its persisted row count.
            if (count == 0) {
                return Status::Corruption(
                        "Incomplete sparse keys in segment {} uid {} path {}: {} keys remaining",
                        segment->id(), parent_uid, sparse_path, remaining);
            }
            for (size_t i = 0; i < count; ++i) {
                auto path = keys->get_data_at(i).to_string();
                if (sparse_paths.contains(path)) {
                    continue;
                }
                // Retain only newly discovered template matches. An unrelated sparse tail may
                // contain millions of names; it keeps its existing routing and bounded stats.
                TabletSchema::SubColumnInfo info;
                if (generate_sub_column_info(target, parent_uid, path, &info)) {
                    sparse_paths.emplace(std::move(path));
                }
            }
            remaining -= count;
        }
    }
    return Status::OK();
}

Result<CollectedCompactionSchema> collect_compaction_schema(
        const std::vector<RowsetSharedPtr>& rowsets, const TabletSchema& target,
        std::unordered_set<int32_t>* ng_root_uids) {
    CollectedCompactionSchema collected;
    const bool needs_variant_extended_info =
            std::ranges::any_of(target.columns(), [](const TabletColumnPtr& column) {
                return column->is_variant_type() && (should_check_variant_path_stats(*column) ||
                                                     column->variant_enable_nested_group());
            });
    if (needs_variant_extended_info) {
        for (const auto& rs : rowsets) {
            RETURN_IF_ERROR_RESULT(
                    VariantCompactionUtil::aggregate_variant_extended_info(rs, &collected));
        }
    }
    *ng_root_uids = collect_nested_group_compaction_root_uids(target, collected);
    for (const auto& column : target.columns()) {
        // Only ordinary template reconciliation needs complete sparse membership. Preserve the
        // existing NG (flag or old-rowset evidence), DOC, typed-to-sparse and non-template
        // collection/dispatch unchanged.
        if (!is_ordinary_template_variant(*column) || ng_root_uids->contains(column->unique_id())) {
            continue;
        }
        for (const auto& rs : rowsets) {
            const auto& source_schema = rs->tablet_schema();
            if (!source_schema->has_column_unique_id(column->unique_id())) {
                continue; // A newly added root has no historical sparse values.
            }
            const auto& source_column = source_schema->column_by_uid(column->unique_id());
            const auto& source_templates = source_column.get_sub_columns();
            const bool has_added_template = std::ranges::any_of(
                    column->get_sub_columns(), [&](const auto& target_template) {
                        return std::ranges::none_of(
                                source_templates, [&](const auto& source_template) {
                                    return same_template_entry(*source_template, *target_template);
                                });
                    });
            // Ordinary writes materialize every template match (select_variant_paths). Only ADD
            // can match an old sparse path; unchanged templates need no keys-page I/O.
            if (!has_added_template) {
                continue;
            }
            DBUG_EXECUTE_IF("variant_util.collect_compaction_schema.added_template_key_scan", {
                return ResultError(Status::InternalError("injected added-template key scan"));
            });
            SegmentCacheHandle segments;
            RETURN_IF_ERROR_RESULT(SegmentLoader::instance()->load_segments(
                    std::static_pointer_cast<BetaRowset>(rs), &segments));
            for (const auto& segment : segments.get_segments()) {
                OlapReaderStatistics stats;
                StorageReadOptions options(stats);
                std::shared_ptr<segment_v2::VariantColumnReader> reader;
                auto status = segment->get_variant_root_reader(*column, options, &reader);
                if (status.is<ErrorCode::NOT_FOUND>()) {
                    continue; // A schema default has no physical sparse keys.
                }
                RETURN_IF_ERROR_RESULT(status);
                RETURN_IF_ERROR_RESULT(reader->load_external_meta_once());
                // Empty (disabled/legacy) or saturated stats do not enumerate sparse paths.
                // An old p="001" omitted from stats must still claim one carrier after ADD.
                if (reader->get_stats()->sparse_column_non_null_size.empty() ||
                    reader->is_exceeded_sparse_column_limit()) {
                    RETURN_IF_ERROR_RESULT(
                            collect_sparse_keys(segment, source_schema, target, column->unique_id(),
                                                collected[column->unique_id()].sparse_paths));
                }
            }
        }
    }
    return collected;
}

enum class CompactionColumnLayout { BASE, NESTED_GROUP, DOC, SUBPATHS, DATA_TYPES };

struct ReconciledCompactionColumn {
    TabletColumnPtr column;
    const VariantExtendedInfo* input = nullptr;
    CompactionColumnLayout layout = CompactionColumnLayout::BASE;
    std::vector<std::pair<std::string, TabletSchema::SubColumnInfo>> typed_columns;
    std::vector<std::pair<std::string, TabletSchema::SubColumnInfo>> nested_columns;
    PathToDataTypes regular_ng_types;
    PathSet template_paths;
    PathSet carrier_paths;
};

Status reconcile_template_columns(const TabletSchema& target,
                                  ReconciledCompactionColumn& reconciled) {
    // Paths with old dynamic or sparse cells. Typed evidence needs no per-path record: D9 never
    // changes an existing template entry, so typed cells already have the target template type.
    std::unordered_set<std::string> dynamic_paths;
    const auto& input = *reconciled.input;
    for (const auto& [path, types] : input.path_to_data_types) {
        if (path.empty()) {
            continue; // The root is not an extracted path (an empty JSON key has a path part).
        }
        if (!path.get_is_typed()) {
            dynamic_paths.insert(path.get_path());
        }
    }
    dynamic_paths.insert(input.sparse_paths.begin(), input.sparse_paths.end());
    auto reconcile_path = [&](const std::string& path, bool input_typed) -> Status {
        TabletSchema::SubColumnInfo info;
        if (!generate_sub_column_info(target, reconciled.column->unique_id(), path, &info)) {
            if (input_typed) {
                // Removal is not supported: retain the historical typed-path error.
                return Status::InternalError("Failed to generate sub column info for path {}",
                                             path);
            }
            return Status::OK(); // An ordinary dynamic path remains subject to the budget.
        }
        reconciled.template_paths.insert(path);
        if (dynamic_paths.contains(path)) {
            // ADD over old dynamic/sparse cells uses one pinned VARIANT column, including when
            // new rows use the template. sub_path_set excludes this owner from sparse routing.
            reconciled.carrier_paths.insert(path);
        } else {
            // Master behavior: a typed owner follows the target template. D9 forbids changing an
            // existing entry, so a stored-type/template-type mismatch is unreachable via ALTER; do
            // not add a new failure mode for never-altered tables if one ever appears.
            inherit_column_attributes(*reconciled.column, info.column);
            reconciled.typed_columns.emplace_back(path, std::move(info));
        }
        return Status::OK();
    };
    // Preserve the existing order of homogeneous typed columns. New carriers are emitted in the
    // dynamic builder's existing ascending path order, once, alongside other dynamic columns.
    for (const auto& path : input.typed_paths) {
        RETURN_IF_ERROR(reconcile_path(path, true));
    }
    for (const auto& path : dynamic_paths) {
        if (!input.typed_paths.contains(path)) {
            RETURN_IF_ERROR(reconcile_path(path, false));
        }
    }
    return Status::OK();
}

Result<ReconciledCompactionColumn> reconcile_compaction_column(const TabletSchema& target,
                                                               const TabletColumnPtr& column,
                                                               const VariantExtendedInfo& input,
                                                               bool use_nested_group_schema) {
    ReconciledCompactionColumn reconciled;
    reconciled.column = column;
    reconciled.input = &input;
    if (!column->is_variant_type()) {
        return reconciled;
    }
    // NG takes precedence over DOC, including NG evidence in an older rowset.
    if (use_nested_group_schema) {
        reconciled.layout = CompactionColumnLayout::NESTED_GROUP;
    } else if (column->variant_enable_doc_mode()) {
        reconciled.layout = CompactionColumnLayout::DOC;
    } else if (column->variant_max_subcolumns_count() > 0 || !column->get_sub_columns().empty()) {
        reconciled.layout = CompactionColumnLayout::SUBPATHS;
    } else {
        reconciled.layout = CompactionColumnLayout::DATA_TYPES;
    }
    if (reconciled.layout == CompactionColumnLayout::DOC) {
        return reconciled;
    }
    if (!column->variant_enable_typed_paths_to_sparse()) {
        if (use_nested_group_schema || column->get_sub_columns().empty()) {
            // Retain NG/no-template behavior, including the unsupported removed-template error.
            for (const auto& path : input.typed_paths) {
                auto info = reconcile_typed_path(target, *column, path);
                if (!info.has_value()) {
                    return ResultError(info.error());
                }
                reconciled.typed_columns.emplace_back(path, std::move(info).value());
            }
        } else {
            RETURN_IF_ERROR_RESULT(reconcile_template_columns(target, reconciled));
        }
    }
    if (use_nested_group_schema) {
        reconciled.regular_ng_types = collect_regular_types_outside_nested_group(input);
    } else {
        // The output schema shadows these indexes and append_column does not change them.
        const auto& parent_indexes = target.inverted_indexs(column->unique_id());
        for (const auto& path : input.nested_paths) {
            // A template carrier/native column already owns this logical path, including old
            // nested evidence. Emitting another nested column would create a second destination.
            if (reconciled.template_paths.contains(path.get_path())) {
                continue;
            }
            auto info =
                    reconcile_nested_path(path, input.path_to_data_types, *column, parent_indexes);
            if (!info.has_value()) {
                return ResultError(info.error());
            }
            reconciled.nested_columns.emplace_back(path.get_path(), std::move(info).value());
        }
    }
    // Dynamic paths retain their type evidence and the existing lazy resolution rule: e.g. a
    // sparse candidate selected by stats becomes VARIANT, without first merging unused types.
    return reconciled;
}

void select_compaction_budget(const ReconciledCompactionColumn& reconciled,
                              VariantCompactionPathsMap& paths) {
    if (reconciled.layout == CompactionColumnLayout::BASE) {
        return;
    }
    auto& column_paths = paths[reconciled.column->unique_id()];
    if (reconciled.layout == CompactionColumnLayout::SUBPATHS ||
        reconciled.layout == CompactionColumnLayout::DATA_TYPES) {
        const auto& input_stats = reconciled.input->path_to_none_null_values;
        if (reconciled.template_paths.empty()) {
            VariantCompactionUtil::get_subpaths(reconciled.column->variant_max_subcolumns_count(),
                                                input_stats, column_paths);
        } else {
            PathToNoneNullValues dynamic_stats;
            for (const auto& [path, count] : input_stats) {
                if (!reconciled.template_paths.contains(path)) {
                    dynamic_stats.emplace(path, count);
                }
            }
            // A carrier is template-owned just like a typed column: it consumes no budget slot.
            // E.g. count=1 plus carrier p still selects the hottest ordinary path z.
            VariantCompactionUtil::get_subpaths(reconciled.column->variant_max_subcolumns_count(),
                                                dynamic_stats, column_paths);
            column_paths.sub_path_set.insert(reconciled.carrier_paths.begin(),
                                             reconciled.carrier_paths.end());
            column_paths.carrier_path_set = reconciled.carrier_paths;
        }
    }
}

Status build_compaction_columns(ReconciledCompactionColumn& reconciled,
                                const TabletSchemaSPtr& target, TabletSchemaSPtr& output_schema,
                                VariantCompactionPathsMap& paths) {
    const auto& column = reconciled.column;
    if (!column->is_extracted_column()) {
        output_schema->append_column(*column);
    }
    if (reconciled.layout == CompactionColumnLayout::BASE) {
        return Status::OK();
    }
    auto& column_paths = paths[column->unique_id()];
    for (auto& [path, info] : reconciled.typed_columns) {
        build_typed_column(path, std::move(info), output_schema, column_paths);
    }
    if (reconciled.layout == CompactionColumnLayout::NESTED_GROUP) {
        // The NG writer owns the tree; only ordinary paths beside it become extracted columns.
        VariantCompactionUtil::get_compaction_subcolumns_from_data_types(
                column_paths, column, target, reconciled.regular_ng_types, output_schema);
        LOG(INFO) << "Variant column uid=" << column->unique_id()
                  << " keeps nested-group root and materializes regular non-NG subcolumns in "
                     "compaction schema";
        return Status::OK();
    }
    if (reconciled.layout == CompactionColumnLayout::DOC) {
        const int bucket_num = std::max(1, column->variant_doc_hash_shard_count());
        for (int b = 0; b < bucket_num; ++b) {
            output_schema->append_column(create_doc_value_compaction_column(*column, b));
        }
        return Status::OK();
    }
    for (auto& [path, info] : reconciled.nested_columns) {
        build_nested_column(path, std::move(info), output_schema, column_paths);
    }
    const auto& input = *reconciled.input;
    if (reconciled.layout == CompactionColumnLayout::SUBPATHS) {
        VariantCompactionUtil::get_compaction_subcolumns_from_subpaths(
                column_paths, column, target, input.path_to_data_types, input.sparse_paths,
                output_schema, reconciled.carrier_paths);
    } else {
        VariantCompactionUtil::get_compaction_subcolumns_from_data_types(
                column_paths, column, target, input.path_to_data_types, output_schema);
    }
    // Only ADD carriers need the one-destination check; never-altered typed paths keep master
    // routing. A violation fails this compaction instead of aborting the BE on persisted data.
    for (const auto& path : reconciled.carrier_paths) {
        if (column_paths.sparse_path_set.contains(path) ||
            column_paths.typed_path_set.contains(path) ==
                    column_paths.sub_path_set.contains(path)) {
            return Status::InternalError(
                    "Variant column uid={} carrier path {} must have exactly one compaction "
                    "destination",
                    column->unique_id(), path);
        }
    }
    const int bucket_num = std::max(1, column->variant_sparse_hash_shard_count());
    if (bucket_num > 1) {
        for (int b = 0; b < bucket_num; ++b) {
            output_schema->append_column(create_sparse_shard_column(*column, b));
        }
    } else {
        output_schema->append_column(create_sparse_column(*column));
    }
    return Status::OK();
}

} // namespace

Status VariantCompactionUtil::get_extended_compaction_schema(
        const std::vector<RowsetSharedPtr>& rowsets, TabletSchemaSPtr& target,
        VariantCompactionPathsMap& paths) {
    std::unordered_set<int32_t> ng_root_uids;
    auto collected = collect_compaction_schema(rowsets, *target, &ng_root_uids);
    if (!collected.has_value()) {
        return collected.error();
    }
    TabletSchemaSPtr output_schema = std::make_shared<TabletSchema>();
    output_schema->shawdow_copy_without_columns(*target);
    VariantCompactionPathsMap output_paths;
    // Process roots in target order, retaining the first failing root/path as well as column order.
    for (const TabletColumnPtr& column : target->columns()) {
        if (column->is_variant_type()) {
            VLOG_DEBUG << "column " << column->name() << " unique id " << column->unique_id();
        }
        const auto info_it = collected->find(column->unique_id());
        const VariantExtendedInfo empty_input;
        const auto& input = info_it == collected->end() ? empty_input : info_it->second;
        auto reconciled = reconcile_compaction_column(*target, column, input,
                                                      ng_root_uids.contains(column->unique_id()));
        if (!reconciled.has_value()) {
            return reconciled.error();
        }
        select_compaction_budget(reconciled.value(), output_paths);
        RETURN_IF_ERROR(
                build_compaction_columns(reconciled.value(), target, output_schema, output_paths));
    }
    target = output_schema;
    paths = std::move(output_paths);
    VLOG_DEBUG << "dump schema " << target->dump_full_schema();
    return Status::OK();
}

// Full-root shredding reapplies load templates. Use C8's extracted layout for ADD carriers,
// even if separate Variant column groups are disabled.
// Even ADD-only histories need planning on later compactions: publication retains the target
// template but drops extracted slots, so template-list equality does not rule out an old carrier.
// Unaltered templates retain their existing full-root write path and physical layout.
// Provenance split when separate column groups are disabled: a merged input schema at version 0
// was never altered, so it skips planning and keeps master's full-root write. Otherwise an ADD
// carrier may exist; a planning failure fails the compaction instead of re-casting old values
// (D1). Contract (H1): every FE ALTER, light or heavy and including Cloud, bumps schema_version,
// and the merged schema takes the inputs' maximum. A future FE ALTER path that does not bump it
// must update this pre-filter. The safe direction: misjudging "altered" costs one planning pass.
Status VariantCompactionUtil::resolve_write_schema(const std::vector<RowsetSharedPtr>& inputs,
                                                   bool prefer_extracted, TabletSchemaSPtr& schema,
                                                   VariantCompactionPathsSPtr& paths,
                                                   bool* use_extracted) {
    *use_extracted = prefer_extracted;
    const bool has_ordinary_template = std::ranges::any_of(
            schema->columns(),
            [](const auto& column) { return is_ordinary_template_variant(*column); });
    if (!prefer_extracted && (!has_ordinary_template || schema->schema_version() == 0)) {
        return Status::OK();
    }
    // Extracted mode keeps the pre-existing in-place extension; otherwise plan on a copy and
    // adopt it only when an ADD carrier needs to bypass the load template.
    TabletSchemaSPtr resolved = schema;
    if (!prefer_extracted) {
        resolved = std::make_shared<TabletSchema>();
        resolved->copy_from(*schema);
    }
    auto resolved_paths = std::make_shared<VariantCompactionPathsMap>();
    auto status = get_extended_compaction_schema(inputs, resolved, *resolved_paths);
    DBUG_EXECUTE_IF("Compaction.resolve_variant_write_schema.planning_failure", {
        status = Status::InternalError("injected variant carrier planning failure");
    });
    RETURN_IF_ERROR(status);
    // Only C8's carriers need the ADD carrier writer; native typed columns keep the template.
    if (prefer_extracted || std::ranges::any_of(*resolved_paths, [](const auto& entry) {
            return !entry.second.carrier_path_set.empty();
        })) {
        schema = std::move(resolved);
        paths = std::move(resolved_paths);
        // Also retain the existing post-build removal of transient extracted schema columns.
        *use_extracted = true;
    }
    return Status::OK();
}

// Calculate statistics about variant data paths from the encoded sparse column
void VariantCompactionUtil::calculate_variant_stats(const IColumn& encoded_sparse_column,
                                                    segment_v2::VariantStatisticsPB* stats,
                                                    size_t max_sparse_column_statistics_size,
                                                    size_t row_pos, size_t num_rows) {
    // Cast input column to ColumnMap type since sparse column is stored as a map
    const auto& map_column = assert_cast<const ColumnMap&>(encoded_sparse_column);

    // Get the keys column which contains the paths as strings
    const auto& sparse_data_paths =
            assert_cast<const ColumnString*>(map_column.get_keys_ptr().get());
    const auto& serialized_sparse_column_offsets = map_column.get_offsets();
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
            else if (count_map.size() < max_sparse_column_statistics_size) {
                count_map.emplace(sparse_path, 1);
            }
        }
    }

    if (stats->sparse_column_non_null_size().size() > max_sparse_column_statistics_size) {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "Sparse column non null size: {} is greater than max statistics size: {}",
                stats->sparse_column_non_null_size().size(), max_sparse_column_statistics_size);
    }
}

/// Calculates number of dimensions in array field.
/// Returns 0 for scalar fields.
class FieldVisitorToNumberOfDimensions : public StaticVisitor<size_t> {
public:
    FieldVisitorToNumberOfDimensions() = default;
    template <PrimitiveType T>
    size_t apply(const typename PrimitiveTypeTraits<T>::CppType& x) {
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
    size_t apply(const typename PrimitiveTypeTraits<T>::CppType& x) {
        if constexpr (T == TYPE_ARRAY) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Array type is not supported");
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
    size_t apply(const typename PrimitiveTypeTraits<T>::CppType& x) {
        if constexpr (T == TYPE_ARRAY) {
            size_t size = x.size();
            for (size_t i = 0; i < size; ++i) {
                apply_visitor(*this, x[i]);
            }
            return 0;
        } else if constexpr (T == TYPE_NULL) {
            have_nulls = true;
            return 0;
        } else {
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

TabletSchemaSPtr VariantCompactionUtil::calculate_variant_extended_schema(
        const std::vector<RowsetSharedPtr>& rowsets, const TabletSchemaSPtr& base_schema) {
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
                std::shared_ptr<segment_v2::VariantColumnReader> variant_column_reader;
                OlapReaderStatistics stats;
                StorageReadOptions read_options(stats);
                st = segment->get_variant_root_reader(*column, read_options,
                                                      &variant_column_reader);
                if (st.is<ErrorCode::NOT_FOUND>()) {
                    // Old segments predating an ALTER-added VARIANT have no physical subcolumn
                    // metadata, so only newer segments contribute to the calculated schema.
                    continue;
                }
                if (!st.ok()) {
                    LOG(WARNING) << "Failed to get VARIANT root reader for column: "
                                 << column->name() << " error: " << st.to_string();
                    // Extended-schema collection is best-effort per segment. Skip this column but
                    // preserve metadata already collected from the remaining readable segments.
                    continue;
                }
                DORIS_CHECK(variant_column_reader != nullptr);
                // load external meta before getting subcolumn meta info
                st = variant_column_reader->load_external_meta_once();
                if (!st.ok()) {
                    LOG(WARNING) << "Failed to load external meta for column: " << column->name()
                                 << " error: " << st.to_string();
                    continue;
                }
                const auto* subcolumn_meta_info = variant_column_reader->get_subcolumns_meta_info();
                for (const auto& entry : *subcolumn_meta_info) {
                    if (entry->path.empty()) {
                        continue;
                    }
                    const std::string& column_name =
                            column->name_lower_case() + "." + entry->path.get_path();
                    const DataTypePtr& data_type = entry->data.file_column_type;
                    PathInDataBuilder full_path_builder;
                    auto full_path = full_path_builder.append(column->name_lower_case(), false)
                                             .append(entry->path.get_parts(), false)
                                             .build();
                    TabletColumn subcolumn =
                            get_column_by_type(data_type, column_name,
                                               ExtraInfo {.unique_id = -1,
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
        const TabletColumn* nested = column.get_sub_columns()[0].get();
        while (nested != nullptr && nested->is_array_type()) {
            if (nested->get_sub_columns().empty()) {
                return false;
            }
            nested = nested->get_sub_columns()[0].get();
        }
        if (nested == nullptr) {
            return false;
        }
        return inherit_index(parent_indexes, subcolumns_indexes, nested->type(),
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
        const ColumnMetaPB* nested = &column_pb.children_columns(0);
        while (nested != nullptr && nested->type() == (int)FieldType::OLAP_FIELD_TYPE_ARRAY) {
            if (nested->children_columns_size() == 0) {
                return false;
            }
            nested = &nested->children_columns(0);
        }
        if (nested == nullptr) {
            return false;
        }
        return inherit_index(parent_indexes, subcolumns_indexes, (FieldType)nested->type(),
                             column_pb.column_path_info().path(), true);
    }
    return inherit_index(parent_indexes, subcolumns_indexes, (FieldType)column_pb.type(),
                         column_pb.column_path_info().path());
}

} // namespace doris::variant_util
