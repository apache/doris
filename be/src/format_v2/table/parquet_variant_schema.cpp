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

#include "format_v2/table/parquet_variant_schema.h"

#include <algorithm>
#include <ranges>
#include <utility>

#include "common/status.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"
#include "format_v2/column_data.h"
#include "format_v2/column_mapper.h"

namespace doris::format {
namespace {

ColumnDefinition* find_file_column(const ColumnDefinition& table_column,
                                   std::vector<ColumnDefinition>* file_schema,
                                   TableColumnMappingMode mode) {
    DORIS_CHECK(file_schema != nullptr);
    const auto* matched = mode == TableColumnMappingMode::BY_FIELD_ID
                                  ? find_column_by_field_id(table_column, *file_schema, false)
                                  : find_column_by_name(table_column, *file_schema);
    return matched == nullptr ? nullptr : &(*file_schema)[matched - file_schema->data()];
}

void rebuild_complex_type(ColumnDefinition* column) {
    DORIS_CHECK(column != nullptr);
    const bool nullable = column->type->is_nullable();
    const auto primitive = remove_nullable(column->type)->get_primitive_type();
    DataTypePtr rebuilt;
    if (primitive == TYPE_ARRAY && column->children.size() == 1) {
        rebuilt = std::make_shared<DataTypeArray>(column->children[0].type);
    } else if (primitive == TYPE_MAP && column->children.size() == 2) {
        rebuilt = std::make_shared<DataTypeMap>(column->children[0].type, column->children[1].type);
    } else if (primitive == TYPE_STRUCT) {
        DataTypes child_types;
        Strings child_names;
        child_types.reserve(column->children.size());
        child_names.reserve(column->children.size());
        for (const auto& child : column->children) {
            child_types.push_back(child.type);
            child_names.push_back(child.name);
        }
        rebuilt = std::make_shared<DataTypeStruct>(std::move(child_types), std::move(child_names));
    }
    if (rebuilt != nullptr) {
        column->type = nullable ? make_nullable(std::move(rebuilt)) : std::move(rebuilt);
    }
}

Status add_variant_schema_override(const std::vector<int32_t>& path,
                                   std::vector<LocalColumnIndex>* overrides) {
    DORIS_CHECK(!path.empty());
    DORIS_CHECK(overrides != nullptr);
    auto projection = LocalColumnIndex::local(path.back());
    for (size_t path_idx = path.size() - 1; path_idx > 0; --path_idx) {
        auto parent = LocalColumnIndex::partial_local(path[path_idx - 1]);
        parent.children.push_back(std::move(projection));
        projection = std::move(parent);
    }
    const auto existing = std::ranges::find_if(*overrides, [&](const auto& override) {
        return override.local_id() == projection.local_id();
    });
    if (existing == overrides->end()) {
        overrides->push_back(std::move(projection));
    } else {
        RETURN_IF_ERROR(merge_local_column_index(&*existing, projection));
    }
    return Status::OK();
}

bool contains_variant_type(const ColumnDefinition& column) {
    if (column.type != nullptr &&
        remove_nullable(column.type)->get_primitive_type() == TYPE_VARIANT) {
        return true;
    }
    return std::ranges::any_of(column.children, contains_variant_type);
}

Status annotate_matched_variant(const ColumnDefinition& table_column, ColumnDefinition* file_column,
                                TableColumnMappingMode mode, const std::vector<int32_t>& prefix,
                                std::vector<LocalColumnIndex>* overrides) {
    DORIS_CHECK(file_column != nullptr);
    if (!contains_variant_type(table_column) || table_column.type == nullptr ||
        file_column->type == nullptr) {
        return Status::OK();
    }
    auto path = prefix;
    path.push_back(file_column->local_id);
    const auto table_primitive = remove_nullable(table_column.type)->get_primitive_type();
    const auto file_primitive = remove_nullable(file_column->type)->get_primitive_type();
    if (table_primitive == TYPE_VARIANT) {
        if (file_primitive == TYPE_STRUCT) {
            // Older writers omit the Parquet VARIANT annotation. Only a matched table Variant may
            // reinterpret this carrier; ordinary STRUCT<value, metadata> must stay a STRUCT.
            DataTypePtr variant = std::make_shared<DataTypeVariantV2>();
            file_column->type = file_column->type->is_nullable() ? make_nullable(std::move(variant))
                                                                 : std::move(variant);
            RETURN_IF_ERROR(add_variant_schema_override(path, overrides));
        }
        return Status::OK();
    }
    if (table_column.children.empty() || file_column->children.empty() ||
        table_primitive != file_primitive) {
        return Status::OK();
    }
    if (table_primitive == TYPE_ARRAY || table_primitive == TYPE_MAP) {
        const auto child_count =
                std::min(table_column.children.size(), file_column->children.size());
        for (size_t child_idx = 0; child_idx < child_count; ++child_idx) {
            // ARRAY/MAP child names are writer-specific structural labels, so match these nodes by
            // position and reserve name/field-id matching for actual STRUCT members.
            RETURN_IF_ERROR(annotate_matched_variant(table_column.children[child_idx],
                                                     &file_column->children[child_idx], mode, path,
                                                     overrides));
        }
    } else if (table_primitive == TYPE_STRUCT) {
        for (const auto& table_child : table_column.children) {
            auto* file_child = find_file_column(table_child, &file_column->children, mode);
            if (file_child != nullptr) {
                RETURN_IF_ERROR(
                        annotate_matched_variant(table_child, file_child, mode, path, overrides));
            }
        }
    }
    rebuild_complex_type(file_column);
    return Status::OK();
}

} // namespace

Status annotate_parquet_variants(const std::vector<ColumnDefinition>& table_schema,
                                 std::vector<ColumnDefinition>* file_schema,
                                 TableColumnMappingMode mode,
                                 std::vector<LocalColumnIndex>* overrides) {
    DORIS_CHECK(file_schema != nullptr);
    for (const auto& table_column : table_schema) {
        if (!contains_variant_type(table_column)) {
            continue;
        }
        auto* file_column = find_file_column(table_column, file_schema, mode);
        if (file_column != nullptr) {
            RETURN_IF_ERROR(
                    annotate_matched_variant(table_column, file_column, mode, {}, overrides));
        }
    }
    return Status::OK();
}

} // namespace doris::format
