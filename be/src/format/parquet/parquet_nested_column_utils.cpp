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

#include "format/parquet/parquet_nested_column_utils.h"

#include <algorithm>
#include <cctype>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "core/data_type/data_type_nullable.h"
#include "format/parquet/schema_desc.h"

namespace doris {
namespace {

enum class NestedPathMode {
    NAME,
    FIELD_ID,
};

void add_column_id_range(const FieldSchema& field_schema, std::set<uint64_t>& column_ids) {
    const uint64_t start_id = field_schema.get_column_id();
    const uint64_t max_column_id = field_schema.get_max_column_id();
    for (uint64_t id = start_id; id <= max_column_id; ++id) {
        column_ids.insert(id);
    }
}

const FieldSchema* find_child_by_structural_name(const FieldSchema& field_schema,
                                                 std::string_view name) {
    std::string lower_name(name);
    std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    for (const auto& child : field_schema.children) {
        if (child.name == name || child.lower_case_name == lower_name) {
            return &child;
        }
    }
    return nullptr;
}

const FieldSchema* find_child_by_exact_name(const FieldSchema& field_schema,
                                            std::string_view name) {
    for (const auto& child : field_schema.children) {
        if (child.name == name) {
            return &child;
        }
    }
    return nullptr;
}

const FieldSchema* find_variant_typed_child_by_key(const FieldSchema& field_schema,
                                                   std::string_view key) {
    return find_child_by_exact_name(field_schema, key);
}

void add_variant_metadata(const FieldSchema& variant_field, std::set<uint64_t>& column_ids) {
    if (const auto* metadata = find_child_by_structural_name(variant_field, "metadata")) {
        add_column_id_range(*metadata, column_ids);
    }
}

bool is_unannotated_variant_value_field(const FieldSchema& field) {
    // VARIANT residual value is raw binary; annotated strings named value are user fields.
    return field.lower_case_name == "value" && field.physical_type == tparquet::Type::BYTE_ARRAY &&
           !field.parquet_schema.__isset.logicalType &&
           !field.parquet_schema.__isset.converted_type;
}

const FieldSchema* find_variant_value_field(const FieldSchema& field_schema) {
    for (const auto& child : field_schema.children) {
        if (is_unannotated_variant_value_field(child)) {
            return &child;
        }
    }
    return nullptr;
}

void add_variant_value(const FieldSchema& variant_field, std::set<uint64_t>& column_ids) {
    add_variant_metadata(variant_field, column_ids);
    if (const auto* value = find_variant_value_field(variant_field)) {
        add_column_id_range(*value, column_ids);
    }
}

struct VariantColumnIdExtractionResult {
    bool has_child_columns = false;
    bool needs_metadata = false;
};

using VariantPathMap = std::unordered_map<std::string, std::vector<std::vector<std::string>>>;

bool is_shredded_variant_field(const FieldSchema& field_schema) {
    bool has_value = false;
    const FieldSchema* typed_value = nullptr;
    for (const auto& child : field_schema.children) {
        if (child.lower_case_name == "value") {
            if (!is_unannotated_variant_value_field(child)) {
                return false;
            }
            has_value = true;
            continue;
        }
        if (child.lower_case_name == "typed_value") {
            typed_value = &child;
            continue;
        }
        return false;
    }
    if (has_value) {
        return true;
    }
    if (typed_value == nullptr) {
        return false;
    }
    const auto type = remove_nullable(typed_value->data_type);
    return type->get_primitive_type() == TYPE_STRUCT || type->get_primitive_type() == TYPE_ARRAY;
}

bool add_shredded_variant_field_value(const FieldSchema& shredded_field,
                                      std::set<uint64_t>& column_ids) {
    if (const auto* value = find_variant_value_field(shredded_field)) {
        add_column_id_range(*value, column_ids);
        return true;
    }
    return false;
}

bool is_variant_array_subscript(std::string_view path) {
    return !path.empty() &&
           std::all_of(path.begin(), path.end(), [](unsigned char c) { return std::isdigit(c); });
}

bool is_terminal_variant_meta_component(std::string_view path) {
    return path == "NULL" || path == "OFFSET";
}

const std::vector<std::string>& effective_variant_path(const std::vector<std::string>& raw_path,
                                                       std::vector<std::string>& stripped_path) {
    if (!raw_path.empty() && is_terminal_variant_meta_component(raw_path.back())) {
        stripped_path.assign(raw_path.begin(), raw_path.end() - 1);
        return stripped_path;
    }
    return raw_path;
}

bool contains_inherited_metadata_value(const FieldSchema& field_schema) {
    if (is_shredded_variant_field(field_schema) &&
        find_variant_value_field(field_schema) != nullptr) {
        return true;
    }
    return std::any_of(
            field_schema.children.begin(), field_schema.children.end(),
            [](const FieldSchema& child) { return contains_inherited_metadata_value(child); });
}

VariantColumnIdExtractionResult extract_variant_typed_nested_column_ids(
        const FieldSchema& field_schema, const std::vector<std::vector<std::string>>& paths,
        std::set<uint64_t>& column_ids, NestedPathMode mode);

VariantColumnIdExtractionResult extract_typed_value_path(const FieldSchema& typed_value,
                                                         const std::vector<std::string>& path,
                                                         std::set<uint64_t>& column_ids,
                                                         NestedPathMode mode) {
    VariantColumnIdExtractionResult result;
    const auto typed_value_type = remove_nullable(typed_value.data_type);
    if (typed_value_type->get_primitive_type() != TYPE_STRUCT) {
        result = extract_variant_typed_nested_column_ids(typed_value, {path}, column_ids, mode);
    } else if (const auto* typed_child = find_variant_typed_child_by_key(typed_value, path[0])) {
        if (path.size() == 1) {
            add_column_id_range(*typed_child, column_ids);
            result.has_child_columns = true;
            result.needs_metadata = contains_inherited_metadata_value(*typed_child);
        } else {
            std::vector<std::vector<std::string>> child_paths {
                    std::vector<std::string>(path.begin() + 1, path.end())};
            result = extract_variant_typed_nested_column_ids(*typed_child, child_paths, column_ids,
                                                             mode);
        }
    }

    if (result.has_child_columns) {
        column_ids.insert(typed_value.get_column_id());
    }
    return result;
}

void add_variant_typed_path(PrimitiveType field_type, const FieldSchema& field_schema,
                            const std::vector<std::string>& path,
                            VariantColumnIdExtractionResult* result, std::set<uint64_t>& column_ids,
                            VariantPathMap* child_paths) {
    if (path.empty()) {
        add_column_id_range(field_schema, column_ids);
        result->has_child_columns = true;
        result->needs_metadata |= contains_inherited_metadata_value(field_schema);
        return;
    }

    const bool is_list = field_type == PrimitiveType::TYPE_ARRAY;
    const bool is_map = field_type == PrimitiveType::TYPE_MAP;
    std::vector<std::string> remaining;
    std::string child_key;
    if (is_list) {
        child_key = "*";
        if (!is_variant_array_subscript(path[0])) {
            remaining.assign(path.begin(), path.end());
        } else if (path.size() > 1) {
            remaining.assign(path.begin() + 1, path.end());
        }
    } else if (is_map) {
        (*child_paths)["KEYS"].emplace_back();
        child_key = "VALUES";
        if (path.size() > 1) {
            remaining.assign(path.begin() + 1, path.end());
        }
    } else {
        child_key = path[0];
        if (path.size() > 1) {
            remaining.assign(path.begin() + 1, path.end());
        }
    }
    (*child_paths)[child_key].push_back(std::move(remaining));
}

std::string variant_typed_child_key(PrimitiveType field_type, const FieldSchema& field_schema,
                                    uint64_t child_index) {
    if (field_type == PrimitiveType::TYPE_ARRAY) {
        return "*";
    }
    if (field_type == PrimitiveType::TYPE_MAP) {
        if (child_index == 0) {
            return "KEYS";
        }
        return child_index == 1 ? "VALUES" : "";
    }
    return field_schema.children[child_index].name;
}

void append_variant_child_paths(const VariantPathMap& paths_by_name, const std::string& key,
                                std::vector<std::vector<std::string>>& child_paths) {
    auto child_paths_it = paths_by_name.find(key);
    if (child_paths_it != paths_by_name.end()) {
        child_paths.insert(child_paths.end(), child_paths_it->second.begin(),
                           child_paths_it->second.end());
    }
}

std::vector<std::vector<std::string>> collect_variant_typed_child_paths(
        const VariantPathMap& paths_by_name, const std::string& child_key) {
    std::vector<std::vector<std::string>> child_paths;
    append_variant_child_paths(paths_by_name, child_key, child_paths);
    return child_paths;
}

void extract_variant_typed_child_column_ids(
        const FieldSchema& child, const std::vector<std::vector<std::string>>& child_paths,
        std::set<uint64_t>& column_ids, NestedPathMode mode,
        VariantColumnIdExtractionResult* result) {
    const bool needs_full_child =
            std::any_of(child_paths.begin(), child_paths.end(),
                        [](const std::vector<std::string>& path) { return path.empty(); });
    if (needs_full_child) {
        add_column_id_range(child, column_ids);
        result->has_child_columns = true;
        result->needs_metadata |= contains_inherited_metadata_value(child);
        return;
    }

    auto child_result =
            extract_variant_typed_nested_column_ids(child, child_paths, column_ids, mode);
    result->has_child_columns |= child_result.has_child_columns;
    result->needs_metadata |= child_result.needs_metadata;
}

VariantColumnIdExtractionResult extract_shredded_variant_field_ids(
        const FieldSchema& shredded_field, const std::vector<std::vector<std::string>>& paths,
        std::set<uint64_t>& column_ids, NestedPathMode mode) {
    const auto* typed_value = find_child_by_structural_name(shredded_field, "typed_value");
    VariantColumnIdExtractionResult result;

    for (const auto& raw_path : paths) {
        std::vector<std::string> stripped_path;
        const auto& path = effective_variant_path(raw_path, stripped_path);
        if (path.empty()) {
            add_column_id_range(shredded_field, column_ids);
            result.has_child_columns = true;
            result.needs_metadata |= contains_inherited_metadata_value(shredded_field);
            continue;
        }

        VariantColumnIdExtractionResult typed_result;
        if (typed_value != nullptr) {
            typed_result = extract_typed_value_path(*typed_value, path, column_ids, mode);
            result.needs_metadata |= typed_result.needs_metadata;
        }
        const bool has_residual_value =
                add_shredded_variant_field_value(shredded_field, column_ids);
        if (has_residual_value) {
            result.needs_metadata = true;
        }
        if (!typed_result.has_child_columns) {
            result.has_child_columns |= has_residual_value;
            continue;
        }
        result.has_child_columns = true;
    }

    if (result.has_child_columns) {
        column_ids.insert(shredded_field.get_column_id());
    }
    return result;
}

VariantColumnIdExtractionResult extract_variant_nested_column_ids(
        const FieldSchema& variant_field, const std::vector<std::vector<std::string>>& paths,
        std::set<uint64_t>& column_ids, NestedPathMode mode) {
    const auto* typed_value = find_child_by_structural_name(variant_field, "typed_value");
    VariantColumnIdExtractionResult result;

    for (const auto& raw_path : paths) {
        std::vector<std::string> stripped_path;
        const auto& path = effective_variant_path(raw_path, stripped_path);
        if (path.empty()) {
            add_column_id_range(variant_field, column_ids);
            result.has_child_columns = true;
            continue;
        }

        VariantColumnIdExtractionResult typed_result;
        if (typed_value != nullptr) {
            typed_result = extract_typed_value_path(*typed_value, path, column_ids, mode);
            if (typed_result.needs_metadata) {
                add_variant_metadata(variant_field, column_ids);
            }
        }

        if (!typed_result.has_child_columns) {
            add_variant_value(variant_field, column_ids);
        }
        result.has_child_columns = true;
    }

    if (result.has_child_columns) {
        column_ids.insert(variant_field.get_column_id());
    }
    return result;
}

VariantColumnIdExtractionResult extract_variant_typed_nested_column_ids(
        const FieldSchema& field_schema, const std::vector<std::vector<std::string>>& paths,
        std::set<uint64_t>& column_ids, NestedPathMode mode) {
    if (remove_nullable(field_schema.data_type)->get_primitive_type() ==
        PrimitiveType::TYPE_VARIANT) {
        return extract_variant_nested_column_ids(field_schema, paths, column_ids, mode);
    }
    if (is_shredded_variant_field(field_schema)) {
        return extract_shredded_variant_field_ids(field_schema, paths, column_ids, mode);
    }

    VariantColumnIdExtractionResult result;
    VariantPathMap child_paths_by_name;
    const auto field_type = remove_nullable(field_schema.data_type)->get_primitive_type();
    for (const auto& path : paths) {
        add_variant_typed_path(field_type, field_schema, path, &result, column_ids,
                               &child_paths_by_name);
    }

    for (uint64_t i = 0; i < field_schema.children.size(); ++i) {
        const auto& child = field_schema.children[i];
        const std::string child_key = variant_typed_child_key(field_type, field_schema, i);
        auto child_paths = collect_variant_typed_child_paths(child_paths_by_name, child_key);
        if (child_paths.empty()) {
            continue;
        }
        extract_variant_typed_child_column_ids(child, child_paths, column_ids, mode, &result);
    }

    if (result.has_child_columns) {
        column_ids.insert(field_schema.get_column_id());
    }
    return result;
}

void normalize_map_wildcard(
        std::unordered_map<std::string, std::vector<std::vector<std::string>>>& child_paths) {
    auto wildcard_it = child_paths.find("*");
    if (wildcard_it == child_paths.end()) {
        return;
    }

    auto wildcard_paths = std::move(wildcard_it->second);
    child_paths.erase(wildcard_it);
    auto& values_paths = child_paths["VALUES"];
    values_paths.insert(values_paths.end(), wildcard_paths.begin(), wildcard_paths.end());
    child_paths["KEYS"].emplace_back();
}

std::string get_nested_child_key(const FieldSchema& field_schema, uint64_t child_index,
                                 NestedPathMode mode) {
    const auto field_type = remove_nullable(field_schema.data_type)->get_primitive_type();
    if (field_type == PrimitiveType::TYPE_ARRAY) {
        return "*";
    }
    if (field_type == PrimitiveType::TYPE_MAP) {
        if (child_index == 0) {
            return "KEYS";
        }
        return child_index == 1 ? "VALUES" : "";
    }

    const auto& child = field_schema.children[child_index];
    if (mode == NestedPathMode::NAME) {
        return child.lower_case_name;
    }
    return std::to_string(child.field_id);
}

bool should_skip_nested_child_key(std::string_view child_key, NestedPathMode mode) {
    return child_key.empty() || (mode == NestedPathMode::FIELD_ID && child_key == "-1");
}

void extract_nested_column_ids_impl(const FieldSchema& field_schema,
                                    const std::vector<std::vector<std::string>>& paths,
                                    std::set<uint64_t>& column_ids, NestedPathMode mode) {
    const auto field_type = remove_nullable(field_schema.data_type)->get_primitive_type();
    if (field_type == PrimitiveType::TYPE_VARIANT) {
        static_cast<void>(extract_variant_nested_column_ids(field_schema, paths, column_ids, mode));
        return;
    }

    std::unordered_map<std::string, std::vector<std::vector<std::string>>> child_paths_by_key;
    for (const auto& path : paths) {
        if (path.empty()) {
            continue;
        }
        std::vector<std::string> remaining;
        if (path.size() > 1) {
            remaining.assign(path.begin() + 1, path.end());
        }
        child_paths_by_key[path[0]].push_back(std::move(remaining));
    }

    if (field_type == PrimitiveType::TYPE_MAP) {
        normalize_map_wildcard(child_paths_by_key);
    }

    bool has_child_columns = false;
    if (field_type == PrimitiveType::TYPE_ARRAY &&
        child_paths_by_key.find("OFFSET") != child_paths_by_key.end()) {
        has_child_columns = true;
    }
    for (uint64_t i = 0; i < field_schema.children.size(); ++i) {
        const auto& child = field_schema.children[i];
        const std::string child_key = get_nested_child_key(field_schema, i, mode);
        if (should_skip_nested_child_key(child_key, mode)) {
            continue;
        }

        if (field_type == PrimitiveType::TYPE_MAP && i == 0) {
            const bool has_keys_access =
                    child_paths_by_key.find("KEYS") != child_paths_by_key.end();
            const bool has_values_access =
                    child_paths_by_key.find("VALUES") != child_paths_by_key.end();
            const bool has_offset_access =
                    child_paths_by_key.find("OFFSET") != child_paths_by_key.end();
            const bool has_null_access =
                    child_paths_by_key.find("NULL") != child_paths_by_key.end();
            if (!has_keys_access && (has_values_access || has_offset_access || has_null_access)) {
                add_column_id_range(child, column_ids);
                has_child_columns = true;
                continue;
            }
        }

        auto child_paths_it = child_paths_by_key.find(child_key);
        if (child_paths_it == child_paths_by_key.end()) {
            continue;
        }

        const auto& child_paths = child_paths_it->second;
        const bool needs_full_child =
                std::any_of(child_paths.begin(), child_paths.end(),
                            [](const std::vector<std::string>& path) { return path.empty(); });

        if (needs_full_child) {
            add_column_id_range(child, column_ids);
            has_child_columns = true;
            continue;
        }

        const size_t before_size = column_ids.size();
        extract_nested_column_ids_impl(child, child_paths, column_ids, mode);
        if (column_ids.size() > before_size) {
            has_child_columns = true;
        }
    }

    if (has_child_columns) {
        column_ids.insert(field_schema.get_column_id());
    }
}

} // namespace

void ParquetNestedColumnUtils::extract_nested_column_ids_by_name(
        const FieldSchema& field_schema, const std::vector<std::vector<std::string>>& paths,
        std::set<uint64_t>& column_ids) {
    extract_nested_column_ids_impl(field_schema, paths, column_ids, NestedPathMode::NAME);
}

void ParquetNestedColumnUtils::extract_nested_column_ids_by_field_id(
        const FieldSchema& field_schema, const std::vector<std::vector<std::string>>& paths,
        std::set<uint64_t>& column_ids) {
    extract_nested_column_ids_impl(field_schema, paths, column_ids, NestedPathMode::FIELD_ID);
}

} // namespace doris
