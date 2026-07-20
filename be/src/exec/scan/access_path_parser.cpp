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

#include "exec/scan/access_path_parser.h"

#include <fmt/format.h>

#include <algorithm>
#include <charconv>
#include <map>
#include <string>
#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "runtime/descriptors.h"
#include "util/string_util.h"

namespace doris {
namespace {

bool is_scanner_materialized_virtual_column(const std::string& column_name) {
    return column_name == BeConsts::ICEBERG_ROWID_COL;
}

bool parse_non_negative_int(std::string_view value, int32_t* result) {
    DORIS_CHECK(result != nullptr);
    int32_t parsed = -1;
    const auto* begin = value.data();
    const auto* end = begin + value.size();
    const auto [ptr, ec] = std::from_chars(begin, end, parsed);
    if (ec != std::errc() || ptr != end || parsed < 0) {
        return false;
    }
    *result = parsed;
    return true;
}

std::string access_path_to_string(const std::vector<std::string>& path) {
    return fmt::format("{}", fmt::join(path, "."));
}

format::ColumnDefinition* find_or_add_child(format::ColumnDefinition* parent, int32_t id,
                                            std::string name, DataTypePtr type) {
    DORIS_CHECK(parent != nullptr);
    for (auto& child : parent->children) {
        if ((child.has_identifier_field_id() && child.get_identifier_field_id() == id) ||
            child.name == name) {
            return &child;
        }
    }
    parent->children.push_back({
            .identifier = Field::create_field<TYPE_INT>(id),
            .name = std::move(name),
            .type = std::move(type),
            .children = {},
            .default_expr = nullptr,
            .is_partition_key = false,
    });
    return &parent->children.back();
}

void inherit_schema_metadata(format::ColumnDefinition* column,
                             const format::ColumnDefinition* schema_column) {
    if (column == nullptr || schema_column == nullptr) {
        return;
    }
    column->name_mapping = schema_column->name_mapping;
    // The presence bit is part of the mapping contract: an explicit empty mapping must remain
    // authoritative after access-path pruning instead of enabling current-name fallback.
    column->has_name_mapping = schema_column->has_name_mapping;
    // Initial defaults describe the logical value of fields absent from older files. Nested
    // access-path pruning must retain them just like it retains rename metadata.
    column->initial_default_value = schema_column->initial_default_value;
    column->initial_default_value_is_base64 = schema_column->initial_default_value_is_base64;
}

const format::ColumnDefinition* find_schema_child_by_path(
        const format::ColumnDefinition* schema_column, const std::string& child_path) {
    if (schema_column == nullptr) {
        return nullptr;
    }
    int32_t parsed_field_id = -1;
    if (parse_non_negative_int(child_path, &parsed_field_id)) {
        const auto child_it = std::ranges::find_if(
                schema_column->children, [&](const format::ColumnDefinition& child) {
                    return child.has_identifier_field_id() &&
                           child.get_identifier_field_id() == parsed_field_id;
                });
        return child_it == schema_column->children.end() ? nullptr : &*child_it;
    }
    // Iceberg can reuse a historical name for a newly added sibling. Current names therefore
    // have precedence across the entire struct; an earlier alias must not steal that access path.
    const auto exact_it = std::ranges::find_if(schema_column->children, [&](const auto& child) {
        return to_lower(child.name) == to_lower(child_path);
    });
    if (exact_it != schema_column->children.end()) {
        return &*exact_it;
    }
    const auto alias_it = std::ranges::find_if(schema_column->children, [&](const auto& child) {
        return std::ranges::any_of(child.name_mapping, [&](const std::string& alias) {
            return to_lower(alias) == to_lower(child_path);
        });
    });
    return alias_it == schema_column->children.end() ? nullptr : &*alias_it;
}

int32_t schema_field_id(const format::ColumnDefinition* schema_column) {
    if (schema_column == nullptr || !schema_column->has_identifier_field_id()) {
        return -1;
    }
    return schema_column->get_identifier_field_id();
}

int32_t schema_field_id_or(const format::ColumnDefinition* schema_column, int32_t fallback) {
    const auto field_id = schema_field_id(schema_column);
    return field_id >= 0 ? field_id : fallback;
}

std::string schema_field_name_or(const format::ColumnDefinition* schema_column,
                                 std::string fallback) {
    return schema_column == nullptr || schema_column->name.empty() ? std::move(fallback)
                                                                   : schema_column->name;
}

struct AccessPathNode {
    bool project_all = false;
    std::map<std::string, AccessPathNode> children;
};

void merge_access_path_node(AccessPathNode* dst, const AccessPathNode& src) {
    DORIS_CHECK(dst != nullptr);
    if (dst->project_all) {
        return;
    }
    if (src.project_all) {
        dst->project_all = true;
        dst->children.clear();
        return;
    }
    for (const auto& [path, child] : src.children) {
        merge_access_path_node(&dst->children[path], child);
    }
}

void insert_access_path(AccessPathNode* root, const std::vector<std::string>& path,
                        size_t path_idx) {
    DORIS_CHECK(root != nullptr);
    if (root->project_all) {
        return;
    }
    if (path_idx >= path.size()) {
        root->project_all = true;
        root->children.clear();
        return;
    }
    insert_access_path(&root->children[path[path_idx]], path, path_idx + 1);
}

Status build_nested_children_from_access_node(format::ColumnDefinition* column,
                                              const DataTypePtr& type, const AccessPathNode& node,
                                              const std::string& path,
                                              const format::ColumnDefinition* schema_column);

// Expand a full complex-column projection into table-schema children when the table format provides
// an external/current schema. Without this, `SELECT complex_col` or `SELECT *` leaves
// ColumnDefinition::children empty, so ColumnMapper treats the root complex column as a scalar
// mapping and later tries to cast the old file shape to the current table shape directly.
//
// Examples:
//   - STRUCT country/city projected from an old file STRUCT country/population/location should
//     create children country and city, so city can be materialized as missing/default.
//   - ARRAY<STRUCT<item, quantity>> should create the array element wrapper and then the element
//     struct children item and quantity.
//   - MAP<STRING, STRUCT<full_name, age>> should create semantic children key/value directly, then
//     expand the value struct children full_name and age. Do not introduce a physical entries
//     wrapper here: ColumnMapper and TableReader treat MAP children as [key, value].
Status build_all_nested_children_from_schema(format::ColumnDefinition* column,
                                             const DataTypePtr& type, const std::string& path,
                                             const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);

    const auto nested_type = remove_nullable(type);
    AccessPathNode project_all;
    project_all.project_all = true;
    switch (nested_type->get_primitive_type()) {
    case TYPE_STRUCT: {
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*nested_type);
        for (size_t field_idx = 0; field_idx < struct_type.get_elements().size(); ++field_idx) {
            const auto field_name = struct_type.get_element_name(field_idx);
            const auto* schema_child = find_schema_child_by_path(schema_column, field_name);
            auto* child = find_or_add_child(
                    column, schema_field_id_or(schema_child, cast_set<int32_t>(field_idx)),
                    schema_field_name_or(schema_child, field_name),
                    struct_type.get_element(field_idx));
            inherit_schema_metadata(child, schema_child);
            RETURN_IF_ERROR(build_nested_children_from_access_node(
                    child, child->type, project_all, path + "." + child->name, schema_child));
        }
        return Status::OK();
    }
    case TYPE_ARRAY: {
        const auto& array_type = assert_cast<const DataTypeArray&>(*nested_type);
        const auto* element_schema = schema_column != nullptr && !schema_column->children.empty()
                                             ? &schema_column->children[0]
                                             : nullptr;
        auto* child = find_or_add_child(column, schema_field_id_or(element_schema, 0), "element",
                                        array_type.get_nested_type());
        inherit_schema_metadata(child, element_schema);
        return build_nested_children_from_access_node(child, child->type, project_all, path + ".*",
                                                      element_schema);
    }
    case TYPE_MAP: {
        const auto& map_type = assert_cast<const DataTypeMap&>(*nested_type);
        const auto* key_schema = schema_column != nullptr && !schema_column->children.empty()
                                         ? &schema_column->children[0]
                                         : nullptr;
        const auto* value_schema = schema_column != nullptr && schema_column->children.size() > 1
                                           ? &schema_column->children[1]
                                           : nullptr;
        auto* key_child = find_or_add_child(column, schema_field_id_or(key_schema, 0), "key",
                                            map_type.get_key_type());
        inherit_schema_metadata(key_child, key_schema);
        RETURN_IF_ERROR(build_nested_children_from_access_node(
                key_child, key_child->type, project_all, path + ".KEYS", key_schema));
        auto* value_child = find_or_add_child(column, schema_field_id_or(value_schema, 1), "value",
                                              map_type.get_value_type());
        inherit_schema_metadata(value_child, value_schema);
        RETURN_IF_ERROR(build_nested_children_from_access_node(
                value_child, value_child->type, project_all, path + ".VALUES", value_schema));
        return Status::OK();
    }
    default:
        return Status::OK();
    }
}

Status build_struct_children_from_access_node(format::ColumnDefinition* column,
                                              const DataTypeStruct& struct_type,
                                              const AccessPathNode& node, const std::string& path,
                                              const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);
    for (const auto& [child_path, child_node] : node.children) {
        // Struct children are resolved by name or schema field id. We do not treat a numeric
        // child token as a struct ordinal, because `col.0` becomes ambiguous once the struct
        // evolves. Position-based access needs a separate design if it is required later.
        if (child_path == "OFFSET" || child_path == "*" || child_path == "KEYS" ||
            child_path == "VALUES") {
            return Status::NotSupported(
                    "AccessPathParser does not support access path {} for slot {}",
                    path + "." + child_path, column->name);
        }

        // Prefer the table/schema ColumnDefinition because it carries field ids and aliases.
        // Fallback to the struct type name only for formats without external schema metadata.
        const auto* schema_child = find_schema_child_by_path(schema_column, child_path);
        int32_t field_id = schema_field_id(schema_child);
        std::string field_name = schema_child == nullptr ? child_path : schema_child->name;
        DataTypePtr field_type = schema_child == nullptr ? nullptr : schema_child->type;
        if (field_id < 0 || field_type == nullptr) {
            for (size_t field_idx = 0; field_idx < struct_type.get_elements().size(); ++field_idx) {
                if (to_lower(struct_type.get_element_name(field_idx)) == to_lower(field_name)) {
                    field_id = cast_set<int32_t>(field_idx);
                    field_name = struct_type.get_element_name(field_idx);
                    field_type = struct_type.get_element(field_idx);
                    break;
                }
            }
        }

        if (field_id < 0 || field_type == nullptr) {
            return Status::NotSupported(
                    "AccessPathParser does not support access path {} for slot {}",
                    path + "." + child_path, column->name);
        }
        // TODO: For TVF Parquet files without field ids, this fallback uses the struct ordinal as
        // the table child identifier. BY_NAME mapping should instead keep a string identifier and
        // let TableColumnMapper resolve the file-local child id from the Parquet schema.
        auto* child = find_or_add_child(column, field_id, field_name, field_type);
        inherit_schema_metadata(child, schema_child);
        RETURN_IF_ERROR(build_nested_children_from_access_node(
                child, child->type, child_node, path + "." + child_path, schema_child));
    }
    return Status::OK();
}

Status build_map_children_from_access_node(format::ColumnDefinition* column,
                                           const DataTypeMap& map_type, const AccessPathNode& node,
                                           const std::string& path,
                                           const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);
    AccessPathNode key_node;
    AccessPathNode value_node;
    bool need_key = false;
    bool need_value = false;

    for (const auto& [child_path, child_node] : node.children) {
        if (child_path == "OFFSET") {
            return Status::NotSupported(
                    "AccessPathParser does not support access path {} for slot {}",
                    path + "." + child_path, column->name);
        }
        if (child_path == "KEYS") {
            need_key = true;
            merge_access_path_node(&key_node, child_node);
            continue;
        }
        if (child_path == "VALUES") {
            need_key = true;
            key_node.project_all = true;
            key_node.children.clear();
            need_value = true;
            merge_access_path_node(&value_node, child_node);
            continue;
        }
        if (child_path == "*") {
            need_key = true;
            key_node.project_all = true;
            key_node.children.clear();
            need_value = true;
            merge_access_path_node(&value_node, child_node);
            continue;
        }
        return Status::NotSupported("AccessPathParser does not support access path {} for slot {}",
                                    path + "." + child_path, column->name);
    }
    if (need_key && !need_value) {
        // A key-only MAP projection is not independently materializable yet. FileScannerV2 can
        // describe a projection such as `m.KEYS`, but the downstream file block -> table block path
        // still builds a ColumnMap from key column + value column + offsets. If the value child is
        // omitted here, TableReader/ColumnMapper cannot reconstruct a valid table MAP column even
        // though the query only needs keys.
        //
        // Example:
        //   SELECT map_keys(m) FROM t;
        // or
        //   SELECT * FROM t WHERE array_contains(map_keys(m), 'k1');
        //
        // The access path only asks for `m.KEYS`, but the scan still has to read `m.VALUES` as a
        // temporary full projection until map materialization supports constructing a table MAP
        // from keys only.
        need_value = true;
        value_node.project_all = true;
        value_node.children.clear();
    }

    if (!need_key && !need_value) {
        return Status::OK();
    }

    const auto* key_schema = schema_column != nullptr && !schema_column->children.empty()
                                     ? &schema_column->children[0]
                                     : nullptr;
    const auto* value_schema = schema_column != nullptr && schema_column->children.size() > 1
                                       ? &schema_column->children[1]
                                       : nullptr;
    if (need_key) {
        auto* key_child = find_or_add_child(column, schema_field_id_or(key_schema, 0), "key",
                                            map_type.get_key_type());
        inherit_schema_metadata(key_child, key_schema);
        RETURN_IF_ERROR(build_nested_children_from_access_node(key_child, key_child->type, key_node,
                                                               path + ".KEYS", key_schema));
    }
    if (need_value) {
        auto* value_child = find_or_add_child(column, schema_field_id_or(value_schema, 1), "value",
                                              map_type.get_value_type());
        inherit_schema_metadata(value_child, value_schema);
        RETURN_IF_ERROR(build_nested_children_from_access_node(
                value_child, value_child->type, value_node, path + ".VALUES", value_schema));
    }
    return Status::OK();
}

Status build_nested_children_from_access_node(format::ColumnDefinition* column,
                                              const DataTypePtr& type, const AccessPathNode& node,
                                              const std::string& path,
                                              const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);
    if (node.project_all || node.children.empty()) {
        return build_all_nested_children_from_schema(column, type, path, schema_column);
    }

    const auto nested_type = remove_nullable(type);
    switch (nested_type->get_primitive_type()) {
    case TYPE_STRUCT:
        return build_struct_children_from_access_node(
                column, assert_cast<const DataTypeStruct&>(*nested_type), node, path,
                schema_column);
    case TYPE_ARRAY: {
        if (node.children.size() != 1 || !node.children.contains("*")) {
            return Status::NotSupported(
                    "AccessPathParser does not support access path {} for slot {}", path,
                    column->name);
        }
        const auto& array_type = assert_cast<const DataTypeArray&>(*nested_type);
        const auto* element_schema = schema_column != nullptr && !schema_column->children.empty()
                                             ? &schema_column->children[0]
                                             : nullptr;
        auto* child = find_or_add_child(column, schema_field_id_or(element_schema, 0), "element",
                                        array_type.get_nested_type());
        inherit_schema_metadata(child, element_schema);
        return build_nested_children_from_access_node(child, child->type, node.children.at("*"),
                                                      path + ".*", element_schema);
    }
    case TYPE_MAP:
        return build_map_children_from_access_node(
                column, assert_cast<const DataTypeMap&>(*nested_type), node, path, schema_column);
    default:
        return Status::NotSupported("AccessPathParser does not support access path {} for slot {}",
                                    path, column->name);
    }
}

} // namespace

Status AccessPathParser::build_nested_children(format::ColumnDefinition* column,
                                               const std::vector<TColumnAccessPath>& access_paths,
                                               const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);
    if (is_scanner_materialized_virtual_column(column->name)) {
        return Status::OK();
    }
    if (!is_complex_type(remove_nullable(column->type)->get_primitive_type())) {
        return Status::OK();
    }

    AccessPathNode root;
    // Build tree for AccessPathNode.
    // For example, for access paths ["a.b", "a.c", "d"], the tree will be:
    // root
    // ├── a
    // │   ├── b
    // │   └── c
    // └── d
    for (const auto& access_path : access_paths) {
        // TODO: Support META access paths if needed. Currently AccessPathParser only supports
        // DATA access paths.
        if (access_path.type != TAccessPathType::DATA || !access_path.__isset.data_access_path) {
            return Status::NotSupported(
                    "AccessPathParser only supports DATA access paths for slot {}", column->name);
        }
        const auto& path = access_path.data_access_path.path;
        if (path.empty()) {
            insert_access_path(&root, path, 0);
            continue;
        }
        int32_t top_level_id = -1;
        if (to_lower(path.front()) != to_lower(column->name) &&
            (!parse_non_negative_int(path.front(), &top_level_id) ||
             !column->has_identifier_field_id() ||
             top_level_id != column->get_identifier_field_id())) {
            return Status::NotSupported("AccessPathParser access path {} does not match slot {}",
                                        access_path_to_string(path), column->name);
        }
        insert_access_path(&root, path, 1);
    }
    // Recursively build nested children for the column based on the AccessPathNode tree.
    return build_nested_children_from_access_node(column, column->type, root, column->name,
                                                  schema_column);
}

Status AccessPathParser::build_nested_children(format::ColumnDefinition* column,
                                               const SlotDescriptor* slot_desc,
                                               const format::ColumnDefinition* schema_column) {
    DORIS_CHECK(column != nullptr);
    DORIS_CHECK(slot_desc != nullptr);
    return build_nested_children(column, slot_desc->all_access_paths(), schema_column);
}

} // namespace doris
