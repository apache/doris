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

#include "vec/exec/format/table/iceberg/iceberg_parquet_nested_column_utils.h"

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "vec/exec/format/parquet/schema_desc.h"
#include "vec/exec/format/table/table_format_reader.h"

namespace doris {
namespace vectorized {

// Merged efficient method for both schema node building and column ID extraction
/*static*/ IcebergParquetNestedColumnUtils::SchemaAndColumnResult
IcebergParquetNestedColumnUtils::_extract_schema_and_columns_efficiently(
        const FieldDescriptor* field_desc,
        const std::unordered_map<int, std::vector<std::vector<int>>>& paths_by_field_id,
        const std::unordered_map<int, std::string>& field_id_to_table_name) {
    if (!field_desc) {
        return SchemaAndColumnResult(nullptr, {});
    }

    // Create root struct node for schema tree
    auto root_struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

    // Output container for column IDs (using set for automatic deduplication)
    std::set<uint64_t> column_ids;

    // Single traversal: process each top-level field in FieldDescriptor
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;

        int field_id = field_schema->field_id;
        if (field_id == -1) continue; // Skip fields without iceberg field_id

        // Check if this field is required
        auto paths_it = paths_by_field_id.find(field_id);
        if (paths_it != paths_by_field_id.end()) {
            const auto& paths = paths_it->second;

            // Check if any path is empty (meaning full column needed)
            bool needs_full_column =
                    std::any_of(paths.begin(), paths.end(),
                                [](const std::vector<int>& path) { return path.empty(); });

            // Build schema node for this field
            std::shared_ptr<TableSchemaChangeHelper::Node> field_node =
                    _build_table_schema_node_from_field_schema(*field_schema, paths);

            // Get table column name
            auto table_name_it = field_id_to_table_name.find(field_id);
            std::string table_column_name = (table_name_it != field_id_to_table_name.end())
                                                    ? table_name_it->second
                                                    : field_schema->name;

            // Add to schema tree
            if (field_node) {
                root_struct_node->add_children(table_column_name, field_schema->name, field_node);
            } else {
                root_struct_node->add_not_exist_children(table_column_name);
            }

            // Extract column IDs simultaneously
            if (needs_full_column) {
                // Add the root column ID
                column_ids.insert(field_schema->get_column_id());
            } else {
                // Extract nested column IDs using the same path logic
                std::set<uint64_t> path_column_ids;

                _extract_nested_column_ids_efficiently(*field_schema, paths, path_column_ids);

                // If nested extraction found any child columns, ensure parent is also included
                if (!path_column_ids.empty()) {
                    // Add parent column ID first
                    column_ids.insert(field_schema->get_column_id());

                    // Add all path column IDs
                    column_ids.insert(path_column_ids.begin(), path_column_ids.end());
                } else {
                    // If no valid paths were found, fallback to full column
                    column_ids.insert(field_schema->get_column_id());
                }
            }
        }
    }

    // Add non-existent columns for field IDs not found in schema (schema tree only)
    for (const auto& [field_id, table_name] : field_id_to_table_name) {
        bool found_in_schema = false;
        for (int i = 0; i < field_desc->size(); ++i) {
            auto field_schema = field_desc->get_column(i);
            if (field_schema && field_schema->field_id == field_id) {
                found_in_schema = true;
                break;
            }
        }
        if (!found_in_schema) {
            root_struct_node->add_not_exist_children(table_name);
        }
    }

    return SchemaAndColumnResult(root_struct_node, std::move(column_ids));
}

void IcebergParquetNestedColumnUtils::_extract_nested_column_ids_efficiently(
        const FieldSchema& field_schema, const std::vector<std::vector<int>>& paths,
        std::set<uint64_t>& column_ids) {
    // Group paths by first field_id - like create_iceberg_projected_layout's grouping
    std::unordered_map<int, std::vector<std::vector<int>>> child_paths_by_field_id;

    for (const auto& path : paths) {
        if (!path.empty()) {
            int first_field_id = path[0];
            std::vector<int> remaining;
            if (path.size() > 1) {
                remaining.assign(path.begin() + 1, path.end());
            }
            child_paths_by_field_id[first_field_id].push_back(std::move(remaining));
        }
    }

    // Track whether any child column was added to determine if parent should be included
    bool has_child_columns = false;

    // Efficiently traverse children - similar to create_iceberg_projected_layout's nested column processing
    for (const auto& child : field_schema.children) {
        if (child.field_id != -1) {
            auto child_paths_it = child_paths_by_field_id.find(child.field_id);
            if (child_paths_it != child_paths_by_field_id.end()) {
                const auto& child_paths = child_paths_it->second;

                // Check if any child path is empty (meaning full child needed)
                bool needs_full_child =
                        std::any_of(child_paths.begin(), child_paths.end(),
                                    [](const std::vector<int>& path) { return path.empty(); });

                if (needs_full_child) {
                    // Add this child's column ID
                    column_ids.insert(child.get_column_id());
                    has_child_columns = true;
                } else {
                    // Store current size to check if recursive call added any columns
                    size_t before_size = column_ids.size();

                    // Recursively extract from child
                    _extract_nested_column_ids_efficiently(child, child_paths, column_ids);

                    // Check if recursive call added any columns
                    if (column_ids.size() > before_size) {
                        has_child_columns = true;
                    }
                }
            }
        }
    }

    // If any child columns were added, also add the parent column ID
    // This ensures parent struct/container nodes are included when their children are needed
    if (has_child_columns) {
        // Set automatically handles deduplication, so no need to check if it already exists
        column_ids.insert(field_schema.get_column_id());
    }
}

/*static*/ void IcebergParquetNestedColumnUtils::_build_iceberg_id_mapping(
        const FieldDescriptor* field_desc,
        std::map<int, const FieldSchema*>& iceberg_id_to_field_schema) {
    if (!field_desc) {
        return;
    }

    // Recursively build mapping from iceberg field_id to FieldSchema
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        _build_iceberg_id_mapping_recursive(field_schema, iceberg_id_to_field_schema);
    }
}

/*static*/ void IcebergParquetNestedColumnUtils::_build_iceberg_id_mapping_recursive(
        const FieldSchema* field_schema,
        std::map<int, const FieldSchema*>& iceberg_id_to_field_schema) {
    if (!field_schema) {
        return;
    }

    // Add this field if it has a valid iceberg field_id
    if (field_schema->field_id != -1) {
        iceberg_id_to_field_schema[field_schema->field_id] = field_schema;
    }

    // Recursively process children
    for (const auto& child : field_schema->children) {
        _build_iceberg_id_mapping_recursive(&child, iceberg_id_to_field_schema);
    }
}

/*static*/ std::shared_ptr<TableSchemaChangeHelper::Node>
IcebergParquetNestedColumnUtils::_build_table_schema_node_from_field_schema(
        const FieldSchema& field_schema, const std::vector<std::vector<int>>& field_paths) {
    // If any path is empty, return fully projected node - like create_iceberg_projected_layout
    bool has_empty = std::any_of(field_paths.begin(), field_paths.end(),
                                 [](const std::vector<int>& path) { return path.empty(); });

    if (has_empty) {
        // Build full node based on field type
        return IcebergParquetNestedColumnUtils::_build_full_table_schema_node(field_schema);
    }

    // Build selective node based on paths and field type
    if (field_schema.data_type->get_primitive_type() == TYPE_STRUCT) {
        auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

        // Group paths by the first field_id in each path - like create_iceberg_projected_layout
        std::unordered_map<int, std::vector<std::vector<int>>> child_paths_by_field_id;

        for (const auto& path : field_paths) {
            if (!path.empty()) {
                int first_field_id = path[0];
                std::vector<int> remaining;
                if (path.size() > 1) {
                    remaining.assign(path.begin() + 1, path.end());
                }
                child_paths_by_field_id[first_field_id].push_back(std::move(remaining));
            }
        }

        // Build nodes for required children only
        for (const auto& child : field_schema.children) {
            if (child.field_id != -1) {
                auto child_paths_it = child_paths_by_field_id.find(child.field_id);
                if (child_paths_it != child_paths_by_field_id.end()) {
                    auto child_node = _build_table_schema_node_from_field_schema(
                            child, child_paths_it->second);
                    if (child_node) {
                        struct_node->add_children(child.name, child.name, child_node);
                    } else {
                        struct_node->add_not_exist_children(child.name);
                    }
                }
            }
        }

        return struct_node;
    } else if (field_schema.data_type->get_primitive_type() == TYPE_ARRAY) {
        if (!field_schema.children.empty()) {
            const auto& element_schema = field_schema.children[0];
            auto element_node =
                    _build_table_schema_node_from_field_schema(element_schema, field_paths);
            if (element_node) {
                return std::make_shared<TableSchemaChangeHelper::ArrayNode>(element_node);
            }
        }
    } else if (field_schema.data_type->get_primitive_type() == TYPE_MAP) {
        if (field_schema.children.size() >= 2) {
            const auto& key_schema = field_schema.children[0];
            const auto& value_schema = field_schema.children[1];

            std::vector<std::vector<int>> empty_paths; // Keys are usually scalar
            auto key_node =
                    IcebergParquetNestedColumnUtils::_build_full_table_schema_node(key_schema);
            auto value_node = _build_table_schema_node_from_field_schema(value_schema, field_paths);

            if (key_node && value_node) {
                return std::make_shared<TableSchemaChangeHelper::MapNode>(key_node, value_node);
            }
        }
    } else {
        // Scalar types
        return std::make_shared<TableSchemaChangeHelper::ScalarNode>();
    }

    return nullptr;
}

std::shared_ptr<TableSchemaChangeHelper::Node>
IcebergParquetNestedColumnUtils::_build_full_table_schema_node(const FieldSchema& field_schema) {
    if (field_schema.data_type->get_primitive_type() == TYPE_STRUCT) {
        auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

        // Include all children
        for (const auto& child : field_schema.children) {
            auto child_node = IcebergParquetNestedColumnUtils::_build_full_table_schema_node(child);
            if (child_node) {
                struct_node->add_children(child.name, child.name, child_node);
            } else {
                struct_node->add_not_exist_children(child.name);
            }
        }

        return struct_node;
    } else if (field_schema.data_type->get_primitive_type() == TYPE_ARRAY) {
        if (!field_schema.children.empty()) {
            const auto& element_schema = field_schema.children[0];
            auto element_node =
                    IcebergParquetNestedColumnUtils::_build_full_table_schema_node(element_schema);
            if (element_node) {
                return std::make_shared<TableSchemaChangeHelper::ArrayNode>(element_node);
            }
        }
    } else if (field_schema.data_type->get_primitive_type() == TYPE_MAP) {
        if (field_schema.children.size() >= 2) {
            const auto& key_schema = field_schema.children[0];
            const auto& value_schema = field_schema.children[1];

            auto key_node =
                    IcebergParquetNestedColumnUtils::_build_full_table_schema_node(key_schema);
            auto value_node =
                    IcebergParquetNestedColumnUtils::_build_full_table_schema_node(value_schema);

            if (key_node && value_node) {
                return std::make_shared<TableSchemaChangeHelper::MapNode>(key_node, value_node);
            }
        }
    } else {
        // Scalar types
        return std::make_shared<TableSchemaChangeHelper::ScalarNode>();
    }

    return std::make_shared<TableSchemaChangeHelper::ScalarNode>(); // Fallback
}

} // namespace vectorized
} // namespace doris