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

#include "vec/exec/format/table/hive/hive_parquet_nested_column_utils.h"

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
/*static*/ ColumnIdResult
HiveParquetNestedColumnUtils::extract_schema_and_columns_efficiently(
        const FieldDescriptor* field_desc,
        const std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>&
                paths_by_table_name) {
    if (!field_desc) {
        return ColumnIdResult({}, {}, nullptr);
    }

    // Create root struct node for schema tree
    auto root_struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

    // Output container for column IDs (using set for automatic deduplication)
    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // Single traversal: process each top-level field in FieldDescriptor
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;

        std::string table_name = field_schema->lower_case_name;
        if (table_name.empty()) continue; // Skip fields without iceberg field_id

        // Check if this field is required
        auto paths_it = paths_by_table_name.find(table_name);
        if (paths_it != paths_by_table_name.end()) {
            const auto& paths = paths_it->second;

            // Check if any path is empty (meaning full column needed)
            bool needs_full_column =
                    std::any_of(paths.begin(), paths.end(),
                                [](const TColumnNameAccessPath& access_path) { return access_path.path.empty(); });

            // Extract column IDs simultaneously
            if (needs_full_column) {
                // Add the root column ID
                column_ids.insert(field_schema->get_column_id());
                
                // Check if any path is a predicate
                bool has_predicate = std::any_of(paths.begin(), paths.end(),
                                                  [](const TColumnNameAccessPath& access_path) { 
                                                      return access_path.is_predicate; 
                                                  });
                if (has_predicate) {
                    filter_column_ids.insert(field_schema->get_column_id());
                }
            } else {
                // Extract nested column IDs using the same path logic
                std::set<uint64_t> path_column_ids;
                std::set<uint64_t> path_filter_column_ids;

                _extract_nested_column_ids_efficiently(*field_schema, paths, path_column_ids,
                                                        path_filter_column_ids);

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

                // If any child is a filter column, parent must also be a filter column
                if (!path_filter_column_ids.empty()) {
                    filter_column_ids.insert(field_schema->get_column_id());
                    filter_column_ids.insert(path_filter_column_ids.begin(), 
                                                path_filter_column_ids.end());
                }
            }
        }
    }


    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids), root_struct_node);
}

/*static*/ ColumnIdResult
HiveParquetNestedColumnUtils::extract_schema_and_columns_efficiently_by_top_level_col_index(
        const FieldDescriptor* field_desc,
        const std::unordered_map<int, std::vector<TColumnNameAccessPath>>&
                paths_by_table_index) {
    if (!field_desc) {
        return ColumnIdResult({}, {}, nullptr);
    }

    // Create root struct node for schema tree
    auto root_struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

    // Output container for column IDs (using set for automatic deduplication)
    std::set<uint64_t> column_ids;
    std::set<uint64_t> filter_column_ids;

    // Single traversal: process each top-level field in FieldDescriptor
    for (int i = 0; i < field_desc->size(); ++i) {
        auto field_schema = field_desc->get_column(i);
        if (!field_schema) continue;

        std::string table_name = field_schema->lower_case_name;
        if (table_name.empty()) continue; // Skip fields without iceberg field_id

        // Check if this field is required
        auto paths_it = paths_by_table_index.find(i);
        if (paths_it != paths_by_table_index.end()) {
            const auto& paths = paths_it->second;

            // Check if any path is empty (meaning full column needed)
            bool needs_full_column =
                    std::any_of(paths.begin(), paths.end(),
                                [](const TColumnNameAccessPath& access_path) { return access_path.path.empty(); });

            // Extract column IDs simultaneously
            if (needs_full_column) {
                // Add the root column ID
                column_ids.insert(field_schema->get_column_id());
                
                // Check if any path is a predicate
                bool has_predicate = std::any_of(paths.begin(), paths.end(),
                                                  [](const TColumnNameAccessPath& access_path) { 
                                                      return access_path.is_predicate; 
                                                  });
                if (has_predicate) {
                    filter_column_ids.insert(field_schema->get_column_id());
                }
            } else {
                // Extract nested column IDs using the same path logic
                std::set<uint64_t> path_column_ids;
                std::set<uint64_t> path_filter_column_ids;

                _extract_nested_column_ids_efficiently(*field_schema, paths, path_column_ids,
                                                        path_filter_column_ids);

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

                // If any child is a filter column, parent must also be a filter column
                if (!path_filter_column_ids.empty()) {
                    filter_column_ids.insert(field_schema->get_column_id());
                    filter_column_ids.insert(path_filter_column_ids.begin(), 
                                                path_filter_column_ids.end());
                }
            }
        }
    }
    
    return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids), root_struct_node);
}

void HiveParquetNestedColumnUtils::_extract_nested_column_ids_efficiently(
        const FieldSchema& field_schema, const std::vector<TColumnNameAccessPath>& paths,
        std::set<uint64_t>& column_ids, std::set<uint64_t>& filter_column_ids) {
    // Group paths by first field_id - like create_iceberg_projected_layout's grouping
    std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>
            child_paths_by_table_name;

    for (const auto& access_path : paths) {
        if (!access_path.path.empty()) {
            std::string first_table_name = access_path.path[0];
            TColumnNameAccessPath remaining;
            if (access_path.path.size() > 1) {
                remaining.path.assign(access_path.path.begin() + 1, access_path.path.end());
            }
            remaining.is_predicate = access_path.is_predicate;
            child_paths_by_table_name[first_table_name].push_back(std::move(remaining));
        }
    }

    // Track whether any child column was added to determine if parent should be included
    bool has_child_columns = false;
    bool has_filter_child_columns = false;

    // Efficiently traverse children - similar to create_iceberg_projected_layout's nested column processing
    for (const auto& child : field_schema.children) {
        // 使用 field_schema 来判断当前字段类型，对于 LIST/MAP 的 element 部分使用 "*"
        std::string child_field_name;

        bool is_list = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY;
        bool is_map = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_MAP;
        // bool is_struct = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_STRUCT;

        if (is_list || is_map) {
            // 对于 LIST 和 MAP 类型，使用 "*" 作为字段名
            child_field_name = "*";
        } else {
            // 对于其他类型（如 STRUCT），使用实际的字段名
            child_field_name = child.lower_case_name;
        }

        if (child_field_name.empty()) {
            continue;
        }

        auto child_paths_it = child_paths_by_table_name.find(child_field_name);
        if (child_paths_it != child_paths_by_table_name.end()) {
            const auto& child_paths = child_paths_it->second;

            // Check if any child path is empty (meaning full child needed)
            bool needs_full_child =
                    std::any_of(child_paths.begin(), child_paths.end(),
                                [](const TColumnNameAccessPath& access_path) { return access_path.path.empty(); });

            if (needs_full_child) {
                // Add this child's column ID
                column_ids.insert(child.get_column_id());
                
                // Check if any child path is a predicate
                bool has_predicate = std::any_of(child_paths.begin(), child_paths.end(),
                                                  [](const TColumnNameAccessPath& access_path) { 
                                                      return access_path.is_predicate; 
                                                  });
                if (has_predicate) {
                    filter_column_ids.insert(child.get_column_id());
                    has_filter_child_columns = true;
                }
                has_child_columns = true;
            } else {
                // Store current size to check if recursive call added any columns
                size_t before_size = column_ids.size();
                size_t before_filter_size = filter_column_ids.size();

                // Recursively extract from child
                _extract_nested_column_ids_efficiently(child, child_paths, column_ids, 
                                                        filter_column_ids);

                // Check if recursive call added any columns
                if (column_ids.size() > before_size) {
                    has_child_columns = true;
                }
                // Check if recursive call added any filter columns
                if (filter_column_ids.size() > before_filter_size) {
                    has_filter_child_columns = true;
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
    
    // If any child filter columns were added, also add the parent column ID to filter_column_ids
    // This ensures parent nodes are marked as filter columns when their children are filter columns
    if (has_filter_child_columns) {
        filter_column_ids.insert(field_schema.get_column_id());
    }
}

// /*static*/ ColumnIdResult
// HiveParquetNestedColumnUtils::_extract_schema_and_columns_efficiently_by_index(
//         const FieldDescriptor* field_desc,
//         const std::unordered_map<int, std::vector<std::vector<int>>>& paths_by_table_index) {
//     if (!field_desc) {
//         return ColumnIdResult({}, {}, nullptr);
//     }

//     // Create root struct node for schema tree
//     auto root_struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

//     // Output container for column IDs (using set for automatic deduplication)
//     std::set<uint64_t> column_ids;
//     std::set<uint64_t> filter_column_ids;

//     // Single traversal: process each top-level field in FieldDescriptor
//     for (int i = 0; i < field_desc->size(); ++i) {
//         auto field_schema = field_desc->get_column(i);
//         if (!field_schema) continue;

//         // std::string table_name = field_schema->lower_case_name;
//         // if (table_name.empty()) continue; // Skip fields without iceberg field_id

//         // Check if this field is required
//         auto paths_it = paths_by_table_index.find(i);
//         if (paths_it != paths_by_table_index.end()) {
//             const auto& paths = paths_it->second;

//             // Check if any path is empty (meaning full column needed)
//             bool needs_full_column =
//                     std::any_of(paths.begin(), paths.end(),
//                                 [](const std::vector<int>& path) { return path.empty(); });

//             // // Build schema node for this field
//             // std::shared_ptr<TableSchemaChangeHelper::Node> field_node =
//             //     _build_table_schema_node_from_field_schema(*field_schema, paths);

//             // // Get table column name
//             // auto table_name_it = field_id_to_table_name.find(field_id);
//             // std::string table_column_name = (table_name_it != field_id_to_table_name.end())
//             //                                ? table_name_it->second : field_schema->name;

//             // // Add to schema tree
//             // if (field_node) {
//             //     root_struct_node->add_children(table_column_name, field_schema->name, field_node);
//             // } else {
//             //     root_struct_node->add_not_exist_children(table_column_name);
//             // }

//             // Extract column IDs simultaneously
//             if (needs_full_column) {
//                 // Add the root column ID
//                 column_ids.insert(field_schema->get_column_id());
//                 // Note: _by_index doesn't support is_predicate tracking
//             } else {
//                 // Extract nested column IDs using the same path logic
//                 std::set<uint64_t> path_column_ids;
//                 std::set<uint64_t> path_filter_column_ids;

//                 _extract_nested_column_ids_efficiently_by_index(*field_schema, paths,
//                                                                 path_column_ids,
//                                                                 path_filter_column_ids);

//                 // If nested extraction found any child columns, ensure parent is also included
//                 if (!path_column_ids.empty()) {
//                     // Add parent column ID first
//                     column_ids.insert(field_schema->get_column_id());

//                     // Add all path column IDs
//                     column_ids.insert(path_column_ids.begin(), path_column_ids.end());
//                     filter_column_ids.insert(path_filter_column_ids.begin(), 
//                                              path_filter_column_ids.end());
//                 } else {
//                     // If no valid paths were found, fallback to full column
//                     column_ids.insert(field_schema->get_column_id());
//                 }
//             }
//         }
//     }

//     // // Add non-existent columns for field IDs not found in schema (schema tree only)
//     // for (const auto& [field_id, table_name] : field_id_to_table_name) {
//     //     bool found_in_schema = false;
//     //     for (int i = 0; i < field_desc->size(); ++i) {
//     //         auto field_schema = field_desc->get_column(i);
//     //         if (field_schema && field_schema->field_id == field_id) {
//     //             found_in_schema = true;
//     //             break;
//     //         }
//     //     }
//     //     if (!found_in_schema) {
//     //         root_struct_node->add_not_exist_children(table_name);
//     //     }
//     // }

//     return ColumnIdResult(std::move(column_ids), std::move(filter_column_ids), root_struct_node);
// }

// void HiveParquetNestedColumnUtils::_extract_nested_column_ids_efficiently_by_index(
//         const FieldSchema& field_schema, const std::vector<std::vector<int>>& paths,
//         std::set<uint64_t>& column_ids, std::set<uint64_t>& filter_column_ids) {
//     // Group paths by first field_id - like create_iceberg_projected_layout's grouping
//     std::unordered_map<int, std::vector<std::vector<int>>> child_paths_by_table_index;

//     for (const auto& path : paths) {
//         if (!path.empty()) {
//             int first_table_index = path[0];
//             std::vector<int> remaining;
//             if (path.size() > 1) {
//                 remaining.assign(path.begin() + 1, path.end());
//             }
//             child_paths_by_table_index[first_table_index].push_back(std::move(remaining));
//         }
//     }

//     // Track whether any child column was added to determine if parent should be included
//     bool has_child_columns = false;

//     // Efficiently traverse children - similar to create_iceberg_projected_layout's nested column processing
//     for (int i = 0; i < field_schema.children.size(); ++i) {
//         auto& child = field_schema.children[i];
//         auto child_paths_it = child_paths_by_table_index.find(i);
//         if (child_paths_it != child_paths_by_table_index.end()) {
//             const auto& child_paths = child_paths_it->second;

//             // Check if any child path is empty (meaning full child needed)
//             bool needs_full_child =
//                     std::any_of(child_paths.begin(), child_paths.end(),
//                                 [](const std::vector<int>& path) { return path.empty(); });

//             if (needs_full_child) {
//                 // Add this child's column ID
//                 column_ids.insert(child.get_column_id());
//                 // Note: _by_index doesn't support is_predicate tracking
//                 has_child_columns = true;
//             } else {
//                 // Store current size to check if recursive call added any columns
//                 size_t before_size = column_ids.size();

//                 // Recursively extract from child
//                 _extract_nested_column_ids_efficiently_by_index(child, child_paths, column_ids,
//                                                                 filter_column_ids);

//                 // Check if recursive call added any columns
//                 if (column_ids.size() > before_size) {
//                     has_child_columns = true;
//                 }
//             }
//         }
//     }

//     // If any child columns were added, also add the parent column ID
//     // This ensures parent struct/container nodes are included when their children are needed
//     if (has_child_columns) {
//         // Set automatically handles deduplication, so no need to check if it already exists
//         column_ids.insert(field_schema.get_column_id());
//     }
// }

} // namespace vectorized
} // namespace doris