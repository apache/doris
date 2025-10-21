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

// // Merged efficient method for both schema node building and column ID extraction
// /*static*/ std::set<uint64_t> HiveParquetNestedColumnUtils::extract_schema_and_columns_efficiently(
//         const FieldDescriptor* field_desc,
//         const std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>&
//                 paths_by_table_col_name) {
//     if (!field_desc) {
//         return {};
//     }

//     // Output container for column IDs (using set for automatic deduplication)
//     std::set<uint64_t> column_ids;

//     // Single traversal: process each top-level field in FieldDescriptor
//     for (int i = 0; i < field_desc->size(); ++i) {
//         auto field_schema = field_desc->get_column(i);
//         if (!field_schema) continue;

//         std::string table_col_name = field_schema->lower_case_name;
//         if (table_col_name.empty()) continue; // Skip fields without iceberg field_id

//         // Check if this field is required
//         auto paths_it = paths_by_table_col_name.find(table_col_name);
//         if (paths_it != paths_by_table_col_name.end()) {
//             const auto& paths = paths_it->second;

//             // Check if any path is empty (meaning full column needed)
//             bool needs_full_column = std::any_of(paths.begin(), paths.end(),
//                                                  [](const TColumnNameAccessPath& access_path) {
//                                                      return access_path.path.empty();
//                                                  });

//             // Extract column IDs simultaneously
//             if (needs_full_column) {
//                 // 直接添加从当前列 ID 到 max_column_id 的所有列 ID
//                 uint64_t start_id = field_schema->get_column_id();
//                 uint64_t max_column_id = field_schema->get_max_column_id();
//                 for (uint64_t id = start_id; id <= max_column_id; ++id) {
//                     column_ids.insert(id);
//                 }
//             } else {
//                 // Extract nested column IDs using the same path logic
//                 std::set<uint64_t> path_column_ids;

//                 _extract_nested_column_ids_efficiently(*field_schema, paths, path_column_ids);

//                 // If nested extraction found any child columns, ensure parent is also included
//                 if (!path_column_ids.empty()) {
//                     // Add parent column ID first
//                     column_ids.insert(field_schema->get_column_id());

//                     // Add all path column IDs
//                     column_ids.insert(path_column_ids.begin(), path_column_ids.end());

//                 } else {
//                     // If no valid paths were found, fallback to full column
//                     column_ids.insert(field_schema->get_column_id());
//                 }
//             }
//         }
//     }

//     return column_ids;
// }

// /*static*/ std::set<uint64_t>
// HiveParquetNestedColumnUtils::extract_schema_and_columns_efficiently_by_top_level_col_index(
//         const FieldDescriptor* field_desc,
//         const std::unordered_map<uint64_t, std::vector<TColumnNameAccessPath>>&
//                 paths_by_table_col_index) {
//     if (!field_desc) {
//         return {};
//     }

//     // Output container for column IDs (using set for automatic deduplication)
//     std::set<uint64_t> column_ids;

//     // Single traversal: process each top-level field in FieldDescriptor
//     for (int i = 0; i < field_desc->size(); ++i) {
//         auto field_schema = field_desc->get_column(i);
//         if (!field_schema) continue;

//         std::string table_col_name = field_schema->lower_case_name;
//         if (table_col_name.empty()) continue; // Skip fields without iceberg field_id

//         // Check if this field is required
//         auto paths_it = paths_by_table_col_index.find(i);
//         if (paths_it != paths_by_table_col_index.end()) {
//             const auto& paths = paths_it->second;

//             // Check if any path is empty (meaning full column needed)
//             bool needs_full_column = std::any_of(paths.begin(), paths.end(),
//                                                  [](const TColumnNameAccessPath& access_path) {
//                                                      return access_path.path.empty();
//                                                  });

//             // Extract column IDs simultaneously
//             if (needs_full_column) {
//                 // 直接添加从当前列 ID 到 max_column_id 的所有列 ID
//                 uint64_t start_id = field_schema->get_column_id();
//                 uint64_t max_column_id = field_schema->get_max_column_id();
//                 for (uint64_t id = start_id; id <= max_column_id; ++id) {
//                     column_ids.insert(id);
//                 }
//             } else {
//                 // Extract nested column IDs using the same path logic
//                 std::set<uint64_t> path_column_ids;

//                 _extract_nested_column_ids_efficiently(*field_schema, paths, path_column_ids);

//                 // If nested extraction found any child columns, ensure parent is also included
//                 if (!path_column_ids.empty()) {
//                     // Add parent column ID first
//                     column_ids.insert(field_schema->get_column_id());

//                     // Add all path column IDs
//                     column_ids.insert(path_column_ids.begin(), path_column_ids.end());

//                 } else {
//                     // If no valid paths were found, fallback to full column
//                     column_ids.insert(field_schema->get_column_id());
//                 }
//             }
//         }
//     }

//     return column_ids;
// }

// void HiveParquetNestedColumnUtils::_extract_nested_column_ids_efficiently(
//         const FieldSchema& field_schema, const std::vector<TColumnNameAccessPath>& paths,
//         std::set<uint64_t>& column_ids) {
//     // Group paths by first field_id - like create_iceberg_projected_layout's grouping
//     std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>
//             child_paths_by_table_col_name;

//     for (const auto& access_path : paths) {
//         if (!access_path.path.empty()) {
//             std::string first_table_col_name = access_path.path[0];
//             TColumnNameAccessPath remaining;
//             if (access_path.path.size() > 1) {
//                 remaining.path.assign(access_path.path.begin() + 1, access_path.path.end());
//             }
//             child_paths_by_table_col_name[first_table_col_name].push_back(std::move(remaining));
//         }
//     }

//     // Track whether any child column was added to determine if parent should be included
//     bool has_child_columns = false;

//     // Efficiently traverse children - similar to create_iceberg_projected_layout's nested column processing
//     bool has_keys = false;
//     for (uint64_t i = 0; i < field_schema.children.size(); ++i) {
//         const auto& child = field_schema.children[i];
//         // 使用 field_schema 来判断当前字段类型，对于 LIST/MAP 的 element 部分使用 "*"
//         std::string child_field_name;

//         bool is_list = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY;
//         bool is_map = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_MAP;
//         // bool is_struct = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_STRUCT;

//         if (is_list) {
//             // 对于 LIST 类型，使用 "*" 作为字段名
//             child_field_name = "*";
//         } else if (is_map) {
//             if (i == 0) {
//                 child_field_name = "KEYS";
//                 if (child_paths_by_table_col_name.find(child_field_name) !=
//                     child_paths_by_table_col_name.end()) {
//                     has_keys = true;
//                 }
//             } else if (i == 1) {
//                 child_field_name = "VALUES";
//                 if (child_paths_by_table_col_name.find(child_field_name) ==
//                     child_paths_by_table_col_name.end()) {
//                     if (!has_keys) { // don't have KEYS and VALUES, use * to find paths.
//                         child_field_name = "*";
//                     }
//                 }
//             }
//         } else {
//             // 对于其他类型（如 STRUCT），使用实际的字段名
//             child_field_name = child.lower_case_name;
//         }

//         if (child_field_name.empty()) {
//             continue;
//         }

//         auto child_paths_it = child_paths_by_table_col_name.find(child_field_name);
//         if (child_paths_it != child_paths_by_table_col_name.end()) {
//             const auto& child_paths = child_paths_it->second;

//             // Check if any child path is empty (meaning full child needed)
//             bool needs_full_child = std::any_of(child_paths.begin(), child_paths.end(),
//                                                 [](const TColumnNameAccessPath& access_path) {
//                                                     return access_path.path.empty();
//                                                 });

//             if (needs_full_child) {
//                 // Add all column IDs from current child node to max_column_id
//                 // This efficiently handles all nested/complex cases in one loop
//                 uint64_t start_id = child.get_column_id();
//                 uint64_t max_column_id = child.get_max_column_id();
//                 for (uint64_t id = start_id; id <= max_column_id; ++id) {
//                     column_ids.insert(id);
//                 }
//                 has_child_columns = true;
//             } else {
//                 // Store current size to check if recursive call added any columns
//                 size_t before_size = column_ids.size();

//                 // Recursively extract from child
//                 _extract_nested_column_ids_efficiently(child, child_paths, column_ids);

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

void HiveParquetNestedColumnUtils::extract_nested_column_ids_efficiently(
        const FieldSchema& field_schema, const std::vector<TColumnNameAccessPath>& paths,
        std::set<uint64_t>& column_ids) {
    // Group paths by first field_id - like create_iceberg_projected_layout's grouping
    std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>
            child_paths_by_table_col_name;

    for (const auto& access_path : paths) {
        if (!access_path.path.empty()) {
            std::string first_table_col_name = access_path.path[0];
            TColumnNameAccessPath remaining;
            if (access_path.path.size() > 1) {
                remaining.path.assign(access_path.path.begin() + 1, access_path.path.end());
            }
            child_paths_by_table_col_name[first_table_col_name].push_back(std::move(remaining));
        }
    }

    // Track whether any child column was added to determine if parent should be included
    bool has_child_columns = false;

    // Efficiently traverse children - similar to create_iceberg_projected_layout's nested column processing
    bool only_access_keys = false;
    bool only_access_values = false;
    for (uint64_t i = 0; i < field_schema.children.size(); ++i) {
        const auto& child = field_schema.children[i];
        // 使用 field_schema 来判断当前字段类型，对于 LIST/MAP 的 element 部分使用 "*"
        std::string child_field_name;

        bool is_list = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY;
        bool is_map = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_MAP;
        // bool is_struct = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_STRUCT;

        if (is_list) {
            // 对于 LIST 类型，使用 "*" 作为字段名
            child_field_name = "*";
        } else if (is_map) {
            if (i == 0) {
                DCHECK(field_schema.children.size() == 2);
                if (child_paths_by_table_col_name.find("KEYS") !=
                        child_paths_by_table_col_name.end()) {
                    only_access_keys = true;
                } else if (child_paths_by_table_col_name.find("VALUES") !=
                        child_paths_by_table_col_name.end()) {
                    only_access_values = true;
                }
            }

            if (i == 0 && only_access_keys) {
                child_field_name = "KEYS";
            } else if (i == 1 && only_access_values) {
                child_field_name = "VALUES";
            }

            if ((!only_access_keys) && (!only_access_values)) {
                child_field_name = "*";
                // map key is primitive type
                if (i == 0 && child_paths_by_table_col_name.find("*") !=
                        child_paths_by_table_col_name.end()) {
                    // Add all column IDs from current child node to max_column_id
                    // This efficiently handles all nested/complex cases in one loop
                    uint64_t start_id = child.get_column_id();
                    uint64_t max_column_id = child.get_max_column_id();
                    for (uint64_t id = start_id; id <= max_column_id; ++id) {
                        column_ids.insert(id);
                    }
                    has_child_columns = true;
                    continue;
                }
            }

        } else {
            // 对于其他类型（如 STRUCT），使用实际的字段名
            child_field_name = child.lower_case_name;
        }

        if (child_field_name.empty()) {
            continue;
        }

        auto child_paths_it = child_paths_by_table_col_name.find(child_field_name);
        if (child_paths_it != child_paths_by_table_col_name.end()) {
            const auto& child_paths = child_paths_it->second;

            // Check if any child path is empty (meaning full child needed)
            bool needs_full_child = std::any_of(child_paths.begin(), child_paths.end(),
                                                [](const TColumnNameAccessPath& access_path) {
                                                    return access_path.path.empty();
                                                });

            if (needs_full_child) {
                // Add all column IDs from current child node to max_column_id
                // This efficiently handles all nested/complex cases in one loop
                uint64_t start_id = child.get_column_id();
                uint64_t max_column_id = child.get_max_column_id();
                for (uint64_t id = start_id; id <= max_column_id; ++id) {
                    column_ids.insert(id);
                }
                has_child_columns = true;
            } else {
                // Store current size to check if recursive call added any columns
                size_t before_size = column_ids.size();

                // Recursively extract from child
                extract_nested_column_ids_efficiently(child, child_paths, column_ids);

                // Check if recursive call added any columns
                if (column_ids.size() > before_size) {
                    has_child_columns = true;
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

} // namespace vectorized
} // namespace doris