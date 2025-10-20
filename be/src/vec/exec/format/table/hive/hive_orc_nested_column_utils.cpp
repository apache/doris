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

#include "vec/exec/format/table/hive/hive_orc_nested_column_utils.h"

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "orc/Type.hh"
#include "vec/exec/format/table/table_format_reader.h"

namespace doris {
namespace vectorized {

/*static*/ std::set<uint64_t> HiveOrcNestedColumnUtils::extract_schema_and_columns_efficiently(
        const orc::Type* orc_type,
        const std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>&
                paths_by_table_col_name) {
    if (!orc_type) {
        return {};
    }

    // Create root struct node for schema tree
    auto root_struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

    // Output container for column IDs (using set for automatic deduplication)
    std::set<uint64_t> column_ids;

    // Single traversal: process each top-level field in FieldDescriptor
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        const std::string& field_name = orc_type->getFieldName(i);
        const orc::Type* sub_type = orc_type->getSubtype(i);

        // Check if this field is required
        auto paths_it = paths_by_table_col_name.find(to_lower(field_name));
        if (paths_it != paths_by_table_col_name.end()) {
            const auto& paths = paths_it->second;

            // Check if any path is empty (meaning full column needed)
            bool needs_full_column = std::any_of(paths.begin(), paths.end(),
                                                 [](const TColumnNameAccessPath& access_path) {
                                                     return access_path.path.empty();
                                                 });

            // Extract column IDs simultaneously
            if (needs_full_column) {
                // 直接添加从当前列 ID 到 max_column_id 的所有列 ID
                uint64_t start_id = sub_type->getColumnId();
                uint64_t max_column_id = sub_type->getMaximumColumnId();
                for (uint64_t id = start_id; id <= max_column_id; ++id) {
                    column_ids.insert(id);
                }
            } else {
                // Extract nested column IDs using the same path logic
                std::set<uint64_t> path_column_ids;

                _extract_nested_column_ids_efficiently(*sub_type, paths, path_column_ids);

                // If nested extraction found any child columns, ensure parent is also included
                if (!path_column_ids.empty()) {
                    // Add parent column ID first
                    // column_ids.insert(sub_type->getColumnId());

                    // Add all path column IDs
                    column_ids.insert(path_column_ids.begin(), path_column_ids.end());
                } else {
                    // If no valid paths were found, fallback to full column
                    column_ids.insert(sub_type->getColumnId());
                }
            }
        }
    }

    return column_ids;
}

std::set<uint64_t>
HiveOrcNestedColumnUtils::extract_schema_and_columns_efficiently_by_top_level_col_index(
        const orc::Type* orc_type,
        const std::unordered_map<uint64_t, std::vector<TColumnNameAccessPath>>&
                paths_by_table_col_index) {
    if (!orc_type) {
        return {};
    }

    // Create root struct node for schema tree
    auto root_struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

    // Output container for column IDs (using set for automatic deduplication)
    std::set<uint64_t> column_ids;

    // Single traversal: process each top-level field in FieldDescriptor
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        // const std::string& field_name = orc_type->getFieldName(i);
        const orc::Type* sub_type = orc_type->getSubtype(i);

        // Check if this field is required
        auto paths_it = paths_by_table_col_index.find(i);
        if (paths_it != paths_by_table_col_index.end()) {
            const auto& paths = paths_it->second;

            // Check if any path is empty (meaning full column needed)
            bool needs_full_column = std::any_of(paths.begin(), paths.end(),
                                                 [](const TColumnNameAccessPath& access_path) {
                                                     return access_path.path.empty();
                                                 });

            // Extract column IDs simultaneously
            if (needs_full_column) {
                // 直接添加从当前列 ID 到 max_column_id 的所有列 ID
                uint64_t start_id = sub_type->getColumnId();
                uint64_t max_column_id = sub_type->getMaximumColumnId();
                for (uint64_t id = start_id; id <= max_column_id; ++id) {
                    column_ids.insert(id);
                }
            } else {
                // Extract nested column IDs using the same path logic
                std::set<uint64_t> path_column_ids;

                _extract_nested_column_ids_efficiently(*sub_type, paths, path_column_ids);

                // If nested extraction found any child columns, ensure parent is also included
                if (!path_column_ids.empty()) {
                    // Add parent column ID first
                    // column_ids.insert(sub_type->getColumnId());

                    // Add all path column IDs
                    column_ids.insert(path_column_ids.begin(), path_column_ids.end());
                } else {
                    // If no valid paths were found, fallback to full column
                    column_ids.insert(sub_type->getColumnId());
                }
            }
        }
    }

    return column_ids;
}

void HiveOrcNestedColumnUtils::_extract_nested_column_ids_efficiently(
        const orc::Type& type, const std::vector<TColumnNameAccessPath>& paths,
        std::set<uint64_t>& column_ids) {
    // Group paths by first field_id - like create_iceberg_projected_layout's grouping
    std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>
            child_paths_by_table_col_name;

    for (const auto& access_path : paths) {
        if (!access_path.path.empty()) {
            std::string first_table_name = access_path.path[0];
            TColumnNameAccessPath remaining;
            if (access_path.path.size() > 1) {
                remaining.path.assign(access_path.path.begin() + 1, access_path.path.end());
            }
            child_paths_by_table_col_name[first_table_name].push_back(std::move(remaining));
        }
    }

    bool has_keys = false;
    // Efficiently traverse children - similar to create_iceberg_projected_layout's nested column processing
    for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
        const orc::Type* child = type.getSubtype(i);

        // ORC 规范中，只有 STRUCT 的子字段有名字；LIST/MAP 使用约定名
        // - LIST: 子类型索引 0 使用 "element"
        // - MAP: 子类型索引 0/1 分别为 "key"/"value"
        // 对于非 STRUCT 类型，避免调用 getFieldName 以防断言失败
        std::string child_field_name;
        switch (type.getKind()) {
        case orc::TypeKind::STRUCT:
            child_field_name = to_lower(type.getFieldName(i));
            break;
        case orc::TypeKind::LIST:
            // child_field_name = "element";
            child_field_name = "*";
            break;
        case orc::TypeKind::MAP:
            // child_field_name = (i == 0 ? "key" : (i == 1 ? "value" : ""));
            if (i == 0) {
                child_field_name = "KEYS";
                if (child_paths_by_table_col_name.find(child_field_name) !=
                    child_paths_by_table_col_name.end()) {
                    has_keys = true;
                }
            } else if (i == 1) {
                child_field_name = "VALUES";
                if (child_paths_by_table_col_name.find(child_field_name) ==
                    child_paths_by_table_col_name.end()) {
                    if (!has_keys) { // don't have KEYS and VALUES, use * to find paths.
                        child_field_name = "*";
                    }
                }
            }
            break;
        default:
            child_field_name = "";
            break;
        }

        if (child_field_name.empty()) {
            continue;
        }

        // 先尝试精确匹配子字段名；若没有，再尝试通配符 "*"
        const auto exact_it = child_paths_by_table_col_name.find(child_field_name);

        if (exact_it != child_paths_by_table_col_name.end()) {
            // 合并两类来源的路径（若均存在）
            std::vector<TColumnNameAccessPath> child_paths;
            if (exact_it != child_paths_by_table_col_name.end()) {
                const auto& v = exact_it->second;
                child_paths.insert(child_paths.end(), v.begin(), v.end());
            }

            // Check if any child path is empty (meaning full child needed)
            bool needs_full_child = std::any_of(child_paths.begin(), child_paths.end(),
                                                [](const TColumnNameAccessPath& access_path) {
                                                    return access_path.path.empty();
                                                });

            if (needs_full_child) {
                // 直接添加从当前子节点 ID 到 max_column_id 的所有列 ID
                uint64_t start_id = child->getColumnId();
                uint64_t max_column_id = child->getMaximumColumnId();
                for (uint64_t id = start_id; id <= max_column_id; ++id) {
                    column_ids.insert(id);
                }
            } else {
                // Recursively extract from child
                _extract_nested_column_ids_efficiently(*child, child_paths, column_ids);
            }
        }
    }
}

void HiveOrcNestedColumnUtils::extract_nested_column_ids_efficiently(
        const orc::Type& type, const std::vector<TColumnNameAccessPath>& paths,
        std::set<uint64_t>& column_ids) {
    // Group paths by first field_id - like create_iceberg_projected_layout's grouping
    std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>
            child_paths_by_table_col_name;

    for (const auto& access_path : paths) {
        if (!access_path.path.empty()) {
            std::string first_table_name = access_path.path[0];
            TColumnNameAccessPath remaining;
            if (access_path.path.size() > 1) {
                remaining.path.assign(access_path.path.begin() + 1, access_path.path.end());
            }
            child_paths_by_table_col_name[first_table_name].push_back(std::move(remaining));
        }
    }

    // Track whether any child column was added to determine if parent should be included
    bool has_child_columns = false;

    bool only_access_keys = false;
    bool only_access_values = false;
    // Efficiently traverse children - similar to create_iceberg_projected_layout's nested column processing
    for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
        const orc::Type* child = type.getSubtype(i);

        // ORC 规范中，只有 STRUCT 的子字段有名字；LIST/MAP 使用约定名
        // - LIST: 子类型索引 0 使用 "element"
        // - MAP: 子类型索引 0/1 分别为 "key"/"value"
        // 对于非 STRUCT 类型，避免调用 getFieldName 以防断言失败
        std::string child_field_name;
        switch (type.getKind()) {
        case orc::TypeKind::STRUCT:
            child_field_name = to_lower(type.getFieldName(i));
            break;
        case orc::TypeKind::LIST:
            // child_field_name = "element";
            child_field_name = "*";
            break;
        case orc::TypeKind::MAP:
            if (i == 0) {
                DCHECK(type.getSubtypeCount() == 2);
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
                    // 直接添加从当前子节点 ID 到 max_column_id 的所有列 ID
                    uint64_t start_id = child->getColumnId();
                    uint64_t max_column_id = child->getMaximumColumnId();
                    for (uint64_t id = start_id; id <= max_column_id; ++id) {
                        column_ids.insert(id);
                    }
                    has_child_columns = true;
                    continue;
                }
            }
            break;
        default:
            child_field_name = "";
            break;
        }

        if (child_field_name.empty()) {
            continue;
        }

        const auto exact_it = child_paths_by_table_col_name.find(child_field_name);

        if (exact_it != child_paths_by_table_col_name.end()) {
            // 合并两类来源的路径（若均存在）
            std::vector<TColumnNameAccessPath> child_paths;
            if (exact_it != child_paths_by_table_col_name.end()) {
                const auto& v = exact_it->second;
                child_paths.insert(child_paths.end(), v.begin(), v.end());
            }

            // Check if any child path is empty (meaning full child needed)
            bool needs_full_child = std::any_of(child_paths.begin(), child_paths.end(),
                                                [](const TColumnNameAccessPath& access_path) {
                                                    return access_path.path.empty();
                                                });

            if (needs_full_child) {
                // 直接添加从当前子节点 ID 到 max_column_id 的所有列 ID
                uint64_t start_id = child->getColumnId();
                uint64_t max_column_id = child->getMaximumColumnId();
                for (uint64_t id = start_id; id <= max_column_id; ++id) {
                    column_ids.insert(id);
                }
                has_child_columns = true;
            } else {
                // Store current size to check if recursive call added any columns
                size_t before_size = column_ids.size();

                // Recursively extract from child
                extract_nested_column_ids_efficiently(*child, child_paths, column_ids);
                
                // Check if recursive call added any columns
                if (column_ids.size() > before_size) {
                    has_child_columns = true;
                };
            }
        }
    }

    // If any child columns were added, also add the parent column ID
    // This ensures parent struct/container nodes are included when their children are needed
    if (has_child_columns) {
        // Set automatically handles deduplication, so no need to check if it already exists
        column_ids.insert(type.getColumnId());
    }
}

} // namespace vectorized
} // namespace doris