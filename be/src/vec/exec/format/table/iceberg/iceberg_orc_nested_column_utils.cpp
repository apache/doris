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

#include "vec/exec/format/table/iceberg/iceberg_orc_nested_column_utils.h"

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

/*static*/ void IcebergOrcNestedColumnUtils::_build_iceberg_id_mapping(
        const orc::Type* orc_type, std::map<int, const orc::Type*>& iceberg_id_to_orc_type) {
    if (!orc_type) {
        return;
    }
    // Recursively build mapping from iceberg field_id to FieldSchema
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        const orc::Type* child = orc_type->getSubtype(i);
        if (child->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
            int field_id = std::stoi(child->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
            iceberg_id_to_orc_type[field_id] = child;
        }
        _build_iceberg_id_mapping(child, iceberg_id_to_orc_type);
    }
}

/*static*/ void IcebergOrcNestedColumnUtils::_build_iceberg_id_mapping_recursive(
        const orc::Type* orc_type, std::map<int, const orc::Type*>& iceberg_id_to_orc_type) {
    if (!orc_type) {
        return;
    }
    // Add this field if it has a valid iceberg field_id
    if (orc_type->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
        int field_id = std::stoi(orc_type->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
        if (field_id != -1) {
            iceberg_id_to_orc_type[field_id] = orc_type;
        }
    }
    // Recursively process children
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        const orc::Type* sub_type = orc_type->getSubtype(i);
        _build_iceberg_id_mapping_recursive(sub_type, iceberg_id_to_orc_type);
    }
}

/*static*/ IcebergOrcNestedColumnUtils::SchemaAndColumnResult
IcebergOrcNestedColumnUtils::_extract_schema_and_columns_efficiently(
        const orc::Type* orc_type,
        const std::unordered_map<int, std::vector<std::vector<int>>>& paths_by_field_id,
        const std::unordered_map<int, std::string>& field_id_to_table_name) {
    if (!orc_type) {
        return SchemaAndColumnResult(nullptr, {});
    }

    // Create root struct node for schema tree
    auto root_struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

    // Output container for column IDs (using set for automatic deduplication)
    std::set<uint64_t> column_ids;

    // Single traversal: process each top-level field in FieldDescriptor
    for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
        const orc::Type* sub_type = orc_type->getSubtype(i);
        if (!sub_type) continue;

        if (!sub_type->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) continue;
        int field_id = std::stoi(sub_type->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
        if (field_id == -1) continue; // Skip fields without iceberg field_id

        // Get field name from parent
        std::string field_name = orc_type->getFieldName(i);

        // Check if this field is required
        auto paths_it = paths_by_field_id.find(field_id);
        if (paths_it != paths_by_field_id.end()) {
            const auto& paths = paths_it->second;

            // Check if any path is empty (meaning full column needed)
            bool needs_full_column =
                    std::any_of(paths.begin(), paths.end(),
                                [](const std::vector<int>& path) { return path.empty(); });

            // Build schema node for this field
            // std::shared_ptr<TableSchemaChangeHelper::Node> field_node =
            //         _build_table_schema_node_from_type(*sub_type, paths);

            // Get table column name
            auto table_name_it = field_id_to_table_name.find(field_id);
            std::string table_column_name = (table_name_it != field_id_to_table_name.end())
                                                    ? table_name_it->second
                                                    : field_name;

            // // Add to schema tree
            // if (field_node) {
            //     root_struct_node->add_children(table_column_name, field_name, field_node);
            // } else {
            //     root_struct_node->add_not_exist_children(table_column_name);
            // }

            // Extract column IDs simultaneously
            if (needs_full_column) {
                // Add the root column ID
                column_ids.insert(sub_type->getColumnId());
            } else {
                // Extract nested column IDs using the same path logic
                std::set<uint64_t> path_column_ids;

                _extract_nested_column_ids_efficiently(*sub_type, paths, path_column_ids);

                // If nested extraction found any child columns, ensure parent is also included
                if (!path_column_ids.empty()) {
                    // Add parent column ID first
                    column_ids.insert(sub_type->getColumnId());

                    // Add all path column IDs
                    column_ids.insert(path_column_ids.begin(), path_column_ids.end());
                } else {
                    // If no valid paths were found, fallback to full column
                    column_ids.insert(sub_type->getColumnId());
                }
            }
        }
    }

    // Add non-existent columns for field IDs not found in schema (schema tree only)
    for (const auto& [field_id, table_name] : field_id_to_table_name) {
        bool found_in_schema = false;
        for (uint64_t i = 0; i < orc_type->getSubtypeCount(); ++i) {
            const orc::Type* sub_type = orc_type->getSubtype(i);
            if (sub_type->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
                int sub_field_id = std::stoi(sub_type->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
                if (sub_field_id == field_id) {
                    found_in_schema = true;
                    break;
                }
            }
        }
        if (!found_in_schema) {
            root_struct_node->add_not_exist_children(table_name);
        }
    }

    return SchemaAndColumnResult(root_struct_node, std::move(column_ids));
}

void IcebergOrcNestedColumnUtils::_extract_nested_column_ids_efficiently(
        const orc::Type& type, const std::vector<std::vector<int>>& paths,
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
    for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
        const orc::Type* child = type.getSubtype(i);

        // for debug start
        std::string child_field_name;
        switch (type.getKind()) {
        case orc::TypeKind::STRUCT:
            child_field_name = type.getFieldName(i);
            break;
        case orc::TypeKind::LIST:
            // child_field_name = "element";
            child_field_name = "*";
            break;
        case orc::TypeKind::MAP:
            // child_field_name = (i == 0 ? "key" : (i == 1 ? "value" : ""));
            child_field_name = "*";
            break;
        default:
            child_field_name = "";
            break;
        }
        std::cout << "Processing child: " << child_field_name
                  << ", columnId=" << child->getColumnId()
                  << ", kind=" << static_cast<int>(child->getKind()) << std::endl;
        // for debug end

        if (child->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
            int child_field_id = std::stoi(child->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
            if (child_field_id != -1) {
                auto child_paths_it = child_paths_by_field_id.find(child_field_id);
                if (child_paths_it != child_paths_by_field_id.end()) {
                    const auto& child_paths = child_paths_it->second;

                    // Check if any child path is empty (meaning full child needed)
                    bool needs_full_child =
                            std::any_of(child_paths.begin(), child_paths.end(),
                                        [](const std::vector<int>& path) { return path.empty(); });

                    if (needs_full_child) {
                        // Add this child's column ID
                        column_ids.insert(child->getColumnId());
                        has_child_columns = true;
                    } else {
                        // Store current size to check if recursive call added any columns
                        size_t before_size = column_ids.size();

                        // Recursively extract from child
                        _extract_nested_column_ids_efficiently(*child, child_paths, column_ids);

                        // Check if recursive call added any columns
                        if (column_ids.size() > before_size) {
                            has_child_columns = true;
                        }
                    }
                }
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

void IcebergOrcNestedColumnUtils::extract_nested_column_ids_efficiently(
        const orc::Type& type, const std::vector<TColumnNameAccessPath>& paths,
        std::set<uint64_t>& column_ids) {
    // Group paths by first field_id - like create_iceberg_projected_layout's grouping
    std::unordered_map<std::string, std::vector<TColumnNameAccessPath>>
            child_paths_by_field_id;

    for (const auto& access_path : paths) {
        if (!access_path.path.empty()) {
            std::string first_field_id = access_path.path[0];
            TColumnNameAccessPath remaining;
            if (access_path.path.size() > 1) {
                remaining.path.assign(access_path.path.begin() + 1, access_path.path.end());
            }
            child_paths_by_field_id[first_field_id].push_back(std::move(remaining));
        }
    }

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
        std::string child_field_id;
        switch (type.getKind()) {
        case orc::TypeKind::STRUCT:
            if (!child->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
                continue;
            }
            child_field_id = child->getAttributeValue(ICEBERG_ORC_ATTRIBUTE);
            break;
        case orc::TypeKind::LIST:
            // child_field_name = "element";
            child_field_id = "*";
            break;
        case orc::TypeKind::MAP:
            if (i == 0) {
                DCHECK(type.getSubtypeCount() == 2);
                if (child_paths_by_field_id.find("KEYS") !=
                        child_paths_by_field_id.end()) {
                    only_access_keys = true;
                } else if (child_paths_by_field_id.find("VALUES") !=
                        child_paths_by_field_id.end()) {
                    only_access_values = true;
                }
            }

            if (i == 0 && only_access_keys) {
                child_field_id = "KEYS";
            } else if (i == 1 && only_access_values) {
                child_field_id = "VALUES";
            }

            if ((!only_access_keys) && (!only_access_values)) {
                child_field_id = "*";
                // map key is primitive type
                if (i == 0 && child_paths_by_field_id.find("*") !=
                        child_paths_by_field_id.end()) {
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
            child_field_id = "";
            break;
        }

        if (child_field_id.empty() || child_field_id == "-1") {
            continue;
        }

        auto child_paths_it = child_paths_by_field_id.find(child_field_id);
        if (child_paths_it != child_paths_by_field_id.end()) {
            const auto& child_paths = child_paths_it->second;

            // Check if any child path is empty (meaning full child needed)
            bool needs_full_child =
                    std::any_of(child_paths.begin(), child_paths.end(),
                                [](const TColumnNameAccessPath& path) { return path.path.empty(); });

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
                }
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

// /*static*/ std::shared_ptr<TableSchemaChangeHelper::Node>
// IcebergOrcNestedColumnUtils::_build_table_schema_node_from_type(
//         const orc::Type& type, const std::vector<std::vector<int>>& field_paths) {
//     // If any path is empty, return fully projected node - like create_iceberg_projected_layout
//     bool has_empty = std::any_of(field_paths.begin(), field_paths.end(),
//                                  [](const std::vector<int>& path) { return path.empty(); });

//     if (has_empty) {
//         // Build full node based on field type
//         return IcebergOrcNestedColumnUtils::_build_full_table_schema_node(type);
//     }

//     // Build selective node based on paths and field type
//     if (type.getKind() == orc::TypeKind::STRUCT) {
//         auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

//         // Group paths by the first field_id in each path - like create_iceberg_projected_layout
//         std::unordered_map<int, std::vector<std::vector<int>>> child_paths_by_field_id;

//         for (const auto& path : field_paths) {
//             if (!path.empty()) {
//                 int first_field_id = path[0];
//                 std::vector<int> remaining;
//                 if (path.size() > 1) {
//                     remaining.assign(path.begin() + 1, path.end());
//                 }
//                 child_paths_by_field_id[first_field_id].push_back(std::move(remaining));
//             }
//         }

//         // Build nodes for required children only
//         for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
//             const orc::Type* child = type.getSubtype(i);
//             if (child->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
//                 int child_field_id = std::stoi(child->getAttributeValue(ICEBERG_ORC_ATTRIBUTE));
//                 if (child_field_id != -1) {
//                     auto child_paths_it = child_paths_by_field_id.find(child_field_id);
//                     if (child_paths_it != child_paths_by_field_id.end()) {
//                         auto child_node =
//                                 _build_table_schema_node_from_type(*child, child_paths_it->second);
//                         if (child_node) {
//                             std::string field_name = type.getFieldName(i);
//                             struct_node->add_children(field_name, field_name, child_node);
//                         } else {
//                             std::string field_name = type.getFieldName(i);
//                             struct_node->add_not_exist_children(field_name);
//                         }
//                     }
//                 }
//             }
//         }

//         return struct_node;
//     } else if (type.getKind() == orc::TypeKind::LIST) {
//         if (type.getSubtypeCount() > 0) {
//             const orc::Type* element_type = type.getSubtype(0);
//             auto element_node = _build_table_schema_node_from_type(*element_type, field_paths);
//             if (element_node) {
//                 return std::make_shared<TableSchemaChangeHelper::ArrayNode>(element_node);
//             }
//         }
//     } else if (type.getKind() == orc::TypeKind::MAP) {
//         if (type.getSubtypeCount() >= 2) {
//             const orc::Type* key_type = type.getSubtype(0);
//             const orc::Type* value_type = type.getSubtype(1);

//             std::vector<std::vector<int>> empty_paths; // Keys are usually scalar
//             auto key_node = IcebergOrcNestedColumnUtils::_build_full_table_schema_node(*key_type);
//             auto value_node = _build_table_schema_node_from_type(*value_type, field_paths);

//             if (key_node && value_node) {
//                 return std::make_shared<TableSchemaChangeHelper::MapNode>(key_node, value_node);
//             }
//         }
//     } else {
//         // Scalar types
//         return std::make_shared<TableSchemaChangeHelper::ScalarNode>();
//     }

//     return nullptr;
// }

// std::shared_ptr<TableSchemaChangeHelper::Node>
// IcebergOrcNestedColumnUtils::_build_full_table_schema_node(const orc::Type& type) {
//     if (type.getKind() == orc::TypeKind::STRUCT) {
//         auto struct_node = std::make_shared<TableSchemaChangeHelper::StructNode>();

//         // Include all children
//         for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
//             const orc::Type* child = type.getSubtype(i);
//             auto child_node = IcebergOrcNestedColumnUtils::_build_full_table_schema_node(*child);
//             if (child_node) {
//                 std::string field_name = type.getFieldName(i);
//                 struct_node->add_children(field_name, field_name, child_node);
//             } else {
//                 std::string field_name = type.getFieldName(i);
//                 struct_node->add_not_exist_children(field_name);
//             }
//         }

//         return struct_node;
//     } else if (type.getKind() == orc::TypeKind::LIST) {
//         if (type.getSubtypeCount() > 0) {
//             const orc::Type* element_type = type.getSubtype(0);
//             auto element_node =
//                     IcebergOrcNestedColumnUtils::_build_full_table_schema_node(*element_type);
//             if (element_node) {
//                 return std::make_shared<TableSchemaChangeHelper::ArrayNode>(element_node);
//             }
//         }
//     } else if (type.getKind() == orc::TypeKind::MAP) {
//         if (type.getSubtypeCount() >= 2) {
//             const orc::Type* key_type = type.getSubtype(0);
//             const orc::Type* value_type = type.getSubtype(1);

//             auto key_node = IcebergOrcNestedColumnUtils::_build_full_table_schema_node(*key_type);
//             auto value_node =
//                     IcebergOrcNestedColumnUtils::_build_full_table_schema_node(*value_type);

//             if (key_node && value_node) {
//                 return std::make_shared<TableSchemaChangeHelper::MapNode>(key_node, value_node);
//             }
//         }
//     } else {
//         // Scalar types
//         return std::make_shared<TableSchemaChangeHelper::ScalarNode>();
//     }

//     return std::make_shared<TableSchemaChangeHelper::ScalarNode>(); // Fallback
// }

} // namespace vectorized
} // namespace doris