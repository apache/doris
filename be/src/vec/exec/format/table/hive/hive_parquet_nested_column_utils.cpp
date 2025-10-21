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

void HiveParquetNestedColumnUtils::extract_nested_column_ids(
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
                extract_nested_column_ids(child, child_paths, column_ids);

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