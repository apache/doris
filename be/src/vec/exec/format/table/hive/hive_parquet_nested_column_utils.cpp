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

namespace doris::vectorized {
#include "common/compile_check_begin.h"

void HiveParquetNestedColumnUtils::extract_nested_column_ids(
        const FieldSchema& field_schema, const std::vector<std::vector<std::string>>& paths,
        std::set<uint64_t>& column_ids) {
    // Group paths by first field_id
    std::unordered_map<std::string, std::vector<std::vector<std::string>>>
            child_paths_by_table_col_name;

    for (const auto& path : paths) {
        if (!path.empty()) {
            std::string first_table_col_name = path[0];
            std::vector<std::string> remaining;
            if (path.size() > 1) {
                remaining.assign(path.begin() + 1, path.end());
            }
            child_paths_by_table_col_name[first_table_col_name].push_back(std::move(remaining));
        }
    }

    // Track whether any child column was added to determine if parent should be included
    bool has_child_columns = false;

    // For MAP type, normalize wildcard "*" to explicit KEYS/VALUES access
    // Wildcard in MAP context means accessing both map keys and values
    // Normalization logic:
    //   path: ["map_col", "*"]              → ["map_col", "VALUES"] + ["map_col", "KEYS"]
    //   path: ["map_col", "*", "field"]     → ["map_col", "VALUES", "field"] + ["map_col", "KEYS"]
    if (field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_MAP) {
        auto wildcard_it = child_paths_by_table_col_name.find("*");
        if (wildcard_it != child_paths_by_table_col_name.end()) {
            auto& wildcard_paths = wildcard_it->second;

            // All wildcard paths go to VALUES
            auto& values_paths = child_paths_by_table_col_name["VALUES"];
            values_paths.insert(values_paths.end(), wildcard_paths.begin(), wildcard_paths.end());

            // Always add KEYS for wildcard access
            auto& keys_paths = child_paths_by_table_col_name["KEYS"];
            // Add an empty path to request full KEYS
            std::vector<std::string> empty_path;
            keys_paths.push_back(empty_path);

            // Remove wildcard entry as it's been expanded
            child_paths_by_table_col_name.erase(wildcard_it);
        }
    }

    // Efficiently traverse children
    for (uint64_t i = 0; i < field_schema.children.size(); ++i) {
        const auto& child = field_schema.children[i];
        std::string child_field_name;

        bool is_list = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY;
        bool is_map = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_MAP;

        if (is_list) {
            child_field_name = "*";
        } else if (is_map) {
            // After wildcard normalization above, all MAP accesses are explicit KEYS/VALUES
            // Simply assign the appropriate field name based on which child we're processing
            if (i == 0) {
                child_field_name = "KEYS";
            } else if (i == 1) {
                child_field_name = "VALUES";
            }

            // Special handling for Parquet MAP structure:
            // When accessing only VALUES, we still need KEY structure for levels
            // Check if we're at key child (i==0) and only VALUES is requested (no KEYS)
            if (i == 0) {
                bool has_keys_access = child_paths_by_table_col_name.find("KEYS") !=
                                       child_paths_by_table_col_name.end();
                bool has_values_access = child_paths_by_table_col_name.find("VALUES") !=
                                         child_paths_by_table_col_name.end();

                // If only VALUES is accessed (not KEYS), still include key structure for RL/DL
                if (!has_keys_access && has_values_access) {
                    // For map_values() queries, we need key's structure for correct RL/DL parsing.
                    // If key is a nested type (e.g., STRUCT), RL/DL info is stored at leaf columns.
                    // Add all column IDs from key's start to max (all leaves + intermediate nodes).
                    uint64_t key_start_id = child.get_column_id();
                    uint64_t key_max_id = child.get_max_column_id();
                    for (uint64_t id = key_start_id; id <= key_max_id; ++id) {
                        column_ids.insert(id);
                    }
                    has_child_columns = true;
                    continue; // Skip further processing of key child
                }
            }

        } else {
            child_field_name = child.lower_case_name;
        }

        if (child_field_name.empty()) {
            continue;
        }

        auto child_paths_it = child_paths_by_table_col_name.find(child_field_name);
        if (child_paths_it != child_paths_by_table_col_name.end()) {
            const auto& child_paths = child_paths_it->second;

            // Check if any child path is empty (meaning full child needed)
            bool needs_full_child =
                    std::any_of(child_paths.begin(), child_paths.end(),
                                [](const std::vector<std::string>& path) { return path.empty(); });

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

} // namespace doris::vectorized
