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

void IcebergParquetNestedColumnUtils::extract_nested_column_ids(
        const FieldSchema& field_schema, const std::vector<TColumnNameAccessPath>& paths,
        std::set<uint64_t>& column_ids) {
    // Group paths by first field_id - like create_iceberg_projected_layout's grouping
    std::unordered_map<std::string, std::vector<TColumnNameAccessPath>> child_paths_by_field_id;

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

    // Track whether any child column was added to determine if parent should be included
    bool has_child_columns = false;

    // For MAP type, check if we have conflicting access patterns and normalize them
    // Problem scenario: Same MAP column has mixed access patterns in different paths:
    //   - Some paths use "*" (need both keys and values)
    //   - Some paths use "KEYS" or "VALUES" (only need one side)
    // Example:
    //   path1: ["map_col", "*", "nested_field"]     -> needs key AND value
    //   path2: ["map_col", "KEYS"]                  -> only needs keys
    // Solution: Merge all into "*" because wildcard is most permissive and subsumes specific access
    bool has_wildcard = child_paths_by_field_id.find("*") != child_paths_by_field_id.end();
    bool has_keys = child_paths_by_field_id.find("KEYS") != child_paths_by_field_id.end();
    bool has_values = child_paths_by_field_id.find("VALUES") != child_paths_by_field_id.end();

    // If wildcard exists with KEYS or VALUES, merge them into wildcard:
    // - Wildcard "*" requires reading both keys and values
    // - Specific KEYS/VALUES requests are subsumed by wildcard
    // - After merge, only wildcard path remains, ensuring correct processing
    if (field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_MAP && has_wildcard &&
        (has_keys || has_values)) {
        // Merge KEYS paths into wildcard if present
        if (has_keys) {
            auto& wildcard_paths = child_paths_by_field_id["*"];
            auto& keys_paths = child_paths_by_field_id["KEYS"];
            wildcard_paths.insert(wildcard_paths.end(), keys_paths.begin(), keys_paths.end());
            child_paths_by_field_id.erase("KEYS");
            has_keys = false;
        }
        // Merge VALUES paths into wildcard if present
        if (has_values) {
            auto& wildcard_paths = child_paths_by_field_id["*"];
            auto& values_paths = child_paths_by_field_id["VALUES"];
            wildcard_paths.insert(wildcard_paths.end(), values_paths.begin(), values_paths.end());
            child_paths_by_field_id.erase("VALUES");
            has_values = false;
        }
    }

    // Efficiently traverse children - similar to create_iceberg_projected_layout's nested column processing
    bool only_access_keys = false;
    bool only_access_values = false;
    for (uint64_t i = 0; i < field_schema.children.size(); ++i) {
        const auto& child = field_schema.children[i];

        // 使用 field_schema 来判断当前字段类型，对于 LIST/MAP 的 element 部分使用 "*"
        std::string child_field_id;

        bool is_list = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY;
        bool is_map = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_MAP;
        // bool is_struct = field_schema.data_type->get_primitive_type() == PrimitiveType::TYPE_STRUCT;

        if (is_list) {
            // 对于 LIST 类型，使用 "*" 作为字段名
            child_field_id = "*";
        } else if (is_map) {
            if (i == 0) {
                DCHECK(field_schema.children.size() == 2);
                if (child_paths_by_field_id.find("KEYS") != child_paths_by_field_id.end()) {
                    only_access_keys = true;
                } else if (child_paths_by_field_id.find("VALUES") !=
                           child_paths_by_field_id.end()) {
                    only_access_values = true;
                }
            }

            if (i == 0 && only_access_keys) {
                child_field_id = "KEYS";
                std::cout << "[IcebergParquetNestedColumnUtils] MAP KEYS optimization: Processing "
                             "key field "
                          << child.name << " (column_id: " << child.get_column_id() << ")"
                          << std::endl;
            } else if (i == 1 && only_access_values) {
                child_field_id = "VALUES";
                std::cout << "[IcebergParquetNestedColumnUtils] MAP VALUES optimization: "
                             "Processing value field "
                          << child.name << " (column_id: " << child.get_column_id() << ")"
                          << std::endl;
            }

            // Special handling for map_values(): Always include key column_id for structure
            // Even when only_access_values is true, we need key's repetition/definition levels
            // to correctly determine MAP boundaries and null information in Parquet format
            if (i == 0 && only_access_values) {
                // For map_values() queries, we still need to read key structure (levels)
                // Add key's column_id to ensure proper MAP structure parsing
                uint64_t key_start_id = child.get_column_id();
                uint64_t key_max_column_id = child.get_max_column_id();
                std::cout << "[IcebergParquetNestedColumnUtils] MAP VALUES optimization: Adding "
                             "key column_ids "
                          << key_start_id << " to " << key_max_column_id
                          << " for structure information (field: " << child.name << ")"
                          << std::endl;
                for (uint64_t id = key_start_id; id <= key_max_column_id; ++id) {
                    column_ids.insert(id);
                }
                has_child_columns = true;
                // Still set child_field_id for further processing if needed
                child_field_id = ""; // Skip normal processing since we handled it above
            }

            if ((!only_access_keys) && (!only_access_values)) {
                child_field_id = "*";
                // map key is primitive type
                if (i == 0 && child_paths_by_field_id.find("*") != child_paths_by_field_id.end()) {
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
            child_field_id = std::to_string(child.field_id);
        }

        if (child_field_id.empty() || child_field_id == "-1") {
            continue;
        }

        auto child_paths_it = child_paths_by_field_id.find(child_field_id);
        if (child_paths_it != child_paths_by_field_id.end()) {
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