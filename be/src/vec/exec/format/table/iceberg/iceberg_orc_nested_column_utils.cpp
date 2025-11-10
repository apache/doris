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

namespace doris::vectorized {
#include "common/compile_check_begin.h"

void IcebergOrcNestedColumnUtils::extract_nested_column_ids(
        const orc::Type& type, const std::vector<std::vector<std::string>>& paths,
        std::set<uint64_t>& column_ids) {
    // Group paths by first field_id
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> child_paths_by_field_id;

    for (const auto& path : paths) {
        if (!path.empty()) {
            std::string first_field_id = path[0];
            std::vector<std::string> remaining;
            if (path.size() > 1) {
                remaining.assign(path.begin() + 1, path.end());
            }
            child_paths_by_field_id[first_field_id].push_back(std::move(remaining));
        }
    }

    // For MAP type, normalize wildcard "*" to explicit KEYS/VALUES access
    // Wildcard in MAP context means accessing both map keys and values
    // Normalization logic:
    //   path: ["map_col", "*"]              → ["map_col", "VALUES"] + ["map_col", "KEYS"]
    //   path: ["map_col", "*", "field"]     → ["map_col", "VALUES", "field"] + ["map_col", "KEYS"]
    if (type.getKind() == orc::TypeKind::MAP) {
        auto wildcard_it = child_paths_by_field_id.find("*");
        if (wildcard_it != child_paths_by_field_id.end()) {
            auto& wildcard_paths = wildcard_it->second;

            // All wildcard paths go to VALUES
            auto& values_paths = child_paths_by_field_id["VALUES"];
            values_paths.insert(values_paths.end(), wildcard_paths.begin(), wildcard_paths.end());

            // Always add KEYS for wildcard access
            auto& keys_paths = child_paths_by_field_id["KEYS"];
            // Add an empty path to request full KEYS
            std::vector<std::string> empty_path;
            keys_paths.push_back(empty_path);

            // Remove wildcard entry as it's been expanded
            child_paths_by_field_id.erase(wildcard_it);
        }
    }

    bool has_child_columns = false;
    // Efficiently traverse children
    for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
        const orc::Type* child = type.getSubtype(i);

        std::string child_field_id;
        switch (type.getKind()) {
        case orc::TypeKind::STRUCT:
            if (!child->hasAttributeKey(ICEBERG_ORC_ATTRIBUTE)) {
                continue;
            }
            child_field_id = child->getAttributeValue(ICEBERG_ORC_ATTRIBUTE);
            break;
        case orc::TypeKind::LIST:
            child_field_id = "*";
            break;
        case orc::TypeKind::MAP:
            // After wildcard normalization above, all MAP accesses are explicit KEYS/VALUES
            // Simply assign the appropriate field name based on which child we're processing
            if (i == 0) {
                child_field_id = "KEYS";
            } else if (i == 1) {
                child_field_id = "VALUES";
            }
            // Special handling for Orc MAP structure:
            // When accessing only VALUES, we still need KEY structure for deduplicate_keys
            // Check if we're at key child (i==0) and only VALUES is requested (no KEYS)
            if (i == 0) {
                bool has_keys_access =
                        child_paths_by_field_id.find("KEYS") != child_paths_by_field_id.end();
                bool has_values_access =
                        child_paths_by_field_id.find("VALUES") != child_paths_by_field_id.end();

                // If only VALUES is accessed (not KEYS), still include key structure for deduplicate_keys
                if (!has_keys_access && has_values_access) {
                    uint64_t key_start_id = child->getColumnId();
                    uint64_t key_max_id = child->getMaximumColumnId();
                    for (uint64_t id = key_start_id; id <= key_max_id; ++id) {
                        column_ids.insert(id);
                    }
                    has_child_columns = true;
                    continue; // Skip further processing of key child
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
                                [](const std::vector<std::string>& path) { return path.empty(); });

            if (needs_full_child) {
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
                extract_nested_column_ids(*child, child_paths, column_ids);

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

} // namespace doris::vectorized
