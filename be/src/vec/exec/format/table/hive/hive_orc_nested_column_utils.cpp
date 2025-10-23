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

#include "common/logging.h"
#include "orc/Type.hh"
#include "vec/exec/format/table/table_format_reader.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

void HiveOrcNestedColumnUtils::extract_nested_column_ids(
        const orc::Type& type, const std::vector<TColumnNameAccessPath>& paths,
        std::set<uint64_t>& column_ids) {
    // Group paths by first field_id
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

    // For MAP type, normalize wildcard "*" to explicit KEYS/VALUES access
    // Wildcard in MAP context means accessing both map keys and values
    // Normalization logic:
    //   path: ["map_col", "*"]              → ["map_col", "VALUES"] + ["map_col", "KEYS"]
    //   path: ["map_col", "*", "field"]     → ["map_col", "VALUES", "field"] + ["map_col", "KEYS"]
    // KEYS are always needed for correct RL/DL computation when accessing MAP via wildcard
    if (type.getKind() == orc::TypeKind::MAP) {
        auto wildcard_it = child_paths_by_table_col_name.find("*");
        if (wildcard_it != child_paths_by_table_col_name.end()) {
            auto& wildcard_paths = wildcard_it->second;

            // All wildcard paths go to VALUES
            auto& values_paths = child_paths_by_table_col_name["VALUES"];
            values_paths.insert(values_paths.end(), wildcard_paths.begin(), wildcard_paths.end());

            // Always add KEYS for wildcard access (needed for RL/DL computation)
            auto& keys_paths = child_paths_by_table_col_name["KEYS"];
            // Add an empty path to request full KEYS
            TColumnNameAccessPath empty_path;
            keys_paths.push_back(empty_path);

            // Remove wildcard entry as it's been expanded
            child_paths_by_table_col_name.erase(wildcard_it);
        }
    }

    for (uint64_t i = 0; i < type.getSubtypeCount(); ++i) {
        const orc::Type* child = type.getSubtype(i);

        std::string child_field_name;
        switch (type.getKind()) {
        case orc::TypeKind::STRUCT:
            child_field_name = to_lower(type.getFieldName(i));
            break;
        case orc::TypeKind::LIST:
            child_field_name = "*";
            break;
        case orc::TypeKind::MAP:
            // After wildcard normalization above, all MAP accesses are explicit KEYS/VALUES
            // Simply assign the appropriate field name based on which child we're processing
            if (i == 0) {
                child_field_name = "KEYS";
            } else if (i == 1) {
                child_field_name = "VALUES";
            }
            break;
        default:
            child_field_name = "";
            break;
        }

        if (child_field_name.empty()) {
            continue;
        }

        const auto child_paths_it = child_paths_by_table_col_name.find(child_field_name);

        if (child_paths_it != child_paths_by_table_col_name.end()) {
            const auto& child_paths = child_paths_it->second;

            // Check if any child path is empty (meaning full child needed)
            bool needs_full_child = std::any_of(child_paths.begin(), child_paths.end(),
                                                [](const TColumnNameAccessPath& access_path) {
                                                    return access_path.path.empty();
                                                });

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

} // namespace doris::vectorized
