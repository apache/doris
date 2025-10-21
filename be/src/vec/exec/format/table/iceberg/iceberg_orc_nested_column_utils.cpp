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

void IcebergOrcNestedColumnUtils::extract_nested_column_ids(
        const orc::Type& type, const std::vector<TColumnNameAccessPath>& paths,
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
                if (child_paths_by_field_id.find("KEYS") != child_paths_by_field_id.end()) {
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
                if (i == 0 && child_paths_by_field_id.find("*") != child_paths_by_field_id.end()) {
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
            bool needs_full_child = std::any_of(
                    child_paths.begin(), child_paths.end(),
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

} // namespace vectorized
} // namespace doris