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

#pragma once

#include <set>
#include <string>
#include <vector>

#include "vec/exec/format/table/table_format_reader.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

// Helper that normalizes access paths and delegates nested column id extraction.
// The caller provides how to access the column id range for the concrete field type
// (Parquet FieldSchema, ORC Type, etc.) plus a nested extractor implementation.
template <typename FieldType, typename ColumnIdGetter, typename MaxColumnIdGetter,
          typename ExtractNestedFunc>
void process_nested_access_paths(const FieldType* field,
                                 const std::vector<TColumnAccessPath>& access_paths,
                                 std::set<uint64_t>& out_ids, ColumnIdGetter&& column_id_getter,
                                 MaxColumnIdGetter&& max_column_id_getter,
                                 ExtractNestedFunc&& extract_nested) {
    if (field == nullptr) {
        return;
    }

    const bool access_paths_empty = access_paths.empty();
    std::vector<std::vector<std::string>> paths;
    paths.reserve(access_paths.size());
    bool has_top_level_only = false;

    for (const auto& access_path : access_paths) {
        const std::vector<std::string>* path_ptr = nullptr;
        if (access_path.type == TAccessPathType::DATA) {
            path_ptr = &access_path.data_access_path.path;
        } else if (access_path.type == TAccessPathType::META) {
            path_ptr = &access_path.meta_access_path.path;
        } else {
            continue;
        }

        const auto& path = *path_ptr;
        std::vector<std::string> remaining_path;
        if (path.size() > 1) {
            remaining_path.assign(path.begin() + 1, path.end());
        }
        if (remaining_path.empty()) {
            has_top_level_only = true;
        }
        paths.push_back(std::move(remaining_path));
    }

    const uint64_t column_id = column_id_getter(field);
    if (has_top_level_only || access_paths_empty) {
        const uint64_t max_column_id = max_column_id_getter(field);
        for (uint64_t id = column_id; id <= max_column_id; ++id) {
            out_ids.insert(id);
        }
    } else if (!paths.empty()) {
        out_ids.insert(column_id);
        extract_nested(*field, paths, out_ids);
    }
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
