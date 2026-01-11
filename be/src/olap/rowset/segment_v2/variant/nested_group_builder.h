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

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_variant.h"
#include "vec/json/path_in_data.h"

namespace doris {
struct JsonbValue;
} // namespace doris

namespace doris::segment_v2 {

/**
 * English comment: NestedGroup is a storage-layer structure used to persist array<object>
 * with shared offsets to preserve per-element field associations.
 *
 * This is intentionally independent from ColumnVariant's in-memory nested structures.
 */
struct NestedGroup {
    // Full array path for top-level group (e.g. "voltage.list"),
    // and relative path for nested groups within another NestedGroup (e.g. "cells").
    vectorized::PathInData path;

    // Offsets per parent row (or per parent element for nested groups).
    vectorized::MutableColumnPtr offsets;

    // Scalar (or flattened object) children under this array path.
    std::unordered_map<vectorized::PathInData, vectorized::ColumnVariant::Subcolumn,
                       vectorized::PathInData::Hash>
            children;

    // Nested array<object> groups under this array path.
    std::unordered_map<vectorized::PathInData, std::shared_ptr<NestedGroup>,
                       vectorized::PathInData::Hash>
            nested_groups;

    size_t current_flat_size = 0;
    bool is_disabled = false;

    enum struct StructureType { UNKNOWN, SCALAR, ARRAY, OBJECT };
    StructureType expected_type = StructureType::UNKNOWN;

    void ensure_offsets();
};

using NestedGroupsMap =
        std::unordered_map<vectorized::PathInData, std::shared_ptr<NestedGroup>,
                           vectorized::PathInData::Hash>;

// Special path marker for top-level array<object> NestedGroup
inline constexpr std::string_view kRootNestedGroupPath = "$root";

/**
 * English comment: Build NestedGroup(s) from JSONB columns at storage finalize stage.
 * The builder scans JSONB values and only expands array<object>.
 */
class NestedGroupBuilder {
public:
    NestedGroupBuilder() = default;

    // Build NestedGroups from a JSONB column. base_path is the path of this JSONB column
    // in ColumnVariant (empty for root JSONB).
    Status build_from_jsonb(const vectorized::ColumnPtr& jsonb_column,
                            const vectorized::PathInData& base_path, NestedGroupsMap& nested_groups,
                            size_t num_rows);

    // Convenience overload for root JSONB.
    Status build_from_jsonb(const vectorized::ColumnPtr& jsonb_column, NestedGroupsMap& nested_groups,
                            size_t num_rows) {
        return build_from_jsonb(jsonb_column, vectorized::PathInData {}, nested_groups, num_rows);
    }

    void set_max_depth(size_t max_depth) { _max_depth = max_depth; }

private:
    Status _process_jsonb_value(const doris::JsonbValue* value,
                               const vectorized::PathInData& current_path,
                               NestedGroupsMap& nested_groups, size_t row_idx, size_t depth);

    Status _process_object_as_paths(const doris::JsonbValue* obj_value,
                                   const vectorized::PathInData& current_prefix,
                                   NestedGroup& group, size_t element_flat_idx,
                                   std::unordered_set<std::string>& seen_child_paths,
                                   std::unordered_set<std::string>& seen_nested_paths,
                                   size_t depth);

    Status _process_array_of_objects(const doris::JsonbValue* arr_value, NestedGroup& group,
                                    size_t parent_row_idx, size_t depth);

    // Return true if this array can be treated as array<object> (nulls allowed).
    bool _is_array_of_objects(const doris::JsonbValue* arr_value) const;

    // Convert a JsonbValue to a scalar Field (or NULL Field). Container types are not supported.
    Status _jsonb_to_field(const doris::JsonbValue* value, vectorized::Field& out) const;

    // Conflict policy placeholder. Returns true if the current value should be discarded.
    bool _handle_conflict(NestedGroup& group, bool is_array_object) const;

private:
    size_t _max_depth = 0; // 0 = unlimited
};

} // namespace doris::segment_v2

