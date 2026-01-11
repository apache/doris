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
#include <utility>
#include <vector>

#include "vec/columns/column_variant.h"
#include "vec/core/field.h"
#include "vec/json/path_in_data.h"

namespace doris::vectorized {

/**
 * Builder for constructing Variant columns with NestedGroup support.
 * Maintains the association of fields within array elements.
 *
 * When enabled, array<object> paths are stored using shared offsets,
 * allowing element-level field associations to be preserved.
 *
 * Example: for JSON {"items": [{"a":1,"b":"x"}, {"a":2,"b":"y"}]}
 *   - Without NestedGroup: items.a = [1,2], items.b = ["x","y"] (association lost)
 *   - With NestedGroup: items has shared offsets, items.a and items.b
 *                       can be correctly associated by element index
 */
class VariantNestedBuilder {
public:
    explicit VariantNestedBuilder(ColumnVariant& column_variant);

    /**
     * Build from parsed JSON result.
     * Groups paths by their array<object> prefix and writes to NestedGroups.
     *
     * @param paths The paths from JSON parsing
     * @param values The values corresponding to each path
     * @param old_num_rows Number of rows before this insert
     */
    void build(const std::vector<PathInData>& paths, const std::vector<Field>& values,
               size_t old_num_rows);

    // Set maximum array depth to track with NestedGroup
    void set_max_depth(size_t max_depth) { _max_depth = max_depth; }

private:
    // Process a single path-value pair (for non-array paths)
    void process_path_value(const PathInData& path, const Field& value, size_t old_num_rows);

    // Write value to nested group (supports multi-level nesting)
    // levels: all nested levels in the path [(path1, depth1), (path2, depth2), ...]
    // leaf_path: the path after all nested levels
    void write_to_nested_group(
            const std::vector<std::pair<PathInData, size_t>>& levels,
            const PathInData& leaf_path,
            const Field& value, const FieldInfo& info, size_t old_num_rows);

    // Recursively write to nested group at a specific level
    void write_to_nested_group_recursive(
            ColumnVariant::NestedGroup* parent_group,
            const std::vector<std::pair<PathInData, size_t>>& levels,
            size_t current_level_idx,
            const PathInData& leaf_path,
            const Field& value, const FieldInfo& info,
            size_t parent_flat_offset);

    // split complex recursive logic into smaller helpers for readability and linting.
    void _write_to_nested_group_exceed_depth(ColumnVariant::NestedGroup* parent_group,
                                            const std::vector<std::pair<PathInData, size_t>>& levels,
                                            size_t current_level_idx,
                                            const PathInData& leaf_path,
                                            const Field& value, const FieldInfo& info);

    void _write_to_nested_group_last_level(ColumnVariant::NestedGroup* parent_group,
                                           const PathInData& leaf_path,
                                           const Field& value, const FieldInfo& info,
                                           size_t parent_flat_offset);

    void _write_to_nested_group_non_last_level(ColumnVariant::NestedGroup* parent_group,
                                               const std::vector<std::pair<PathInData, size_t>>& levels,
                                               size_t current_level_idx,
                                               const PathInData& leaf_path,
                                               const Field& value, const FieldInfo& info);

    // Write to regular subcolumn (for non-nested paths)
    void write_to_subcolumn(const PathInData& path, const Field& value, const FieldInfo& info,
                            size_t old_num_rows);

    // Get relative path between two paths
    static PathInData get_relative_path(const PathInData& full_path, const PathInData& prefix);

    // Get field info from value
    static FieldInfo get_field_info(const Field& value);

    // Extract array size from a field (for determining offsets)
    static size_t get_array_size(const Field& value, const FieldInfo& info);

    // Flatten nested array values to the deepest level
    static void flatten_array_values(const Field& value, size_t target_depth,
                                     std::vector<Field>& flat_values,
                                     std::vector<size_t>& level_sizes);

    ColumnVariant& _column_variant;
    size_t _max_depth = 3;
};

} // namespace doris::vectorized
