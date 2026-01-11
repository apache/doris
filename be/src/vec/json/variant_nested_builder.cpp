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

#include "vec/json/variant_nested_builder.h"

#include <glog/logging.h>

#include <unordered_map>

#include "olap/rowset/segment_v2/variant/offset_manager.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"

namespace doris::vectorized {

VariantNestedBuilder::VariantNestedBuilder(ColumnVariant& column_variant)
        : _column_variant(column_variant) {}

void VariantNestedBuilder::build(const std::vector<PathInData>& paths,
                                 const std::vector<Field>& values, size_t old_num_rows) {
    // Group paths by their first-level nested path
    // Key: first-level array_path
    // Value: list of (path_index, all_levels) pairs
    using PathInfo = std::pair<size_t, std::vector<std::pair<PathInData, size_t>>>;
    std::unordered_map<PathInData, std::vector<PathInfo>, PathInData::Hash> paths_by_first_level;

    // Separate paths into nested and non-nested
    std::vector<size_t> non_nested_indices;

    for (size_t i = 0; i < paths.size(); ++i) {
        auto levels = ColumnVariant::get_nested_levels(paths[i]);

        if (levels.empty()) {
            // Non-nested path
            non_nested_indices.push_back(i);
        } else if (levels[0].second > _max_depth) {
            // Exceeds max depth, treat as non-nested (will be stored as JSONB)
            non_nested_indices.push_back(i);
        } else {
            // Group by first level
            paths_by_first_level[levels[0].first].emplace_back(i, std::move(levels));
        }
    }

    // Process non-nested paths
    for (size_t idx : non_nested_indices) {
        process_path_value(paths[idx], values[idx], old_num_rows);
    }

    // Process each first-level nested group
    for (auto& [first_level_path, path_infos] : paths_by_first_level) {
        // Check for conflict
        FieldInfo dummy_info;
        if (_column_variant.check_path_conflict(first_level_path, 1, dummy_info)) {
            VLOG_DEBUG << "Path conflict detected for array path: " << first_level_path.get_path();
            // Disable the path and fall back to regular subcolumn processing
            _column_variant.disable_path(first_level_path);

            // Write conflicting paths to regular subcolumns (will be stored as JSONB)
            for (const auto& [path_idx, levels] : path_infos) {
                process_path_value(paths[path_idx], values[path_idx], old_num_rows);
            }
            continue;
        }

        // Get or create first-level NestedGroup
        auto* group = _column_variant.get_or_create_nested_group(first_level_path);
        if (!group) {
            LOG(WARNING) << "Failed to create NestedGroup for path: " << first_level_path.get_path();
            continue;
        }

        // Determine array size from first value (all paths in same group should have same outer array size)
        size_t first_level_array_size = 0;
        if (!path_infos.empty()) {
            FieldInfo info = get_field_info(values[path_infos[0].first]);
            first_level_array_size = get_array_size(values[path_infos[0].first], info);
        }

        // Update first-level offsets
        segment_v2::OffsetManager::append_offset(*group, first_level_array_size);

        // Set expected type
        if (group->expected_type == ColumnVariant::NestedGroup::StructureType::UNKNOWN) {
            group->expected_type = ColumnVariant::NestedGroup::StructureType::ARRAY;
            group->expected_array_depth = 1;
        }

        // Process each path in this group
        for (const auto& [path_idx, levels] : path_infos) {
            PathInData leaf_path = ColumnVariant::get_leaf_path_after_nested(paths[path_idx]);
            FieldInfo info = get_field_info(values[path_idx]);
            write_to_nested_group(levels, leaf_path, values[path_idx], info, old_num_rows);
        }
    }
}

void VariantNestedBuilder::write_to_nested_group(
        const std::vector<std::pair<PathInData, size_t>>& levels,
        const PathInData& leaf_path,
        const Field& value, const FieldInfo& info, size_t old_num_rows) {
    if (levels.empty()) {
        return;
    }

    // Get the first-level group
    auto* first_group = _column_variant.get_nested_group(levels[0].first);
    if (!first_group || first_group->is_disabled) {
        return;
    }

    // Start recursive processing from level 0
    write_to_nested_group_recursive(first_group, levels, 0, leaf_path, value, info, 0);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
void VariantNestedBuilder::write_to_nested_group_recursive(
        ColumnVariant::NestedGroup* parent_group,
        const std::vector<std::pair<PathInData, size_t>>& levels,
        size_t current_level_idx,
        const PathInData& leaf_path,
        const Field& value, const FieldInfo& info,
        size_t parent_flat_offset) {

    if (current_level_idx >= levels.size()) {
        return;
    }

    bool is_last_level = (current_level_idx == levels.size() - 1);
    size_t current_depth = levels[current_level_idx].second;

    // Check if exceeds max depth
    if (current_depth > _max_depth) {
        _write_to_nested_group_exceed_depth(parent_group, levels, current_level_idx, leaf_path,
                                            value, info);
        return;
    }

    if (is_last_level) {
        _write_to_nested_group_last_level(parent_group, leaf_path, value, info, parent_flat_offset);
    } else {
        _write_to_nested_group_non_last_level(parent_group, levels, current_level_idx, leaf_path,
                                              value, info);
    }
}

void VariantNestedBuilder::_write_to_nested_group_exceed_depth(
        ColumnVariant::NestedGroup* parent_group,
        const std::vector<std::pair<PathInData, size_t>>& levels,
        size_t current_level_idx,
        const PathInData& leaf_path,
        const Field& value, const FieldInfo& info) {
    const auto& current_level_path = levels[current_level_idx].first;
    PathInData relative_path = get_relative_path(
            leaf_path.empty() ? current_level_path
                              : PathInData(current_level_path.get_path() + "." + leaf_path.get_path()),
            levels[0].first);
    auto it = parent_group->children.find(relative_path);
    if (it == parent_group->children.end()) {
        ColumnVariant::Subcolumn subcolumn(0, true);
        parent_group->children[relative_path] = std::move(subcolumn);
        it = parent_group->children.find(relative_path);
    }
    it->second.insert(value, info);
}

void VariantNestedBuilder::_write_to_nested_group_last_level(ColumnVariant::NestedGroup* parent_group,
                                                             const PathInData& leaf_path,
                                                             const Field& value,
                                                             const FieldInfo& info,
                                                             size_t parent_flat_offset) {
    if (info.num_dimensions > 0 && value.get_type() == PrimitiveType::TYPE_ARRAY) {
        const auto& arr = value.get<Array>();
        auto it = parent_group->children.find(leaf_path);
        if (it == parent_group->children.end()) {
            ColumnVariant::Subcolumn subcolumn(0, true);
            if (parent_flat_offset > 0 || parent_group->current_flat_size > arr.size()) {
                size_t defaults_needed = parent_group->current_flat_size - arr.size();
                if (defaults_needed > 0) {
                    subcolumn.insert_many_defaults(defaults_needed);
                }
            }
            parent_group->children[leaf_path] = std::move(subcolumn);
            it = parent_group->children.find(leaf_path);
        }
        for (const auto& elem : arr) {
            FieldInfo elem_info;
            schema_util::get_field_info(elem, &elem_info);
            it->second.insert(elem, elem_info);
        }
        return;
    }
    auto it = parent_group->children.find(leaf_path);
    if (it == parent_group->children.end()) {
        ColumnVariant::Subcolumn subcolumn(0, true);
        parent_group->children[leaf_path] = std::move(subcolumn);
        it = parent_group->children.find(leaf_path);
    }
    it->second.insert(value, info);
}

void VariantNestedBuilder::_write_to_nested_group_non_last_level(
        ColumnVariant::NestedGroup* parent_group,
        const std::vector<std::pair<PathInData, size_t>>& levels,
        size_t current_level_idx,
        const PathInData& leaf_path,
        const Field& value, const FieldInfo& info) {
    const auto& current_level_path = levels[current_level_idx].first;
    const auto& next_level_path = levels[current_level_idx + 1].first;
    PathInData relative_nested_path = get_relative_path(next_level_path, current_level_path);
    auto nested_it = parent_group->nested_groups.find(relative_nested_path);
    if (nested_it == parent_group->nested_groups.end()) {
        auto nested_group = std::make_shared<ColumnVariant::NestedGroup>();
        nested_group->path = relative_nested_path;
        nested_group->offsets = ColumnOffset64::create();
        parent_group->nested_groups[relative_nested_path] = std::move(nested_group);
        nested_it = parent_group->nested_groups.find(relative_nested_path);
    }
    auto* nested_group = nested_it->second.get();
    if (info.num_dimensions <= 0 || value.get_type() != PrimitiveType::TYPE_ARRAY) {
        return;
    }
    const auto& arr = value.get<Array>();
    for (const auto& elem : arr) {
        if (elem.get_type() != PrimitiveType::TYPE_ARRAY) {
            continue;
        }
        const auto& inner_arr = elem.get<Array>();
        const size_t prev_nested_offset = nested_group->current_flat_size;
        segment_v2::OffsetManager::append_offset(*nested_group, inner_arr.size());
        FieldInfo inner_info;
        schema_util::get_field_info(elem, &inner_info);
        write_to_nested_group_recursive(nested_group, levels, current_level_idx + 1, leaf_path, elem,
                                        inner_info, prev_nested_offset);
    }
}

void VariantNestedBuilder::write_to_subcolumn(const PathInData& path, const Field& value,
                                              const FieldInfo& info, size_t old_num_rows) {
    // Use existing ColumnVariant API for non-nested paths
    if (_column_variant.get_subcolumn(path) == nullptr) {
        if (path.has_nested_part()) {
            _column_variant.add_nested_subcolumn(path, info, old_num_rows);
        } else {
            _column_variant.add_sub_column(path, old_num_rows);
        }
    }

    auto* subcolumn = _column_variant.get_subcolumn(path);
    if (!subcolumn) {
        LOG(WARNING) << "Failed to get subcolumn for path: " << path.get_path();
        return;
    }

    // Handle pending defaults
    if (subcolumn->cur_num_of_defaults() > 0) {
        subcolumn->insert_many_defaults(subcolumn->cur_num_of_defaults());
        subcolumn->reset_current_num_of_defaults();
    }

    // Insert the value
    subcolumn->insert(value, info);
}

void VariantNestedBuilder::process_path_value(const PathInData& path, const Field& value,
                                              size_t old_num_rows) {
    FieldInfo info = get_field_info(value);

    if (info.scalar_type_id == PrimitiveType::INVALID_TYPE) {
        // Skip invalid types
        return;
    }

    write_to_subcolumn(path, value, info, old_num_rows);
}

PathInData VariantNestedBuilder::get_relative_path(const PathInData& full_path,
                                                    const PathInData& prefix) {
    const auto& full_parts = full_path.get_parts();
    const auto& prefix_parts = prefix.get_parts();

    if (prefix_parts.size() >= full_parts.size()) {
        return PathInData {};
    }

    // Verify prefix matches
    for (size_t i = 0; i < prefix_parts.size(); ++i) {
        if (full_parts[i].key != prefix_parts[i].key) {
            return full_path; // Prefix doesn't match, return full path
        }
    }

    // Return the remaining parts
    PathInData::Parts relative_parts(full_parts.begin() + prefix_parts.size(), full_parts.end());
    return PathInData(relative_parts);
}

FieldInfo VariantNestedBuilder::get_field_info(const Field& value) {
    FieldInfo info;
    schema_util::get_field_info(value, &info);
    return info;
}

size_t VariantNestedBuilder::get_array_size(const Field& value, const FieldInfo& info) {
    if (info.num_dimensions > 0 && value.get_type() == PrimitiveType::TYPE_ARRAY) {
        return value.get<Array>().size();
    }
    // Scalar value treated as single element
    return 1;
}

void VariantNestedBuilder::flatten_array_values(const Field& value, size_t target_depth,
                                                 std::vector<Field>& flat_values,
                                                 std::vector<size_t>& level_sizes) {
    if (target_depth == 0 || value.get_type() != PrimitiveType::TYPE_ARRAY) {
        flat_values.push_back(value);
        return;
    }

    const auto& arr = value.get<Array>();
    level_sizes.push_back(arr.size());

    for (const auto& elem : arr) {
        flatten_array_values(elem, target_depth - 1, flat_values, level_sizes);
    }
}

} // namespace doris::vectorized
