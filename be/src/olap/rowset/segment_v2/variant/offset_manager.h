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

namespace doris::segment_v2 {

// Forward declarations - avoiding circular dependencies
struct NestedGroup;
template <typename K, typename V, typename H>
class NestedGroupsMapType;

/**
 * OffsetManager provides utility functions to manage offset padding logic for NestedGroups.
 *
 * This class consolidates the repeated offset padding patterns found in:
 * - nested_group_builder.cpp: pad_all_groups_to_row lambda (lines 61-75)
 * - nested_group_builder.cpp: nested group offsets padding (lines 295-304)
 * - nested_group_builder.cpp: backfill in _process_array_of_objects (lines 193-197)
 * - variant_nested_builder.cpp: similar offset padding patterns (lines 95-99)
 *
 * Template methods are used to support both segment_v2::NestedGroup and
 * vectorized::ColumnVariant::NestedGroup, which have identical interfaces.
 */
class OffsetManager {
public:
    /**
     * Pad a single group's offsets to cover up to and including the target row.
     * If the offsets column size is less than or equal to target_row, pad with
     * empty array entries (current_flat_size) until offsets.size() > target_row.
     *
     * @tparam NestedGroupT The NestedGroup type (segment_v2::NestedGroup or ColumnVariant::NestedGroup)
     * @param group The NestedGroup to pad
     * @param target_row The target row index (0-based) that should have an offset entry
     */
    template <typename NestedGroupT>
    static void pad_group_to_row(NestedGroupT& group, size_t target_row);

    /**
     * Pad all groups in the map to cover up to and including the target row.
     * Skips disabled groups and null group pointers.
     *
     * @tparam NestedGroupsMapT The map type (e.g., unordered_map<PathInData, shared_ptr<NestedGroup>>)
     * @param groups The map of NestedGroups to pad
     * @param target_row The target row index (0-based) that should have an offset entry
     */
    template <typename NestedGroupsMapT>
    static void pad_all_groups_to_row(NestedGroupsMapT& groups, size_t target_row);

    /**
     * Append a new offset entry for the given array size.
     * Calculates new_total = current_flat_size + array_size and appends it.
     * Updates group.current_flat_size to new_total.
     *
     * @tparam NestedGroupT The NestedGroup type (segment_v2::NestedGroup or ColumnVariant::NestedGroup)
     * @param group The NestedGroup to update
     * @param array_size The size of the array being added
     */
    template <typename NestedGroupT>
    static void append_offset(NestedGroupT& group, size_t array_size);

    /**
     * Backfill missing offsets until the offsets column size reaches element_idx.
     * This handles the case when a NestedGroup is created mid-batch (e.g., when
     * mixing top-level arrays and objects), ensuring earlier elements have proper offsets.
     *
     * Note: This pads to size < element_idx, not size <= element_idx (different from pad_group_to_row).
     *
     * @tparam NestedGroupT The NestedGroup type (segment_v2::NestedGroup or ColumnVariant::NestedGroup)
     * @param group The NestedGroup to backfill
     * @param element_idx The target element index that offsets.size() should reach
     */
    template <typename NestedGroupT>
    static void backfill_to_element(NestedGroupT& group, size_t element_idx);
};

} // namespace doris::segment_v2

// Template implementations
#include "olap/rowset/segment_v2/variant/offset_manager_impl.h"
