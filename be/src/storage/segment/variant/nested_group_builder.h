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

#include <parallel_hashmap/phmap.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_variant.h"
#include "storage/segment/variant/nested_group_path.h"
#include "util/json/path_in_data.h"

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
    PathInData path;

    // Offsets per parent row (or per parent element for nested groups).
    MutableColumnPtr offsets;

    // Scalar (or flattened object) children under this array path.
    phmap::flat_hash_map<PathInData, ColumnVariant::Subcolumn, PathInData::Hash> children;
    // Sparse row positions for each child subcolumn value in flattened element space.
    // When present for a child path, children[path] stores only non-missing values and
    // child_rowids[path][i] is the logical row index for children[path][i].
    phmap::flat_hash_map<PathInData, std::vector<uint32_t>, PathInData::Hash> child_rowids;

    // Nested array<object> groups under this array path.
    phmap::flat_hash_map<PathInData, std::shared_ptr<NestedGroup>, PathInData::Hash> nested_groups;

    size_t current_flat_size = 0;
    bool is_disabled = false;

    enum struct StructureType { UNKNOWN, SCALAR, ARRAY, OBJECT };
    StructureType expected_type = StructureType::UNKNOWN;

    void ensure_offsets();
};

using NestedGroupsMap =
        phmap::flat_hash_map<PathInData, std::shared_ptr<NestedGroup>, PathInData::Hash>;

// NestedGroup marker/path constants are defined in nested_group_path.h

/**
 * English comment: Build NestedGroup(s) from JSONB columns at storage finalize stage.
 * The builder scans JSONB values and only expands array<object>.
 */
class NestedGroupBuilder {
public:
    NestedGroupBuilder() = default;

    // Build NestedGroups from a JSONB column. base_path is the path of this JSONB column
    // in ColumnVariant (empty for root JSONB).
    Status build_from_jsonb(const ColumnPtr& jsonb_column, const PathInData& base_path,
                            NestedGroupsMap& nested_groups, size_t num_rows);

    // Convenience overload for root JSONB.
    Status build_from_jsonb(const ColumnPtr& jsonb_column, NestedGroupsMap& nested_groups,
                            size_t num_rows) {
        return build_from_jsonb(jsonb_column, PathInData {}, nested_groups, num_rows);
    }

    // Collect paths that have ARRAY<OBJECT> vs non-array structural conflicts.
    // Returned paths are de-duplicated and sorted.
    void collect_conflict_paths(std::vector<std::string>* out_paths) const;

    void set_max_depth(size_t max_depth) { _max_depth = max_depth; }

private:
    using AppendedPathCache =
            phmap::flat_hash_map<std::string, PathInData, phmap::priv::StringHashEqT<char>::Hash,
                                 phmap::priv::StringHashEqT<char>::Eq>;

    enum class PathShape { ARRAY_OBJECT, NON_ARRAY };

    struct PathShapeState {
        bool has_array_object = false;
        bool has_non_array = false;
    };

    const PathInData& _normalize_group_path(const PathInData& path) const;
    void _record_path_shape(const PathInData& path, PathShape shape);
    PathInData _append_path_cached(const PathInData& base, std::string_view suffix);

    Status _process_jsonb_value(const doris::JsonbValue* value, const PathInData& current_path,
                                NestedGroupsMap& nested_groups, size_t row_idx, size_t depth);

    Status _process_object_as_paths(const doris::JsonbValue* obj_value,
                                    const PathInData& current_prefix, NestedGroup& group,
                                    size_t element_flat_idx, size_t depth,
                                    const PathInData& group_absolute_path);

    Status _process_array_of_objects(const doris::JsonbValue* arr_value, NestedGroup& group,
                                     size_t parent_row_idx, size_t depth,
                                     const PathInData& group_absolute_path);
    Status _finalize_group(NestedGroup& group);

    // Process nested object field by recursively flattening into dotted paths.
    Status _process_object_field(const doris::JsonbValue* obj_value, const PathInData& next_prefix,
                                 NestedGroup& group, size_t element_flat_idx, size_t depth,
                                 const PathInData& group_absolute_path);

    // Process nested array<object> field within a NestedGroup.
    Status _process_nested_array_field(const doris::JsonbValue* arr_value,
                                       const PathInData& next_prefix, NestedGroup& group,
                                       size_t element_flat_idx, size_t depth,
                                       const PathInData& group_absolute_path);

    // Process scalar field and insert into subcolumn.
    Status _process_scalar_field(const doris::JsonbValue* value, const PathInData& next_prefix,
                                 NestedGroup& group, size_t element_flat_idx);

    // Return true if this array can be treated as array<object> (nulls allowed).
    bool _is_array_of_objects(const doris::JsonbValue* arr_value) const;

    // Convert a JsonbValue to a scalar Field (or NULL Field). Container types are not supported.
    Status _jsonb_to_field(const doris::JsonbValue* value, Field& out) const;

    // Conflict policy placeholder. Returns true if the current value should be discarded.
    bool _handle_conflict(NestedGroup& group, bool is_array_object) const;

private:
    size_t _max_depth = 0; // 0 = unlimited
    phmap::flat_hash_map<PathInData, PathShapeState, PathInData::Hash> _path_shape_states;
    phmap::flat_hash_set<std::string> _conflict_paths;
    phmap::flat_hash_map<PathInData, AppendedPathCache, PathInData::Hash> _appended_path_cache;
};

} // namespace doris::segment_v2
