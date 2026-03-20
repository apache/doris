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

#include <string>
#include <vector>

#include "common/status.h"

namespace doris {
class ColumnVariant;
} // namespace doris

namespace doris::segment_v2 {

// Policy for handling NestedGroup vs scalar conflicts.
// When the same path has both array<object> and scalar data:
//   DISCARD_SCALAR: silently drop scalar data, keep nested data (default)
//   ERROR: report an error when conflict is detected
enum class NestedGroupConflictPolicy {
    DISCARD_SCALAR = 0,
    ERROR = 1,
};

// Routing plan for NestedGroup write path. Controls which subcolumn paths
// are excluded from regular writes because they are handled by NestedGroup.
//
// Simplified model:
// - Only NON-conflict NG paths go into ng_only_prefixes.
// - Conflict paths stay in regular subcolumns (not excluded), so routing can
//   remain compatible with cross-segment compaction where NG payload may
//   become non-JSONB after merge.
struct NestedGroupRoutingPlan {
    bool exclude_all_subcolumns = false;
    bool has_conflict_paths = false;
    std::vector<std::string> ng_only_prefixes;
    NestedGroupConflictPolicy conflict_policy = NestedGroupConflictPolicy::DISCARD_SCALAR;

    // Returns true if |path| should be excluded from regular subcolumn writes.
    bool is_excluded_subcolumn(const std::string& path) const;

    // Returns true if the plan has any active exclusions (NG paths found).
    bool has_exclusions() const { return exclude_all_subcolumns || !ng_only_prefixes.empty(); }

    // Returns true if root JSONB can be safely replaced with empty defaults.
    // Only safe when there are NG exclusions AND no conflict paths.
    // With conflicts, root JSONB may carry data needed by the NG provider.
    bool can_remove_root_jsonb() const { return has_exclusions() && !has_conflict_paths; }
};

// Build NG routing plan from variant content. Scans the variant for
// array<object> paths, detects conflicts, and populates the plan.
Status build_nested_group_routing_plan(const ColumnVariant& variant, NestedGroupRoutingPlan* plan);

// Build NG routing plan from pre-collected NG and conflict paths.
Status build_nested_group_routing_plan_from_candidates(
        const ColumnVariant& variant, const std::vector<std::string>& ng_candidate_paths,
        const std::vector<std::string>& conflict_candidate_paths, NestedGroupRoutingPlan* plan);

// Collect NG routing metadata from variant content:
// - out_ng_paths: all NG candidate paths
// - out_conflict_paths: NG paths that have ARRAY<OBJECT> vs non-array structural conflicts
// Both outputs are de-duplicated and sorted.
Status collect_nested_group_routing_paths_from_variant_jsonb(
        const ColumnVariant& variant, std::vector<std::string>* out_ng_paths,
        std::vector<std::string>* out_conflict_paths);

// Get the current global conflict policy (driven by config).
NestedGroupConflictPolicy get_nested_group_conflict_policy();

// Shared helpers for conflict reporting / validation across write and compaction planners.
std::string format_nested_group_conflict_paths(const std::vector<std::string>& conflict_paths);
Status validate_nested_group_conflicts(const std::vector<std::string>& conflict_paths,
                                       NestedGroupConflictPolicy policy);

} // namespace doris::segment_v2
