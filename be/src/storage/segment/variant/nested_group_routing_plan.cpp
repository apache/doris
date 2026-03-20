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

#include "storage/segment/variant/nested_group_routing_plan.h"

#include <algorithm>
#include <string_view>
#include <unordered_set>

#include "common/config.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_nullable.h"
#include "exec/common/variant_util.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {

// --------------------------------------------------------------------------
// Path prefix utilities
// --------------------------------------------------------------------------

static bool _is_excluded_by_prefixes(std::string_view path,
                                     const std::vector<std::string>& excluded_prefixes,
                                     bool exclude_all_paths) {
    if (exclude_all_paths) {
        return true;
    }
    for (const auto& prefix : excluded_prefixes) {
        if (nested_group_path_has_prefix(path, prefix)) {
            return true;
        }
    }
    return false;
}

bool NestedGroupRoutingPlan::is_excluded_subcolumn(const std::string& path) const {
    return _is_excluded_by_prefixes(path, ng_only_prefixes, exclude_all_subcolumns);
}

// --------------------------------------------------------------------------
// Routing plan builder helpers
// --------------------------------------------------------------------------

static std::vector<std::string> _compact_prefixes(std::vector<std::string> prefixes) {
    std::sort(prefixes.begin(), prefixes.end());
    prefixes.erase(std::unique(prefixes.begin(), prefixes.end()), prefixes.end());
    std::sort(prefixes.begin(), prefixes.end(), [](const std::string& lhs, const std::string& rhs) {
        return lhs.size() < rhs.size();
    });
    std::vector<std::string> compacted;
    for (auto& p : prefixes) {
        bool redundant = false;
        for (const auto& c : compacted) {
            if (nested_group_path_has_prefix(p, c)) {
                redundant = true;
                break;
            }
        }
        if (!redundant) {
            compacted.push_back(std::move(p));
        }
    }
    return compacted;
}

static bool _is_array_variant_type(const DataTypePtr& type) {
    if (!type) return false;
    auto base_type = variant_util::get_base_type_of_array(type);
    return base_type != nullptr &&
           remove_nullable(base_type)->get_primitive_type() == PrimitiveType::TYPE_VARIANT;
}

std::string format_nested_group_conflict_paths(const std::vector<std::string>& conflict_paths) {
    std::string paths_str;
    for (const auto& path : conflict_paths) {
        if (!paths_str.empty()) {
            paths_str += ", ";
        }
        paths_str += path;
    }
    return paths_str;
}

Status validate_nested_group_conflicts(const std::vector<std::string>& conflict_paths,
                                       NestedGroupConflictPolicy policy) {
    if (policy == NestedGroupConflictPolicy::ERROR && !conflict_paths.empty()) {
        return Status::InvalidArgument("NestedGroup conflict detected (policy=ERROR) at paths: {}",
                                       format_nested_group_conflict_paths(conflict_paths));
    }
    return Status::OK();
}

// Routing builder: only NON-conflict NG paths go into ng_only_prefixes.
// Conflict paths are NOT excluded from subcolumn writes so compaction/write
// can still preserve conflict-path payload in regular subcolumns.
static Status _build_ng_routing_from_columns(
        const ColumnVariant& variant, const std::vector<std::string>& ng_candidate_paths,
        const std::vector<std::string>& conflict_candidate_paths,
        std::vector<std::string>* ng_only_prefixes, bool* exclude_all_subcolumns,
        NestedGroupConflictPolicy* conflict_policy, bool* has_conflict_paths) {
    if (ng_only_prefixes == nullptr || exclude_all_subcolumns == nullptr ||
        conflict_policy == nullptr || has_conflict_paths == nullptr) {
        return Status::InvalidArgument("output argument is null");
    }

    ng_only_prefixes->clear();
    *exclude_all_subcolumns = false;
    *conflict_policy = get_nested_group_conflict_policy();
    *has_conflict_paths = !conflict_candidate_paths.empty();

    if (ng_candidate_paths.empty()) {
        return Status::OK();
    }

    RETURN_IF_ERROR(validate_nested_group_conflicts(conflict_candidate_paths, *conflict_policy));

    // Build the conflict set for quick lookup.
    std::unordered_set<std::string> conflict_set(conflict_candidate_paths.begin(),
                                                 conflict_candidate_paths.end());

    // Log conflict paths under DISCARD_SCALAR policy.
    if (!conflict_candidate_paths.empty()) {
        for (const auto& p : conflict_candidate_paths) {
            LOG(WARNING) << "NestedGroup conflict at path '" << p
                         << "': policy=DISCARD_SCALAR (prefer nested data; scalar payload on this "
                            "path may be dropped). The path remains in regular subcolumns for "
                            "cross-segment compatibility.";
        }
    }

    // Only NON-conflict NG paths go into ng_only_prefixes.
    // Conflict paths are kept as regular subcolumns to avoid data loss.
    for (const auto& path : ng_candidate_paths) {
        if (conflict_set.contains(path)) {
            continue; // Skip conflict paths — they stay as regular subcolumns.
        }
        ng_only_prefixes->emplace_back(path);
        // For root path that is purely array<variant>, exclude all subcolumns.
        if (is_root_nested_group_path(path) && _is_array_variant_type(variant.get_root_type())) {
            *exclude_all_subcolumns = true;
        }
    }

    *ng_only_prefixes = _compact_prefixes(std::move(*ng_only_prefixes));
    return Status::OK();
}

// --------------------------------------------------------------------------
// Public API
// --------------------------------------------------------------------------

Status build_nested_group_routing_plan(const ColumnVariant& variant, NestedGroupRoutingPlan* plan) {
    std::vector<std::string> ng_candidate_paths;
    std::vector<std::string> conflict_candidate_paths;
    RETURN_IF_ERROR(collect_nested_group_routing_paths_from_variant_jsonb(
            variant, &ng_candidate_paths, &conflict_candidate_paths));
    return build_nested_group_routing_plan_from_candidates(variant, ng_candidate_paths,
                                                           conflict_candidate_paths, plan);
}

Status build_nested_group_routing_plan_from_candidates(
        const ColumnVariant& variant, const std::vector<std::string>& ng_candidate_paths,
        const std::vector<std::string>& conflict_candidate_paths, NestedGroupRoutingPlan* plan) {
    if (plan == nullptr) {
        return Status::InvalidArgument("plan is null");
    }
    *plan = NestedGroupRoutingPlan {};

    RETURN_IF_ERROR(_build_ng_routing_from_columns(
            variant, ng_candidate_paths, conflict_candidate_paths, &plan->ng_only_prefixes,
            &plan->exclude_all_subcolumns, &plan->conflict_policy, &plan->has_conflict_paths));
    return Status::OK();
}

NestedGroupConflictPolicy get_nested_group_conflict_policy() {
    if (config::variant_nested_group_discard_scalar_on_conflict) {
        return NestedGroupConflictPolicy::DISCARD_SCALAR;
    }
    return NestedGroupConflictPolicy::ERROR;
}

} // namespace doris::segment_v2
