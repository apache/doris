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

#include "storage/segment/variant/nested_group_streaming_write_plan.h"

#include <algorithm>
#include <map>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "core/data_type/get_least_supertype.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "storage/segment/variant/variant_column_reader.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

using MutableGroupMap = std::map<std::string, std::unique_ptr<struct MutableGroup>>;
using PathToDataTypes = std::unordered_map<PathInData, DataTypes, PathInData::Hash>;

struct MutableGroup {
    std::string group_name;
    std::string physical_path;
    std::string logical_path;
    size_t depth = 1;
    std::map<std::string, DataTypes> child_types_by_rel_path;
    MutableGroupMap nested_groups;
};

void collect_group_paths(const NestedGroupReaders& readers, const std::string& physical_prefix,
                         const std::string& logical_prefix, MutableGroupMap* groups,
                         std::unordered_set<std::string>* ng_owned_paths) {
    for (const auto& [group_name, reader] : readers) {
        std::string physical_path =
                physical_prefix.empty()
                        ? group_name
                        : physical_prefix + nested_group_marker_token() + group_name;
        std::string logical_path =
                logical_prefix.empty() ? group_name : logical_prefix + "." + group_name;

        auto& group_holder = (*groups)[group_name];
        if (group_holder == nullptr) {
            group_holder = std::make_unique<MutableGroup>();
        }
        auto& group = *group_holder;
        group.group_name = group_name;
        group.physical_path = physical_path;
        group.logical_path = logical_path;
        group.depth = reader->depth;

        ng_owned_paths->insert(logical_path);
        for (const auto& [relative_path, child_reader] : reader->child_readers) {
            group.child_types_by_rel_path[relative_path].push_back(
                    child_reader->get_vec_data_type());
            ng_owned_paths->insert(logical_path + "." + relative_path);
        }

        collect_group_paths(reader->nested_group_readers, physical_path, logical_path,
                            &group.nested_groups, ng_owned_paths);
    }
}

void append_types_from_segment_reader(const VariantColumnReader& variant_reader,
                                      PathToDataTypes* regular_path_types, MutableGroupMap* groups,
                                      std::unordered_set<std::string>* ng_owned_paths) {
    PathToDataTypes current_types;
    variant_reader.get_subcolumns_types(&current_types);
    for (auto& [path, types] : current_types) {
        const std::string& path_str = path.get_path();
        if (path_str.empty() || contains_nested_group_marker(path_str) ||
            is_root_nested_group_path(path_str)) {
            continue;
        }
        auto& out_types = (*regular_path_types)[path];
        out_types.insert(out_types.end(), types.begin(), types.end());
    }
    collect_group_paths(variant_reader.get_nested_group_readers(), "", "", groups, ng_owned_paths);
}

void build_final_group_plan(const MutableGroupMap& groups,
                            std::vector<NestedGroupStreamingGroupWritePlan>* out_groups) {
    out_groups->clear();
    out_groups->reserve(groups.size());
    for (const auto& [_, group_ptr] : groups) {
        DCHECK(group_ptr != nullptr);
        NestedGroupStreamingGroupWritePlan group_plan;
        group_plan.group_name = group_ptr->group_name;
        group_plan.physical_path = group_ptr->physical_path;
        group_plan.logical_path = group_ptr->logical_path;
        group_plan.depth = group_ptr->depth;

        for (const auto& [relative_path, data_types] : group_ptr->child_types_by_rel_path) {
            DataTypePtr final_type;
            get_least_supertype_jsonb(data_types, &final_type);
            if (final_type == nullptr) {
                continue;
            }
            NestedGroupStreamingChildWritePlan child_plan;
            child_plan.relative_path = relative_path;
            child_plan.logical_path = group_ptr->logical_path + "." + relative_path;
            child_plan.relative_path_in_data = PathInData(relative_path);
            child_plan.logical_path_in_data = PathInData(child_plan.logical_path);
            child_plan.data_type = std::move(final_type);
            group_plan.children.push_back(std::move(child_plan));
        }

        build_final_group_plan(group_ptr->nested_groups, &group_plan.nested_groups);
        out_groups->push_back(std::move(group_plan));
    }
}

Status append_plan_from_rowset_reader(const RowsetReaderSharedPtr& input_rs_reader,
                                      int32_t variant_uid, PathToDataTypes* regular_path_types,
                                      MutableGroupMap* groups,
                                      std::unordered_set<std::string>* ng_owned_paths) {
    if (input_rs_reader == nullptr) {
        return Status::InvalidArgument("input rowset reader is null");
    }

    auto rowset = input_rs_reader->rowset();
    if (rowset == nullptr) {
        return Status::InvalidArgument("rowset reader returned null rowset");
    }

    SegmentCacheHandle segment_cache;
    RETURN_IF_ERROR(SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(rowset), &segment_cache));

    for (const auto& segment : segment_cache.get_segments()) {
        std::shared_ptr<ColumnReader> column_reader;
        OlapReaderStatistics stats;
        Status st = segment->get_column_reader(variant_uid, &column_reader, &stats);
        if (st.is<ErrorCode::NOT_FOUND>()) {
            continue;
        }
        RETURN_IF_ERROR(st);
        if (column_reader == nullptr) {
            continue;
        }
        auto* variant_reader = dynamic_cast<VariantColumnReader*>(column_reader.get());
        if (variant_reader == nullptr) {
            return Status::InternalError("column uid {} is not a VariantColumnReader", variant_uid);
        }
        RETURN_IF_ERROR(variant_reader->load_external_meta_once());
        append_types_from_segment_reader(*variant_reader, regular_path_types, groups,
                                         ng_owned_paths);
    }

    return Status::OK();
}

} // namespace

Status build_nested_group_streaming_write_plan(
        const std::vector<RowsetReaderSharedPtr>& input_rs_readers,
        const TabletColumn& variant_column, NestedGroupStreamingWritePlan* plan) {
    if (plan == nullptr) {
        return Status::InvalidArgument("plan is null");
    }

    *plan = NestedGroupStreamingWritePlan {};
    plan->conflict_policy = get_nested_group_conflict_policy();

    PathToDataTypes regular_path_types;
    MutableGroupMap groups;
    std::unordered_set<std::string> ng_owned_paths;

    for (const auto& input_rs_reader : input_rs_readers) {
        RETURN_IF_ERROR(append_plan_from_rowset_reader(input_rs_reader, variant_column.unique_id(),
                                                       &regular_path_types, &groups,
                                                       &ng_owned_paths));
    }

    build_final_group_plan(groups, &plan->nested_groups);
    for (const auto& group : plan->nested_groups) {
        if (is_root_nested_group_path(group.logical_path)) {
            plan->has_root_nested_group = true;
            break;
        }
    }

    std::unordered_set<std::string> conflict_path_set;
    for (const auto& [path, _] : regular_path_types) {
        const std::string& regular_path = path.get_path();
        for (const auto& ng_owned_path : ng_owned_paths) {
            if (nested_group_paths_overlap(regular_path, ng_owned_path)) {
                conflict_path_set.insert(regular_path);
                break;
            }
        }
    }

    plan->conflict_paths.assign(conflict_path_set.begin(), conflict_path_set.end());
    std::sort(plan->conflict_paths.begin(), plan->conflict_paths.end());
    plan->has_conflict_paths = !plan->conflict_paths.empty();
    RETURN_IF_ERROR(validate_nested_group_conflicts(plan->conflict_paths, plan->conflict_policy));

    for (const auto& [path, types] : regular_path_types) {
        if (conflict_path_set.contains(path.get_path())) {
            continue;
        }
        DataTypePtr final_type;
        get_least_supertype_jsonb(types, &final_type);
        if (final_type == nullptr) {
            continue;
        }
        NestedGroupStreamingRegularSubcolumnPlan regular_plan;
        regular_plan.path = path.get_path();
        regular_plan.path_in_data = path;
        regular_plan.data_type = std::move(final_type);
        plan->regular_subcolumns.push_back(std::move(regular_plan));
    }

    std::sort(plan->regular_subcolumns.begin(), plan->regular_subcolumns.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.path < rhs.path; });
    return Status::OK();
}

} // namespace doris::segment_v2
