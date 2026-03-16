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
#include <string>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/segment/variant/nested_group_routing_plan.h"
#include "util/json/path_in_data.h"

namespace doris {
class TabletColumn;

namespace segment_v2 {

struct NestedGroupStreamingChildWritePlan {
    std::string relative_path;
    std::string logical_path;
    PathInData relative_path_in_data;
    PathInData logical_path_in_data;
    DataTypePtr data_type;
};

struct NestedGroupStreamingGroupWritePlan {
    std::string group_name;
    std::string physical_path;
    std::string logical_path;
    size_t depth = 1;
    std::vector<NestedGroupStreamingChildWritePlan> children;
    std::vector<NestedGroupStreamingGroupWritePlan> nested_groups;
};

struct NestedGroupStreamingRegularSubcolumnPlan {
    std::string path;
    PathInData path_in_data;
    DataTypePtr data_type;
};

struct NestedGroupStreamingWritePlan {
    NestedGroupConflictPolicy conflict_policy = NestedGroupConflictPolicy::DISCARD_SCALAR;
    bool has_conflict_paths = false;
    bool has_root_nested_group = false;
    std::vector<std::string> conflict_paths;
    std::vector<NestedGroupStreamingRegularSubcolumnPlan> regular_subcolumns;
    std::vector<NestedGroupStreamingGroupWritePlan> nested_groups;

    bool can_remove_root_jsonb() const { return has_root_nested_group && !has_conflict_paths; }
};

Status build_nested_group_streaming_write_plan(
        const std::vector<RowsetReaderSharedPtr>& input_rs_readers,
        const TabletColumn& variant_column, NestedGroupStreamingWritePlan* plan);

} // namespace segment_v2
} // namespace doris
