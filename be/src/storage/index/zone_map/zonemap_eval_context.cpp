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

#include "storage/index/zone_map/zonemap_eval_context.h"

#include <algorithm>
#include <string>

#include "runtime/runtime_profile.h"

namespace doris {

const segment_v2::ZoneMap* ZoneMapEvalContext::zone_map(int slot_index) const {
    auto it = slots.find(slot_index);
    if (it == slots.end() || !it->second.zone_map.has_value()) {
        return nullptr;
    }
    return &*it->second.zone_map;
}

const DataTypePtr* ZoneMapEvalContext::data_type(int slot_index) const {
    auto it = slots.find(slot_index);
    if (it == slots.end()) {
        return nullptr;
    }
    return &it->second.data_type;
}

void ZoneMapEvalStats::merge_page_eval_stats(const ZoneMapEvalStats& src) {
    // Page-level evaluation repeats the same conjuncts for many pages. Keep structural
    // diagnostics once per column, while operation counters still reflect actual page checks.
    unsupported_expr_count = std::max(unsupported_expr_count, src.unsupported_expr_count);
    in_zonemap_point_check_count += src.in_zonemap_point_check_count;
    in_zonemap_range_only_count += src.in_zonemap_range_only_count;
}

void ZoneMapEvalStats::accumulate_into_profile(RuntimeProfile* profile) const {
    if (profile == nullptr) {
        return;
    }
    auto update_counter = [profile](const std::string& name, int64_t value) {
        if (value == 0) {
            return;
        }
        profile->add_counter(name, TUnit::UNIT)->update(value);
    };
    update_counter("ExprZoneMapUnsupportedExprs", unsupported_expr_count);
    update_counter("InZoneMapPointCheckCount", in_zonemap_point_check_count);
    update_counter("InZoneMapRangeOnlyCount", in_zonemap_range_only_count);
}

void ZoneMapEvalContext::accumulate_into_profile(RuntimeProfile* profile) const {
    stats.accumulate_into_profile(profile);
}

} // namespace doris
