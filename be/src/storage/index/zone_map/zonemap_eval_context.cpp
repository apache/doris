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

namespace doris {

const ZoneMapEvalContext::SlotZoneMap* ZoneMapEvalContext::lookup(
        const std::vector<int>& slot_index) const {
    if (slot_index.size() == 1) {
        auto it = slots.find(slot_index[0]);
        return it == slots.end() ? nullptr : &it->second;
    }
    auto it = nested_slots.find(slot_index);
    return it == nested_slots.end() ? nullptr : &it->second;
}

std::shared_ptr<const segment_v2::ZoneMap> ZoneMapEvalContext::zone_map(
        const std::vector<int>& slot_index) const {
    const auto* szm = lookup(slot_index);
    return szm ? szm->zone_map : nullptr;
}

DataTypePtr ZoneMapEvalContext::data_type(const std::vector<int>& slot_index) const {
    const auto* szm = lookup(slot_index);
    return szm ? szm->data_type : nullptr;
}

bool ZoneMapEvalContext::floating_nan_count_unknown(const std::vector<int>& slot_index) const {
    const auto* szm = lookup(slot_index);
    return szm != nullptr && szm->floating_nan_count_unknown;
}

void ZoneMapEvalStats::merge_page_eval_stats(const ZoneMapEvalStats& src) {
    // Page-level evaluation repeats the same conjuncts for many pages. Keep structural
    // diagnostics once per column, while operation counters still reflect actual page checks.
    unusable_zonemap_eval_count =
            std::max(unusable_zonemap_eval_count, src.unusable_zonemap_eval_count);
    in_zonemap_point_check_count += src.in_zonemap_point_check_count;
    in_zonemap_range_only_count += src.in_zonemap_range_only_count;
}

} // namespace doris
