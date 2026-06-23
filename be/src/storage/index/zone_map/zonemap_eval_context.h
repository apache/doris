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

#include <optional>

#include "core/data_type/data_type.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/index/zone_map/zonemap_filter_result.h"

namespace doris {

class RuntimeProfile;

class ZoneMapEvalStats {
public:
    void merge_page_eval_stats(const ZoneMapEvalStats& src);
    void accumulate_into_profile(RuntimeProfile* profile) const;

    template <typename ReaderStatistics>
    void accumulate_to(ReaderStatistics* stats) const {
        if (stats == nullptr) {
            return;
        }
        stats->expr_zonemap_unusable_evals += unusable_zonemap_eval_count;
        stats->in_zonemap_point_check_count += in_zonemap_point_check_count;
        stats->in_zonemap_range_only_count += in_zonemap_range_only_count;
    }

    // Evaluations that reached expr-zonemap but could not use the current zone map/context.
    int64_t unusable_zonemap_eval_count = 0;
    int64_t in_zonemap_point_check_count = 0;
    int64_t in_zonemap_range_only_count = 0;
};

enum class ZoneMapMonotonicity {
    kIncreasing,
    kDecreasing,
    kConstant,
    kNotMonotonic,
    kUnsupported,
};

struct ExprDerivedZoneMap {
    DataTypePtr data_type;
    segment_v2::ZoneMap zone_map;
};

class ZoneMapEvalContext {
public:
    struct SlotZoneMap {
        DataTypePtr data_type;
        std::optional<segment_v2::ZoneMap> zone_map;
    };

    const segment_v2::ZoneMap* zone_map(int slot_index) const;
    const DataTypePtr* data_type(int slot_index) const;
    void accumulate_into_profile(RuntimeProfile* profile) const;

    phmap::flat_hash_map<int, SlotZoneMap> slots;

    mutable ZoneMapEvalStats stats;
};

inline ZoneMapFilterResult unsupported_zonemap_filter(const ZoneMapEvalContext& ctx) {
    ++ctx.stats.unusable_zonemap_eval_count;
    return ZoneMapFilterResult::kUnsupported;
}

} // namespace doris
