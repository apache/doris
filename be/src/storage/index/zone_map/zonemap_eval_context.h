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

#include <memory>

#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/index/zone_map/zonemap_filter_result.h"

namespace doris {

class ZoneMapEvalStats {
public:
    void merge_page_eval_stats(const ZoneMapEvalStats& src);

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

class ZoneMapEvalContext {
public:
    struct SlotZoneMap {
        DataTypePtr data_type;
        std::shared_ptr<const segment_v2::ZoneMap> zone_map;
        // Parquet min/max does not expose whether a floating chunk also contains NaNs.
        bool floating_nan_count_unknown = false;

        // Every caller that fills this from Parquet statistics must go through here. Setting
        // data_type without the flag silently re-enables pruning that Parquet bounds cannot
        // support, so the two fields are assigned together on purpose.
        void set_data_type_from_parquet(const DataTypePtr& type) {
            data_type = type;
            // SlotDescriptor's test-only default constructor leaves the type null, and the callers
            // used to just copy it through.
            floating_nan_count_unknown = false;
            if (type == nullptr) {
                return;
            }
            const auto primitive_type = remove_nullable(type)->get_primitive_type();
            floating_nan_count_unknown =
                    primitive_type == TYPE_FLOAT || primitive_type == TYPE_DOUBLE;
        }
    };

    std::shared_ptr<const segment_v2::ZoneMap> zone_map(int slot_index) const;
    DataTypePtr data_type(int slot_index) const;
    bool floating_nan_count_unknown(int slot_index) const;

    phmap::flat_hash_map<int, SlotZoneMap> slots;

    mutable ZoneMapEvalStats stats;
};

inline ZoneMapFilterResult unsupported_zonemap_filter(const ZoneMapEvalContext& ctx) {
    ++ctx.stats.unusable_zonemap_eval_count;
    return ZoneMapFilterResult::kUnsupported;
}

} // namespace doris
