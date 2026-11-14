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

#include <map>
#include <memory>
#include <vector>

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

    // Unified lookup for every slot, keyed by a slot path: a single element is the top-level column
    // id, while more than one addresses a nested STRUCT subfield as [column_id, field_ordinal, ...]
    // from root to leaf. Returns nullptr when no zone map is available, so callers fall back to
    // "unsupported" without branching on nesting. Top-level (size-1) lookups stay on the fast
    // flat_hash_map; only nested paths touch the std::map, so the common column path pays no
    // tree-lookup penalty.
    const SlotZoneMap* lookup(const std::vector<int>& slot_index) const;

    // Accessors keyed by the unified slot path (a single element is the top-level column id; more
    // than one addresses a nested STRUCT subfield as [column_id, field_ordinal, ...]). They are thin
    // wrappers over lookup, so both top-level and nested slots share one path and one dispatch.
    std::shared_ptr<const segment_v2::ZoneMap> zone_map(const std::vector<int>& slot_index) const;
    DataTypePtr data_type(const std::vector<int>& slot_index) const;
    bool floating_nan_count_unknown(const std::vector<int>& slot_index) const;

    // Top-level zone maps, keyed by column id. Backed by flat_hash_map for the common, hot path.
    phmap::flat_hash_map<int, SlotZoneMap> slots;
    // Structured-field zone maps for STRUCT columns, keyed by the full slot path (column id followed
    // by field ordinals). Backed by std::map because the vector key is orderable but not hashable;
    // STRUCT subfields are far less frequent than top-level columns.
    std::map<std::vector<int>, SlotZoneMap> nested_slots;

    mutable ZoneMapEvalStats stats;
};

inline ZoneMapFilterResult unsupported_zonemap_filter(const ZoneMapEvalContext& ctx) {
    ++ctx.stats.unusable_zonemap_eval_count;
    return ZoneMapFilterResult::kUnsupported;
}

} // namespace doris
