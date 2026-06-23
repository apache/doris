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

#include <cstdint>
#include <vector>

#include "storage/rowset/rowset_fwd.h"

namespace doris {

class Tablet;

class BinlogCompactionPolicy {
public:
    static constexpr int8_t kBinlogCompactionMaxLevel = 3;

    // Binlog compaction selection rules (tiered, L0..LMax)
    //
    // Score / Permits
    // - For LMax, treat Base([0-x]) as score/permit=1, others use RowsetMeta::get_compaction_score().
    //
    // Trigger (all levels): merge when ANY holds
    // - size >= binlog_compaction_goal_size_mbytes * 1MB
    // - score >= binlog_compaction_file_count_threshold
    // - time >= binlog_compaction_time_threshold_seconds
    //
    // LMax "Base + `ENOUGH` + remaining" model (oldest -> newest)
    //   | Base([0-x]) | `ENOUGH` rowsets | remaining rowsets ... |
    // `ENOUGH` is computed dynamically on LMax (not persisted):
    //      (rowset_size >= goal_size) OR (rowset_score >= file_count_threshold)
    //
    // Input Rowsets selection:
    // - If physical rewrite trigger is NOT met: try quick compact first (requires Base([0-x])).
    // - If both quick compact and physical rewrite are possible: compare score and pick the higher.
    //
    // Quick compact output must be OVERLAPPING.
    int pick_input_rowsets(Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
                           int8_t compaction_level,
                           std::vector<RowsetSharedPtr>* input_rowsets) const;

    uint32_t calc_binlog_compaction_score(Tablet* tablet, int8_t* prefer_compaction_level) const;
    uint32_t calc_binlog_compaction_level_score(Tablet* tablet, int8_t level) const;

    void update_compaction_level(Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
                                 RowsetSharedPtr output_rowset);
};

} // namespace doris
