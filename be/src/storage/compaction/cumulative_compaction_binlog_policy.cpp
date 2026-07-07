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

#include "storage/compaction/cumulative_compaction_binlog_policy.h"

#include <algorithm>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "util/time.h"

namespace doris {

bool BinlogCumulativeCompactionPolicy::is_compaction_enough(
        const RowsetMetaSharedPtr& rowset_meta) const {
    if (rowset_meta->compaction_level() != kBinlogCompactionMaxLevel - 1) {
        return false;
    }
    if (rowset_meta->start_version() == 0) {
        return true;
    }
    return rowset_meta->data_disk_size() >=
                   config::binlog_compaction_goal_size_mbytes * 1024 * 1024 ||
           rowset_meta->get_compaction_score() >= config::binlog_compaction_file_count_threshold;
}

void BinlogCumulativeCompactionPolicy::calculate_cumulative_point(
        Tablet* tablet, const RowsetMetaMapContainer& all_rowsets,
        int64_t current_cumulative_point, int64_t* cumulative_point) {
    *cumulative_point = Tablet::K_INVALID_CUMULATIVE_POINT;
    if (current_cumulative_point != Tablet::K_INVALID_CUMULATIVE_POINT || all_rowsets.empty()) {
        return;
    }

    std::vector<RowsetMetaSharedPtr> rowset_metas;
    rowset_metas.reserve(all_rowsets.size());
    for (const auto& [_, rs_meta] : all_rowsets) {
        if (rs_meta->is_local()) {
            rowset_metas.emplace_back(rs_meta);
        }
    }
    std::sort(rowset_metas.begin(), rowset_metas.end(), RowsetMeta::comparator);

    int64_t prev_version = -1;
    for (const auto& rs_meta : rowset_metas) {
        if (*cumulative_point == Tablet::K_INVALID_CUMULATIVE_POINT) {
            *cumulative_point = rs_meta->start_version();
        }
        if (rs_meta->start_version() > prev_version + 1 || !is_compaction_enough(rs_meta)) {
            break;
        }
        prev_version = rs_meta->end_version();
        *cumulative_point = prev_version + 1;
    }

    VLOG_NOTICE << "binlog compaction policy, calculate cumulative point value="
                << *cumulative_point << ", tablet=" << tablet->tablet_id();
}

void BinlogCumulativeCompactionPolicy::update_cumulative_point(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
        RowsetSharedPtr output_rowset, Version& last_delete_version) {
    if (tablet->tablet_state() != TABLET_RUNNING || input_rowsets.empty() ||
        output_rowset->num_segments() == 0 ||
        output_rowset->rowset_meta()->compaction_level() != kBinlogCompactionMaxLevel - 1 ||
        !is_compaction_enough(output_rowset->rowset_meta())) {
        return;
    }

    const int64_t point = tablet->cumulative_layer_point();
    if (point == Tablet::K_INVALID_CUMULATIVE_POINT || output_rowset->start_version() > point) {
        return;
    }
    tablet->set_cumulative_layer_point(output_rowset->end_version() + 1);
}

int BinlogCumulativeCompactionPolicy::pick_input_rowsets(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        int8_t compaction_level, int64_t max_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets) const {
    // 1) Filter rowsets by `compaction_level`
    std::vector<RowsetSharedPtr> level_rowsets;
    level_rowsets.reserve(candidate_rowsets.size());
    for (const auto& rs : candidate_rowsets) {
        if (!rs->is_local() || rs->rowset_meta()->compaction_level() != compaction_level) {
            continue;
        }
        if (!level_rowsets.empty() &&
            rs->start_version() != level_rowsets.back()->end_version() + 1) {
            LOG(WARNING) << "rowset is non-continuous in the same compaction_level of binlog "
                            "compaction. tablet="
                         << tablet->tablet_id()
                         << ", compaction_level=" << std::to_string(compaction_level)
                         << ", prev_version=" << level_rowsets.back()->version()
                         << ", next_version=" << rs->version();
            return 0;
        }
        level_rowsets.push_back(rs);
    }

    // 2) Split `level_rowsets` into `compact_enough_rowsets` and `remaining_rowsets`.
    //    - L0/L1: only physical rewrite.
    //    - LMax: rowsets before cumulative point are candidates for quick compact.
    std::vector<RowsetSharedPtr> compact_enough_rowsets;
    std::vector<RowsetSharedPtr> remaining_rowsets;
    compact_enough_rowsets.reserve(level_rowsets.size());
    remaining_rowsets.reserve(level_rowsets.size());

    int compact_enough_size = 0;
    const int64_t point = tablet->cumulative_layer_point();
    if (compaction_level == kBinlogCompactionMaxLevel - 1 &&
        point != Tablet::K_INVALID_CUMULATIVE_POINT) {
        for (const auto& rs : level_rowsets) {
            if (rs->end_version() < point) {
                compact_enough_rowsets.push_back(rs);
                ++compact_enough_size;
                continue;
            }
            remaining_rowsets.push_back(rs);
        }
    } else {
        remaining_rowsets = level_rowsets;
    }

    // 3) Pick rowsets from `remaining_rowsets` (physical rewrite path).
    std::vector<RowsetSharedPtr> picked_rowsets;
    picked_rowsets.reserve(remaining_rowsets.size());
    int transient_size = 0;
    int64_t total_size = 0;
    int64_t compaction_score = 0;
    for (const auto& rs : remaining_rowsets) {
        int64_t rs_score = rs->rowset_meta()->get_compaction_score();
        if (transient_size >= max_compaction_score) {
            break;
        }
        picked_rowsets.push_back(rs);
        ++transient_size;
        total_size += rs->data_disk_size();
        compaction_score += rs_score;
    }

    // 4) Trigger check
    //    - L0/L1: only physical rewrite if trigger is met.
    //    - LMax: if physical rewrite trigger is NOT met, try quick compact; if both met, compare score.
    bool can_do_binlog_compaction = false;
    if (total_size >= config::binlog_compaction_goal_size_mbytes * 1024 * 1024 ||
        compaction_score >= config::binlog_compaction_file_count_threshold) {
        can_do_binlog_compaction = true;
    } else if ((UnixMillis() - tablet->last_cumu_compaction_success_time()) / 1000 >=
               config::binlog_compaction_time_threshold_seconds) {
        can_do_binlog_compaction = true;
    }

    // 5) LMax quick compact vs physical rewrite.
    if (compaction_level == kBinlogCompactionMaxLevel - 1 && compact_enough_size > 1) {
        int64_t quick_merge_score = compact_enough_size;

        if (!can_do_binlog_compaction || quick_merge_score > compaction_score) {
            input_rowsets->swap(compact_enough_rowsets);
            return compact_enough_size;
        }
    }

    if (transient_size < 2) {
        can_do_binlog_compaction = false;
    }

    if (can_do_binlog_compaction) {
        input_rowsets->swap(picked_rowsets);
        return transient_size;
    }

    input_rowsets->clear();
    return 0;
}

uint32_t BinlogCumulativeCompactionPolicy::calc_binlog_compaction_level_score(
        Tablet* tablet, int8_t level) const {
    uint32_t score = 0;
    const int64_t point = tablet->cumulative_layer_point();
    // Binlog tiered compaction score (L0..LMax)
    //
    //   L0/L1/... : | rowsets ... |
    //
    //   LMax:
    //        version: 0                                   point
    //                 |------------------------------------|------------------- ...
    //        rowsets: | compact enough  |  compact enough  | compacting rowsets ...
    //        score :  |       1         |       1          | get_compaction_score()
    //
    // Each rowset before cumulative point is compact-complete, and its score is treated as 1.
    for (const auto& [_, rs_meta] : tablet->tablet_meta()->all_rs_metas()) {
        if (!rs_meta->is_local() || rs_meta->compaction_level() != level) {
            continue;
        }
        if (level == kBinlogCompactionMaxLevel - 1 && point != Tablet::K_INVALID_CUMULATIVE_POINT &&
            rs_meta->end_version() < point) {
            score += 1;
            continue;
        }
        score += rs_meta->get_compaction_score();
    }
    return score;
}

uint32_t BinlogCumulativeCompactionPolicy::calc_binlog_compaction_level_score(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets, int8_t level) const {
    uint32_t score = 0;
    const int64_t point = tablet->cumulative_layer_point();
    for (const auto& rs : candidate_rowsets) {
        const auto& rs_meta = rs->rowset_meta();
        if (!rs->is_local() || rs_meta->compaction_level() != level) {
            continue;
        }
        if (level == kBinlogCompactionMaxLevel - 1 && point != Tablet::K_INVALID_CUMULATIVE_POINT &&
            rs_meta->end_version() < point) {
            score += 1;
            continue;
        }
        score += rs_meta->get_compaction_score();
    }
    return score;
}

uint32_t BinlogCumulativeCompactionPolicy::calc_binlog_compaction_score(
        Tablet* tablet, int8_t* prefer_compaction_level) const {
    uint32_t max_score = 0;
    int8_t max_level = -1;
    for (int8_t level = 0; level < kBinlogCompactionMaxLevel; ++level) {
        uint32_t score = calc_binlog_compaction_level_score(tablet, level);
        if (score > max_score) {
            max_score = score;
            max_level = level;
        }
    }
    if (prefer_compaction_level != nullptr) {
        *prefer_compaction_level = max_level;
    }
    return max_score;
}

uint32_t BinlogCumulativeCompactionPolicy::calc_binlog_compaction_score(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        int8_t* prefer_compaction_level) const {
    uint32_t max_score = 0;
    int8_t max_level = -1;
    for (int8_t level = 0; level < kBinlogCompactionMaxLevel; ++level) {
        uint32_t score = calc_binlog_compaction_level_score(tablet, candidate_rowsets, level);
        if (score > max_score) {
            max_score = score;
            max_level = level;
        }
    }
    if (prefer_compaction_level != nullptr) {
        *prefer_compaction_level = max_level;
    }
    return max_score;
}

int BinlogCumulativeCompactionPolicy::pick_input_rowsets(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        const int64_t max_compaction_score, const int64_t min_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score, bool allow_delete) {
    int8_t compaction_level = -1;
    calc_binlog_compaction_score(tablet, candidate_rowsets, &compaction_level);
    if (compaction_level < 0) {
        *compaction_score = 0;
        return 0;
    }

    int picked_score = pick_input_rowsets(tablet, candidate_rowsets, compaction_level,
                                          max_compaction_score, input_rowsets);
    *compaction_score = picked_score;
    return picked_score;
}

uint32_t BinlogCumulativeCompactionPolicy::calc_cumulative_compaction_score(Tablet* tablet) {
    return calc_binlog_compaction_score(tablet);
}

int64_t BinlogCumulativeCompactionPolicy::get_compaction_level(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
        RowsetSharedPtr output_rowset) {
    DCHECK(!input_rowsets.empty()) << "tablet=" << tablet->tablet_id();
    int64_t first_level = input_rowsets.front()->rowset_meta()->compaction_level();
    for (size_t i = 1; i < input_rowsets.size(); ++i) {
        int64_t cur_level = input_rowsets[i]->rowset_meta()->compaction_level();
        DCHECK_EQ(first_level, cur_level)
                << "Compaction level mismatch, first_level: " << first_level
                << ", cur_level: " << cur_level << ", tablet: " << tablet->tablet_id();
        if (first_level != cur_level) {
            LOG(WARNING) << "Failed to check compaction level, first_level: " << first_level
                         << ", cur_level: " << cur_level << ", tablet: " << tablet->tablet_id();
        }

        DCHECK_EQ(input_rowsets[i]->start_version(), input_rowsets[i - 1]->end_version() + 1)
                << "Binlog compaction input rowsets are non-continuous, prev_version: "
                << input_rowsets[i - 1]->version()
                << ", cur_version: " << input_rowsets[i]->version()
                << ", tablet: " << tablet->tablet_id();
        if (input_rowsets[i]->start_version() != input_rowsets[i - 1]->end_version() + 1) {
            LOG(WARNING) << "Failed to check binlog compaction input rowsets continuity, "
                         << "prev_version=" << input_rowsets[i - 1]->version()
                         << ", cur_version=" << input_rowsets[i]->version()
                         << ", tablet=" << tablet->tablet_id();
        }
    }

    if (first_level == kBinlogCompactionMaxLevel - 1) {
        return first_level;
    }
    return first_level + 1;
}

} // namespace doris
