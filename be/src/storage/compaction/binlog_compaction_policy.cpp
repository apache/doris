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

#include "storage/compaction/binlog_compaction_policy.h"

#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "util/time.h"

namespace doris {

int BinlogCompactionPolicy::pick_input_rowsets(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        int8_t compaction_level, std::vector<RowsetSharedPtr>* input_rowsets) const {
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

    // 2) Split `level_rowsets` into `full_enough_rowsets` and `remaining_rowsets`.
    //    - L0/L1: only physical rewrite.
    //    - LMax: Base([0-x]) + prefix ENOUGH rowsets are candidates for quick compact.
    std::vector<RowsetSharedPtr> full_enough_rowsets;
    std::vector<RowsetSharedPtr> remaining_rowsets;
    full_enough_rowsets.reserve(level_rowsets.size());
    remaining_rowsets.reserve(level_rowsets.size());

    bool find_base = false;
    int full_enough_size = 0;
    size_t idx = 0;
    if (compaction_level == kBinlogCompactionMaxLevel - 1) {
        if (!level_rowsets.empty() && level_rowsets[0]->start_version() == 0) {
            find_base = true;
            full_enough_rowsets.push_back(level_rowsets[0]);
            ++full_enough_size;
            idx = 1;
            for (; idx < level_rowsets.size(); ++idx) {
                const auto& rs = level_rowsets[idx];
                int64_t rs_score = rs->rowset_meta()->get_compaction_score();
                if (rs->data_disk_size() >=
                            config::binlog_compaction_goal_size_mbytes * 1024 * 1024 ||
                    rs_score >= config::binlog_compaction_file_count_threshold) {
                    full_enough_rowsets.push_back(rs);
                    ++full_enough_size;
                    continue;
                }
                break;
            }
            for (; idx < level_rowsets.size(); ++idx) {
                remaining_rowsets.push_back(level_rowsets[idx]);
            }
        } else {
            remaining_rowsets = level_rowsets;
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
    const int64_t max_binlog_permits = (config::total_permits_for_compaction_score *
                                                config::binlog_compaction_permits_percent +
                                        99) /
                                       100;
    for (const auto& rs : remaining_rowsets) {
        int64_t rs_score = rs->rowset_meta()->get_compaction_score();
        if (transient_size >= config::binlog_level_compaction_max_deltas ||
            compaction_score + rs_score > max_binlog_permits) {
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
    } else if ((UnixMillis() - tablet->last_binlog_compaction_success_time(compaction_level)) /
                       1000 >=
               config::binlog_compaction_time_threshold_seconds) {
        can_do_binlog_compaction = true;
    }

    // 5) LMax quick compact vs physical rewrite.
    if (compaction_level == kBinlogCompactionMaxLevel - 1 && find_base && full_enough_size > 1) {
        int64_t quick_merge_score = 1;
        for (int i = 1; i < full_enough_size; ++i) {
            quick_merge_score += full_enough_rowsets[i]->rowset_meta()->get_compaction_score();
        }

        if (!can_do_binlog_compaction || quick_merge_score > compaction_score) {
            input_rowsets->swap(full_enough_rowsets);
            return full_enough_size;
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

uint32_t BinlogCompactionPolicy::calc_binlog_compaction_level_score(Tablet* tablet,
                                                                    int8_t level) const {
    uint32_t score = 0;
    // Binlog tiered compaction score (L0..LMax)
    //
    //   L0/L1/... : | rowsets ... |
    //   LMax      : | Base([0-x]) | remaining rowsets ... |
    //
    // Base only performs meta-only merge (merge rowset meta), so get_compaction_score()
    // treats Base score as 1.
    for (const auto& [_, rs_meta] : tablet->tablet_meta()->all_rs_metas()) {
        if (!rs_meta->is_local() || rs_meta->compaction_level() != level) {
            continue;
        }
        score += rs_meta->get_compaction_score();
    }
    return score;
}

uint32_t BinlogCompactionPolicy::calc_binlog_compaction_score(
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

void BinlogCompactionPolicy::update_compaction_level(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
        RowsetSharedPtr output_rowset) {
    if (tablet->tablet_state() != TABLET_RUNNING || output_rowset->num_segments() == 0) {
        return;
    }

    int64_t first_level = 0;
    for (size_t i = 0; i < input_rowsets.size(); ++i) {
        int64_t cur_level = input_rowsets[i]->rowset_meta()->compaction_level();
        if (i == 0) {
            first_level = cur_level;
            continue;
        }

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
        // level max do not update compaction level
        output_rowset->rowset_meta()->set_compaction_level(first_level);
        return;
    }

    output_rowset->rowset_meta()->set_compaction_level(first_level + 1);
}

} // namespace doris
