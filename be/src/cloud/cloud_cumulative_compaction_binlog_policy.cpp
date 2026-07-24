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

#include "cloud/cloud_cumulative_compaction_binlog_policy.h"

#include "cloud/cloud_tablet.h"
#include "common/config.h"
#include "storage/compaction/cumulative_compaction_binlog_policy.h"
#include "storage/tablet/tablet.h"
#include "util/time.h"

namespace doris {

bool CloudBinlogCumulativeCompactionPolicy::is_compaction_enough(
        const RowsetMetaSharedPtr& rowset_meta) const {
    if (rowset_meta->compaction_level() !=
        BinlogCumulativeCompactionPolicy::kBinlogCompactionMaxLevel - 1) {
        return false;
    }
    if (rowset_meta->start_version() == 0) {
        return true;
    }
    return rowset_meta->data_disk_size() >=
                   config::binlog_compaction_goal_size_mbytes * 1024 * 1024 ||
           rowset_meta->get_compaction_score() >= config::binlog_compaction_file_count_threshold;
}

int64_t CloudBinlogCumulativeCompactionPolicy::new_cumulative_point(
        CloudTablet* tablet, const RowsetSharedPtr& output_rowset, Version& last_delete_version,
        int64_t last_cumulative_point) {
    if (output_rowset->num_segments() == 0 || !is_compaction_enough(output_rowset->rowset_meta())) {
        return last_cumulative_point;
    }
    return output_rowset->end_version() + 1;
}

int64_t CloudBinlogCumulativeCompactionPolicy::get_compaction_level(
        CloudTablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
        RowsetSharedPtr output_rowset) {
    DCHECK(!input_rowsets.empty()) << "tablet=" << tablet->tablet_id();
    int64_t first_level = input_rowsets.front()->rowset_meta()->compaction_level();
    for (size_t i = 1; i < input_rowsets.size(); ++i) {
        DCHECK_EQ(first_level, input_rowsets[i]->rowset_meta()->compaction_level())
                << "tablet=" << tablet->tablet_id();
        DCHECK_EQ(input_rowsets[i]->start_version(), input_rowsets[i - 1]->end_version() + 1)
                << "tablet=" << tablet->tablet_id();
    }

    if (first_level == BinlogCumulativeCompactionPolicy::kBinlogCompactionMaxLevel - 1) {
        return first_level;
    }
    return first_level + 1;
}

uint32_t CloudBinlogCumulativeCompactionPolicy::calc_binlog_compaction_level_score(
        CloudTablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        int8_t level) const {
    uint32_t score = 0;
    const int64_t point = tablet->cumulative_layer_point();
    for (const auto& rs : candidate_rowsets) {
        auto rs_meta = rs->rowset_meta();
        if (rs_meta->compaction_level() != level) {
            continue;
        }
        if (level == BinlogCumulativeCompactionPolicy::kBinlogCompactionMaxLevel - 1 &&
            point != Tablet::K_INVALID_CUMULATIVE_POINT && rs_meta->end_version() < point) {
            score += 1;
            continue;
        }
        score += rs_meta->get_compaction_score();
    }
    return score;
}

uint32_t CloudBinlogCumulativeCompactionPolicy::calc_binlog_compaction_score(
        CloudTablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        int8_t* compaction_level) const {
    uint32_t max_score = 0;
    int8_t max_level = -1;
    for (int8_t level = 0; level < BinlogCumulativeCompactionPolicy::kBinlogCompactionMaxLevel;
         ++level) {
        uint32_t score = calc_binlog_compaction_level_score(tablet, candidate_rowsets, level);
        if (score > max_score) {
            max_score = score;
            max_level = level;
        }
    }
    if (compaction_level != nullptr) {
        *compaction_level = max_level;
    }
    return max_score;
}

void CloudBinlogCumulativeCompactionPolicy::filter_new_visible_rowsets(
        const std::vector<RowsetSharedPtr>& candidate_rowsets,
        std::vector<RowsetSharedPtr>* output_rowsets) const {
    output_rowsets->clear();
    int64_t now = UnixSeconds();
    int64_t max_processable_old_version = 0;
    bool filter_new_rowset =
            candidate_rowsets.size() <= config::binlog_compaction_file_count_threshold;
    for (const auto& rs : candidate_rowsets) {
        if (filter_new_rowset && rs->rowset_meta()->is_singleton_delta() &&
            rs->rowset_meta()->newest_write_timestamp() +
                            config::binlog_compaction_wait_timesec_after_visible >
                    now) {
            continue;
        }
        max_processable_old_version = std::max(max_processable_old_version, rs->end_version());
    }

    output_rowsets->reserve(candidate_rowsets.size());
    for (const auto& rs : candidate_rowsets) {
        if (rs->start_version() <= max_processable_old_version) {
            output_rowsets->push_back(rs);
        }
    }
}

int64_t CloudBinlogCumulativeCompactionPolicy::pick_input_rowsets(
        CloudTablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        const int64_t max_compaction_score, const int64_t min_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score, bool allow_delete) {
    std::vector<RowsetSharedPtr> filtered_rowsets;
    filter_new_visible_rowsets(candidate_rowsets, &filtered_rowsets);

    int8_t compaction_level = -1;
    calc_binlog_compaction_score(tablet, filtered_rowsets, &compaction_level);
    if (compaction_level < 0) {
        *compaction_score = 0;
        return 0;
    }

    std::vector<RowsetSharedPtr> level_rowsets;
    level_rowsets.reserve(filtered_rowsets.size());
    for (const auto& rs : filtered_rowsets) {
        if (rs->rowset_meta()->compaction_level() != compaction_level) {
            continue;
        }
        if (!level_rowsets.empty() &&
            rs->start_version() != level_rowsets.back()->end_version() + 1) {
            LOG(WARNING) << "rowset is non-continuous in the same compaction_level of binlog "
                            "compaction. tablet="
                         << tablet->tablet_id()
                         << ", compaction_level=" << static_cast<int>(compaction_level)
                         << ", prev_version=" << level_rowsets.back()->version()
                         << ", next_version=" << rs->version();
            *compaction_score = 0;
            return 0;
        }
        level_rowsets.push_back(rs);
    }

    std::vector<RowsetSharedPtr> remaining_rowsets;
    remaining_rowsets.reserve(level_rowsets.size());
    const int64_t point = tablet->cumulative_layer_point();
    for (const auto& rs : level_rowsets) {
        if (compaction_level == BinlogCumulativeCompactionPolicy::kBinlogCompactionMaxLevel - 1 &&
            point != Tablet::K_INVALID_CUMULATIVE_POINT && rs->end_version() < point) {
            continue;
        }
        remaining_rowsets.push_back(rs);
    }

    std::vector<RowsetSharedPtr> picked_rowsets;
    picked_rowsets.reserve(remaining_rowsets.size());
    int transient_size = 0;
    int64_t total_size = 0;
    int64_t picked_score = 0;
    for (const auto& rs : remaining_rowsets) {
        if (transient_size >= max_compaction_score) {
            break;
        }
        picked_rowsets.push_back(rs);
        ++transient_size;
        total_size += rs->data_disk_size();
        picked_score += rs->rowset_meta()->get_compaction_score();
    }

    bool can_do_binlog_compaction =
            total_size >= config::binlog_compaction_goal_size_mbytes * 1024 * 1024 ||
            picked_score >= config::binlog_compaction_file_count_threshold ||
            (UnixMillis() - tablet->last_cumu_compaction_success_time()) / 1000 >=
                    config::binlog_compaction_time_threshold_seconds;
    if (transient_size < 2) {
        can_do_binlog_compaction = false;
    }
    if (can_do_binlog_compaction) {
        input_rowsets->swap(picked_rowsets);
        *compaction_score = picked_score;
        return transient_size;
    }

    input_rowsets->clear();
    *compaction_score = 0;
    return 0;
}

} // namespace doris
