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

#include "olap/cumulative_compaction_time_series_policy.h"

#include <algorithm>

#include "common/logging.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "util/time.h"
namespace doris {

uint32_t TimeSeriesCumulativeCompactionPolicy::calc_cumulative_compaction_score(Tablet* tablet) {
    uint32_t score = 0;
    uint32_t level0_score = 0;
    bool base_rowset_exist = false;
    const int64_t point = tablet->cumulative_layer_point();

    int64_t level0_total_size = 0;
    RowsetMetaSharedPtr first_meta;
    int64_t first_version = INT64_MAX;
    std::list<RowsetMetaSharedPtr> checked_rs_metas;
    // NOTE: tablet._meta_lock is hold
    auto& rs_metas = tablet->tablet_meta()->all_rs_metas();
    // check the base rowset and collect the rowsets of cumulative part
    for (auto& rs_meta : rs_metas) {
        if (rs_meta->start_version() < first_version) {
            first_version = rs_meta->start_version();
            first_meta = rs_meta;
        }
        // check base rowset
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
        if (rs_meta->end_version() < point || !rs_meta->is_local()) {
            // all_rs_metas() is not sorted, so we use _continue_ other than _break_ here.
            continue;
        } else {
            // collect the rowsets of cumulative part
            score += rs_meta->get_compaction_score();
            if (rs_meta->compaction_level() == 0) {
                level0_total_size += rs_meta->total_disk_size();
                level0_score += rs_meta->get_compaction_score();
            } else {
                checked_rs_metas.push_back(rs_meta);
            }
        }
    }

    if (first_meta == nullptr) {
        return 0;
    }

    // If base version does not exist, but its state is RUNNING.
    // It is abnormal, do not select it and set *score = 0
    if (!base_rowset_exist && tablet->tablet_state() == TABLET_RUNNING) {
        LOG(WARNING) << "tablet state is running but have no base version";
        return 0;
    }

    // Condition 1: the size of input files for compaction meets the requirement of parameter compaction_goal_size
    int64_t compaction_goal_size_mbytes =
            tablet->tablet_meta()->time_series_compaction_goal_size_mbytes();
    if (level0_total_size >= compaction_goal_size_mbytes * 1024 * 1024) {
        return score;
    }

    // Condition 2: the number of input files reaches the threshold specified by parameter compaction_file_count_threshold
    if (level0_score >= tablet->tablet_meta()->time_series_compaction_file_count_threshold()) {
        return score;
    }

    // Condition 3: level1 achieve compaction_goal_size
    if (tablet->tablet_meta()->time_series_compaction_level_threshold() >= 2) {
        checked_rs_metas.sort([](const RowsetMetaSharedPtr& a, const RowsetMetaSharedPtr& b) {
            return a->version().first < b->version().first;
        });
        int32_t rs_meta_count = 0;
        int64_t continuous_size = 0;
        for (const auto& rs_meta : checked_rs_metas) {
            rs_meta_count++;
            continuous_size += rs_meta->total_disk_size();
            if (rs_meta_count >= 2) {
                if (continuous_size >= compaction_goal_size_mbytes * 1024 * 1024) {
                    return score;
                }
            }
        }
    }

    int64_t now = UnixMillis();
    int64_t last_cumu = tablet->last_cumu_compaction_success_time();
    if (last_cumu != 0) {
        int64_t cumu_interval = now - last_cumu;

        // Condition 4: the time interval between compactions exceeds the value specified by parameter _compaction_time_threshold_second
        if (cumu_interval >
            (tablet->tablet_meta()->time_series_compaction_time_threshold_seconds() * 1000)) {
            return score;
        }
    } else if (score > 0) {
        // If the compaction process has not been successfully executed,
        // the condition for triggering compaction based on the last successful compaction time (condition 3) will never be met
        tablet->set_last_cumu_compaction_success_time(now);
    }

    // Condition 5: If there is a continuous set of empty rowsets, prioritize merging.
    std::vector<RowsetSharedPtr> input_rowsets;
    std::vector<RowsetSharedPtr> candidate_rowsets =
            tablet->pick_candidate_rowsets_to_cumulative_compaction();
    tablet->calc_consecutive_empty_rowsets(
            &input_rowsets, candidate_rowsets,
            tablet->tablet_meta()->time_series_compaction_empty_rowsets_threshold());
    if (!input_rowsets.empty()) {
        return score;
    }

    return 0;
}

void TimeSeriesCumulativeCompactionPolicy::calculate_cumulative_point(
        Tablet* tablet, const std::vector<RowsetMetaSharedPtr>& all_metas,
        int64_t current_cumulative_point, int64_t* ret_cumulative_point) {
    *ret_cumulative_point = Tablet::K_INVALID_CUMULATIVE_POINT;
    if (current_cumulative_point != Tablet::K_INVALID_CUMULATIVE_POINT) {
        // only calculate the point once.
        // after that, cumulative point will be updated along with compaction process.
        return;
    }
    // empty return
    if (all_metas.empty()) {
        return;
    }

    std::list<RowsetMetaSharedPtr> existing_rss;
    for (auto& rs : all_metas) {
        existing_rss.emplace_back(rs);
    }

    // sort the existing rowsets by version in ascending order
    existing_rss.sort([](const RowsetMetaSharedPtr& a, const RowsetMetaSharedPtr& b) {
        // simple because 2 versions are certainly not overlapping
        return a->version().first < b->version().first;
    });

    // calculate promotion size
    auto base_rowset_meta = existing_rss.begin();

    if (tablet->tablet_state() != TABLET_RUNNING) return;
    // check base rowset first version must be zero
    // for tablet which state is not TABLET_RUNNING, there may not have base version.
    CHECK((*base_rowset_meta)->start_version() == 0);

    int64_t prev_version = -1;
    for (const RowsetMetaSharedPtr& rs : existing_rss) {
        if (rs->version().first > prev_version + 1) {
            // There is a hole, do not continue
            break;
        }

        bool is_delete = rs->has_delete_predicate();

        // break the loop if segments in this rowset is overlapping.
        if (!is_delete && rs->is_segments_overlapping()) {
            *ret_cumulative_point = rs->version().first;
            break;
        }

        // check if the rowset has already been compacted
        // [2-11] : rowset has been compacted
        if (!is_delete && rs->version().first == rs->version().second) {
            *ret_cumulative_point = rs->version().first;
            break;
        }

        // check if the rowset has been compacted, but it is a empty rowset
        if (!is_delete && rs->version().first != 0 && rs->version().first != rs->version().second &&
            rs->num_segments() == 0) {
            *ret_cumulative_point = rs->version().first;
            break;
        }

        // upgrade: [0 0 2 1 1 0 0]
        if (!is_delete && tablet->tablet_meta()->time_series_compaction_level_threshold() >= 2 &&
            rs->compaction_level() == 1) {
            *ret_cumulative_point = rs->version().first;
            break;
        }

        // include one situation: When the segment is not deleted, and is singleton delta, and is NONOVERLAPPING, ret_cumulative_point increase
        prev_version = rs->version().second;
        *ret_cumulative_point = prev_version + 1;
    }
    VLOG_NOTICE << "cumulative compaction time serires policy, calculate cumulative point value = "
                << *ret_cumulative_point << " tablet = " << tablet->tablet_id();
}

int TimeSeriesCumulativeCompactionPolicy::pick_input_rowsets(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        const int64_t max_compaction_score, const int64_t min_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score, bool allow_delete) {
    if (tablet->tablet_state() == TABLET_NOTREADY) {
        return 0;
    }
    input_rowsets->clear();

    int64_t compaction_goal_size_mbytes =
            tablet->tablet_meta()->time_series_compaction_goal_size_mbytes();

    int transient_size = 0;
    *compaction_score = 0;
    int64_t total_size = 0;

    // when single replica compaction is enabled and BE1 fetchs merged rowsets from BE2, and then BE2 goes offline.
    // BE1 should performs compaction on its own, the time series compaction may re-compact previously fetched rowsets.
    // time series compaction policy needs to skip over the fetched rowset
    const auto& first_rowset_iter = std::find_if(
            candidate_rowsets.begin(), candidate_rowsets.end(), [](const RowsetSharedPtr& rs) {
                return rs->start_version() == rs->end_version() || rs->num_segments() == 0;
            });
    for (auto it = first_rowset_iter; it != candidate_rowsets.end(); ++it) {
        const auto& rowset = *it;
        // check whether this rowset is delete version
        if (!allow_delete && rowset->rowset_meta()->has_delete_predicate()) {
            *last_delete_version = rowset->version();
            if (!input_rowsets->empty()) {
                // we meet a delete version, and there were other versions before.
                // we should compact those version before handling them over to base compaction
                break;
            } else {
                // we meet a delete version, and no other versions before, skip it and continue
                input_rowsets->clear();
                *compaction_score = 0;
                transient_size = 0;
                total_size = 0;
                continue;
            }
        }

        *compaction_score += rowset->rowset_meta()->get_compaction_score();
        total_size += rowset->rowset_meta()->total_disk_size();

        transient_size += 1;
        input_rowsets->push_back(rowset);

        // Condition 1: the size of input files for compaction meets the requirement of parameter compaction_goal_size
        if (total_size >= (compaction_goal_size_mbytes * 1024 * 1024)) {
            if (input_rowsets->size() == 1 &&
                !input_rowsets->front()->rowset_meta()->is_segments_overlapping()) {
                // Only 1 non-overlapping rowset, skip it
                input_rowsets->clear();
                *compaction_score = 0;
                total_size = 0;
                continue;
            }
            return transient_size;
        }
    }

    // if there is delete version, do compaction directly
    if (last_delete_version->first != -1) {
        // if there is only one rowset and not overlapping,
        // we do not need to do cumulative compaction
        if (input_rowsets->size() == 1 &&
            !input_rowsets->front()->rowset_meta()->is_segments_overlapping()) {
            input_rowsets->clear();
            *compaction_score = 0;
        }
        return transient_size;
    }

    // Condition 2: the number of input files reaches the threshold specified by parameter compaction_file_count_threshold
    if (*compaction_score >= tablet->tablet_meta()->time_series_compaction_file_count_threshold()) {
        return transient_size;
    }

    // Condition 3: level1 achieve compaction_goal_size
    std::vector<RowsetSharedPtr> level1_rowsets;
    if (tablet->tablet_meta()->time_series_compaction_level_threshold() >= 2) {
        int64_t continuous_size = 0;
        for (const auto& rowset : candidate_rowsets) {
            const auto& rs_meta = rowset->rowset_meta();
            if (rs_meta->compaction_level() == 0) {
                break;
            }
            level1_rowsets.push_back(rowset);
            continuous_size += rs_meta->total_disk_size();
            if (level1_rowsets.size() >= 2) {
                if (continuous_size >= compaction_goal_size_mbytes * 1024 * 1024) {
                    input_rowsets->swap(level1_rowsets);
                    return input_rowsets->size();
                }
            }
        }
    }

    int64_t now = UnixMillis();
    int64_t last_cumu = tablet->last_cumu_compaction_success_time();
    if (last_cumu != 0) {
        int64_t cumu_interval = now - last_cumu;

        // Condition 4: the time interval between compactions exceeds the value specified by parameter compaction_time_threshold_second
        if (cumu_interval >
            (tablet->tablet_meta()->time_series_compaction_time_threshold_seconds() * 1000)) {
            if (tablet->tablet_meta()->time_series_compaction_level_threshold() >= 2) {
                if (input_rowsets->empty() && level1_rowsets.size() >= 2) {
                    input_rowsets->swap(level1_rowsets);
                    return input_rowsets->size();
                }
            }
            return transient_size;
        }
    }

    input_rowsets->clear();
    // Condition 5: If their are many empty rowsets, maybe should be compacted
    tablet->calc_consecutive_empty_rowsets(
            input_rowsets, candidate_rowsets,
            tablet->tablet_meta()->time_series_compaction_empty_rowsets_threshold());
    if (!input_rowsets->empty()) {
        VLOG_NOTICE << "tablet is " << tablet->tablet_id()
                    << ", there are too many consecutive empty rowsets, size is "
                    << input_rowsets->size();
        return 0;
    }

    *compaction_score = 0;

    return 0;
}

void TimeSeriesCumulativeCompactionPolicy::update_cumulative_point(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
        RowsetSharedPtr output_rowset, Version& last_delete_version) {
    if (tablet->tablet_state() != TABLET_RUNNING || output_rowset->num_segments() == 0) {
        // if tablet under alter process, do not update cumulative point
        // if the merged output rowset is empty, do not update cumulative point
        return;
    }

    if (tablet->tablet_meta()->time_series_compaction_level_threshold() >= 2 &&
        output_rowset->rowset_meta()->compaction_level() < 2) {
        return;
    }

    tablet->set_cumulative_layer_point(output_rowset->end_version() + 1);
}

void TimeSeriesCumulativeCompactionPolicy::update_compaction_level(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
        RowsetSharedPtr output_rowset) {
    if (tablet->tablet_state() != TABLET_RUNNING || output_rowset->num_segments() == 0) {
        return;
    }

    int64_t first_level = 0;
    for (size_t i = 0; i < input_rowsets.size(); i++) {
        int64_t cur_level = input_rowsets[i]->rowset_meta()->compaction_level();
        if (i == 0) {
            first_level = cur_level;
        } else {
            if (first_level != cur_level) {
                LOG(ERROR) << "Failed to check compaction level, first_level: " << first_level
                           << ", cur_level: " << cur_level;
            }
        }
    }

    output_rowset->rowset_meta()->set_compaction_level(first_level + 1);
}

} // namespace doris