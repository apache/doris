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

#include "olap/cumulative_compaction_policy.h"

#include <boost/algorithm/string.hpp>
#include <string>

#include "util/time.h"

namespace doris {

SizeBasedCumulativeCompactionPolicy::SizeBasedCumulativeCompactionPolicy(
        int64_t size_based_promotion_size, double size_based_promotion_ratio,
        int64_t size_based_promotion_min_size, int64_t size_based_compaction_lower_bound_size)
        : CumulativeCompactionPolicy(),
          _size_based_promotion_size(size_based_promotion_size),
          _size_based_promotion_ratio(size_based_promotion_ratio),
          _size_based_promotion_min_size(size_based_promotion_min_size),
          _size_based_compaction_lower_bound_size(size_based_compaction_lower_bound_size) {
    // init _levels by divide 2 between size_based_compaction_lower_bound_size and 1K
    // cu compaction handle file size less then size_based_compaction_lower_bound_size
    int64_t i_size = size_based_promotion_size / 2;

    while (i_size >= 1024) {
        _levels.push_back(i_size);
        i_size /= 2;
    }
}

void SizeBasedCumulativeCompactionPolicy::calculate_cumulative_point(
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

    if (tablet->tablet_state() == TABLET_RUNNING) {
        // check base rowset first version must be zero
        // for tablet which state is not TABLET_RUNNING, there may not have base version.
        CHECK((*base_rowset_meta)->start_version() == 0);

        int64_t promotion_size = 0;
        _calc_promotion_size(*base_rowset_meta, &promotion_size);

        int64_t prev_version = -1;
        for (const RowsetMetaSharedPtr& rs : existing_rss) {
            if (rs->version().first > prev_version + 1) {
                // There is a hole, do not continue
                break;
            }

            bool is_delete = tablet->version_for_delete_predicate(rs->version());

            // break the loop if segments in this rowset is overlapping.
            if (!is_delete && rs->is_segments_overlapping()) {
                *ret_cumulative_point = rs->version().first;
                break;
            }

            // check the rowset is whether less than promotion size
            if (!is_delete && rs->version().first != 0 && rs->total_disk_size() < promotion_size) {
                *ret_cumulative_point = rs->version().first;
                break;
            }

            // include one situation: When the segment is not deleted, and is singleton delta, and is NONOVERLAPPING, ret_cumulative_point increase
            prev_version = rs->version().second;
            *ret_cumulative_point = prev_version + 1;
        }
        VLOG_NOTICE
                << "cumulative compaction size_based policy, calculate cumulative point value = "
                << *ret_cumulative_point << ", calc promotion size value = " << promotion_size
                << " tablet = " << tablet->full_name();
    } else if (tablet->tablet_state() == TABLET_NOTREADY) {
        // tablet under alter process
        // we choose version next to the base version as cumulative point
        for (const RowsetMetaSharedPtr& rs : existing_rss) {
            if (rs->version().first > 0) {
                *ret_cumulative_point = rs->version().first;
                break;
            }
        }
    }
}

void SizeBasedCumulativeCompactionPolicy::_calc_promotion_size(RowsetMetaSharedPtr base_rowset_meta,
                                                               int64_t* promotion_size) {
    int64_t base_size = base_rowset_meta->total_disk_size();
    *promotion_size = base_size * _size_based_promotion_ratio;

    // promotion_size is between _size_based_promotion_size and _size_based_promotion_min_size
    if (*promotion_size >= _size_based_promotion_size) {
        *promotion_size = _size_based_promotion_size;
    } else if (*promotion_size <= _size_based_promotion_min_size) {
        *promotion_size = _size_based_promotion_min_size;
    }
    _refresh_tablet_size_based_promotion_size(*promotion_size);
}

void SizeBasedCumulativeCompactionPolicy::_refresh_tablet_size_based_promotion_size(
        int64_t promotion_size) {
    _tablet_size_based_promotion_size = promotion_size;
}

void SizeBasedCumulativeCompactionPolicy::update_cumulative_point(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
        RowsetSharedPtr output_rowset, Version& last_delete_version) {
    if (tablet->tablet_state() != TABLET_RUNNING) {
        // if tablet under alter process, do not update cumulative point
        return;
    }
    // if rowsets have delete version, move to the last directly
    if (last_delete_version.first != -1) {
        tablet->set_cumulative_layer_point(output_rowset->end_version() + 1);
    } else {
        // if rowsets have no delete version, check output_rowset total disk size
        // satisfies promotion size.
        size_t total_size = output_rowset->rowset_meta()->total_disk_size();
        if (total_size >= _tablet_size_based_promotion_size) {
            tablet->set_cumulative_layer_point(output_rowset->end_version() + 1);
        }
    }
}

void SizeBasedCumulativeCompactionPolicy::calc_cumulative_compaction_score(
        TabletState state, const std::vector<RowsetMetaSharedPtr>& all_metas,
        int64_t current_cumulative_point, uint32_t* score) {
    bool base_rowset_exist = false;
    const int64_t point = current_cumulative_point;
    int64_t promotion_size = 0;

    std::vector<RowsetMetaSharedPtr> rowset_to_compact;
    int64_t total_size = 0;

    // check the base rowset and collect the rowsets of cumulative part
    auto rs_meta_iter = all_metas.begin();
    RowsetMetaSharedPtr first_meta;
    int64_t first_version = INT64_MAX;
    for (; rs_meta_iter != all_metas.end(); rs_meta_iter++) {
        auto rs_meta = *rs_meta_iter;
        if (rs_meta->start_version() < first_version) {
            first_version = rs_meta->start_version();
            first_meta = rs_meta;
        }
        // check base rowset
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
        if (rs_meta->end_version() < point) {
            // all_rs_metas() is not sorted, so we use _continue_ other than _break_ here.
            continue;
        } else {
            // collect the rowsets of cumulative part
            total_size += rs_meta->total_disk_size();
            *score += rs_meta->get_compaction_score();
            rowset_to_compact.push_back(rs_meta);
        }
    }

    if (first_meta == nullptr) {
        *score = 0;
        return;
    }

    // Use "first"(not base) version to calc promotion size
    // because some tablet do not have base version(under alter operation)
    _calc_promotion_size(first_meta, &promotion_size);

    // If base version does not exist, but its state is RUNNING.
    // It is abnormal, do not select it and set *score = 0
    if (!base_rowset_exist && state == TABLET_RUNNING) {
        LOG(WARNING) << "tablet state is running but have no base version";
        *score = 0;
        return;
    }

    // if total_size is greater than promotion_size, return total score
    if (total_size >= promotion_size) {
        return;
    }

    // sort the rowsets of cumulative part
    std::sort(rowset_to_compact.begin(), rowset_to_compact.end(), RowsetMeta::comparator);

    // calculate the rowsets to do cumulative compaction
    // eg: size of rowset_to_compact are:
    // 128, 16, 16, 16
    // we will choose [16,16,16] to compact.
    for (auto& rs_meta : rowset_to_compact) {
        int current_level = _level_size(rs_meta->total_disk_size());
        int remain_level = _level_size(total_size - rs_meta->total_disk_size());
        // if current level less then remain level, score contains current rowset
        // and process return; otherwise, score does not contains current rowset.
        if (current_level <= remain_level) {
            return;
        }
        total_size -= rs_meta->total_disk_size();
        *score -= rs_meta->get_compaction_score();
    }
}

int SizeBasedCumulativeCompactionPolicy::pick_input_rowsets(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        const int64_t max_compaction_score, const int64_t min_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score) {
    size_t promotion_size = _tablet_size_based_promotion_size;
    int transient_size = 0;
    *compaction_score = 0;
    int64_t total_size = 0;
    for (size_t i = 0; i < candidate_rowsets.size(); ++i) {
        RowsetSharedPtr rowset = candidate_rowsets[i];
        // check whether this rowset is delete version
        if (tablet->version_for_delete_predicate(rowset->version())) {
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
                continue;
            }
        }
        if (*compaction_score >= max_compaction_score) {
            // got enough segments
            break;
        }
        *compaction_score += rowset->rowset_meta()->get_compaction_score();
        total_size += rowset->rowset_meta()->total_disk_size();

        transient_size += 1;
        input_rowsets->push_back(rowset);
    }

    if (total_size >= promotion_size) {
        return transient_size;
    }

    // if there is delete version, do compaction directly
    if (last_delete_version->first != -1) {
        if (input_rowsets->size() == 1) {
            auto rs_meta = input_rowsets->front()->rowset_meta();
            // if there is only one rowset and not overlapping,
            // we do not need to do cumulative compaction
            if (!rs_meta->is_segments_overlapping()) {
                input_rowsets->clear();
                *compaction_score = 0;
            }
        }
        return transient_size;
    }

    auto rs_iter = input_rowsets->begin();
    while (rs_iter != input_rowsets->end()) {
        auto rs_meta = (*rs_iter)->rowset_meta();
        int current_level = _level_size(rs_meta->total_disk_size());
        int remain_level = _level_size(total_size - rs_meta->total_disk_size());
        // if current level less then remain level, input rowsets contain current rowset
        // and process return; otherwise, input rowsets do not contain current rowset.
        if (current_level <= remain_level) {
            break;
        }
        total_size -= rs_meta->total_disk_size();
        *compaction_score -= rs_meta->get_compaction_score();

        rs_iter = input_rowsets->erase(rs_iter);
    }

    VLOG_CRITICAL << "cumulative compaction size_based policy, compaction_score = "
                  << *compaction_score << ", total_size = " << total_size
                  << ", calc promotion size value = " << promotion_size
                  << ", tablet = " << tablet->full_name() << ", input_rowset size "
                  << input_rowsets->size();

    // empty return
    if (input_rowsets->empty()) {
        return transient_size;
    }

    // if we have a sufficient number of segments, we should process the compaction.
    // otherwise, we check number of segments and total_size whether can do compaction.
    if (total_size < _size_based_compaction_lower_bound_size &&
        *compaction_score < min_compaction_score) {
        input_rowsets->clear();
        *compaction_score = 0;
    } else if (total_size >= _size_based_compaction_lower_bound_size &&
               input_rowsets->size() == 1) {
        auto rs_meta = input_rowsets->front()->rowset_meta();
        // if there is only one rowset and not overlapping,
        // we do not need to do compaction
        if (!rs_meta->is_segments_overlapping()) {
            input_rowsets->clear();
            *compaction_score = 0;
        }
    }
    return transient_size;
}

int SizeBasedCumulativeCompactionPolicy::_level_size(const int64_t size) {
    for (auto& i : _levels) {
        if (size >= i) {
            return i;
        }
    }
    return 0;
}

void CumulativeCompactionPolicy::pick_candidate_rowsets(
        const std::unordered_map<Version, RowsetSharedPtr, HashOfVersion>& rs_version_map,
        int64_t cumulative_point, std::vector<RowsetSharedPtr>* candidate_rowsets) {
    for (const auto& [version, rs] : rs_version_map) {
        if (version.first >= cumulative_point && rs->is_local()) {
            candidate_rowsets->push_back(rs);
        }
    }
    std::sort(candidate_rowsets->begin(), candidate_rowsets->end(), Rowset::comparator);
}

std::shared_ptr<CumulativeCompactionPolicy>
CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy() {
    return std::unique_ptr<CumulativeCompactionPolicy>(new SizeBasedCumulativeCompactionPolicy());
}

} // namespace doris
