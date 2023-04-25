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

#include <algorithm>
#include <list>
#include <ostream>
#include <string>

#include "common/logging.h"
#include "olap/olap_common.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"

namespace doris {

SizeBasedCumulativeCompactionPolicy::SizeBasedCumulativeCompactionPolicy(
        int64_t promotion_size, double promotion_ratio, int64_t promotion_min_size,
        int64_t compaction_min_size)
        : _promotion_size(promotion_size),
          _promotion_ratio(promotion_ratio),
          _promotion_min_size(promotion_min_size),
          _compaction_min_size(compaction_min_size) {}

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
        _calc_promotion_size(tablet, *base_rowset_meta, &promotion_size);

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

void SizeBasedCumulativeCompactionPolicy::_calc_promotion_size(Tablet* tablet,
                                                               RowsetMetaSharedPtr base_rowset_meta,
                                                               int64_t* promotion_size) {
    int64_t base_size = base_rowset_meta->total_disk_size();
    *promotion_size = base_size * _promotion_ratio;

    // promotion_size is between _promotion_size and _promotion_min_size
    if (*promotion_size >= _promotion_size) {
        *promotion_size = _promotion_size;
    } else if (*promotion_size <= _promotion_min_size) {
        *promotion_size = _promotion_min_size;
    }
    _refresh_tablet_promotion_size(tablet, *promotion_size);
}

void SizeBasedCumulativeCompactionPolicy::_refresh_tablet_promotion_size(Tablet* tablet,
                                                                         int64_t promotion_size) {
    tablet->set_cumulative_promotion_size(promotion_size);
}

void SizeBasedCumulativeCompactionPolicy::update_cumulative_point(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
        RowsetSharedPtr output_rowset, Version& last_delete_version) {
    if (tablet->tablet_state() != TABLET_RUNNING) {
        // if tablet under alter process, do not update cumulative point
        return;
    }

    int64_t promotion_size = tablet->cumulative_promotion_size();
    auto can_forward = [=](const RowsetMetaSharedPtr& rs_meta) {
        return rs_meta->has_delete_predicate() || (!rs_meta->is_segments_overlapping() &&
                                                   rs_meta->total_disk_size() >= promotion_size);
    };

    auto rowsets = tablet->pick_candidate_rowsets_to_cumulative_compaction();
    int64_t new_point = tablet->cumulative_layer_point();
    // first, forward cumulative_point if has delete predicate or size exceeded promotion_size
    for (auto& rs : rowsets) {
        if (rs->start_version() == new_point && can_forward(rs->rowset_meta())) {
            new_point = rs->end_version() + 1;
        } else {
            break;
        }
    }

    bool need_forward_cumulative_point = false;
    if (new_point == output_rowset->start_version()) {
        if (last_delete_version.first != -1) {
            new_point = output_rowset->end_version() + 1;
            need_forward_cumulative_point = true;
        } else if (output_rowset->rowset_meta()->total_disk_size() >= promotion_size) {
            new_point = output_rowset->end_version() + 1;
            need_forward_cumulative_point = true;
        }
    }

    if (need_forward_cumulative_point) {
        for (auto& rs : rowsets) {
            if (rs->start_version() < new_point) {
                continue;
            }

            if (rs->start_version() > new_point) {
                break;
            }

            if (can_forward(rs->rowset_meta())) {
                new_point = rs->end_version() + 1;
            }
        }
    }

    if (new_point > tablet->cumulative_layer_point()) {
        tablet->set_cumulative_layer_point(new_point);
        LOG(INFO) << "successfully forward cumulative_point to " << new_point
                  << ", output_rowset=" << output_rowset->version()
                  << ", tablet=" << tablet->full_name();
    } else {
        LOG(INFO) << "cannot forward cumulative_point, current point="
                  << tablet->cumulative_layer_point()
                  << ", out size=" << output_rowset->rowset_meta()->total_disk_size()
                  << ", promotion_size=" << promotion_size
                  << ", output_rowset=" << output_rowset->version()
                  << ", tablet=" << tablet->full_name();
    }
}

uint32_t SizeBasedCumulativeCompactionPolicy::_calc_max_score(
        const std::vector<RowsetSharedPtr>& rowsets, int64_t promotion_size,
        std::vector<RowsetSharedPtr>* max_score_rowsets) const {
    uint32_t max_score = 0;
    size_t rowsets_sz = rowsets.size();
    for (size_t idx = 0; idx < rowsets_sz;) {
        // firstly skip the rowsets whose size exceeds promoto_size
        for (; idx < rowsets_sz; ++idx) {
            if (rowsets[idx]->is_segments_overlapping() ||
                rowsets[idx]->rowset_meta()->total_disk_size() < promotion_size) {
                break;
            }
        }

        if (idx >= rowsets_sz) {
            break;
        }

        // get successive version
        std::vector<RowsetSharedPtr> rowset_to_compact;
        {
            rowset_to_compact.push_back(rowsets[idx]);
            size_t inner_idx = idx + 1;
            for (; inner_idx < rowsets_sz; ++inner_idx) {
                // break if the version isn't successive
                if (rowsets[inner_idx]->start_version() !=
                    rowset_to_compact.back()->end_version() + 1) {
                    break;
                }
                rowset_to_compact.push_back(rowsets[inner_idx]);
            }
            idx = inner_idx;
        }

        // calcuate compaction score of the successive rowsets
        uint32_t score = 0;
        int64_t total_size = 0;
        for (auto& rs : rowset_to_compact) {
            total_size += rs->rowset_meta()->total_disk_size();
            score += rs->rowset_meta()->get_compaction_score();
        }

        // pruning
        if (score <= max_score) {
            continue;
        }

        if (total_size < promotion_size) {
            // calculate the rowsets to do cumulative compaction
            // eg: size of rowset_to_compact are:
            // 128, 16, 16, 16
            // we will choose [16,16,16] to compact.
            for (auto& rs : rowset_to_compact) {
                int current_level = _level_size(rs->rowset_meta()->total_disk_size());
                int remain_level = _level_size(total_size - rs->rowset_meta()->total_disk_size());
                // if current level less then remain level, score contains current rowset
                // and process return; otherwise, score does not contains current rowset.
                if (current_level <= remain_level) {
                    break;
                }
                total_size -= rs->rowset_meta()->total_disk_size();
                score -= rs->rowset_meta()->get_compaction_score();
            }
        }

        if (score > max_score) {
            max_score = score;
            if (max_score_rowsets != nullptr) {
                *max_score_rowsets = std::move(rowset_to_compact);
            }
        }
    }

    return max_score;
}

uint32_t SizeBasedCumulativeCompactionPolicy::calc_cumulative_compaction_score(Tablet* tablet) {
    bool base_rowset_exist = false;
    int64_t promotion_size = 0;

    RowsetMetaSharedPtr first_meta;
    int64_t first_version = INT64_MAX;
    // NOTE: tablet._meta_lock is held
    auto& rs_metas = tablet->tablet_meta()->all_rs_metas();

    // check the base rowset
    for (auto& rs_meta : rs_metas) {
        if (rs_meta->start_version() < first_version) {
            first_version = rs_meta->start_version();
            first_meta = rs_meta;
        }
        // check base rowset
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
    }

    if (first_meta == nullptr) {
        return 0;
    }

    // Use "first"(not base) version to calc promotion size
    // because some tablet do not have base version(under alter operation)
    _calc_promotion_size(tablet, first_meta, &promotion_size);

    // If base version does not exist, but its state is RUNNING.
    // It is abnormal, do not select it and set *score = 0
    if (!base_rowset_exist && tablet->tablet_state() == TABLET_RUNNING) {
        LOG(WARNING) << "tablet state is running but have no base version";
        return 0;
    }

    auto rowsets = tablet->pick_rowsets_not_in_compaction();
    return _calc_max_score(rowsets, promotion_size);
}

int SizeBasedCumulativeCompactionPolicy::pick_input_rowsets(
        Tablet* tablet, std::vector<RowsetSharedPtr>& candidate_rowsets,
        const int64_t max_compaction_score, const int64_t min_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score) {
    size_t promotion_size = tablet->cumulative_promotion_size();
    {
        // candidate_rowsets may not be continuous
        // we need to choose the successive rowsets with max score.
        std::vector<RowsetSharedPtr> rowsets_to_compact;
        _calc_max_score(candidate_rowsets, promotion_size, &rowsets_to_compact);

        // change candidate_rowsets because the caller will use it
        candidate_rowsets = std::move(rowsets_to_compact);

        if (candidate_rowsets.empty()) {
            return 0;
        }
    }

    auto max_version = tablet->max_version().first;
    int transient_size = 0;
    *compaction_score = 0;
    int64_t total_size = 0;
    for (auto& rowset : candidate_rowsets) {
        // check whether this rowset is delete version
        if (rowset->rowset_meta()->has_delete_predicate()) {
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
        if (tablet->tablet_state() == TABLET_NOTREADY) {
            // If tablet under alter, keep latest 10 version so that base tablet max version
            // not merged in new tablet, and then we can copy data from base tablet
            if (rowset->version().second < max_version - 10) {
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
    if (total_size < _compaction_min_size && *compaction_score < min_compaction_score) {
        input_rowsets->clear();
        *compaction_score = 0;
    } else if (total_size >= _compaction_min_size && input_rowsets->size() == 1) {
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

int64_t SizeBasedCumulativeCompactionPolicy::_level_size(const int64_t size) const {
    if (size < 1024) return 0;
    int64_t max_level = (int64_t)1
                        << (sizeof(_promotion_size) * 8 - 1 - __builtin_clzl(_promotion_size / 2));
    if (size >= max_level) return max_level;
    return (int64_t)1 << (sizeof(size) * 8 - 1 - __builtin_clzl(size));
}

std::shared_ptr<CumulativeCompactionPolicy>
CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy() {
    return std::unique_ptr<CumulativeCompactionPolicy>(new SizeBasedCumulativeCompactionPolicy());
}

} // namespace doris
