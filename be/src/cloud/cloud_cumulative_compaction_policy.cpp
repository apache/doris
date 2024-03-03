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

#include "cloud/cloud_cumulative_compaction_policy.h"

#include <algorithm>
#include <list>
#include <ostream>
#include <string>

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "olap/olap_common.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"

namespace doris {

CloudSizeBasedCumulativeCompactionPolicy::CloudSizeBasedCumulativeCompactionPolicy(
        int64_t promotion_size, double promotion_ratio, int64_t promotion_min_size,
        int64_t compaction_min_size)
        : _promotion_size(promotion_size),
          _promotion_ratio(promotion_ratio),
          _promotion_min_size(promotion_min_size),
          _compaction_min_size(compaction_min_size) {}

int64_t CloudSizeBasedCumulativeCompactionPolicy::_level_size(const int64_t size) {
    if (size < 1024) return 0;
    int64_t max_level = (int64_t)1
                        << (sizeof(_promotion_size) * 8 - 1 - __builtin_clzl(_promotion_size / 2));
    if (size >= max_level) return max_level;
    return (int64_t)1 << (sizeof(size) * 8 - 1 - __builtin_clzl(size));
}

int CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets(
        CloudTablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        const int64_t max_compaction_score, const int64_t min_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score, bool allow_delete) {
    //size_t promotion_size = tablet->cumulative_promotion_size();
    auto max_version = tablet->max_version().first;
    int transient_size = 0;
    *compaction_score = 0;
    int64_t total_size = 0;
    for (auto& rowset : candidate_rowsets) {
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

    auto rs_begin = input_rowsets->begin();
    size_t new_compaction_score = *compaction_score;
    while (rs_begin != input_rowsets->end()) {
        auto& rs_meta = (*rs_begin)->rowset_meta();
        int current_level = _level_size(rs_meta->total_disk_size());
        int remain_level = _level_size(total_size - rs_meta->total_disk_size());
        // if current level less then remain level, input rowsets contain current rowset
        // and process return; otherwise, input rowsets do not contain current rowset.
        if (current_level <= remain_level) {
            break;
        }
        total_size -= rs_meta->total_disk_size();
        new_compaction_score -= rs_meta->get_compaction_score();
        ++rs_begin;
    }
    if (rs_begin == input_rowsets->end()) { // No suitable level size found in `input_rowsets`
        if (config::prioritize_query_perf_in_compaction && tablet->keys_type() != DUP_KEYS) {
            // While tablet's key type is not `DUP_KEYS`, compacting rowset in such tablets has a significant
            // positive impact on queries and reduces space amplification, so we ignore level limitation and
            // pick candidate rowsets as input rowsets.
            return transient_size;
        } else if (*compaction_score >= max_compaction_score) {
            // Score of `input_rowsets` exceed max compaction score, which means `input_rowsets` will never change and
            // this tablet will never execute cumulative compaction. MUST execute compaction on these `input_rowsets`
            // to reduce compaction score.
            RowsetSharedPtr rs_with_max_score;
            uint32_t max_score = 1;
            for (auto& rs : *input_rowsets) {
                if (rs->rowset_meta()->get_compaction_score() > max_score) {
                    max_score = rs->rowset_meta()->get_compaction_score();
                    rs_with_max_score = rs;
                }
            }
            if (rs_with_max_score) {
                input_rowsets->clear();
                input_rowsets->push_back(std::move(rs_with_max_score));
                *compaction_score = max_score;
                return transient_size;
            }
            // Exceeding max compaction score, do compaction on all candidate rowsets anyway
            return transient_size;
        }
    }
    input_rowsets->erase(input_rowsets->begin(), rs_begin);
    *compaction_score = new_compaction_score;

    VLOG_CRITICAL << "cumulative compaction size_based policy, compaction_score = "
                  << *compaction_score << ", total_size = "
                  << total_size
                  //<< ", calc promotion size value = " << promotion_size
                  << ", tablet = " << tablet->tablet_id() << ", input_rowset size "
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

int64_t CloudSizeBasedCumulativeCompactionPolicy::cloud_promotion_size(CloudTablet* t) const {
    int64_t promotion_size = t->base_size() * _promotion_ratio;
    // promotion_size is between _size_based_promotion_size and _size_based_promotion_min_size
    return promotion_size > _promotion_size       ? _promotion_size
           : promotion_size < _promotion_min_size ? _promotion_min_size
                                                  : promotion_size;
}

int64_t CloudSizeBasedCumulativeCompactionPolicy::new_cumulative_point(
        CloudTablet* tablet, const RowsetSharedPtr& output_rowset, Version& last_delete_version,
        int64_t last_cumulative_point) {
    TEST_INJECTION_POINT_RETURN_WITH_VALUE("new_cumulative_point", int64_t(0), output_rowset.get(),
                                           last_cumulative_point);
    // if rowsets have delete version, move to the last directly.
    // if rowsets have no delete version, check output_rowset total disk size satisfies promotion size.
    return output_rowset->start_version() == last_cumulative_point &&
                           (last_delete_version.first != -1 ||
                            output_rowset->data_disk_size() >= cloud_promotion_size(tablet))
                   ? output_rowset->end_version() + 1
                   : last_cumulative_point;
}

} // namespace doris
