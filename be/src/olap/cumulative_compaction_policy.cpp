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

#include <string>

#include "olap/cumulative_compaction_policy.h"
#include "util/time.h"

#include <boost/algorithm/string.hpp>

namespace doris {

void OriginalCumulativeCompactionPolicy::update_cumulative_point(std::vector<RowsetSharedPtr>& input_rowsets,
                                  RowsetSharedPtr _output_rowset) {
    
    // use the version after end version of the last input rowsets to update cumulative point
    int64_t ret_cumulative_point = input_rowsets.back()->end_version() + 1;
    _tablet->set_cumulative_layer_point(ret_cumulative_point);
}

void OriginalCumulativeCompactionPolicy::pick_input_rowsets(
        std::vector<RowsetSharedPtr>& candidate_rowsets, const int64_t max_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score) {
    
    *compaction_score = 0;
    for (size_t i = 0; i < candidate_rowsets.size(); ++i) {
        RowsetSharedPtr rowset = candidate_rowsets[i];
        // check whether this rowset is delete version
        if (_tablet->version_for_delete_predicate(rowset->version())) {
            *last_delete_version = rowset->version();
            if (!input_rowsets->empty()) {
                // we meet a delete version, and there were other versions before.
                // we should compact those version before handling them over to base compaction
                break;
            } else {
                // we meet a delete version, and no other versions before, skip it and continue
                input_rowsets->clear();
                *compaction_score = 0;
                continue;
            }
        }
        if (*compaction_score >= max_compaction_score) {
            // got enough segments
            break;
        }
        *compaction_score += rowset->rowset_meta()->get_compaction_score();
        input_rowsets->push_back(rowset); 
    }
}

void OriginalCumulativeCompactionPolicy::calc_cumulative_compaction_score(const std::vector<RowsetMetaSharedPtr>& all_rowsets,
                                          const int64_t current_cumulative_point,
                                          uint32_t* score) {
    bool base_rowset_exist = false;
    const int64_t point = current_cumulative_point;
    for (auto& rs_meta : all_rowsets) {
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
        if (rs_meta->start_version() < point) {
            // all_rs_metas() is not sorted, so we use _continue_ other than _break_ here.
            continue;
        }
        *score += rs_meta->get_compaction_score();
    }

    // If base version does not exist, it may be that tablet is doing alter table. 
    // Do not select it and set *score = 0
    if (!base_rowset_exist) {
        *score = 0;
    }
}

void OriginalCumulativeCompactionPolicy::calculate_cumulative_point(const std::vector<RowsetMetaSharedPtr>& all_metas, 
        const int64_t kInvalidCumulativePoint, int64_t current_cumulative_point, int64_t* ret_cumulative_point) {

    *ret_cumulative_point = kInvalidCumulativePoint;
    if (current_cumulative_point != kInvalidCumulativePoint) {
        // only calculate the point once.
        // after that, cumulative point will be updated along with compaction process.
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

    int64_t prev_version = -1;
    for (const RowsetMetaSharedPtr& rs : existing_rss) {
        if (rs->version().first > prev_version + 1) {
            // There is a hole, do not continue
            break;
        }
        // break the loop if segments in this rowset is overlapping, or is a singleton.
        if (rs->is_segments_overlapping() || rs->is_singleton_delta()) {
            *ret_cumulative_point = rs->version().first;
            break;
        }

        prev_version = rs->version().second;
        *ret_cumulative_point = prev_version + 1;
    }
}

void CumulativeCompactionPolicy::pick_candicate_rowsets(int64_t skip_window_sec, 
        std::unordered_map<Version, RowsetSharedPtr, HashOfVersion>& rs_version_map,
        int64_t cumulative_point, std::vector<RowsetSharedPtr>* candidate_rowsets) {

    int64_t now = UnixSeconds();
    for (auto& it : rs_version_map) {
        // find all rowset version greater than cumulative_point and skip the create time in skip_window_sec
        if (it.first.first >= cumulative_point && (it.second->creation_time() + skip_window_sec < now)) {
            candidate_rowsets->push_back(it.second);
        }
    }
    std::sort(candidate_rowsets->begin(), candidate_rowsets->end(), Rowset::comparator);

}

std::shared_ptr<CumulativeCompactionPolicy> CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(std::string type,
                            Tablet* tablet) {

    CompactionPolicyType policy_type;
    _parse_cumulative_compaction_policy(type, &policy_type);

    if (policy_type == CUMULATIVE_ORIGINAL_POLICY) {
        return std::shared_ptr<CumulativeCompactionPolicy>(new OriginalCumulativeCompactionPolicy(tablet));
    }
    else if(policy_type == CUMULATIVE_UNIVERSAL_POLICY) {
        return std::shared_ptr<CumulativeCompactionPolicy>(new UniversalCumulativeCompactionPolicy(tablet));
    }

    return std::shared_ptr<CumulativeCompactionPolicy>(new OriginalCumulativeCompactionPolicy(tablet));
}

void CumulativeCompactionPolicyFactory::_parse_cumulative_compaction_policy(std::string type, CompactionPolicyType *policy_type) {

    boost::to_upper(type);
    if (type == CUMULATIVE_ORIGINAL_POLICY_TYPE) {
        *policy_type = CUMULATIVE_ORIGINAL_POLICY;
    }
    else if (type == CUMULATIVE_UNIVERSAL_POLICY_TYPE) {
        *policy_type = CUMULATIVE_UNIVERSAL_POLICY;
        LOG(FATAL) << "not supported policy type UNIVERSAL";

    } else {
        LOG(FATAL) << "parse cumulative compaction policy error " << type;
    }
}
}