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

#include "olap/cumulative_compaction.h"
#include "util/doris_metrics.h"
#include "util/time.h"

namespace doris {

CumulativeCompaction::CumulativeCompaction(TabletSharedPtr tablet)
    : Compaction(tablet),
      _cumulative_rowset_size_threshold(config::cumulative_compaction_budgeted_bytes)
{ }

CumulativeCompaction::~CumulativeCompaction() { }

OLAPStatus CumulativeCompaction::compact() {
    if (!_tablet->init_succeeded()) {
        return OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS;
    }

    MutexLock lock(_tablet->get_cumulative_lock(), TRY_LOCK);
    if (!lock.own_lock()) {
        LOG(INFO) << "The tablet is under cumulative compaction. tablet=" << _tablet->full_name();
        return OLAP_ERR_CE_TRY_CE_LOCK_ERROR;
    }

    // 1.calculate cumulative point 
    RETURN_NOT_OK(_tablet->calculate_cumulative_point());

    // 2. pick rowsets to compact
    RETURN_NOT_OK(pick_rowsets_to_compact());

    // 3. do cumulative compaction, merge rowsets
    RETURN_NOT_OK(do_compaction());

    // 4. set state to success
    _state = CompactionState::SUCCESS;

    // 5. set cumulative point
    _tablet->set_cumulative_layer_point(_input_rowsets.back()->end_version() + 1);
    
    // 6. garbage collect input rowsets after cumulative compaction 
    RETURN_NOT_OK(gc_unused_rowsets());

    // 7. add metric to cumulative compaction
    DorisMetrics::cumulative_compaction_deltas_total.increment(_input_rowsets.size());
    DorisMetrics::cumulative_compaction_bytes_total.increment(_input_rowsets_size);

    return OLAP_SUCCESS;
}

OLAPStatus CumulativeCompaction::pick_rowsets_to_compact() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    _tablet->pick_candicate_rowsets_to_cumulative_compaction(
        config::cumulative_compaction_skip_window_seconds, &candidate_rowsets);

    if (candidate_rowsets.empty()) {
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    RETURN_NOT_OK(check_version_continuity(candidate_rowsets));

    std::vector<RowsetSharedPtr> transient_rowsets;
    size_t compaction_score = 0;
    // the last delete version we meet when traversing candidate_rowsets
    Version last_delete_version { -1, -1 };

    for (size_t i = 0; i < candidate_rowsets.size(); ++i) {
        RowsetSharedPtr rowset = candidate_rowsets[i];
        if (_tablet->version_for_delete_predicate(rowset->version())) {
            last_delete_version = rowset->version();
            if (!transient_rowsets.empty()) {
                // we meet a delete version, and there were other versions before.
                // we should compact those version before handling them over to base compaction
                _input_rowsets = transient_rowsets;
                break;
            }

            // we meet a delete version, and no other versions before, skip it and continue
            transient_rowsets.clear();
            compaction_score = 0;
            continue;
        }

        if (compaction_score >= config::max_cumulative_compaction_num_singleton_deltas) {
            // got enough segments
            break;
        }

        compaction_score += rowset->rowset_meta()->get_compaction_score();
        transient_rowsets.push_back(rowset); 
    }

    // if we have a sufficient number of segments,
    // or have other versions before encountering the delete version, we should process the compaction.
    if (compaction_score >= config::min_cumulative_compaction_num_singleton_deltas
        || (last_delete_version.first != -1 && !transient_rowsets.empty())) {
        _input_rowsets = transient_rowsets;
    }

    // Cumulative compaction will process with at least 1 rowset.
    // So when there is no rowset being chosen, we should return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS:
    if (_input_rowsets.empty()) {
        if (last_delete_version.first != -1) {
            // we meet a delete version, should increase the cumulative point to let base compaction handle the delete version.
            // plus 1 to skip the delete version.
            // NOTICE: after that, the cumulative point may be larger than max version of this tablet, but it doen't matter.
            _tablet->set_cumulative_layer_point(last_delete_version.first + 1);
            return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
        }

        // we did not meet any delete version. which means compaction_score is not enough to do cumulative compaction.
        // We should wait until there are more rowsets to come, and keep the cumulative point unchanged.
        // But in order to avoid the stall of compaction because no new rowset arrives later, we should increase
        // the cumulative point after waiting for a long time, to ensure that the base compaction can continue.

        // check both last success time of base and cumulative compaction
        int64_t now = UnixMillis();
        int64_t last_cumu = _tablet->last_cumu_compaction_success_time();
        int64_t last_base = _tablet->last_base_compaction_success_time();
        if (last_cumu != 0 || last_base != 0) {
            int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation * 1000;
            int64_t cumu_interval = now - last_cumu;
            int64_t base_interval = now - last_base;
            if (cumu_interval > interval_threshold && base_interval > interval_threshold) {
                // before increasing cumulative point, we should make sure all rowsets are non-overlapping.
                // if at least one rowset is overlapping, we should compact them first.
                CHECK(candidate_rowsets.size() == transient_rowsets.size())
                    << "tablet: " << _tablet->full_name() << ", "<<  candidate_rowsets.size() << " vs. " << transient_rowsets.size();
                for (auto& rs : candidate_rowsets) {
                    if (rs->rowset_meta()->is_segments_overlapping()) {
                        _input_rowsets = candidate_rowsets;
                        return OLAP_SUCCESS;
                    }
                }

                // all candicate rowsets are non-overlapping, increase the cumulative point
                _tablet->set_cumulative_layer_point(candidate_rowsets.back()->start_version() + 1);
            }
        } else {
            // init the compaction success time for first time
            if (last_cumu == 0) {
                _tablet->set_last_cumu_compaction_success_time(now);
            }

            if (last_base == 0) {
                _tablet->set_last_base_compaction_success_time(now);
            }
        }

        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    return OLAP_SUCCESS;
}

}  // namespace doris

