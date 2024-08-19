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

#include <memory>
#include <mutex>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet.h"
#include "runtime/thread_context.h"
#include "util/doris_metrics.h"
#include "util/time.h"
#include "util/trace.h"

namespace doris {
using namespace ErrorCode;

void CumulativeCompaction::find_longest_consecutive_version(std::vector<RowsetSharedPtr>* rowsets,
                                                            std::vector<Version>* missing_version) {
    if (rowsets->empty()) {
        return;
    }

    RowsetSharedPtr prev_rowset = rowsets->front();
    size_t i = 1;
    int max_start = 0;
    int max_length = 1;

    int start = 0;
    int length = 1;
    for (; i < rowsets->size(); ++i) {
        RowsetSharedPtr rowset = (*rowsets)[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            if (missing_version != nullptr) {
                missing_version->push_back(prev_rowset->version());
                missing_version->push_back(rowset->version());
            }
            start = i;
            length = 1;
        } else {
            length++;
        }

        if (length > max_length) {
            max_start = start;
            max_length = length;
        }

        prev_rowset = rowset;
    }
    *rowsets = {rowsets->begin() + max_start, rowsets->begin() + max_start + max_length};
}

CumulativeCompaction::CumulativeCompaction(StorageEngine& engine, const TabletSharedPtr& tablet)
        : CompactionMixin(engine, tablet,
                          "CumulativeCompaction:" + std::to_string(tablet->tablet_id())) {}

CumulativeCompaction::~CumulativeCompaction() = default;

Status CumulativeCompaction::prepare_compact() {
    if (!tablet()->init_succeeded()) {
        return Status::Error<CUMULATIVE_INVALID_PARAMETERS, false>("_tablet init failed");
    }

    std::unique_lock<std::mutex> lock(tablet()->get_cumulative_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED, false>(
                "The tablet is under cumulative compaction. tablet={}", _tablet->tablet_id());
    }

    tablet()->calculate_cumulative_point();
    VLOG_CRITICAL << "after calculate, current cumulative point is "
                  << tablet()->cumulative_layer_point() << ", tablet=" << _tablet->tablet_id();

    RETURN_IF_ERROR(pick_rowsets_to_compact());
    COUNTER_UPDATE(_input_rowsets_counter, _input_rowsets.size());

    return Status::OK();
}

Status CumulativeCompaction::execute_compact() {
    std::unique_lock<std::mutex> lock(tablet()->get_cumulative_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED, false>(
                "The tablet is under cumulative compaction. tablet={}", _tablet->tablet_id());
    }

    SCOPED_ATTACH_TASK(_mem_tracker);

    RETURN_IF_ERROR(CompactionMixin::execute_compact());
    DCHECK_EQ(_state, CompactionState::SUCCESS);
    if (tablet()->tablet_meta()->time_series_compaction_level_threshold() >= 2) {
        tablet()->cumulative_compaction_policy()->update_compaction_level(tablet(), _input_rowsets,
                                                                          _output_rowset);
    }

    tablet()->cumulative_compaction_policy()->update_cumulative_point(
            tablet(), _input_rowsets, _output_rowset, _last_delete_version);
    VLOG_CRITICAL << "after cumulative compaction, current cumulative point is "
                  << tablet()->cumulative_layer_point() << ", tablet=" << _tablet->tablet_id();
    // TIME_SERIES_POLICY, generating an empty rowset doesn't need to update the timestamp.
    if (!(tablet()->tablet_meta()->compaction_policy() == CUMULATIVE_TIME_SERIES_POLICY &&
          _output_rowset->num_segments() == 0)) {
        tablet()->set_last_cumu_compaction_success_time(UnixMillis());
    }
    DorisMetrics::instance()->cumulative_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->cumulative_compaction_bytes_total->increment(_input_rowsets_size);

    return Status::OK();
}

Status CumulativeCompaction::pick_rowsets_to_compact() {
    auto candidate_rowsets = tablet()->pick_candidate_rowsets_to_cumulative_compaction();
    if (candidate_rowsets.empty()) {
        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>("candidate_rowsets is empty");
    }

    // candidate_rowsets may not be continuous
    // So we need to choose the longest continuous path from it.
    std::vector<Version> missing_versions;
    find_longest_consecutive_version(&candidate_rowsets, &missing_versions);
    if (!missing_versions.empty()) {
        DCHECK(missing_versions.size() % 2 == 0);
        LOG(WARNING) << "There are missed versions among rowsets. "
                     << "total missed version size: " << missing_versions.size() / 2
                     << " first missed version prev rowset verison=" << missing_versions[0]
                     << ", first missed version next rowset version=" << missing_versions[1]
                     << ", tablet=" << _tablet->tablet_id();
    }

    int64_t max_score = config::cumulative_compaction_max_deltas;
    auto process_memory_usage = doris::GlobalMemoryArbitrator::process_memory_usage();
    bool memory_usage_high = process_memory_usage > MemInfo::soft_mem_limit() * 0.8;
    if (tablet()->last_compaction_status.is<ErrorCode::MEM_LIMIT_EXCEEDED>() || memory_usage_high) {
        max_score = std::max(config::cumulative_compaction_max_deltas /
                                     config::cumulative_compaction_max_deltas_factor,
                             config::cumulative_compaction_min_deltas + 1);
    }

    size_t compaction_score = 0;
    tablet()->cumulative_compaction_policy()->pick_input_rowsets(
            tablet(), candidate_rowsets, max_score, config::cumulative_compaction_min_deltas,
            &_input_rowsets, &_last_delete_version, &compaction_score,
            _allow_delete_in_cumu_compaction);

    // Cumulative compaction will process with at least 1 rowset.
    // So when there is no rowset being chosen, we should return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>():
    if (_input_rowsets.empty()) {
        if (_last_delete_version.first != -1) {
            // we meet a delete version, should increase the cumulative point to let base compaction handle the delete version.
            // plus 1 to skip the delete version.
            // NOTICE: after that, the cumulative point may be larger than max version of this tablet, but it doesn't matter.
            tablet()->set_cumulative_layer_point(_last_delete_version.first + 1);
            return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>(
                    "_last_delete_version.first not equal to -1");
        }

        // we did not meet any delete version. which means compaction_score is not enough to do cumulative compaction.
        // We should wait until there are more rowsets to come, and keep the cumulative point unchanged.
        // But in order to avoid the stall of compaction because no new rowset arrives later, we should increase
        // the cumulative point after waiting for a long time, to ensure that the base compaction can continue.

        // check both last success time of base and cumulative compaction
        int64_t now = UnixMillis();
        int64_t last_cumu = tablet()->last_cumu_compaction_success_time();
        int64_t last_base = tablet()->last_base_compaction_success_time();
        if (last_cumu != 0 || last_base != 0) {
            int64_t interval_threshold = config::pick_rowset_to_compact_interval_sec * 1000;
            int64_t cumu_interval = now - last_cumu;
            int64_t base_interval = now - last_base;
            if (cumu_interval > interval_threshold && base_interval > interval_threshold) {
                // before increasing cumulative point, we should make sure all rowsets are non-overlapping.
                // if at least one rowset is overlapping, we should compact them first.
                for (auto& rs : candidate_rowsets) {
                    if (rs->rowset_meta()->is_segments_overlapping()) {
                        _input_rowsets = candidate_rowsets;
                        return Status::OK();
                    }
                }

                // all candidate rowsets are non-overlapping, increase the cumulative point
                tablet()->set_cumulative_layer_point(candidate_rowsets.back()->start_version() + 1);
            }
        } else {
            // init the compaction success time for first time
            if (last_cumu == 0) {
                tablet()->set_last_cumu_compaction_success_time(now);
            }

            if (last_base == 0) {
                tablet()->set_last_base_compaction_success_time(now);
            }
        }

        return Status::Error<CUMULATIVE_NO_SUITABLE_VERSION>("_input_rowsets is empty");
    }

    return Status::OK();
}

} // namespace doris
