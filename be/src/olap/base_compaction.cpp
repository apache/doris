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

#include "olap/base_compaction.h"

#include "util/doris_metrics.h"
#include "util/trace.h"

namespace doris {

BaseCompaction::BaseCompaction(TabletSharedPtr tablet)
        : Compaction(tablet, "BaseCompaction:" + std::to_string(tablet->tablet_id())) {}

BaseCompaction::~BaseCompaction() {}

Status BaseCompaction::prepare_compact() {
    if (!_tablet->init_succeeded()) {
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    std::unique_lock<std::mutex> lock(_tablet->get_base_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        LOG(WARNING) << "another base compaction is running. tablet=" << _tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_BE_TRY_BE_LOCK_ERROR);
    }
    TRACE("got base compaction lock");

    // 1. pick rowsets to compact
    RETURN_NOT_OK(pick_rowsets_to_compact());
    TRACE("rowsets picked");
    TRACE_COUNTER_INCREMENT("input_rowsets_count", _input_rowsets.size());
    _tablet->set_clone_occurred(false);

    return Status::OK();
}

Status BaseCompaction::execute_compact_impl() {
#ifndef __APPLE__
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }
#endif
    std::unique_lock<std::mutex> lock(_tablet->get_base_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        LOG(WARNING) << "another base compaction is running. tablet=" << _tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_BE_TRY_BE_LOCK_ERROR);
    }
    TRACE("got base compaction lock");

    // Clone task may happen after compaction task is submitted to thread pool, and rowsets picked
    // for compaction may change. In this case, current compaction task should not be executed.
    if (_tablet->get_clone_occurred()) {
        _tablet->set_clone_occurred(false);
        return Status::OLAPInternalError(OLAP_ERR_BE_CLONE_OCCURRED);
    }

    SCOPED_ATTACH_TASK(_mem_tracker, ThreadContext::TaskType::COMPACTION);

    // 2. do base compaction, merge rowsets
    int64_t permits = get_compaction_permits();
    RETURN_NOT_OK(do_compaction(permits));
    TRACE("compaction finished");

    // 3. set state to success
    _state = CompactionState::SUCCESS;

    // 4. add metric to base compaction
    DorisMetrics::instance()->base_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->base_compaction_bytes_total->increment(_input_rowsets_size);
    TRACE("save base compaction metrics");

    return Status::OK();
}

void BaseCompaction::_filter_input_rowset() {
    // if enable dup key skip big file and no delete predicate
    // we skip big files too save resources
    if (!config::enable_dup_key_base_compaction_skip_big_file ||
        _tablet->keys_type() != KeysType::DUP_KEYS || _tablet->delete_predicates().size() != 0) {
        return;
    }
    int64_t max_size = config::base_compaction_dup_key_max_file_size_mbytes * 1024 * 1024;
    // first find a proper rowset for start
    auto rs_iter = _input_rowsets.begin();
    while (rs_iter != _input_rowsets.end()) {
        if ((*rs_iter)->rowset_meta()->total_disk_size() >= max_size) {
            rs_iter = _input_rowsets.erase(rs_iter);
        } else {
            break;
        }
    }
}

Status BaseCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();
    std::shared_lock rdlock(_tablet->get_header_lock());
    _tablet->pick_candidate_rowsets_to_base_compaction(&_input_rowsets, rdlock);
    std::sort(_input_rowsets.begin(), _input_rowsets.end(), Rowset::comparator);
    RETURN_NOT_OK(check_version_continuity(_input_rowsets));
    RETURN_NOT_OK(_check_rowset_overlapping(_input_rowsets));
    _filter_input_rowset();
    if (_input_rowsets.size() <= 1) {
        return Status::OLAPInternalError(OLAP_ERR_BE_NO_SUITABLE_VERSION);
    }

    // If there are delete predicate rowsets in tablet, start_version > 0 implies some rowsets before
    // delete version cannot apply these delete predicates, which can cause incorrect query result.
    // So we must abort this base compaction.
    // A typical scenario is that some rowsets before cumulative point are on remote storage.
    if (_input_rowsets.front()->start_version() > 0) {
        bool has_delete_predicate = false;
        for (const auto& rs : _input_rowsets) {
            if (rs->rowset_meta()->has_delete_predicate()) {
                has_delete_predicate = true;
                break;
            }
        }
        if (has_delete_predicate) {
            LOG(WARNING)
                    << "Some rowsets cannot apply delete predicates in base compaction. tablet_id="
                    << _tablet->tablet_id();
            return Status::OLAPInternalError(OLAP_ERR_BE_NO_SUITABLE_VERSION);
        }
    }

    if (_input_rowsets.size() == 2 && _input_rowsets[0]->end_version() == 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do base compaction.
        return Status::OLAPInternalError(OLAP_ERR_BE_NO_SUITABLE_VERSION);
    }

    // 1. cumulative rowset must reach base_compaction_num_cumulative_deltas threshold
    if (_input_rowsets.size() > config::base_compaction_num_cumulative_deltas) {
        VLOG_NOTICE << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                    << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
                    << ", base_compaction_num_cumulative_rowsets="
                    << config::base_compaction_num_cumulative_deltas;
        return Status::OK();
    }

    // 2. the ratio between base rowset and all input cumulative rowsets reaches the threshold
    // `_input_rowsets` has been sorted by end version, so we consider `_input_rowsets[0]` is the base rowset.
    int64_t base_size = _input_rowsets.front()->data_disk_size();
    int64_t cumulative_total_size = 0;
    for (auto it = _input_rowsets.begin() + 1; it != _input_rowsets.end(); ++it) {
        cumulative_total_size += (*it)->data_disk_size();
    }

    double base_cumulative_delta_ratio = config::base_cumulative_delta_ratio;
    if (base_size == 0) {
        // base_size == 0 means this may be a base version [0-1], which has no data.
        // set to 1 to void divide by zero
        base_size = 1;
    }
    double cumulative_base_ratio = static_cast<double>(cumulative_total_size) / base_size;

    if (cumulative_base_ratio > base_cumulative_delta_ratio) {
        VLOG_NOTICE << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                    << ", cumulative_total_size=" << cumulative_total_size
                    << ", base_size=" << base_size
                    << ", cumulative_base_ratio=" << cumulative_base_ratio
                    << ", policy_ratio=" << base_cumulative_delta_ratio;
        return Status::OK();
    }

    // 3. the interval since last base compaction reaches the threshold
    int64_t base_creation_time = _input_rowsets[0]->creation_time();
    int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation;
    int64_t interval_since_last_base_compaction = time(nullptr) - base_creation_time;
    if (interval_since_last_base_compaction > interval_threshold) {
        VLOG_NOTICE << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                    << ", interval_since_last_base_compaction="
                    << interval_since_last_base_compaction
                    << ", interval_threshold=" << interval_threshold;
        return Status::OK();
    }

    VLOG_NOTICE << "don't satisfy the base compaction policy. tablet=" << _tablet->full_name()
                << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
                << ", cumulative_base_ratio=" << cumulative_base_ratio
                << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction;
    return Status::OLAPInternalError(OLAP_ERR_BE_NO_SUITABLE_VERSION);
}

Status BaseCompaction::_check_rowset_overlapping(const std::vector<RowsetSharedPtr>& rowsets) {
    for (auto& rs : rowsets) {
        if (rs->rowset_meta()->is_segments_overlapping()) {
            LOG(WARNING) << "There is overlapping rowset before cumulative point, "
                         << "rowset version=" << rs->start_version() << "-" << rs->end_version()
                         << ", cumulative point=" << _tablet->cumulative_layer_point()
                         << ", tablet=" << _tablet->full_name();
            return Status::OLAPInternalError(OLAP_ERR_BE_SEGMENTS_OVERLAPPING);
        }
    }
    return Status::OK();
}

} // namespace doris
