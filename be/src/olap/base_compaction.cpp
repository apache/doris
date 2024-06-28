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

#include <gen_cpp/olap_file.pb.h>

#include <memory>
#include <mutex>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "olap/compaction.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet.h"
#include "runtime/thread_context.h"
#include "util/doris_metrics.h"
#include "util/thread.h"
#include "util/trace.h"

namespace doris {
using namespace ErrorCode;

BaseCompaction::BaseCompaction(StorageEngine& engine, const TabletSharedPtr& tablet)
        : CompactionMixin(engine, tablet, "BaseCompaction:" + std::to_string(tablet->tablet_id())) {
}

BaseCompaction::~BaseCompaction() = default;

Status BaseCompaction::prepare_compact() {
    if (!tablet()->init_succeeded()) {
        return Status::Error<INVALID_ARGUMENT, false>("_tablet init failed");
    }

    std::unique_lock<std::mutex> lock(tablet()->get_base_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED, false>(
                "another base compaction is running. tablet={}", _tablet->tablet_id());
    }

    // 1. pick rowsets to compact
    RETURN_IF_ERROR(pick_rowsets_to_compact());
    COUNTER_UPDATE(_input_rowsets_counter, _input_rowsets.size());

    return Status::OK();
}

Status BaseCompaction::execute_compact() {
#ifndef __APPLE__
    if (config::enable_base_compaction_idle_sched) {
        Thread::set_idle_sched();
    }
#endif
    std::unique_lock<std::mutex> lock(tablet()->get_base_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED, false>(
                "another base compaction is running. tablet={}", _tablet->tablet_id());
    }

    SCOPED_ATTACH_TASK(_mem_tracker);

    RETURN_IF_ERROR(CompactionMixin::execute_compact());
    DCHECK_EQ(_state, CompactionState::SUCCESS);

    tablet()->set_last_base_compaction_success_time(UnixMillis());
    DorisMetrics::instance()->base_compaction_deltas_total->increment(_input_rowsets.size());
    DorisMetrics::instance()->base_compaction_bytes_total->increment(_input_rowsets_size);

    return Status::OK();
}

void BaseCompaction::_filter_input_rowset() {
    // if dup_key and no delete predicate
    // we skip big files to save resources
    if (_tablet->keys_type() != KeysType::DUP_KEYS) {
        return;
    }
    for (auto& rs : _input_rowsets) {
        if (rs->rowset_meta()->has_delete_predicate()) {
            return;
        }
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
    _input_rowsets = tablet()->pick_candidate_rowsets_to_base_compaction();
    RETURN_IF_ERROR(check_version_continuity(_input_rowsets));
    _filter_input_rowset();
    if (_input_rowsets.size() <= 1) {
        return Status::Error<BE_NO_SUITABLE_VERSION>("_input_rowsets.size() is 1");
    }

    // There are two occasions, first is that we set enable_delete_when_cumu_compaction false:
    // If there are delete predicate rowsets in tablet, start_version > 0 implies some rowsets before
    // delete version cannot apply these delete predicates, which can cause incorrect query result.
    // So we must abort this base compaction.
    // A typical scenario is that some rowsets before cumulative point are on remote storage.
    // For example, consider rowset[0,3] is on remote storage, now we pass [4,4],[5,5],[6,9]
    // to do base compaction and rowset[5,5] is delete predicate rowset, if we allow them to do
    // such procedure, then we'll get [4,9] while it will lose the delete predicate information in [5,5]
    // which rusult in data in [0,3] will not be deleted.
    // Another occasion is that we set enable_delete_when_cumu_compaction true:
    // Then whatever the _input_rowsets.front()->start_version() > 0 or not, once the output
    // rowset's start version is bigger than 2, we'll always remain the delete pred information inside
    // the output rowset so the rowsets whose version is less than _input_rowsets.front()->start_version() > 0
    // would apply the delete pred in the end.
    if (!_allow_delete_in_cumu_compaction && _input_rowsets.front()->start_version() > 0) {
        bool has_delete_predicate = false;
        for (const auto& rs : _input_rowsets) {
            if (rs->rowset_meta()->has_delete_predicate()) {
                has_delete_predicate = true;
                break;
            }
        }
        if (has_delete_predicate) {
            return Status::Error<BE_NO_SUITABLE_VERSION>(
                    "Some rowsets cannot apply delete predicates in base compaction. tablet_id={}",
                    _tablet->tablet_id());
        }
    }

    if (_input_rowsets.size() == 2 && _input_rowsets[0]->end_version() == 1) {
        return Status::Error<BE_NO_SUITABLE_VERSION>(
                "the tablet is with rowset: [0-1], [2-y], and [0-1] has no data. in this "
                "situation, no need to do base compaction.");
    }

    int score = 0;
    int rowset_cnt = 0;
    while (rowset_cnt < _input_rowsets.size()) {
        score += _input_rowsets[rowset_cnt++]->rowset_meta()->get_compaction_score();
        if (score > config::base_compaction_max_compaction_score) {
            break;
        }
    }
    _input_rowsets.resize(rowset_cnt);

    // 1. cumulative rowset must reach base_compaction_num_cumulative_deltas threshold
    if (_input_rowsets.size() > config::base_compaction_min_rowset_num) {
        VLOG_NOTICE << "satisfy the base compaction policy. tablet=" << _tablet->tablet_id()
                    << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
                    << ", base_compaction_num_cumulative_rowsets="
                    << config::base_compaction_min_rowset_num;
        return Status::OK();
    }

    // 2. the ratio between base rowset and all input cumulative rowsets reaches the threshold
    // `_input_rowsets` has been sorted by end version, so we consider `_input_rowsets[0]` is the base rowset.
    int64_t base_size = _input_rowsets.front()->data_disk_size();
    int64_t cumulative_total_size = 0;
    for (auto it = _input_rowsets.begin() + 1; it != _input_rowsets.end(); ++it) {
        cumulative_total_size += (*it)->data_disk_size();
    }

    double min_data_ratio = config::base_compaction_min_data_ratio;
    if (base_size == 0) {
        // base_size == 0 means this may be a base version [0-1], which has no data.
        // set to 1 to void divide by zero
        base_size = 1;
    }
    double cumulative_base_ratio = static_cast<double>(cumulative_total_size) / base_size;

    if (cumulative_base_ratio > min_data_ratio) {
        VLOG_NOTICE << "satisfy the base compaction policy. tablet=" << _tablet->tablet_id()
                    << ", cumulative_total_size=" << cumulative_total_size
                    << ", base_size=" << base_size
                    << ", cumulative_base_ratio=" << cumulative_base_ratio
                    << ", policy_min_data_ratio=" << min_data_ratio;
        return Status::OK();
    }

    // 3. the interval since last base compaction reaches the threshold
    int64_t base_creation_time = _input_rowsets[0]->creation_time();
    int64_t interval_threshold = 86400;
    int64_t interval_since_last_base_compaction = time(nullptr) - base_creation_time;
    if (interval_since_last_base_compaction > interval_threshold) {
        VLOG_NOTICE << "satisfy the base compaction policy. tablet=" << _tablet->tablet_id()
                    << ", interval_since_last_base_compaction="
                    << interval_since_last_base_compaction
                    << ", interval_threshold=" << interval_threshold;
        return Status::OK();
    }

    return Status::Error<BE_NO_SUITABLE_VERSION>(
            "don't satisfy the base compaction policy. tablet={}, num_cumulative_rowsets={}, "
            "cumulative_base_ratio={}, interval_since_last_base_compaction={}",
            _tablet->tablet_id(), _input_rowsets.size() - 1, cumulative_base_ratio,
            interval_since_last_base_compaction);
}

} // namespace doris
