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

#include "olap/full_compaction.h"

#include <time.h>

#include <mutex>
#include <ostream>

#include "common/config.h"
#include "olap/olap_define.h"
#include "runtime/thread_context.h"
#include "util/thread.h"
#include "util/trace.h"

namespace doris {
using namespace ErrorCode;

FullCompaction::FullCompaction(const TabletSharedPtr& tablet)
        : Compaction(tablet, "FullCompaction:" + std::to_string(tablet->tablet_id())) {}

FullCompaction::~FullCompaction() {}

Status FullCompaction::prepare_compact() {
    if (!_tablet->init_succeeded()) {
        return Status::Error<INVALID_ARGUMENT>();
    }

    std::unique_lock<std::mutex> lock(_tablet->get_full_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        LOG(WARNING) << "another full compaction is running. tablet=" << _tablet->full_name();
        return Status::Error<TRY_LOCK_FAILED>();
    }

    // 1. pick rowsets to compact
    RETURN_IF_ERROR(pick_rowsets_to_compact());
    TRACE_COUNTER_INCREMENT("input_rowsets_count", _input_rowsets.size());
    _tablet->set_clone_occurred(false);

    return Status::OK();
}

Status FullCompaction::execute_compact_impl() {
    std::unique_lock<std::mutex> lock(_tablet->get_full_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        LOG(WARNING) << "another full compaction is running. tablet=" << _tablet->full_name();
        return Status::Error<TRY_LOCK_FAILED>();
    }

    // Clone task may happen after compaction task is submitted to thread pool, and rowsets picked
    // for compaction may change. In this case, current compaction task should not be executed.
    if (_tablet->get_clone_occurred()) {
        _tablet->set_clone_occurred(false);
        return Status::Error<BE_CLONE_OCCURRED>();
    }

    SCOPED_ATTACH_TASK(_mem_tracker);

    // 2. do base compaction, merge rowsets
    int64_t permits = get_compaction_permits();
    RETURN_IF_ERROR(do_compaction(permits));

    // 3. set state to success
    _state = CompactionState::SUCCESS;

    return Status::OK();
}

Status FullCompaction::pick_rowsets_to_compact() {
    _input_rowsets = _tablet->pick_candidate_rowsets_to_full_compaction();
    RETURN_IF_ERROR(check_version_continuity(_input_rowsets));
    RETURN_IF_ERROR(_check_rowset_overlapping(_input_rowsets));
    if (_input_rowsets.size() <= 1) {
        return Status::Error<BE_NO_SUITABLE_VERSION>();
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
            return Status::Error<BE_NO_SUITABLE_VERSION>();
        }
    }

    if (_input_rowsets.size() == 2 && _input_rowsets[0]->end_version() == 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do base compaction.
        return Status::Error<BE_NO_SUITABLE_VERSION>();
    }

    return Status::OK();
}

Status FullCompaction::_check_rowset_overlapping(const std::vector<RowsetSharedPtr>& rowsets) {
    for (auto& rs : rowsets) {
        if (rs->rowset_meta()->is_segments_overlapping()) {
            LOG(WARNING) << "There is overlapping rowset before cumulative point, "
                         << "rowset version=" << rs->start_version() << "-" << rs->end_version()
                         << ", cumulative point=" << _tablet->cumulative_layer_point()
                         << ", tablet=" << _tablet->full_name();
            return Status::Error<BE_SEGMENTS_OVERLAPPING>();
        }
    }
    return Status::OK();
}

Status FullCompaction::modify_rowsets(const Merger::Statistics* stats) {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);
    std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
    RETURN_IF_ERROR(_tablet->modify_rowsets(output_rowsets, _input_rowsets, true));
    std::shared_lock rlock(_tablet->get_header_lock());
    _tablet->save_meta();
    return Status::OK();
}

} // namespace doris
