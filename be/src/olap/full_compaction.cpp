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
#include "olap/cumulative_compaction_policy.h"
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

    std::unique_lock<std::mutex> full_lock(_tablet->get_full_compaction_lock());
    std::unique_lock<std::mutex> base_lock(_tablet->get_base_compaction_lock());
    std::unique_lock<std::mutex> cumu_lock(_tablet->get_cumulative_compaction_lock());

    // 1. pick rowsets to compact
    RETURN_IF_ERROR(pick_rowsets_to_compact());
    _tablet->set_clone_occurred(false);

    return Status::OK();
}

Status FullCompaction::execute_compact_impl() {
    std::unique_lock<std::mutex> full_lock(_tablet->get_full_compaction_lock());
    std::unique_lock<std::mutex> base_lock(_tablet->get_base_compaction_lock());
    std::unique_lock<std::mutex> cumu_lock(_tablet->get_cumulative_compaction_lock());

    // Clone task may happen after compaction task is submitted to thread pool, and rowsets picked
    // for compaction may change. In this case, current compaction task should not be executed.
    if (_tablet->get_clone_occurred()) {
        _tablet->set_clone_occurred(false);
        return Status::Error<BE_CLONE_OCCURRED>();
    }

    SCOPED_ATTACH_TASK(_mem_tracker);

    // 2. do full compaction, merge rowsets
    int64_t permits = get_compaction_permits();
    RETURN_IF_ERROR(do_compaction(permits));

    // 3. set state to success
    _state = CompactionState::SUCCESS;

    // 4. set cumulative point
    Version last_version = _input_rowsets.back()->version();
    _tablet->cumulative_compaction_policy()->update_cumulative_point(_tablet.get(), _input_rowsets,
                                                                     _output_rowset, last_version);
    VLOG_CRITICAL << "after cumulative compaction, current cumulative point is "
                  << _tablet->cumulative_layer_point() << ", tablet=" << _tablet->full_name();

    return Status::OK();
}

Status FullCompaction::pick_rowsets_to_compact() {
    _input_rowsets = _tablet->pick_candidate_rowsets_to_full_compaction();
    RETURN_IF_ERROR(check_version_continuity(_input_rowsets));
    RETURN_IF_ERROR(check_all_version(_input_rowsets));
    if (_input_rowsets.size() <= 1) {
        return Status::Error<FULL_NO_SUITABLE_VERSION>();
    }

    if (_input_rowsets.size() == 2 && _input_rowsets[0]->end_version() == 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do base compaction.
        return Status::Error<FULL_NO_SUITABLE_VERSION>();
    }

    return Status::OK();
}

Status FullCompaction::modify_rowsets(const Merger::Statistics* stats) {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);
    std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
    RETURN_IF_ERROR(_tablet->modify_rowsets(output_rowsets, _input_rowsets, true));
    _tablet->save_meta();
    return Status::OK();
}

} // namespace doris
