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

#include "storage/compaction/binlog_compaction.h"

#include <cpp/sync_point.h>

#include <mutex>
#include <string>
#include <vector>

#include "common/logging.h"
#include "runtime/thread_context.h"
#include "storage/compaction/binlog_compaction_policy.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet/tablet.h"
#include "util/defer_op.h"
#include "util/time.h"
#include "util/trace.h"

namespace doris {
using namespace ErrorCode;

BinlogCompaction::BinlogCompaction(StorageEngine& engine, const TabletSharedPtr& tablet,
                                   int8_t compaction_level)
        : CompactionMixin(engine, tablet,
                          "BinlogCompaction:" + std::to_string(tablet->tablet_id())),
          _compaction_level(compaction_level) {}

BinlogCompaction::~BinlogCompaction() = default;

Status BinlogCompaction::prepare_compact() {
    Status st;
    Defer defer_set_st([&] {
        if (!st.ok()) {
            tablet()->set_last_binlog_compaction_status(st.to_string());
        }
    });

    if (!tablet()->init_succeeded()) {
        st = Status::Error<BINLOG_COMPACTION_INVALID_PARAMETERS, false>("_tablet init failed");
        return st;
    }

    std::unique_lock<std::mutex> lock(tablet()->get_binlog_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        st = Status::Error<TRY_LOCK_FAILED, false>(
                "The tablet is under binlog compaction. tablet={}", _tablet->tablet_id());
        return st;
    }

    st = pick_rowsets_to_compact();
    RETURN_IF_ERROR(st);

    COUNTER_UPDATE(_input_rowsets_counter, _input_rowsets.size());

    st = Status::OK();
    return st;
}

Status BinlogCompaction::execute_compact() {
    Status st;
    Defer defer_set_st([&] {
        tablet()->set_last_binlog_compaction_status(st.to_string());
        if (!st.ok()) {
            tablet()->set_last_binlog_compaction_failure_time(UnixMillis());
        } else {
            tablet()->set_last_binlog_compaction_success_time(_compaction_level, UnixMillis());
        }
    });

    std::unique_lock<std::mutex> lock(tablet()->get_binlog_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        st = Status::Error<TRY_LOCK_FAILED, false>(
                "The tablet is under binlog compaction. tablet={}", _tablet->tablet_id());
        return st;
    }

    SCOPED_ATTACH_TASK(_mem_tracker);

    st = CompactionMixin::execute_compact();
    RETURN_IF_ERROR(st);

    TEST_SYNC_POINT_RETURN_WITH_VALUE("binlog_compaction::BinlogCompaction::execute_compact",
                                      Status::OK());

    DCHECK_EQ(_state, CompactionState::SUCCESS);

    st = Status::OK();
    return st;
}

Status BinlogCompaction::pick_rowsets_to_compact() {
    auto candidate_rowsets = tablet()->pick_candidate_rowsets_to_binlog_compaction();
    if (candidate_rowsets.empty()) {
        return Status::Error<BINLOG_COMPACTION_NO_SUITABLE_VERSION>("candidate_rowsets is empty");
    }

    // candidate_rowsets may not be continuous
    // So we need to choose the longest continuous path from it.
    std::vector<Version> missing_versions;
    find_longest_consecutive_version(&candidate_rowsets, &missing_versions);
    if (!missing_versions.empty()) {
        DCHECK(missing_versions.size() % 2 == 0);
        LOG(WARNING) << "There are missed versions among binlog rowsets. "
                     << "total missed version size: " << missing_versions.size() / 2
                     << ", first missed version prev rowset verison=" << missing_versions[0]
                     << ", first missed version next rowset version=" << missing_versions[1]
                     << ", tablet=" << _tablet->tablet_id();
    }

    tablet()->binlog_compaction_policy()->pick_input_rowsets(tablet(), candidate_rowsets,
                                                             _compaction_level, &_input_rowsets);

    // Binlog compaction will process with at least 1 rowset.
    // So when there is no rowset being chosen, we should return Status::Error<BINLOG_COMPACTION_NO_SUITABLE_VERSION>():
    if (_input_rowsets.empty()) {
        VLOG_DEBUG << "binlog compaction can't get input rowsets, tablet=" << _tablet->tablet_id();
        return Status::Error<BINLOG_COMPACTION_NO_SUITABLE_VERSION>("_input_rowsets is empty");
    }

    return Status::OK();
}

Status BinlogCompaction::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);

    if (_is_ordered_data_compaction &&
        _compaction_level == BinlogCompactionPolicy::kBinlogCompactionMaxLevel - 1) {
        _output_rowset->rowset_meta()->set_segments_overlap(OVERLAPPING);
    }
    tablet()->binlog_compaction_policy()->update_compaction_level(tablet(), _input_rowsets,
                                                                  _output_rowset);
    {
        std::lock_guard<std::mutex> wrlock_(tablet()->get_rowset_update_lock());
        std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        RETURN_IF_ERROR(tablet()->modify_row_binlog_rowsets(output_rowsets, _input_rowsets));
    }
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        tablet()->save_meta();
    }
    return Status::OK();
}

} // namespace doris
