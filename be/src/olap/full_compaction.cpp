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

#include <glog/logging.h>
#include <time.h>

#include <memory>
#include <mutex>
#include <ostream>
#include <shared_mutex>

#include "common/config.h"
#include "common/status.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/schema_change.h"
#include "olap/tablet_meta.h"
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
        return Status::Error<INVALID_ARGUMENT, false>("Full compaction init failed");
    }

    std::unique_lock full_lock(_tablet->get_full_compaction_lock());
    std::unique_lock base_lock(_tablet->get_base_compaction_lock());
    std::unique_lock cumu_lock(_tablet->get_cumulative_compaction_lock());

    // 1. pick rowsets to compact
    RETURN_IF_ERROR(pick_rowsets_to_compact());
    _tablet->set_clone_occurred(false);

    return Status::OK();
}

Status FullCompaction::execute_compact_impl() {
    std::unique_lock full_lock(_tablet->get_full_compaction_lock());
    std::unique_lock base_lock(_tablet->get_base_compaction_lock());
    std::unique_lock cumu_lock(_tablet->get_cumulative_compaction_lock());

    // Clone task may happen after compaction task is submitted to thread pool, and rowsets picked
    // for compaction may change. In this case, current compaction task should not be executed.
    if (_tablet->get_clone_occurred()) {
        _tablet->set_clone_occurred(false);
        return Status::Error<BE_CLONE_OCCURRED, false>("get_clone_occurred failed");
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
                  << _tablet->cumulative_layer_point() << ", tablet=" << _tablet->tablet_id();

    return Status::OK();
}

Status FullCompaction::pick_rowsets_to_compact() {
    _input_rowsets = _tablet->pick_candidate_rowsets_to_full_compaction();
    RETURN_IF_ERROR(check_version_continuity(_input_rowsets));
    RETURN_IF_ERROR(_check_all_version(_input_rowsets));
    if (_input_rowsets.size() <= 1) {
        return Status::Error<FULL_NO_SUITABLE_VERSION>("There is no suitable version");
    }

    if (_input_rowsets.size() == 2 && _input_rowsets[0]->end_version() == 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do full compaction.
        return Status::Error<FULL_NO_SUITABLE_VERSION>("There is no suitable version");
    }

    return Status::OK();
}

Status FullCompaction::modify_rowsets(const Merger::Statistics* stats) {
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        RETURN_IF_ERROR(
                _full_compaction_update_delete_bitmap(_output_rowset, _output_rs_writer.get()));
    }
    std::vector<RowsetSharedPtr> output_rowsets(1, _output_rowset);
    RETURN_IF_ERROR(_tablet->modify_rowsets(output_rowsets, _input_rowsets, true));
    _tablet->save_meta();
    return Status::OK();
}

Status FullCompaction::_check_all_version(const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.empty()) {
        return Status::Error<FULL_MISS_VERSION, false>(
                "There is no input rowset when do full compaction");
    }
    const RowsetSharedPtr& last_rowset = rowsets.back();
    const RowsetSharedPtr& first_rowset = rowsets.front();
    if (last_rowset->version() != _tablet->max_version() || first_rowset->version().first != 0) {
        return Status::Error<FULL_MISS_VERSION, false>(
                "Full compaction rowsets' versions not equal to all exist rowsets' versions. "
                "full compaction rowsets max version={}-{}"
                ", current rowsets max version={}-{}"
                "full compaction rowsets min version={}-{}, current rowsets min version=0-1",
                last_rowset->start_version(), last_rowset->end_version(),
                _tablet->max_version().first, _tablet->max_version().second,
                first_rowset->start_version(), first_rowset->end_version());
    }
    return Status::OK();
}

Status FullCompaction::_full_compaction_update_delete_bitmap(const RowsetSharedPtr& rowset,
                                                             RowsetWriter* rowset_writer) {
    std::vector<RowsetSharedPtr> tmp_rowsets {};

    // tablet is under alter process. The delete bitmap will be calculated after conversion.
    if (_tablet->tablet_state() == TABLET_NOTREADY &&
        SchemaChangeHandler::tablet_in_converting(_tablet->tablet_id())) {
        LOG(INFO) << "tablet is under alter process, update delete bitmap later, tablet_id="
                  << _tablet->tablet_id();
        return Status::OK();
    }

    int64_t max_version = _tablet->max_version().second;
    DCHECK(max_version >= rowset->version().second);
    if (max_version > rowset->version().second) {
        RETURN_IF_ERROR(_tablet->capture_consistent_rowsets(
                {rowset->version().second + 1, max_version}, &tmp_rowsets));
    }

    for (const auto& it : tmp_rowsets) {
        const int64_t& cur_version = it->rowset_meta()->start_version();
        RETURN_IF_ERROR(
                _full_compaction_calc_delete_bitmap(it, rowset, cur_version, rowset_writer));
    }

    std::lock_guard rowset_update_lock(_tablet->get_rowset_update_lock());
    std::lock_guard header_lock(_tablet->get_header_lock());
    SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
    for (const auto& it : _tablet->rowset_map()) {
        const int64_t& cur_version = it.first.first;
        const RowsetSharedPtr& published_rowset = it.second;
        if (cur_version > max_version) {
            RETURN_IF_ERROR(_full_compaction_calc_delete_bitmap(published_rowset, rowset,
                                                                cur_version, rowset_writer));
        }
    }

    return Status::OK();
}

Status FullCompaction::_full_compaction_calc_delete_bitmap(const RowsetSharedPtr& published_rowset,
                                                           const RowsetSharedPtr& rowset,
                                                           const int64_t& cur_version,
                                                           RowsetWriter* rowset_writer) {
    std::vector<segment_v2::SegmentSharedPtr> segments;
    auto beta_rowset = reinterpret_cast<BetaRowset*>(published_rowset.get());
    RETURN_IF_ERROR(beta_rowset->load_segments(&segments));
    DeleteBitmapPtr delete_bitmap =
            std::make_shared<DeleteBitmap>(_tablet->tablet_meta()->tablet_id());
    std::vector<RowsetSharedPtr> specified_rowsets(1, rowset);

    OlapStopWatch watch;
    RETURN_IF_ERROR(_tablet->calc_delete_bitmap(published_rowset, segments, specified_rowsets,
                                                delete_bitmap, nullptr, cur_version, nullptr,
                                                rowset_writer));
    size_t total_rows = std::accumulate(
            segments.begin(), segments.end(), 0,
            [](size_t sum, const segment_v2::SegmentSharedPtr& s) { return sum += s->num_rows(); });
    VLOG_DEBUG << "[Full compaction] construct delete bitmap tablet: " << _tablet->tablet_id()
               << ", published rowset version: [" << published_rowset->version().first << "-"
               << published_rowset->version().second << "]"
               << ", full compaction rowset version: [" << rowset->version().first << "-"
               << rowset->version().second << "]"
               << ", cost: " << watch.get_elapse_time_us() << "(us), total rows: " << total_rows;

    for (const auto& [k, v] : delete_bitmap->delete_bitmap) {
        _tablet->tablet_meta()->delete_bitmap().merge({std::get<0>(k), std::get<1>(k), cur_version},
                                                      v);
    }

    return Status::OK();
}

} // namespace doris
