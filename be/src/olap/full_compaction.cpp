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
#include "olap/base_tablet.h"
#include "olap/compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/schema_change.h"
#include "olap/tablet_meta.h"
#include "runtime/thread_context.h"
#include "util/debug_points.h"
#include "util/thread.h"
#include "util/trace.h"

namespace doris {
using namespace ErrorCode;

FullCompaction::FullCompaction(StorageEngine& engine, const TabletSharedPtr& tablet)
        : CompactionMixin(engine, tablet, "FullCompaction:" + std::to_string(tablet->tablet_id())) {
}

FullCompaction::~FullCompaction() {
    tablet()->set_is_full_compaction_running(false);
}

Status FullCompaction::prepare_compact() {
    Status st;
    Defer defer_set_st([&] {
        if (!st.ok()) {
            tablet()->set_last_full_compaction_status(st.to_string());
            tablet()->set_last_full_compaction_failure_time(UnixMillis());
        }
    });
    if (!tablet()->init_succeeded()) {
        st = Status::Error<INVALID_ARGUMENT, false>("Full compaction init failed");
        return st;
    }

    std::unique_lock base_lock(tablet()->get_base_compaction_lock());
    std::unique_lock cumu_lock(tablet()->get_cumulative_compaction_lock());
    tablet()->set_is_full_compaction_running(true);

    DBUG_EXECUTE_IF("FullCompaction.prepare_compact.set_cumu_point",
                    { tablet()->set_cumulative_layer_point(tablet()->max_version_unlocked() + 1); })

    // 1. pick rowsets to compact
    st = pick_rowsets_to_compact();
    RETURN_IF_ERROR(st);

    st = Status::OK();
    return st;
}

Status FullCompaction::execute_compact() {
    Status st;
    Defer defer_set_st([&] {
        tablet()->set_last_full_compaction_status(st.to_string());
        if (!st.ok()) {
            tablet()->set_last_full_compaction_failure_time(UnixMillis());
        } else {
            tablet()->set_last_full_compaction_success_time(UnixMillis());
        }
    });
    std::unique_lock base_lock(tablet()->get_base_compaction_lock());
    std::unique_lock cumu_lock(tablet()->get_cumulative_compaction_lock());

    SCOPED_ATTACH_TASK(_mem_tracker);

    st = CompactionMixin::execute_compact();
    RETURN_IF_ERROR(st);

    tablet()->cumulative_compaction_policy()->update_compaction_level(tablet(), _input_rowsets,
                                                                      _output_rowset);

    Version last_version = _input_rowsets.back()->version();
    tablet()->cumulative_compaction_policy()->update_cumulative_point(tablet(), _input_rowsets,
                                                                      _output_rowset, last_version);
    VLOG_CRITICAL << "after cumulative compaction, current cumulative point is "
                  << tablet()->cumulative_layer_point() << ", tablet=" << _tablet->tablet_id();

    st = Status::OK();
    return st;
}

Status FullCompaction::pick_rowsets_to_compact() {
    _input_rowsets = tablet()->pick_candidate_rowsets_to_full_compaction();
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

Status FullCompaction::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets {_output_rowset};
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        std::vector<RowsetSharedPtr> tmp_rowsets {};

        // tablet is under alter process. The delete bitmap will be calculated after conversion.
        if (_tablet->tablet_state() == TABLET_NOTREADY) {
            LOG(INFO) << "tablet is under alter process, update delete bitmap later, tablet_id="
                      << _tablet->tablet_id();
            return Status::OK();
        }

        int64_t max_version = tablet()->max_version().second;
        DCHECK(max_version >= _output_rowset->version().second);
        if (max_version > _output_rowset->version().second) {
            auto ret = DORIS_TRY(_tablet->capture_consistent_rowsets_unlocked(
                    {_output_rowset->version().second + 1, max_version}, CaptureRowsetOps {}));
            tmp_rowsets = std::move(ret.rowsets);
        }

        for (const auto& it : tmp_rowsets) {
            const int64_t& cur_version = it->rowset_meta()->start_version();
            RETURN_IF_ERROR(_full_compaction_calc_delete_bitmap(it, _output_rowset, cur_version,
                                                                _output_rs_writer.get()));
        }

        DBUG_EXECUTE_IF("FullCompaction.modify_rowsets.before.block", DBUG_BLOCK);
        std::lock_guard rowset_update_lock(tablet()->get_rowset_update_lock());
        std::lock_guard header_lock(_tablet->get_header_lock());
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        for (const auto& it : tablet()->rowset_map()) {
            const int64_t& cur_version = it.first.first;
            const RowsetSharedPtr& published_rowset = it.second;
            if (cur_version > max_version) {
                RETURN_IF_ERROR(_full_compaction_calc_delete_bitmap(
                        published_rowset, _output_rowset, cur_version, _output_rs_writer.get()));
            }
        }
        RETURN_IF_ERROR(tablet()->modify_rowsets(output_rowsets, _input_rowsets, true));
        DBUG_EXECUTE_IF("FullCompaction.modify_rowsets.sleep", { sleep(5); })
        tablet()->save_meta();
    } else {
        DBUG_EXECUTE_IF("FullCompaction.modify_rowsets.before.block", DBUG_BLOCK);
        std::lock_guard<std::mutex> rowset_update_wlock(tablet()->get_rowset_update_lock());
        std::lock_guard<std::shared_mutex> meta_wlock(_tablet->get_header_lock());
        RETURN_IF_ERROR(tablet()->modify_rowsets(output_rowsets, _input_rowsets, true));
        DBUG_EXECUTE_IF("FullCompaction.modify_rowsets.sleep", { sleep(5); })
        tablet()->save_meta();
    }
    return Status::OK();
}

Status FullCompaction::_check_all_version(const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.empty()) {
        return Status::Error<FULL_MISS_VERSION, false>(
                "There is no input rowset when do full compaction");
    }
    const RowsetSharedPtr& last_rowset = rowsets.back();
    const RowsetSharedPtr& first_rowset = rowsets.front();
    auto max_version = tablet()->max_version();
    if (last_rowset->version() != max_version || first_rowset->version().first != 0) {
        return Status::Error<FULL_MISS_VERSION, false>(
                "Full compaction rowsets' versions not equal to all exist rowsets' versions. "
                "full compaction rowsets max version={}-{}"
                ", current rowsets max version={}-{}"
                ", full compaction rowsets min version={}-{}, current rowsets min version=0-1",
                last_rowset->start_version(), last_rowset->end_version(), max_version.first,
                max_version.second, first_rowset->start_version(), first_rowset->end_version());
    }
    return Status::OK();
}

Status FullCompaction::_full_compaction_calc_delete_bitmap(const RowsetSharedPtr& published_rowset,
                                                           const RowsetSharedPtr& rowset,
                                                           int64_t cur_version,
                                                           RowsetWriter* rowset_writer) {
    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_IF_ERROR(
            std::static_pointer_cast<BetaRowset>(published_rowset)->load_segments(&segments));
    DeleteBitmapPtr delete_bitmap =
            std::make_shared<DeleteBitmap>(_tablet->tablet_meta()->tablet_id());
    std::vector<RowsetSharedPtr> specified_rowsets {rowset};

    OlapStopWatch watch;
    RETURN_IF_ERROR(BaseTablet::calc_delete_bitmap(_tablet, published_rowset, segments,
                                                   specified_rowsets, delete_bitmap, cur_version,
                                                   nullptr, rowset_writer));
    size_t total_rows = std::accumulate(
            segments.begin(), segments.end(), 0,
            [](size_t sum, const segment_v2::SegmentSharedPtr& s) { return sum += s->num_rows(); });
    for (const auto& [k, v] : delete_bitmap->delete_bitmap) {
        if (std::get<1>(k) != DeleteBitmap::INVALID_SEGMENT_ID) {
            _tablet->tablet_meta()->delete_bitmap()->merge(
                    {std::get<0>(k), std::get<1>(k), cur_version}, v);
        }
    }
    VLOG_DEBUG << "[Full compaction] construct delete bitmap tablet: " << _tablet->tablet_id()
               << ", published rowset version: [" << published_rowset->version().first << "-"
               << published_rowset->version().second << "]"
               << ", full compaction rowset version: [" << rowset->version().first << "-"
               << rowset->version().second << "]"
               << ", cost: " << watch.get_elapse_time_us() << "(us), total rows: " << total_rows;
    return Status::OK();
}

} // namespace doris
