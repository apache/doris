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

#include "olap/compaction.h"

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet.h"
#include "util/time.h"
#include "util/trace.h"

using std::vector;

namespace doris {

Compaction::Compaction(TabletSharedPtr tablet, const std::string& label)
        : _tablet(tablet),
          _input_rowsets_size(0),
          _input_row_num(0),
          _state(CompactionState::INITED) {
    _mem_tracker = std::make_shared<MemTrackerLimiter>(MemTrackerLimiter::Type::COMPACTION, label);
}

Compaction::~Compaction() {}

Status Compaction::compact() {
    RETURN_NOT_OK(prepare_compact());
    RETURN_NOT_OK(execute_compact());
    return Status::OK();
}

Status Compaction::execute_compact() {
    Status st = execute_compact_impl();
    if (!st.ok()) {
        gc_output_rowset();
    }
    return st;
}

Status Compaction::quick_rowsets_compact() {
    std::unique_lock<std::mutex> lock(_tablet->get_cumulative_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        LOG(WARNING) << "The tablet is under cumulative compaction. tablet="
                     << _tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_CE_TRY_CE_LOCK_ERROR);
    }

    // Clone task may happen after compaction task is submitted to thread pool, and rowsets picked
    // for compaction may change. In this case, current compaction task should not be executed.
    if (_tablet->get_clone_occurred()) {
        _tablet->set_clone_occurred(false);
        return Status::OLAPInternalError(OLAP_ERR_CUMULATIVE_CLONE_OCCURRED);
    }

    _input_rowsets.clear();
    int version_count = _tablet->version_count();
    MonotonicStopWatch watch;
    watch.start();
    int64_t permits = 0;
    _tablet->pick_quick_compaction_rowsets(&_input_rowsets, &permits);
    std::vector<Version> missedVersions;
    find_longest_consecutive_version(&_input_rowsets, &missedVersions);
    if (missedVersions.size() != 0) {
        LOG(WARNING) << "quick_rowsets_compaction, find missed version"
                     << ",input_size:" << _input_rowsets.size();
    }
    int nums = _input_rowsets.size();
    if (_input_rowsets.size() >= config::quick_compaction_min_rowsets) {
        Status st = check_version_continuity(_input_rowsets);
        if (!st.ok()) {
            LOG(WARNING) << "quick_rowsets_compaction failed, cause version not continuous";
            return st;
        }
        st = do_compaction(permits);
        if (!st.ok()) {
            gc_output_rowset();
            LOG(WARNING) << "quick_rowsets_compaction failed";
        } else {
            LOG(INFO) << "quick_compaction succ"
                      << ", before_versions:" << version_count
                      << ", after_versions:" << _tablet->version_count()
                      << ", cost:" << (watch.elapsed_time() / 1000 / 1000) << "ms"
                      << ", merged: " << nums << ", batch:" << config::quick_compaction_batch_size
                      << ", segments:" << permits << ", tabletid:" << _tablet->tablet_id();
            _tablet->set_last_quick_compaction_success_time(UnixMillis());
        }
    }
    return Status::OK();
}

Status Compaction::do_compaction(int64_t permits) {
    TRACE("start to do compaction");
    _tablet->data_dir()->disks_compaction_score_increment(permits);
    _tablet->data_dir()->disks_compaction_num_increment(1);
    Status st = do_compaction_impl(permits);
    _tablet->data_dir()->disks_compaction_score_increment(-permits);
    _tablet->data_dir()->disks_compaction_num_increment(-1);
    return st;
}

Status Compaction::do_compaction_impl(int64_t permits) {
    OlapStopWatch watch;

    // 1. prepare input and output parameters
    int64_t segments_num = 0;
    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_row_num += rowset->num_rows();
        segments_num += rowset->num_segments();
    }
    TRACE_COUNTER_INCREMENT("input_rowsets_data_size", _input_rowsets_size);
    TRACE_COUNTER_INCREMENT("input_row_num", _input_row_num);
    TRACE_COUNTER_INCREMENT("input_segments_num", segments_num);

    _output_version =
            Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());

    _oldest_write_timestamp = _input_rowsets.front()->oldest_write_timestamp();
    _newest_write_timestamp = _input_rowsets.back()->newest_write_timestamp();

    auto use_vectorized_compaction = config::enable_vectorized_compaction;
    string merge_type = use_vectorized_compaction ? "v" : "";

    LOG(INFO) << "start " << merge_type << compaction_name() << ". tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version << ", permits: " << permits;
    // get cur schema if rowset schema exist, rowset schema must be newer than tablet schema
    std::vector<RowsetMetaSharedPtr> rowset_metas(_input_rowsets.size());
    std::transform(_input_rowsets.begin(), _input_rowsets.end(), rowset_metas.begin(),
                   [](const RowsetSharedPtr& rowset) { return rowset->rowset_meta(); });
    TabletSchemaSPtr cur_tablet_schema =
            _tablet->rowset_meta_with_max_schema_version(rowset_metas)->tablet_schema();

    RETURN_NOT_OK(construct_output_rowset_writer(cur_tablet_schema));
    RETURN_NOT_OK(construct_input_rowset_readers());
    TRACE("prepare finished");

    // 2. write merged rows to output rowset
    // The test results show that merger is low-memory-footprint, there is no need to tracker its mem pool
    Merger::Statistics stats;
    Status res;
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        stats.rowid_conversion = &_rowid_conversion;
    }

    if (use_vectorized_compaction) {
        res = Merger::vmerge_rowsets(_tablet, compaction_type(), cur_tablet_schema,
                                     _input_rs_readers, _output_rs_writer.get(), &stats);
    } else {
        res = Merger::merge_rowsets(_tablet, compaction_type(), cur_tablet_schema,
                                    _input_rs_readers, _output_rs_writer.get(), &stats);
    }

    if (!res.ok()) {
        LOG(WARNING) << "fail to do " << merge_type << compaction_name() << ". res=" << res
                     << ", tablet=" << _tablet->full_name()
                     << ", output_version=" << _output_version;
        return res;
    }
    TRACE("merge rowsets finished");
    TRACE_COUNTER_INCREMENT("merged_rows", stats.merged_rows);
    TRACE_COUNTER_INCREMENT("filtered_rows", stats.filtered_rows);

    _output_rowset = _output_rs_writer->build();
    if (_output_rowset == nullptr) {
        LOG(WARNING) << "rowset writer build failed. writer version:"
                     << ", output_version=" << _output_version;
        return Status::OLAPInternalError(OLAP_ERR_ROWSET_BUILDER_INIT);
    }
    TRACE_COUNTER_INCREMENT("output_rowset_data_size", _output_rowset->data_disk_size());
    TRACE_COUNTER_INCREMENT("output_row_num", _output_rowset->num_rows());
    TRACE_COUNTER_INCREMENT("output_segments_num", _output_rowset->num_segments());
    TRACE("output rowset built");

    // 3. check correctness
    RETURN_NOT_OK(check_correctness(stats));
    TRACE("check correctness finished");

    // 4. modify rowsets in memory
    RETURN_NOT_OK(modify_rowsets());
    TRACE("modify rowsets finished");

    // 5. update last success compaction time
    int64_t now = UnixMillis();
    // TODO(yingchun): do the judge in Tablet class
    if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_success_time(now);
    } else {
        _tablet->set_last_base_compaction_success_time(now);
    }

    int64_t current_max_version;
    {
        std::shared_lock rdlock(_tablet->get_header_lock());
        RowsetSharedPtr max_rowset = _tablet->rowset_with_max_version();
        if (max_rowset == nullptr) {
            current_max_version = -1;
        } else {
            current_max_version = _tablet->rowset_with_max_version()->end_version();
        }
    }

    auto cumu_policy = _tablet->cumulative_compaction_policy();
    LOG(INFO) << "succeed to do " << merge_type << compaction_name()
              << ". tablet=" << _tablet->full_name() << ", output_version=" << _output_version
              << ", current_max_version=" << current_max_version
              << ", disk=" << _tablet->data_dir()->path() << ", segments=" << segments_num
              << ", input_row_num=" << _input_row_num
              << ", output_row_num=" << _output_rowset->num_rows()
              << ". elapsed time=" << watch.get_elapse_second()
              << "s. cumulative_compaction_policy="
              << (cumu_policy == nullptr ? "quick" : cumu_policy->name())
              << ", compact_row_per_second=" << int(_input_row_num / watch.get_elapse_second());

    return Status::OK();
}

Status Compaction::construct_output_rowset_writer(TabletSchemaSPtr schema) {
    return _tablet->create_rowset_writer(_output_version, VISIBLE, NONOVERLAPPING, schema,
                                         _oldest_write_timestamp, _newest_write_timestamp,
                                         &_output_rs_writer);
}

Status Compaction::construct_input_rowset_readers() {
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_NOT_OK(rowset->create_reader(&rs_reader));
        _input_rs_readers.push_back(std::move(rs_reader));
    }
    return Status::OK();
}

Status Compaction::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);
    {
        std::lock_guard<std::mutex> wrlock_(_tablet->get_rowset_update_lock());
        std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());

        // update dst rowset delete bitmap
        if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
            _tablet->enable_unique_key_merge_on_write()) {
            _tablet->tablet_meta()->update_delete_bitmap(
                    _input_rowsets, _output_rs_writer->version(), _rowid_conversion);
        }

        RETURN_NOT_OK(_tablet->modify_rowsets(output_rowsets, _input_rowsets, true));
    }
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        _tablet->save_meta();
    }
    return Status::OK();
}

void Compaction::gc_output_rowset() {
    if (_state != CompactionState::SUCCESS && _output_rowset != nullptr) {
        StorageEngine::instance()->add_unused_rowset(_output_rowset);
    }
}

// Find the longest consecutive version path in "rowset", from beginning.
// Two versions before and after the missing version will be saved in missing_version,
// if missing_version is not null.
Status Compaction::find_longest_consecutive_version(std::vector<RowsetSharedPtr>* rowsets,
                                                    std::vector<Version>* missing_version) {
    if (rowsets->empty()) {
        return Status::OK();
    }
    RowsetSharedPtr prev_rowset = rowsets->front();
    size_t i = 1;
    for (; i < rowsets->size(); ++i) {
        RowsetSharedPtr rowset = (*rowsets)[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            if (missing_version != nullptr) {
                missing_version->push_back(prev_rowset->version());
                missing_version->push_back(rowset->version());
            }
            break;
        }
        prev_rowset = rowset;
    }

    rowsets->resize(i);
    return Status::OK();
}

Status Compaction::check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.empty()) {
        return Status::OK();
    }
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (size_t i = 1; i < rowsets.size(); ++i) {
        RowsetSharedPtr rowset = rowsets[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            LOG(WARNING) << "There are missed versions among rowsets. "
                         << "prev_rowset version=" << prev_rowset->start_version() << "-"
                         << prev_rowset->end_version()
                         << ", rowset version=" << rowset->start_version() << "-"
                         << rowset->end_version();
            return Status::OLAPInternalError(OLAP_ERR_CUMULATIVE_MISS_VERSION);
        }
        prev_rowset = rowset;
    }

    return Status::OK();
}

Status Compaction::check_correctness(const Merger::Statistics& stats) {
    // 1. check row number
    if (_input_row_num != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
        LOG(WARNING) << "row_num does not match between cumulative input and output! "
                     << "tablet=" << _tablet->full_name() << ", input_row_num=" << _input_row_num
                     << ", merged_row_num=" << stats.merged_rows
                     << ", filtered_row_num=" << stats.filtered_rows
                     << ", output_row_num=" << _output_rowset->num_rows();
        return Status::OLAPInternalError(OLAP_ERR_CHECK_LINES_ERROR);
    }
    return Status::OK();
}

int64_t Compaction::get_compaction_permits() {
    int64_t permits = 0;
    for (auto rowset : _input_rowsets) {
        permits += rowset->rowset_meta()->get_compaction_score();
    }
    return permits;
}

} // namespace doris
