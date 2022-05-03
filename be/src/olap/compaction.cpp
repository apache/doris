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

#include "gutil/strings/substitute.h"
#include "olap/rowset/rowset_factory.h"
#include "util/time.h"
#include "util/trace.h"

using std::vector;

namespace doris {

Compaction::Compaction(TabletSharedPtr tablet, const std::string& label)
        : _mem_tracker(MemTracker::create_tracker(-1, label, nullptr, MemTrackerLevel::INSTANCE)),
          _tablet(tablet),
          _input_rowsets_size(0),
          _input_row_num(0),
          _state(CompactionState::INITED) {}

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

    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version << ", permits: " << permits;

    RETURN_NOT_OK(construct_output_rowset_writer());
    RETURN_NOT_OK(construct_input_rowset_readers());
    TRACE("prepare finished");

    // 2. write merged rows to output rowset
    // The test results show that merger is low-memory-footprint, there is no need to tracker its mem pool
    Merger::Statistics stats;
    Status res;
    if (config::enable_vectorized_compaction) {
        res = Merger::vmerge_rowsets(_tablet, compaction_type(), _input_rs_readers,
                                     _output_rs_writer.get(), &stats);
    } else {
        res = Merger::merge_rowsets(_tablet, compaction_type(), _input_rs_readers,
                                    _output_rs_writer.get(), &stats);
    }
    string merge_type = config::enable_vectorized_compaction ? "v" : "";
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
        return Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
    }
    TRACE_COUNTER_INCREMENT("output_rowset_data_size", _output_rowset->data_disk_size());
    TRACE_COUNTER_INCREMENT("output_row_num", _output_rowset->num_rows());
    TRACE_COUNTER_INCREMENT("output_segments_num", _output_rowset->num_segments());
    TRACE("output rowset built");

    // 3. check correctness
    RETURN_NOT_OK(check_correctness(stats));
    TRACE("check correctness finished");

    // 4. modify rowsets in memory
    modify_rowsets();
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
        current_max_version = _tablet->rowset_with_max_version()->end_version();
    }

    LOG(INFO) << "succeed to do " << merge_type << compaction_name()
              << ". tablet=" << _tablet->full_name() << ", output_version=" << _output_version
              << ", current_max_version=" << current_max_version
              << ", disk=" << _tablet->data_dir()->path() << ", segments=" << segments_num
              << ". elapsed time=" << watch.get_elapse_second()
              << "s. cumulative_compaction_policy="
              << _tablet->cumulative_compaction_policy()->name() << ".";

    return Status::OK();
}

Status Compaction::construct_output_rowset_writer() {
    RowsetWriterContext context;
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = _tablet->tablet_uid();
    context.tablet_id = _tablet->tablet_id();
    context.partition_id = _tablet->partition_id();
    context.tablet_schema_hash = _tablet->schema_hash();
    context.data_dir = _tablet->data_dir();
    context.rowset_type = StorageEngine::instance()->default_rowset_type();
    if (_tablet->tablet_meta()->preferred_rowset_type() == BETA_ROWSET) {
        context.rowset_type = BETA_ROWSET;
    }
    context.path_desc = _tablet->tablet_path_desc();
    context.tablet_schema = &(_tablet->tablet_schema());
    context.rowset_state = VISIBLE;
    context.version = _output_version;
    context.segments_overlap = NONOVERLAPPING;
    // The test results show that one rs writer is low-memory-footprint, there is no need to tracker its mem pool
    RETURN_NOT_OK(RowsetFactory::create_rowset_writer(context, &_output_rs_writer));
    return Status::OK();
}

Status Compaction::construct_input_rowset_readers() {
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_NOT_OK(rowset->create_reader(&rs_reader));
        _input_rs_readers.push_back(std::move(rs_reader));
    }
    return Status::OK();
}

void Compaction::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);

    std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
    _tablet->modify_rowsets(output_rowsets, _input_rowsets);
    _tablet->save_meta();
}

void Compaction::gc_output_rowset() {
    if (_state != CompactionState::SUCCESS && _output_rowset != nullptr) {
        StorageEngine::instance()->add_unused_rowset(_output_rowset);
    }
}

// Find the longest consecutive version path in "rowset", from begining.
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
                     << "input_row_num=" << _input_row_num
                     << ", merged_row_num=" << stats.merged_rows
                     << ", filtered_row_num=" << stats.filtered_rows
                     << ", output_row_num=" << _output_rowset->num_rows();

        // ATTN(cmy): We found that the num_rows in some rowset meta may be set to the wrong value,
        // but it is not known which version of the code has the problem. So when the compaction
        // result is inconsistent, we then try to verify by num_rows recorded in segment_groups.
        // If the check passes, ignore the error and set the correct value in the output rowset meta
        // to fix this problem.
        // Only handle alpha rowset because we only find this bug in alpha rowset
        int64_t num_rows = _get_input_num_rows_from_seg_grps();
        if (num_rows == -1) {
            return Status::OLAPInternalError(OLAP_ERR_CHECK_LINES_ERROR);
        }
        if (num_rows != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
            // If it is still incorrect, it may be another problem
            LOG(WARNING) << "row_num got from seg groups does not match between cumulative input "
                            "and output! "
                         << "input_row_num=" << num_rows << ", merged_row_num=" << stats.merged_rows
                         << ", filtered_row_num=" << stats.filtered_rows
                         << ", output_row_num=" << _output_rowset->num_rows();

            return Status::OLAPInternalError(OLAP_ERR_CHECK_LINES_ERROR);
        }
    }
    return Status::OK();
}

int64_t Compaction::_get_input_num_rows_from_seg_grps() {
    int64_t num_rows = 0;
    for (auto& rowset : _input_rowsets) {
        if (rowset->rowset_meta()->rowset_type() != RowsetTypePB::ALPHA_ROWSET) {
            return -1;
        }
        for (auto& seg_grp : rowset->rowset_meta()->alpha_rowset_extra_meta_pb().segment_groups()) {
            num_rows += seg_grp.num_rows();
        }
    }
    return num_rows;
}

int64_t Compaction::get_compaction_permits() {
    int64_t permits = 0;
    for (auto rowset : _input_rowsets) {
        permits += rowset->rowset_meta()->get_compaction_score();
    }
    return permits;
}

} // namespace doris
