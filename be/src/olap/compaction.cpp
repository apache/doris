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
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet.h"
#include "olap/task/engine_checksum_task.h"
#include "util/time.h"
#include "util/trace.h"

using std::vector;

namespace doris {

Compaction::Compaction(TabletSharedPtr tablet, const std::string& label)
        : _tablet(tablet),
          _input_rowsets_size(0),
          _input_row_num(0),
          _input_num_segments(0),
          _input_index_size(0),
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

Status Compaction::do_compaction(int64_t permits) {
    TRACE("start to do compaction");
    uint32_t checksum_before;
    uint32_t checksum_after;
    if (config::enable_compaction_checksum) {
        EngineChecksumTask checksum_task(_tablet->tablet_id(), _tablet->schema_hash(),
                                         _input_rowsets.back()->end_version(), &checksum_before);
        checksum_task.execute();
    }

    _tablet->data_dir()->disks_compaction_score_increment(permits);
    _tablet->data_dir()->disks_compaction_num_increment(1);
    Status st = do_compaction_impl(permits);
    _tablet->data_dir()->disks_compaction_score_increment(-permits);
    _tablet->data_dir()->disks_compaction_num_increment(-1);

    if (config::enable_compaction_checksum) {
        EngineChecksumTask checksum_task(_tablet->tablet_id(), _tablet->schema_hash(),
                                         _input_rowsets.back()->end_version(), &checksum_after);
        checksum_task.execute();
        if (checksum_before != checksum_after) {
            LOG(WARNING) << "Compaction tablet=" << _tablet->tablet_id()
                         << " checksum not consistent"
                         << ", before=" << checksum_before << ", checksum_after=" << checksum_after;
        }
    }
    return st;
}

bool Compaction::should_vertical_compaction() {
    // some conditions that not use vertical compaction
    if (!config::enable_vertical_compaction) {
        return false;
    }
    if (_tablet->enable_unique_key_merge_on_write()) {
        return false;
    }
    return true;
}

int64_t Compaction::get_avg_segment_rows() {
    // take care of empty rowset
    // input_rowsets_size is total disk_size of input_rowset, this size is the
    // final size after codec and compress, so expect dest segment file size
    // in disk is config::writer_buffer_size
    return config::write_buffer_size / (_input_rowsets_size / (_input_row_num + 1) + 1);
}

bool Compaction::is_rowset_tidy(std::string& pre_max_key, const RowsetSharedPtr& rhs) {
    size_t min_tidy_size = config::ordered_data_compaction_min_segment_size;
    if (rhs->num_segments() == 0) {
        return true;
    }
    if (rhs->is_segments_overlapping()) {
        return false;
    }
    // check segment size
    auto beta_rowset = reinterpret_cast<BetaRowset*>(rhs.get());
    std::vector<size_t> segments_size;
    beta_rowset->get_segments_size(&segments_size);
    for (auto segment_size : segments_size) {
        // is segment is too small, need to do compaction
        if (segment_size < min_tidy_size) {
            return false;
        }
    }
    std::string min_key;
    auto ret = rhs->min_key(&min_key);
    if (!ret) {
        return false;
    }
    if (min_key < pre_max_key) {
        return false;
    }
    CHECK(rhs->max_key(&pre_max_key));

    return true;
}

Status Compaction::do_compact_ordered_rowsets() {
    build_basic_info();
    RETURN_NOT_OK(construct_output_rowset_writer());

    LOG(INFO) << "start to do ordered data compaction, tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version;
    // link data to new rowset
    auto seg_id = 0;
    std::vector<KeyBoundsPB> segment_key_bounds;
    for (auto rowset : _input_rowsets) {
        RETURN_NOT_OK(rowset->link_files_to(_tablet->tablet_path(), _output_rs_writer->rowset_id(),
                                            seg_id));
        seg_id += rowset->num_segments();

        std::vector<KeyBoundsPB> key_bounds;
        rowset->get_segments_key_bounds(&key_bounds);
        segment_key_bounds.insert(segment_key_bounds.end(), key_bounds.begin(), key_bounds.end());
    }
    // build output rowset
    RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
    rowset_meta->set_num_rows(_input_row_num);
    rowset_meta->set_total_disk_size(_input_rowsets_size);
    rowset_meta->set_data_disk_size(_input_rowsets_size);
    rowset_meta->set_index_disk_size(_input_index_size);
    rowset_meta->set_empty(_input_row_num == 0);
    rowset_meta->set_num_segments(_input_num_segments);
    rowset_meta->set_segments_overlap(NONOVERLAPPING);
    rowset_meta->set_rowset_state(VISIBLE);

    rowset_meta->set_segments_key_bounds(segment_key_bounds);
    _output_rowset = _output_rs_writer->manual_build(rowset_meta);
    return Status::OK();
}

void Compaction::build_basic_info() {
    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_index_size += rowset->index_disk_size();
        _input_row_num += rowset->num_rows();
        _input_num_segments += rowset->num_segments();
    }
    TRACE_COUNTER_INCREMENT("input_rowsets_data_size", _input_rowsets_size);
    TRACE_COUNTER_INCREMENT("input_row_num", _input_row_num);
    TRACE_COUNTER_INCREMENT("input_segments_num", _input_num_segments);

    _output_version =
            Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());

    _oldest_write_timestamp = _input_rowsets.front()->oldest_write_timestamp();
    _newest_write_timestamp = _input_rowsets.back()->newest_write_timestamp();

    std::vector<RowsetMetaSharedPtr> rowset_metas(_input_rowsets.size());
    std::transform(_input_rowsets.begin(), _input_rowsets.end(), rowset_metas.begin(),
                   [](const RowsetSharedPtr& rowset) { return rowset->rowset_meta(); });
    _cur_tablet_schema =
            _tablet->rowset_meta_with_max_schema_version(rowset_metas)->tablet_schema();
}

bool Compaction::handle_ordered_data_compaction() {
    if (!config::enable_ordered_data_compaction) {
        return false;
    }
    // check delete version: if compaction type is base compaction and
    // has a delete version, use original compaction
    if (compaction_type() == ReaderType::READER_BASE_COMPACTION) {
        for (auto rowset : _input_rowsets) {
            if (_tablet->version_for_delete_predicate(rowset->version())) {
                return false;
            }
        }
    }

    // check if rowsets are tidy so we can just modify meta and do link
    // files to handle compaction
    auto input_size = _input_rowsets.size();
    std::string pre_max_key;
    for (auto i = 0; i < input_size; ++i) {
        if (!is_rowset_tidy(pre_max_key, _input_rowsets[i])) {
            if (i <= input_size / 2) {
                return false;
            } else {
                _input_rowsets.resize(i);
                break;
            }
        }
    }
    // most rowset of current compaction is nonoverlapping
    // just handle nonoverlappint rowsets
    auto st = do_compact_ordered_rowsets();
    if (!st.ok()) {
        return false;
    }
    return true;
}

Status Compaction::do_compaction_impl(int64_t permits) {
    OlapStopWatch watch;

    auto use_vectorized_compaction = config::enable_vectorized_compaction;
    string merge_type = use_vectorized_compaction ? "v" : "";

    if (handle_ordered_data_compaction()) {
        RETURN_NOT_OK(modify_rowsets());
        TRACE("modify rowsets finished");

        int64_t now = UnixMillis();
        if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
            _tablet->set_last_cumu_compaction_success_time(now);
        } else {
            _tablet->set_last_base_compaction_success_time(now);
        }
        auto cumu_policy = _tablet->cumulative_compaction_policy();
        LOG(INFO) << "succeed to do ordered data " << merge_type << compaction_name()
                  << ". tablet=" << _tablet->full_name() << ", output_version=" << _output_version
                  << ", disk=" << _tablet->data_dir()->path()
                  << ", segments=" << _input_num_segments << ", input_row_num=" << _input_row_num
                  << ", output_row_num=" << _output_rowset->num_rows()
                  << ". elapsed time=" << watch.get_elapse_second()
                  << "s. cumulative_compaction_policy="
                  << (cumu_policy == nullptr ? "quick" : cumu_policy->name());
        return Status::OK();
    }
    build_basic_info();

    LOG(INFO) << "start " << merge_type << compaction_name() << ". tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version << ", permits: " << permits;
    bool vertical_compaction = should_vertical_compaction();
    RETURN_NOT_OK(construct_input_rowset_readers());
    RETURN_NOT_OK(construct_output_rowset_writer(vertical_compaction));
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
        if (vertical_compaction) {
            res = Merger::vertical_merge_rowsets(_tablet, compaction_type(), _cur_tablet_schema,
                                                 _input_rs_readers, _output_rs_writer.get(),
                                                 get_avg_segment_rows(), &stats);
        } else {
            res = Merger::vmerge_rowsets(_tablet, compaction_type(), _cur_tablet_schema,
                                         _input_rs_readers, _output_rs_writer.get(), &stats);
        }
    } else {
        res = Merger::merge_rowsets(_tablet, compaction_type(), _cur_tablet_schema,
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
    DCHECK(cumu_policy);
    LOG(INFO) << "succeed to do " << merge_type << compaction_name()
              << " is_vertical=" << vertical_compaction << ". tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version
              << ", current_max_version=" << current_max_version
              << ", disk=" << _tablet->data_dir()->path() << ", segments=" << _input_num_segments
              << ", input_row_num=" << _input_row_num
              << ", output_row_num=" << _output_rowset->num_rows()
              << ". elapsed time=" << watch.get_elapse_second()
              << "s. cumulative_compaction_policy=" << cumu_policy->name()
              << ", compact_row_per_second=" << int(_input_row_num / watch.get_elapse_second());

    return Status::OK();
}

Status Compaction::construct_output_rowset_writer(bool is_vertical) {
    if (is_vertical) {
        return _tablet->create_vertical_rowset_writer(_output_version, VISIBLE, NONOVERLAPPING,
                                                      _cur_tablet_schema, _oldest_write_timestamp,
                                                      _newest_write_timestamp, &_output_rs_writer);
    }
    return _tablet->create_rowset_writer(_output_version, VISIBLE, NONOVERLAPPING,
                                         _cur_tablet_schema, _oldest_write_timestamp,
                                         _newest_write_timestamp, &_output_rs_writer);
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

#ifdef BE_TEST
void Compaction::set_input_rowset(const std::vector<RowsetSharedPtr>& rowsets) {
    _input_rowsets = rowsets;
}

RowsetSharedPtr Compaction::output_rowset() {
    return _output_rowset;
}
#endif
} // namespace doris
