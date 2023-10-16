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

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <set>
#include <shared_mutex>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/remote_file_system.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/inverted_index_compaction.h"
#include "olap/rowset/segment_v2/inverted_index_compound_directory.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/task/engine_checksum_task.h"
#include "olap/txn_manager.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/time.h"
#include "util/trace.h"

using std::vector;

namespace doris {
using namespace ErrorCode;

Compaction::Compaction(const TabletSharedPtr& tablet, const std::string& label)
        : _tablet(tablet),
          _input_rowsets_size(0),
          _input_row_num(0),
          _input_num_segments(0),
          _input_index_size(0),
          _state(CompactionState::INITED),
          _allow_delete_in_cumu_compaction(config::enable_delete_when_cumu_compaction) {
    _mem_tracker = std::make_shared<MemTrackerLimiter>(MemTrackerLimiter::Type::COMPACTION, label);
    init_profile(label);
}

Compaction::~Compaction() {}

void Compaction::init_profile(const std::string& label) {
    _profile = std::make_unique<RuntimeProfile>(label);

    _input_rowsets_data_size_counter =
            ADD_COUNTER(_profile, "input_rowsets_data_size", TUnit::BYTES);
    _input_rowsets_counter = ADD_COUNTER(_profile, "input_rowsets_count", TUnit::UNIT);
    _input_row_num_counter = ADD_COUNTER(_profile, "input_row_num", TUnit::UNIT);
    _input_segments_num_counter = ADD_COUNTER(_profile, "input_segments_num", TUnit::UNIT);
    _merged_rows_counter = ADD_COUNTER(_profile, "merged_rows", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "filtered_rows", TUnit::UNIT);
    _output_rowset_data_size_counter =
            ADD_COUNTER(_profile, "output_rowset_data_size", TUnit::BYTES);
    _output_row_num_counter = ADD_COUNTER(_profile, "output_row_num", TUnit::UNIT);
    _output_segments_num_counter = ADD_COUNTER(_profile, "output_segments_num", TUnit::UNIT);
    _merge_rowsets_latency_timer = ADD_TIMER(_profile, "merge_rowsets_latency");
}

Status Compaction::compact() {
    RETURN_IF_ERROR(prepare_compact());
    RETURN_IF_ERROR(execute_compact());
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
    uint32_t checksum_before;
    uint32_t checksum_after;
    if (config::enable_compaction_checksum) {
        EngineChecksumTask checksum_task(_tablet->tablet_id(), _tablet->schema_hash(),
                                         _input_rowsets.back()->end_version(), &checksum_before);
        static_cast<void>(checksum_task.execute());
    }

    _tablet->data_dir()->disks_compaction_score_increment(permits);
    _tablet->data_dir()->disks_compaction_num_increment(1);
    Status st = do_compaction_impl(permits);
    _tablet->data_dir()->disks_compaction_score_increment(-permits);
    _tablet->data_dir()->disks_compaction_num_increment(-1);

    if (config::enable_compaction_checksum) {
        EngineChecksumTask checksum_task(_tablet->tablet_id(), _tablet->schema_hash(),
                                         _input_rowsets.back()->end_version(), &checksum_after);
        static_cast<void>(checksum_task.execute());
        if (checksum_before != checksum_after) {
            LOG(WARNING) << "Compaction tablet=" << _tablet->tablet_id()
                         << " checksum not consistent"
                         << ", before=" << checksum_before << ", checksum_after=" << checksum_after;
        }
    }
    if (st.ok()) {
        _load_segment_to_cache();
    }
    return st;
}

bool Compaction::should_vertical_compaction() {
    // some conditions that not use vertical compaction
    if (!config::enable_vertical_compaction) {
        return false;
    }
    return true;
}

int64_t Compaction::get_avg_segment_rows() {
    // take care of empty rowset
    // input_rowsets_size is total disk_size of input_rowset, this size is the
    // final size after codec and compress, so expect dest segment file size
    // in disk is config::vertical_compaction_max_segment_size
    const auto& meta = _tablet->tablet_meta();
    if (meta->compaction_policy() == CUMULATIVE_TIME_SERIES_POLICY) {
        int64_t compaction_goal_size_mbytes = meta->time_series_compaction_goal_size_mbytes();
        return (compaction_goal_size_mbytes * 1024 * 1024 * 2) /
               (_input_rowsets_size / (_input_row_num + 1) + 1);
    }
    return config::vertical_compaction_max_segment_size /
           (_input_rowsets_size / (_input_row_num + 1) + 1);
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
    static_cast<void>(beta_rowset->get_segments_size(&segments_size));
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
    RowsetWriterContext ctx;
    RETURN_IF_ERROR(construct_output_rowset_writer(ctx));

    LOG(INFO) << "start to do ordered data compaction, tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version;
    // link data to new rowset
    auto seg_id = 0;
    std::vector<KeyBoundsPB> segment_key_bounds;
    for (auto rowset : _input_rowsets) {
        RETURN_IF_ERROR(rowset->link_files_to(_tablet->tablet_path(),
                                              _output_rs_writer->rowset_id(), seg_id));
        seg_id += rowset->num_segments();

        std::vector<KeyBoundsPB> key_bounds;
        static_cast<void>(rowset->get_segments_key_bounds(&key_bounds));
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
    COUNTER_UPDATE(_input_rowsets_data_size_counter, _input_rowsets_size);
    COUNTER_UPDATE(_input_row_num_counter, _input_row_num);
    COUNTER_UPDATE(_input_segments_num_counter, _input_num_segments);

    _output_version =
            Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());

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
    if (compaction_type() == ReaderType::READER_COLD_DATA_COMPACTION) {
        // The remote file system does not support to link files.
        return false;
    }
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        return false;
    }
    // check delete version: if compaction type is base compaction and
    // has a delete version, use original compaction
    if (compaction_type() == ReaderType::READER_BASE_COMPACTION) {
        for (auto& rowset : _input_rowsets) {
            if (rowset->rowset_meta()->has_delete_predicate()) {
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

    if (handle_ordered_data_compaction()) {
        RETURN_IF_ERROR(modify_rowsets());

        int64_t now = UnixMillis();
        if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
            _tablet->set_last_cumu_compaction_success_time(now);
        } else if (compaction_type() == ReaderType::READER_BASE_COMPACTION) {
            _tablet->set_last_base_compaction_success_time(now);
        } else if (compaction_type() == ReaderType::READER_FULL_COMPACTION) {
            _tablet->set_last_full_compaction_success_time(now);
        }
        auto cumu_policy = _tablet->cumulative_compaction_policy();
        LOG(INFO) << "succeed to do ordered data " << compaction_name()
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

    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version << ", permits: " << permits;
    bool vertical_compaction = should_vertical_compaction();
    RowsetWriterContext ctx;
    RETURN_IF_ERROR(construct_input_rowset_readers());
    RETURN_IF_ERROR(construct_output_rowset_writer(ctx, vertical_compaction));
    if (compaction_type() == ReaderType::READER_COLD_DATA_COMPACTION) {
        Tablet::add_pending_remote_rowset(_output_rs_writer->rowset_id().to_string());
    }

    // 2. write merged rows to output rowset
    // The test results show that merger is low-memory-footprint, there is no need to tracker its mem pool
    Merger::Statistics stats;
    // if ctx.skip_inverted_index.size() > 0, it means we need to do inverted index compaction.
    // the row ID conversion matrix needs to be used for inverted index compaction.
    if (ctx.skip_inverted_index.size() > 0 || (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                                               _tablet->enable_unique_key_merge_on_write())) {
        stats.rowid_conversion = &_rowid_conversion;
    }

    Status res;
    {
        SCOPED_TIMER(_merge_rowsets_latency_timer);
        if (vertical_compaction) {
            res = Merger::vertical_merge_rowsets(_tablet, compaction_type(), _cur_tablet_schema,
                                                 _input_rs_readers, _output_rs_writer.get(),
                                                 get_avg_segment_rows(), &stats);
        } else {
            res = Merger::vmerge_rowsets(_tablet, compaction_type(), _cur_tablet_schema,
                                         _input_rs_readers, _output_rs_writer.get(), &stats);
        }
    }

    if (!res.ok()) {
        LOG(WARNING) << "fail to do " << compaction_name() << ". res=" << res
                     << ", tablet=" << _tablet->full_name()
                     << ", output_version=" << _output_version;
        return res;
    }
    COUNTER_UPDATE(_merged_rows_counter, stats.merged_rows);
    COUNTER_UPDATE(_filtered_rows_counter, stats.filtered_rows);

    _output_rowset = _output_rs_writer->build();
    if (_output_rowset == nullptr) {
        return Status::Error<ROWSET_BUILDER_INIT>("rowset writer build failed. output_version: {}",
                                                  _output_version.to_string());
    }
    // Now we support delete in cumu compaction, to make all data in rowsets whose version
    // is below output_version to be delete in the future base compaction, we should carry
    // all delete predicate in the output rowset.
    // Output start version > 2 means we must set the delete predicate in the output rowset
    if (allow_delete_in_cumu_compaction() && _output_rowset->version().first > 2) {
        DeletePredicatePB delete_predicate;
        std::accumulate(
                _input_rs_readers.begin(), _input_rs_readers.end(), &delete_predicate,
                [](DeletePredicatePB* delete_predicate, const RowsetReaderSharedPtr& reader) {
                    if (reader->rowset()->rowset_meta()->has_delete_predicate()) {
                        delete_predicate->MergeFrom(
                                reader->rowset()->rowset_meta()->delete_predicate());
                    }
                    return delete_predicate;
                });
        // now version in delete_predicate is deprecated
        if (!delete_predicate.in_predicates().empty() ||
            !delete_predicate.sub_predicates_v2().empty() ||
            !delete_predicate.sub_predicates().empty()) {
            _output_rowset->rowset_meta()->set_delete_predicate(std::move(delete_predicate));
        }
    }

    COUNTER_UPDATE(_output_rowset_data_size_counter, _output_rowset->data_disk_size());
    COUNTER_UPDATE(_output_row_num_counter, _output_rowset->num_rows());
    COUNTER_UPDATE(_output_segments_num_counter, _output_rowset->num_segments());

    // 3. check correctness
    RETURN_IF_ERROR(check_correctness(stats));

    if (_input_row_num > 0 && stats.rowid_conversion && config::inverted_index_compaction_enable) {
        OlapStopWatch inverted_watch;
        // translation vec
        // <<dest_idx_num, dest_docId>>
        // the first level vector: index indicates src segment.
        // the second level vector: index indicates row id of source segment,
        // value indicates row id of destination segment.
        // <UINT32_MAX, UINT32_MAX> indicates current row not exist.
        std::vector<std::vector<std::pair<uint32_t, uint32_t>>> trans_vec =
                stats.rowid_conversion->get_rowid_conversion_map();

        // source rowset,segment -> index_id
        std::map<std::pair<RowsetId, uint32_t>, uint32_t> src_seg_to_id_map =
                stats.rowid_conversion->get_src_segment_to_id_map();
        // dest rowset id
        RowsetId dest_rowset_id = stats.rowid_conversion->get_dst_rowset_id();
        // dest segment id -> num rows
        std::vector<uint32_t> dest_segment_num_rows;
        RETURN_IF_ERROR(_output_rs_writer->get_segment_num_rows(&dest_segment_num_rows));

        auto src_segment_num = src_seg_to_id_map.size();
        auto dest_segment_num = dest_segment_num_rows.size();

        if (dest_segment_num > 0) {
            // src index files
            // format: rowsetId_segmentId
            std::vector<std::string> src_index_files(src_segment_num);
            for (auto m : src_seg_to_id_map) {
                std::pair<RowsetId, uint32_t> p = m.first;
                src_index_files[m.second] = p.first.to_string() + "_" + std::to_string(p.second);
            }

            // dest index files
            // format: rowsetId_segmentId
            std::vector<std::string> dest_index_files(dest_segment_num);
            for (int i = 0; i < dest_segment_num; ++i) {
                auto prefix = dest_rowset_id.to_string() + "_" + std::to_string(i);
                dest_index_files[i] = prefix;
            }

            // create index_writer to compaction indexes
            auto& fs = _output_rowset->rowset_meta()->fs();
            auto& tablet_path = _tablet->tablet_path();

            // we choose the first destination segment name as the temporary index writer path
            // Used to distinguish between different index compaction
            auto index_writer_path = tablet_path + "/" + dest_index_files[0];
            LOG(INFO) << "start index compaction"
                      << ". tablet=" << _tablet->full_name()
                      << ", source index size=" << src_segment_num
                      << ", destination index size=" << dest_segment_num << ".";
            std::for_each(
                    ctx.skip_inverted_index.cbegin(), ctx.skip_inverted_index.cend(),
                    [&src_segment_num, &dest_segment_num, &index_writer_path, &src_index_files,
                     &dest_index_files, &fs, &tablet_path, &trans_vec, &dest_segment_num_rows,
                     this](int32_t column_uniq_id) {
                        auto st = compact_column(
                                _cur_tablet_schema->get_inverted_index(column_uniq_id)->index_id(),
                                src_segment_num, dest_segment_num, src_index_files,
                                dest_index_files, fs, index_writer_path, tablet_path, trans_vec,
                                dest_segment_num_rows);
                        if (!st.ok()) {
                            LOG(ERROR) << "failed to do index compaction"
                                       << ". tablet=" << _tablet->full_name()
                                       << ". column uniq id=" << column_uniq_id << ". index_id= "
                                       << _cur_tablet_schema->get_inverted_index(column_uniq_id)
                                                  ->index_id();
                        }
                    });

            LOG(INFO) << "succeed to do index compaction"
                      << ". tablet=" << _tablet->full_name()
                      << ", input row number=" << _input_row_num
                      << ", output row number=" << _output_rowset->num_rows()
                      << ". elapsed time=" << inverted_watch.get_elapse_second() << "s.";
        } else {
            LOG(INFO) << "skip doing index compaction due to no output segments"
                      << ". tablet=" << _tablet->full_name()
                      << ", input row number=" << _input_row_num
                      << ", output row number=" << _output_rowset->num_rows()
                      << ". elapsed time=" << inverted_watch.get_elapse_second() << "s.";
        }
    }

    // 4. modify rowsets in memory
    RETURN_IF_ERROR(modify_rowsets(&stats));

    // 5. update last success compaction time
    int64_t now = UnixMillis();
    // TODO(yingchun): do the judge in Tablet class
    if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_success_time(now);
    } else if (compaction_type() == ReaderType::READER_BASE_COMPACTION) {
        _tablet->set_last_base_compaction_success_time(now);
    } else if (compaction_type() == ReaderType::READER_FULL_COMPACTION) {
        _tablet->set_last_full_compaction_success_time(now);
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
    LOG(INFO) << "succeed to do " << compaction_name() << " is_vertical=" << vertical_compaction
              << ". tablet=" << _tablet->full_name() << ", output_version=" << _output_version
              << ", current_max_version=" << current_max_version
              << ", disk=" << _tablet->data_dir()->path() << ", segments=" << _input_num_segments
              << ", input_row_num=" << _input_row_num
              << ", output_row_num=" << _output_rowset->num_rows()
              << ", filtered_row_num=" << stats.filtered_rows
              << ", merged_row_num=" << stats.merged_rows
              << ". elapsed time=" << watch.get_elapse_second()
              << "s. cumulative_compaction_policy=" << cumu_policy->name()
              << ", compact_row_per_second=" << int(_input_row_num / watch.get_elapse_second());

    return Status::OK();
}

Status Compaction::construct_output_rowset_writer(RowsetWriterContext& ctx, bool is_vertical) {
    ctx.version = _output_version;
    ctx.rowset_state = VISIBLE;
    ctx.segments_overlap = NONOVERLAPPING;
    ctx.tablet_schema = _cur_tablet_schema;
    ctx.newest_write_timestamp = _newest_write_timestamp;
    ctx.write_type = DataWriteType::TYPE_COMPACTION;
    if (config::inverted_index_compaction_enable &&
        ((_tablet->keys_type() == KeysType::UNIQUE_KEYS ||
          _tablet->keys_type() == KeysType::DUP_KEYS))) {
        for (auto& index : _cur_tablet_schema->indexes()) {
            if (index.index_type() == IndexType::INVERTED) {
                auto unique_id = index.col_unique_ids()[0];
                //NOTE: here src_rs may be in building index progress, so it would not contain inverted index info.
                bool all_have_inverted_index = std::all_of(
                        _input_rowsets.begin(), _input_rowsets.end(), [&](const auto& src_rs) {
                            BetaRowsetSharedPtr rowset =
                                    std::static_pointer_cast<BetaRowset>(src_rs);
                            if (rowset == nullptr) {
                                LOG(WARNING) << "tablet[" << _tablet->tablet_id()
                                             << "] rowset is null, will skip index compaction";
                                return false;
                            }
                            auto fs = rowset->rowset_meta()->fs();

                            auto index_meta =
                                    rowset->tablet_schema()->get_inverted_index(unique_id);
                            if (index_meta == nullptr) {
                                LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] index_unique_id[" << unique_id
                                             << "] index meta is null, will skip index compaction";
                                return false;
                            }
                            for (auto i = 0; i < rowset->num_segments(); i++) {
                                auto segment_file = rowset->segment_file_path(i);
                                std::string inverted_index_src_file_path =
                                        InvertedIndexDescriptor::get_index_file_name(
                                                segment_file, index_meta->index_id());
                                bool exists = false;
                                if (!fs->exists(inverted_index_src_file_path, &exists).ok()) {
                                    LOG(ERROR)
                                            << inverted_index_src_file_path << " fs->exists error";
                                    return false;
                                }
                                if (!exists) {
                                    LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] index_unique_id["
                                                 << unique_id << "]," << inverted_index_src_file_path
                                                 << " is not exists, will skip index compaction";
                                    return false;
                                }

                                // check idx file size
                                int64_t file_size = 0;
                                if (fs->file_size(inverted_index_src_file_path, &file_size) !=
                                    Status::OK()) {
                                    LOG(ERROR)
                                            << inverted_index_src_file_path << " fs->file_size error";
                                    return false;
                                }
                                if (file_size == 0) {
                                    LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] index_unique_id["
                                                 << unique_id << "]," << inverted_index_src_file_path
                                                 << " is empty file, will skip index compaction";
                                    return false;
                                }

                                // check index meta
                                std::filesystem::path p(inverted_index_src_file_path);
                                std::string dir_str = p.parent_path().string();
                                std::string file_str = p.filename().string();
                                lucene::store::Directory* dir =
                                        DorisCompoundDirectory::getDirectory(fs, dir_str.c_str());
                                auto reader = new DorisCompoundReader(dir, file_str.c_str());
                                std::vector<std::string> files;
                                reader->list(&files);

                                // why is 3?
                                // bkd index will write at least 3 files
                                if (files.size() < 3) {
                                    LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] index_unique_id["
                                                 << unique_id << "]," << inverted_index_src_file_path
                                                 << " is corrupted, will skip index compaction";
                                    return false;
                                }
                            }
                            return true;
                        });
                if (all_have_inverted_index &&
                    field_is_slice_type(_cur_tablet_schema->column_by_uid(unique_id).type())) {
                    ctx.skip_inverted_index.insert(unique_id);
                }
            }
        }
    }
    if (compaction_type() == ReaderType::READER_COLD_DATA_COMPACTION) {
        // write output rowset to storage policy resource
        auto storage_policy = get_storage_policy(_tablet->storage_policy_id());
        if (storage_policy == nullptr) {
            return Status::InternalError("could not find storage_policy, storage_policy_id={}",
                                         _tablet->storage_policy_id());
        }
        auto resource = get_storage_resource(storage_policy->resource_id);
        if (resource.fs == nullptr) {
            return Status::InternalError("could not find resource, resouce_id={}",
                                         storage_policy->resource_id);
        }
        DCHECK(atol(resource.fs->id().c_str()) == storage_policy->resource_id);
        DCHECK(resource.fs->type() != io::FileSystemType::LOCAL);
        ctx.fs = std::move(resource.fs);
    }
    if (is_vertical) {
        return _tablet->create_vertical_rowset_writer(ctx, &_output_rs_writer);
    }
    return _tablet->create_rowset_writer(ctx, &_output_rs_writer);
}

Status Compaction::construct_input_rowset_readers() {
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(rowset->create_reader(&rs_reader));
        _input_rs_readers.push_back(std::move(rs_reader));
    }
    return Status::OK();
}

Status Compaction::modify_rowsets(const Merger::Statistics* stats) {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);

    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        Version version = _tablet->max_version();
        DeleteBitmap output_rowset_delete_bitmap(_tablet->tablet_id());
        std::set<RowLocation> missed_rows;
        std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>> location_map;
        // Convert the delete bitmap of the input rowsets to output rowset.
        // New loads are not blocked, so some keys of input rowsets might
        // be deleted during the time. We need to deal with delete bitmap
        // of incremental data later.
        // TODO(LiaoXin): check if there are duplicate keys
        std::size_t missed_rows_size = 0;
        if (!allow_delete_in_cumu_compaction()) {
            _tablet->calc_compaction_output_rowset_delete_bitmap(
                    _input_rowsets, _rowid_conversion, 0, version.second + 1, &missed_rows,
                    &location_map, _tablet->tablet_meta()->delete_bitmap(),
                    &output_rowset_delete_bitmap);
            missed_rows_size = missed_rows.size();
            if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION && stats != nullptr &&
                stats->merged_rows != missed_rows_size) {
                std::string err_msg = fmt::format(
                        "cumulative compaction: the merged rows({}) is not equal to missed "
                        "rows({}) in rowid conversion, tablet_id: {}, table_id:{}",
                        stats->merged_rows, missed_rows_size, _tablet->tablet_id(),
                        _tablet->table_id());
                DCHECK(false) << err_msg;
                LOG(WARNING) << err_msg;
            }
        }

        RETURN_IF_ERROR(_tablet->check_rowid_conversion(_output_rowset, location_map));
        location_map.clear();

        {
            std::lock_guard<std::mutex> wrlock_(_tablet->get_rowset_update_lock());
            std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
            SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);

            // Here we will calculate all the rowsets delete bitmaps which are committed but not published to reduce the calculation pressure
            // of publish phase.
            // All rowsets which need to recalculate have been published so we don't need to acquire lock.
            // Step1: collect this tablet's all committed rowsets' delete bitmaps
            CommitTabletTxnInfoVec commit_tablet_txn_info_vec {};
            StorageEngine::instance()->txn_manager()->get_all_commit_tablet_txn_info_by_tablet(
                    _tablet, &commit_tablet_txn_info_vec);

            // Step2: calculate all rowsets' delete bitmaps which are published during compaction.
            for (auto& it : commit_tablet_txn_info_vec) {
                if (!_check_if_includes_input_rowsets(it.rowset_ids)) {
                    // When calculating the delete bitmap of all committed rowsets relative to the compaction,
                    // there may be cases where the compacted rowsets are newer than the committed rowsets.
                    // At this time, row number conversion cannot be performed, otherwise data will be missing.
                    // Therefore, we need to check if every committed rowset has calculated delete bitmap for
                    // all compaction input rowsets.
                    continue;
                } else {
                    DeleteBitmap txn_output_delete_bitmap(_tablet->tablet_id());
                    _tablet->calc_compaction_output_rowset_delete_bitmap(
                            _input_rowsets, _rowid_conversion, 0, UINT64_MAX, &missed_rows,
                            &location_map, *it.delete_bitmap.get(), &txn_output_delete_bitmap);
                    if (config::enable_merge_on_write_correctness_check) {
                        RowsetIdUnorderedSet rowsetids;
                        rowsetids.insert(_output_rowset->rowset_id());
                        _tablet->add_sentinel_mark_to_delete_bitmap(&txn_output_delete_bitmap,
                                                                    rowsetids);
                    }
                    it.delete_bitmap->merge(txn_output_delete_bitmap);
                    // Step3: write back updated delete bitmap and tablet info.
                    it.rowset_ids.insert(_output_rowset->rowset_id());
                    StorageEngine::instance()->txn_manager()->set_txn_related_delete_bitmap(
                            it.partition_id, it.transaction_id, _tablet->tablet_id(),
                            _tablet->tablet_uid(), true, it.delete_bitmap, it.rowset_ids);
                }
            }

            // Convert the delete bitmap of the input rowsets to output rowset for
            // incremental data.
            _tablet->calc_compaction_output_rowset_delete_bitmap(
                    _input_rowsets, _rowid_conversion, version.second, UINT64_MAX, &missed_rows,
                    &location_map, _tablet->tablet_meta()->delete_bitmap(),
                    &output_rowset_delete_bitmap);

            if (!allow_delete_in_cumu_compaction() &&
                compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
                DCHECK_EQ(missed_rows.size(), missed_rows_size);
                if (missed_rows.size() != missed_rows_size) {
                    LOG(WARNING) << "missed rows don't match, before: " << missed_rows_size
                                 << " after: " << missed_rows.size();
                }
            }

            RETURN_IF_ERROR(_tablet->check_rowid_conversion(_output_rowset, location_map));

            _tablet->merge_delete_bitmap(output_rowset_delete_bitmap);
            RETURN_IF_ERROR(_tablet->modify_rowsets(output_rowsets, _input_rowsets, true));
        }
    } else {
        std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        RETURN_IF_ERROR(_tablet->modify_rowsets(output_rowsets, _input_rowsets, true));
    }

    if (config::tablet_rowset_stale_sweep_by_size &&
        _tablet->tablet_meta()->all_stale_rs_metas().size() >=
                config::tablet_rowset_stale_sweep_threshold_size) {
        _tablet->delete_expired_stale_rowset();
    }

    int64_t cur_max_version = 0;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        cur_max_version = _tablet->max_version_unlocked().second;
        _tablet->save_meta();
    }
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        auto st = TabletMetaManager::remove_old_version_delete_bitmap(
                _tablet->data_dir(), _tablet->tablet_id(), cur_max_version);
        if (!st.ok()) {
            LOG(WARNING) << "failed to remove old version delete bitmap, st: " << st;
        }
    }
    return Status::OK();
}

bool Compaction::_check_if_includes_input_rowsets(
        const RowsetIdUnorderedSet& commit_rowset_ids_set) const {
    std::vector<RowsetId> commit_rowset_ids {};
    commit_rowset_ids.insert(commit_rowset_ids.end(), commit_rowset_ids_set.begin(),
                             commit_rowset_ids_set.end());
    std::sort(commit_rowset_ids.begin(), commit_rowset_ids.end());
    std::vector<RowsetId> input_rowset_ids {};
    for (const auto& rowset : _input_rowsets) {
        input_rowset_ids.emplace_back(rowset->rowset_meta()->rowset_id());
    }
    std::sort(input_rowset_ids.begin(), input_rowset_ids.end());
    return std::includes(commit_rowset_ids.begin(), commit_rowset_ids.end(),
                         input_rowset_ids.begin(), input_rowset_ids.end());
}

void Compaction::gc_output_rowset() {
    if (_state != CompactionState::SUCCESS && _output_rowset != nullptr) {
        if (!_output_rowset->is_local()) {
            Tablet::erase_pending_remote_rowset(_output_rowset->rowset_id().to_string());
            _tablet->record_unused_remote_rowset(_output_rowset->rowset_id(),
                                                 _output_rowset->rowset_meta()->resource_id(),
                                                 _output_rowset->num_segments());
            return;
        }
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
            return Status::Error<CUMULATIVE_MISS_VERSION>(
                    "There are missed versions among rowsets. prev_rowset version={}-{}, rowset "
                    "version={}-{}",
                    prev_rowset->start_version(), prev_rowset->end_version(),
                    rowset->start_version(), rowset->end_version());
        }
        prev_rowset = rowset;
    }

    return Status::OK();
}

Status Compaction::check_correctness(const Merger::Statistics& stats) {
    // 1. check row number
    if (_input_row_num != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
        return Status::Error<CHECK_LINES_ERROR>(
                "row_num does not match between cumulative input and output! tablet={}, "
                "input_row_num={}, merged_row_num={}, filtered_row_num={}, output_row_num={}",
                _tablet->full_name(), _input_row_num, stats.merged_rows, stats.filtered_rows,
                _output_rowset->num_rows());
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

void Compaction::_load_segment_to_cache() {
    // Load new rowset's segments to cache.
    SegmentCacheHandle handle;
    auto st = SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(_output_rowset), &handle, true);
    if (!st.ok()) {
        LOG(WARNING) << "failed to load segment to cache! output rowset version="
                     << _output_rowset->start_version() << "-" << _output_rowset->end_version()
                     << ".";
    }
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
