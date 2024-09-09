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
#include <atomic>
#include <cstdlib>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <numeric>
#include <ostream>
#include <set>
#include <shared_mutex>
#include <utility>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/remote_file_system.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/inverted_index_compaction.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/task/engine_checksum_task.h"
#include "olap/txn_manager.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/time.h"
#include "util/trace.h"

using std::vector;

namespace doris {
using namespace ErrorCode;

namespace {

bool is_rowset_tidy(std::string& pre_max_key, const RowsetSharedPtr& rhs) {
    size_t min_tidy_size = config::ordered_data_compaction_min_segment_size;
    if (rhs->num_segments() == 0) {
        return true;
    }
    if (rhs->is_segments_overlapping()) {
        return false;
    }
    // check segment size
    auto* beta_rowset = reinterpret_cast<BetaRowset*>(rhs.get());
    std::vector<size_t> segments_size;
    RETURN_FALSE_IF_ERROR(beta_rowset->get_segments_size(&segments_size));
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
    if (min_key <= pre_max_key) {
        return false;
    }
    CHECK(rhs->max_key(&pre_max_key));

    return true;
}

} // namespace

Compaction::Compaction(BaseTabletSPtr tablet, const std::string& label)
        : _mem_tracker(
                  MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::COMPACTION, label)),
          _tablet(std::move(tablet)),
          _is_vertical(config::enable_vertical_compaction),
          _allow_delete_in_cumu_compaction(config::enable_delete_when_cumu_compaction) {
    ;
    init_profile(label);
}

Compaction::~Compaction() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    _output_rs_writer.reset();
    _tablet.reset();
    _input_rowsets.clear();
    _output_rowset.reset();
    _cur_tablet_schema.reset();
}

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

int64_t Compaction::merge_way_num() {
    int64_t way_num = 0;
    for (auto&& rowset : _input_rowsets) {
        way_num += rowset->rowset_meta()->get_merge_way_num();
    }

    return way_num;
}

Status Compaction::merge_input_rowsets() {
    std::vector<RowsetReaderSharedPtr> input_rs_readers;
    input_rs_readers.reserve(_input_rowsets.size());
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(rowset->create_reader(&rs_reader));
        input_rs_readers.push_back(std::move(rs_reader));
    }

    RowsetWriterContext ctx;
    RETURN_IF_ERROR(construct_output_rowset_writer(ctx));

    // write merged rows to output rowset
    // The test results show that merger is low-memory-footprint, there is no need to tracker its mem pool
    // if ctx.skip_inverted_index.size() > 0, it means we need to do inverted index compaction.
    // the row ID conversion matrix needs to be used for inverted index compaction.
    if (!ctx.skip_inverted_index.empty() || (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                                             _tablet->enable_unique_key_merge_on_write())) {
        _stats.rowid_conversion = &_rowid_conversion;
    }

    int64_t way_num = merge_way_num();

    Status res;
    {
        SCOPED_TIMER(_merge_rowsets_latency_timer);
        if (_is_vertical) {
            res = Merger::vertical_merge_rowsets(_tablet, compaction_type(), *_cur_tablet_schema,
                                                 input_rs_readers, _output_rs_writer.get(),
                                                 get_avg_segment_rows(), way_num, &_stats);
        } else {
            res = Merger::vmerge_rowsets(_tablet, compaction_type(), *_cur_tablet_schema,
                                         input_rs_readers, _output_rs_writer.get(), &_stats);
        }
    }

    _tablet->last_compaction_status = res;

    if (!res.ok()) {
        return res;
    }

    COUNTER_UPDATE(_merged_rows_counter, _stats.merged_rows);
    COUNTER_UPDATE(_filtered_rows_counter, _stats.filtered_rows);

    RETURN_NOT_OK_STATUS_WITH_WARN(_output_rs_writer->build(_output_rowset),
                                   fmt::format("rowset writer build failed. output_version: {}",
                                               _output_version.to_string()));

    //RETURN_IF_ERROR(_engine.meta_mgr().commit_rowset(*_output_rowset->rowset_meta().get()));

    // Now we support delete in cumu compaction, to make all data in rowsets whose version
    // is below output_version to be delete in the future base compaction, we should carry
    // all delete predicate in the output rowset.
    // Output start version > 2 means we must set the delete predicate in the output rowset
    if (_allow_delete_in_cumu_compaction && _output_rowset->version().first > 2) {
        DeletePredicatePB delete_predicate;
        std::accumulate(_input_rowsets.begin(), _input_rowsets.end(), &delete_predicate,
                        [](DeletePredicatePB* delete_predicate, const RowsetSharedPtr& rs) {
                            if (rs->rowset_meta()->has_delete_predicate()) {
                                delete_predicate->MergeFrom(rs->rowset_meta()->delete_predicate());
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

    return check_correctness();
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

CompactionMixin::CompactionMixin(StorageEngine& engine, TabletSharedPtr tablet,
                                 const std::string& label)
        : Compaction(tablet, label), _engine(engine) {}

CompactionMixin::~CompactionMixin() {
    if (_state != CompactionState::SUCCESS && _output_rowset != nullptr) {
        if (!_output_rowset->is_local()) {
            tablet()->record_unused_remote_rowset(_output_rowset->rowset_id(),
                                                  _output_rowset->rowset_meta()->resource_id(),
                                                  _output_rowset->num_segments());
            return;
        }
        _engine.add_unused_rowset(_output_rowset);
    }
}

Tablet* CompactionMixin::tablet() {
    return static_cast<Tablet*>(_tablet.get());
}

Status CompactionMixin::do_compact_ordered_rowsets() {
    build_basic_info();
    RowsetWriterContext ctx;
    RETURN_IF_ERROR(construct_output_rowset_writer(ctx));

    LOG(INFO) << "start to do ordered data compaction, tablet=" << _tablet->tablet_id()
              << ", output_version=" << _output_version;
    // link data to new rowset
    auto seg_id = 0;
    std::vector<KeyBoundsPB> segment_key_bounds;
    for (auto rowset : _input_rowsets) {
        RETURN_IF_ERROR(rowset->link_files_to(tablet()->tablet_path(),
                                              _output_rs_writer->rowset_id(), seg_id));
        seg_id += rowset->num_segments();

        std::vector<KeyBoundsPB> key_bounds;
        RETURN_IF_ERROR(rowset->get_segments_key_bounds(&key_bounds));
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

void CompactionMixin::build_basic_info() {
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
    _cur_tablet_schema = _tablet->tablet_schema_with_merged_max_schema_version(rowset_metas);
}

bool CompactionMixin::handle_ordered_data_compaction() {
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

    if (_tablet->tablet_meta()->tablet_schema()->skip_write_index_on_load()) {
        // Expected to create index through normal compaction
        return false;
    }

    // check delete version: if compaction type is base compaction and
    // has a delete version, use original compaction
    if (compaction_type() == ReaderType::READER_BASE_COMPACTION ||
        (_allow_delete_in_cumu_compaction &&
         compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION)) {
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
        LOG(WARNING) << "failed to compact ordered rowsets: " << st;
        _pending_rs_guard.drop();
    }

    return st.ok();
}

Status CompactionMixin::execute_compact() {
    uint32_t checksum_before;
    uint32_t checksum_after;
    bool enable_compaction_checksum = config::enable_compaction_checksum;
    if (enable_compaction_checksum) {
        EngineChecksumTask checksum_task(_engine, _tablet->tablet_id(), _tablet->schema_hash(),
                                         _input_rowsets.back()->end_version(), &checksum_before);
        RETURN_IF_ERROR(checksum_task.execute());
    }

    auto* data_dir = tablet()->data_dir();
    int64_t permits = get_compaction_permits();
    data_dir->disks_compaction_score_increment(permits);
    data_dir->disks_compaction_num_increment(1);

    auto record_compaction_stats = [&](const doris::Exception& ex) {
        _tablet->compaction_count.fetch_add(1, std::memory_order_relaxed);
        data_dir->disks_compaction_score_increment(-permits);
        data_dir->disks_compaction_num_increment(-1);
    };

    HANDLE_EXCEPTION_IF_CATCH_EXCEPTION(execute_compact_impl(permits), record_compaction_stats);
    record_compaction_stats(doris::Exception());

    if (enable_compaction_checksum) {
        EngineChecksumTask checksum_task(_engine, _tablet->tablet_id(), _tablet->schema_hash(),
                                         _input_rowsets.back()->end_version(), &checksum_after);
        RETURN_IF_ERROR(checksum_task.execute());
        if (checksum_before != checksum_after) {
            return Status::InternalError(
                    "compaction tablet checksum not consistent, before={}, after={}, tablet_id={}",
                    checksum_before, checksum_after, _tablet->tablet_id());
        }
    }

    _load_segment_to_cache();
    return Status::OK();
}

Status CompactionMixin::execute_compact_impl(int64_t permits) {
    OlapStopWatch watch;

    if (handle_ordered_data_compaction()) {
        RETURN_IF_ERROR(modify_rowsets());
        LOG(INFO) << "succeed to do ordered data " << compaction_name()
                  << ". tablet=" << _tablet->tablet_id() << ", output_version=" << _output_version
                  << ", disk=" << tablet()->data_dir()->path()
                  << ", segments=" << _input_num_segments << ", input_row_num=" << _input_row_num
                  << ", output_row_num=" << _output_rowset->num_rows()
                  << ", input_rowset_size=" << _input_rowsets_size
                  << ", output_rowset_size=" << _output_rowset->data_disk_size()
                  << ". elapsed time=" << watch.get_elapse_second() << "s.";
        _state = CompactionState::SUCCESS;
        return Status::OK();
    }
    build_basic_info();

    VLOG_DEBUG << "dump tablet schema: " << _cur_tablet_schema->dump_structure();

    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->tablet_id()
              << ", output_version=" << _output_version << ", permits: " << permits;

    RETURN_IF_ERROR(merge_input_rowsets());

    RETURN_IF_ERROR(do_inverted_index_compaction());

    RETURN_IF_ERROR(modify_rowsets());

    auto* cumu_policy = tablet()->cumulative_compaction_policy();
    DCHECK(cumu_policy);
    LOG(INFO) << "succeed to do " << compaction_name() << " is_vertical=" << _is_vertical
              << ". tablet=" << _tablet->tablet_id() << ", output_version=" << _output_version
              << ", current_max_version=" << tablet()->max_version().second
              << ", disk=" << tablet()->data_dir()->path() << ", segments=" << _input_num_segments
              << ", input_rowset_size=" << _input_rowsets_size
              << ", output_rowset_size=" << _output_rowset->data_disk_size()
              << ", input_row_num=" << _input_row_num
              << ", output_row_num=" << _output_rowset->num_rows()
              << ", filtered_row_num=" << _stats.filtered_rows
              << ", merged_row_num=" << _stats.merged_rows
              << ". elapsed time=" << watch.get_elapse_second()
              << "s. cumulative_compaction_policy=" << cumu_policy->name()
              << ", compact_row_per_second=" << int(_input_row_num / watch.get_elapse_second());

    _state = CompactionState::SUCCESS;

    return Status::OK();
}

Status Compaction::do_inverted_index_compaction() {
    const auto& ctx = _output_rs_writer->context();
    if (!config::inverted_index_compaction_enable || _input_row_num <= 0 ||
        !_stats.rowid_conversion || ctx.skip_inverted_index.empty()) {
        return Status::OK();
    }

    OlapStopWatch inverted_watch;

    int64_t cur_max_version = 0;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        cur_max_version = _tablet->max_version_unlocked();
    }

    DeleteBitmap output_rowset_delete_bitmap(_tablet->tablet_id());
    std::set<RowLocation> missed_rows;
    std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>> location_map;
    // Convert the delete bitmap of the input rowsets to output rowset.
    _tablet->calc_compaction_output_rowset_delete_bitmap(
            _input_rowsets, _rowid_conversion, 0, cur_max_version + 1, &missed_rows, &location_map,
            _tablet->tablet_meta()->delete_bitmap(), &output_rowset_delete_bitmap);

    if (!_allow_delete_in_cumu_compaction) {
        if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION &&
            _stats.merged_rows != missed_rows.size() && _tablet->tablet_state() == TABLET_RUNNING) {
            std::string err_msg = fmt::format(
                    "cumulative compaction: the merged rows({}) is not equal to missed "
                    "rows({}) in rowid conversion, tablet_id: {}, table_id:{}",
                    _stats.merged_rows, missed_rows.size(), _tablet->tablet_id(),
                    _tablet->table_id());
            if (config::enable_mow_compaction_correctness_check_core) {
                CHECK(false) << err_msg;
            } else {
                DCHECK(false) << err_msg;
            }
            // log here just for debugging, do not return error
            LOG(WARNING) << err_msg;
        }
    }

    RETURN_IF_ERROR(_tablet->check_rowid_conversion(_output_rowset, location_map));

    // translation vec
    // <<dest_idx_num, dest_docId>>
    // the first level vector: index indicates src segment.
    // the second level vector: index indicates row id of source segment,
    // value indicates row id of destination segment.
    // <UINT32_MAX, UINT32_MAX> indicates current row not exist.
    const auto& trans_vec = _stats.rowid_conversion->get_rowid_conversion_map();

    // source rowset,segment -> index_id
    const auto& src_seg_to_id_map = _stats.rowid_conversion->get_src_segment_to_id_map();

    // dest rowset id
    RowsetId dest_rowset_id = _stats.rowid_conversion->get_dst_rowset_id();
    // dest segment id -> num rows
    std::vector<uint32_t> dest_segment_num_rows;
    RETURN_IF_ERROR(_output_rs_writer->get_segment_num_rows(&dest_segment_num_rows));

    auto src_segment_num = src_seg_to_id_map.size();
    auto dest_segment_num = dest_segment_num_rows.size();

    if (dest_segment_num <= 0) {
        LOG(INFO) << "skip doing index compaction due to no output segments"
                  << ". tablet=" << _tablet->tablet_id() << ", input row number=" << _input_row_num
                  << ", output row number=" << _output_rowset->num_rows()
                  << ". elapsed time=" << inverted_watch.get_elapse_second() << "s.";
        return Status::OK();
    }

    // Only write info files when debug index compaction is enabled.
    // The files are used to debug index compaction and works with index_tool.
    if (config::debug_inverted_index_compaction) {
        // src index files
        // format: rowsetId_segmentId
        std::vector<std::string> src_index_files(src_segment_num);
        for (const auto& m : src_seg_to_id_map) {
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

        auto write_json_to_file = [&](const nlohmann::json& json_obj,
                                      const std::string& file_name) {
            io::FileWriterPtr file_writer;
            std::string file_path =
                    fmt::format("{}/{}.json", std::string(getenv("LOG_DIR")), file_name);
            RETURN_IF_ERROR(io::global_local_filesystem()->create_file(file_path, &file_writer));
            RETURN_IF_ERROR(file_writer->append(json_obj.dump()));
            RETURN_IF_ERROR(file_writer->append("\n"));
            return file_writer->close();
        };

        // Convert trans_vec to JSON and print it
        nlohmann::json trans_vec_json = trans_vec;
        auto output_version =
                _output_version.to_string().substr(1, _output_version.to_string().size() - 2);
        RETURN_IF_ERROR(write_json_to_file(
                trans_vec_json,
                fmt::format("trans_vec_{}_{}", _tablet->tablet_id(), output_version)));

        nlohmann::json src_index_files_json = src_index_files;
        RETURN_IF_ERROR(write_json_to_file(
                src_index_files_json,
                fmt::format("src_idx_dirs_{}_{}", _tablet->tablet_id(), output_version)));

        nlohmann::json dest_index_files_json = dest_index_files;
        RETURN_IF_ERROR(write_json_to_file(
                dest_index_files_json,
                fmt::format("dest_idx_dirs_{}_{}", _tablet->tablet_id(), output_version)));

        nlohmann::json dest_segment_num_rows_json = dest_segment_num_rows;
        RETURN_IF_ERROR(write_json_to_file(
                dest_segment_num_rows_json,
                fmt::format("dest_seg_num_rows_{}_{}", _tablet->tablet_id(), output_version)));
    }

    // create index_writer to compaction indexes
    std::unordered_map<RowsetId, Rowset*> rs_id_to_rowset_map;
    for (auto&& rs : _input_rowsets) {
        rs_id_to_rowset_map.emplace(rs->rowset_id(), rs.get());
    }

    // src index dirs
    std::vector<std::unique_ptr<InvertedIndexFileReader>> inverted_index_file_readers(
            src_segment_num);
    for (const auto& m : src_seg_to_id_map) {
        const auto& [rowset_id, seg_id] = m.first;

        auto find_it = rs_id_to_rowset_map.find(rowset_id);
        if (find_it == rs_id_to_rowset_map.end()) [[unlikely]] {
            DCHECK(false) << _tablet->tablet_id() << ' ' << rowset_id;
            return Status::InternalError("cannot find rowset. tablet_id={} rowset_id={}",
                                         _tablet->tablet_id(), rowset_id.to_string());
        }

        auto* rowset = find_it->second;
        const auto& fs = rowset->rowset_meta()->fs();
        if (!fs) {
            return Status::InternalError("get fs failed, resource_id={}",
                                         rowset->rowset_meta()->resource_id());
        }

        auto seg_path = DORIS_TRY(rowset->segment_path(seg_id));
        auto inverted_index_file_reader = std::make_unique<InvertedIndexFileReader>(
                fs, std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)},
                _cur_tablet_schema->get_inverted_index_storage_format());
        bool open_idx_file_cache = false;
        RETURN_NOT_OK_STATUS_WITH_WARN(
                inverted_index_file_reader->init(config::inverted_index_read_buffer_size,
                                                 open_idx_file_cache),
                "inverted_index_file_reader init failed");
        inverted_index_file_readers[m.second] = std::move(inverted_index_file_reader);
    }

    // dest index files
    // format: rowsetId_segmentId
    std::vector<std::unique_ptr<InvertedIndexFileWriter>> inverted_index_file_writers(
            dest_segment_num);

    // Some columns have already been indexed
    // key: seg_id, value: inverted index file size
    std::unordered_map<int, int64_t> compacted_idx_file_size;
    for (int seg_id = 0; seg_id < dest_segment_num; ++seg_id) {
        std::string index_path_prefix {
                InvertedIndexDescriptor::get_index_file_path_prefix(ctx.segment_path(seg_id))};
        auto inverted_index_file_reader = std::make_unique<InvertedIndexFileReader>(
                ctx.fs(), index_path_prefix,
                _cur_tablet_schema->get_inverted_index_storage_format());
        bool open_idx_file_cache = false;
        auto st = inverted_index_file_reader->init(config::inverted_index_read_buffer_size,
                                                   open_idx_file_cache);
        if (st.ok()) {
            auto index_not_need_to_compact =
                    DORIS_TRY(inverted_index_file_reader->get_all_directories());
            // V1: each index is a separate file
            // V2: all indexes are in a single file
            if (_cur_tablet_schema->get_inverted_index_storage_format() !=
                doris::InvertedIndexStorageFormatPB::V1) {
                int64_t fsize = 0;
                st = ctx.fs()->file_size(
                        InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix), &fsize);
                if (!st.ok()) {
                    LOG(ERROR) << "file size error in index compaction, error:" << st.msg();
                    return st;
                }
                compacted_idx_file_size[seg_id] = fsize;
            }
            auto inverted_index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                    ctx.fs(), index_path_prefix, ctx.rowset_id.to_string(), seg_id,
                    _cur_tablet_schema->get_inverted_index_storage_format());
            RETURN_IF_ERROR(inverted_index_file_writer->initialize(index_not_need_to_compact));
            inverted_index_file_writers[seg_id] = std::move(inverted_index_file_writer);
        } else if (st.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>()) {
            auto inverted_index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                    ctx.fs(), index_path_prefix, ctx.rowset_id.to_string(), seg_id,
                    _cur_tablet_schema->get_inverted_index_storage_format());
            inverted_index_file_writers[seg_id] = std::move(inverted_index_file_writer);
            // no index file
            compacted_idx_file_size[seg_id] = 0;
        } else {
            LOG(ERROR) << "inverted_index_file_reader init failed in index compaction, error:"
                       << st;
            return st;
        }
    }
    for (const auto& writer : inverted_index_file_writers) {
        writer->set_file_writer_opts(ctx.get_file_writer_options());
    }

    // use tmp file dir to store index files
    auto tmp_file_dir = ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir();
    auto index_tmp_path = tmp_file_dir / dest_rowset_id.to_string();
    LOG(INFO) << "start index compaction"
              << ". tablet=" << _tablet->tablet_id() << ", source index size=" << src_segment_num
              << ", destination index size=" << dest_segment_num << ".";

    auto error_handler = [this](int64_t index_id, int64_t column_uniq_id) {
        LOG(WARNING) << "failed to do index compaction"
                     << ". tablet=" << _tablet->tablet_id() << ". column uniq id=" << column_uniq_id
                     << ". index_id=" << index_id;
        for (auto& rowset : _input_rowsets) {
            rowset->set_skip_index_compaction(column_uniq_id);
            LOG(INFO) << "mark skipping inverted index compaction next time"
                      << ". tablet=" << _tablet->tablet_id() << ", rowset=" << rowset->rowset_id()
                      << ", column uniq id=" << column_uniq_id << ", index_id=" << index_id;
        }
    };

    Status status = Status::OK();
    for (auto&& column_uniq_id : ctx.skip_inverted_index) {
        auto col = _cur_tablet_schema->column_by_uid(column_uniq_id);
        const auto* index_meta = _cur_tablet_schema->get_inverted_index(col);

        // if index properties are different, index compaction maybe needs to be skipped.
        bool is_continue = false;
        std::optional<std::map<std::string, std::string>> first_properties;
        for (const auto& rowset : _input_rowsets) {
            const auto* tablet_index = rowset->tablet_schema()->get_inverted_index(col);
            const auto& properties = tablet_index->properties();
            if (!first_properties.has_value()) {
                first_properties = properties;
            } else {
                if (properties != first_properties.value()) {
                    error_handler(index_meta->index_id(), column_uniq_id);
                    status = Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(
                            "if index properties are different, index compaction needs to be "
                            "skipped.");
                    is_continue = true;
                    break;
                }
            }
        }
        if (is_continue) {
            continue;
        }

        std::vector<lucene::store::Directory*> dest_index_dirs(dest_segment_num);
        std::vector<lucene::store::Directory*> src_index_dirs(src_segment_num);
        try {
            for (int src_segment_id = 0; src_segment_id < src_segment_num; src_segment_id++) {
                auto src_dir =
                        DORIS_TRY(inverted_index_file_readers[src_segment_id]->open(index_meta));
                src_index_dirs[src_segment_id] = src_dir.release();
            }
            for (int dest_segment_id = 0; dest_segment_id < dest_segment_num; dest_segment_id++) {
                auto* dest_dir =
                        DORIS_TRY(inverted_index_file_writers[dest_segment_id]->open(index_meta));
                dest_index_dirs[dest_segment_id] = dest_dir;
            }
            auto st = compact_column(index_meta->index_id(), src_index_dirs, dest_index_dirs,
                                     index_tmp_path.native(), trans_vec, dest_segment_num_rows);
            if (!st.ok()) {
                error_handler(index_meta->index_id(), column_uniq_id);
                status = Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(st.msg());
            }
        } catch (CLuceneError& e) {
            error_handler(index_meta->index_id(), column_uniq_id);
            status = Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(e.what());
        }
    }

    std::vector<InvertedIndexFileInfo> all_inverted_index_file_info(dest_segment_num);
    uint64_t inverted_index_file_size = 0;
    for (int seg_id = 0; seg_id < dest_segment_num; ++seg_id) {
        auto inverted_index_file_writer = inverted_index_file_writers[seg_id].get();
        if (Status st = inverted_index_file_writer->close(); !st.ok()) {
            status = Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(st.msg());
        } else {
            inverted_index_file_size += inverted_index_file_writer->get_index_file_total_size();
            inverted_index_file_size -= compacted_idx_file_size[seg_id];
        }
        all_inverted_index_file_info[seg_id] = inverted_index_file_writer->get_index_file_info();
    }
    // check index compaction status. If status is not ok, we should return error and end this compaction round.
    if (!status.ok()) {
        return status;
    }

    // index compaction should update total disk size and index disk size
    _output_rowset->rowset_meta()->set_data_disk_size(_output_rowset->data_disk_size() +
                                                      inverted_index_file_size);
    _output_rowset->rowset_meta()->set_total_disk_size(_output_rowset->data_disk_size() +
                                                       inverted_index_file_size);
    _output_rowset->rowset_meta()->set_index_disk_size(_output_rowset->index_disk_size() +
                                                       inverted_index_file_size);

    _output_rowset->rowset_meta()->update_inverted_index_files_info(all_inverted_index_file_info);
    COUNTER_UPDATE(_output_rowset_data_size_counter, _output_rowset->data_disk_size());

    LOG(INFO) << "succeed to do index compaction"
              << ". tablet=" << _tablet->tablet_id() << ", input row number=" << _input_row_num
              << ", output row number=" << _output_rowset->num_rows()
              << ", input_rowset_size=" << _input_rowsets_size
              << ", output_rowset_size=" << _output_rowset->data_disk_size()
              << ", inverted index file size=" << inverted_index_file_size
              << ". elapsed time=" << inverted_watch.get_elapse_second() << "s.";

    return Status::OK();
}

void Compaction::construct_skip_inverted_index(RowsetWriterContext& ctx) {
    for (const auto& index : _cur_tablet_schema->indexes()) {
        if (index.index_type() != IndexType::INVERTED) {
            continue;
        }

        auto col_unique_id = index.col_unique_ids()[0];
        auto has_inverted_index = [&](const RowsetSharedPtr& src_rs) {
            auto* rowset = static_cast<BetaRowset*>(src_rs.get());
            if (rowset->is_skip_index_compaction(col_unique_id)) {
                LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] rowset["
                             << rowset->rowset_id() << "] column_unique_id[" << col_unique_id
                             << "] skip inverted index compaction due to last failure";
                return false;
            }

            const auto& fs = rowset->rowset_meta()->fs();
            if (!fs) {
                LOG(WARNING) << "get fs failed, resource_id="
                             << rowset->rowset_meta()->resource_id();
                return false;
            }

            const auto* index_meta = rowset->tablet_schema()->get_inverted_index(col_unique_id, "");
            if (index_meta == nullptr) {
                LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] column_unique_id["
                             << col_unique_id << "] index meta is null, will skip index compaction";
                return false;
            }

            for (auto i = 0; i < rowset->num_segments(); i++) {
                // TODO: inverted_index_path
                auto seg_path = rowset->segment_path(i);
                if (!seg_path) {
                    LOG(WARNING) << seg_path.error();
                    return false;
                }

                auto inverted_index_file_reader = std::make_unique<InvertedIndexFileReader>(
                        fs,
                        std::string {
                                InvertedIndexDescriptor::get_index_file_path_prefix(*seg_path)},
                        _cur_tablet_schema->get_inverted_index_storage_format());
                bool open_idx_file_cache = false;
                auto st = inverted_index_file_reader->init(config::inverted_index_read_buffer_size,
                                                           open_idx_file_cache);
                if (!st.ok()) {
                    LOG(WARNING) << "init index "
                                 << inverted_index_file_reader->get_index_file_path(index_meta)
                                 << " error:" << st;
                    return false;
                }

                bool exists = false;
                if (!inverted_index_file_reader->index_file_exist(index_meta, &exists).ok()) {
                    LOG(ERROR) << inverted_index_file_reader->get_index_file_path(index_meta)
                               << " fs->exists error";
                    return false;
                }

                if (!exists) {
                    LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] column_unique_id["
                                 << col_unique_id << "],"
                                 << inverted_index_file_reader->get_index_file_path(index_meta)
                                 << " is not exists, will skip index compaction";
                    return false;
                }

                // check index meta
                auto result = inverted_index_file_reader->open(index_meta);
                if (!result.has_value()) {
                    LOG(WARNING) << "open index "
                                 << inverted_index_file_reader->get_index_file_path(index_meta)
                                 << " error:" << result.error();
                    return false;
                }
                auto reader = std::move(result.value());
                std::vector<std::string> files;
                reader->list(&files);
                reader->close();

                // why is 3?
                // bkd index will write at least 3 files
                if (files.size() < 3) {
                    LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] column_unique_id["
                                 << col_unique_id << "],"
                                 << inverted_index_file_reader->get_index_file_path(index_meta)
                                 << " is corrupted, will skip index compaction";
                    return false;
                }
            }
            return true;
        };

        bool all_have_inverted_index = std::all_of(_input_rowsets.begin(), _input_rowsets.end(),
                                                   std::move(has_inverted_index));

        if (all_have_inverted_index &&
            field_is_slice_type(_cur_tablet_schema->column_by_uid(col_unique_id).type())) {
            ctx.skip_inverted_index.insert(col_unique_id);
        }
    }
}

Status CompactionMixin::construct_output_rowset_writer(RowsetWriterContext& ctx) {
    // only do index compaction for dup_keys and unique_keys with mow enabled
    if (config::inverted_index_compaction_enable &&
        (((_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
           _tablet->enable_unique_key_merge_on_write()) ||
          _tablet->keys_type() == KeysType::DUP_KEYS)) &&
        _cur_tablet_schema->get_inverted_index_storage_format() ==
                InvertedIndexStorageFormatPB::V1) {
        construct_skip_inverted_index(ctx);
    }
    ctx.version = _output_version;
    ctx.rowset_state = VISIBLE;
    ctx.segments_overlap = NONOVERLAPPING;
    ctx.tablet_schema = _cur_tablet_schema;
    ctx.newest_write_timestamp = _newest_write_timestamp;
    ctx.write_type = DataWriteType::TYPE_COMPACTION;
    _output_rs_writer = DORIS_TRY(_tablet->create_rowset_writer(ctx, _is_vertical));
    _pending_rs_guard = _engine.add_pending_rowset(ctx);
    return Status::OK();
}

Status CompactionMixin::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);

    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        Version version = tablet()->max_version();
        DeleteBitmap output_rowset_delete_bitmap(_tablet->tablet_id());
        std::unique_ptr<RowLocationSet> missed_rows;
        if ((config::enable_missing_rows_correctness_check ||
             config::enable_mow_compaction_correctness_check_core) &&
            !_allow_delete_in_cumu_compaction &&
            compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
            missed_rows = std::make_unique<RowLocationSet>();
            LOG(INFO) << "RowLocation Set inited succ for tablet:" << _tablet->tablet_id();
        }
        std::unique_ptr<std::map<RowsetSharedPtr, RowLocationPairList>> location_map;
        if (config::enable_rowid_conversion_correctness_check) {
            location_map = std::make_unique<std::map<RowsetSharedPtr, RowLocationPairList>>();
            LOG(INFO) << "Location Map inited succ for tablet:" << _tablet->tablet_id();
        }
        // Convert the delete bitmap of the input rowsets to output rowset.
        // New loads are not blocked, so some keys of input rowsets might
        // be deleted during the time. We need to deal with delete bitmap
        // of incremental data later.
        // TODO(LiaoXin): check if there are duplicate keys
        std::size_t missed_rows_size = 0;
        tablet()->calc_compaction_output_rowset_delete_bitmap(
                _input_rowsets, _rowid_conversion, 0, version.second + 1, missed_rows.get(),
                location_map.get(), _tablet->tablet_meta()->delete_bitmap(),
                &output_rowset_delete_bitmap);
        if (missed_rows) {
            missed_rows_size = missed_rows->size();
            std::size_t merged_missed_rows_size = _stats.merged_rows;
            if (!_tablet->tablet_meta()->tablet_schema()->cluster_key_idxes().empty()) {
                merged_missed_rows_size += _stats.filtered_rows;
            }
            if (_tablet->tablet_state() == TABLET_RUNNING &&
                merged_missed_rows_size != missed_rows_size) {
                std::stringstream ss;
                ss << "cumulative compaction: the merged rows(" << _stats.merged_rows
                   << "), filtered rows(" << _stats.filtered_rows
                   << ") is not equal to missed rows(" << missed_rows_size
                   << ") in rowid conversion, tablet_id: " << _tablet->tablet_id()
                   << ", table_id:" << _tablet->table_id();
                if (missed_rows_size == 0) {
                    ss << ", debug info: ";
                    DeleteBitmap subset_map(_tablet->tablet_id());
                    for (auto rs : _input_rowsets) {
                        _tablet->tablet_meta()->delete_bitmap().subset(
                                {rs->rowset_id(), 0, 0},
                                {rs->rowset_id(), rs->num_segments(), version.second + 1},
                                &subset_map);
                        ss << "(rowset id: " << rs->rowset_id()
                           << ", delete bitmap cardinality: " << subset_map.cardinality() << ")";
                    }
                    ss << ", version[0-" << version.second + 1 << "]";
                }
                std::string err_msg = fmt::format(
                        "cumulative compaction: the merged rows({}), filtered rows({})"
                        " is not equal to missed rows({}) in rowid conversion,"
                        " tablet_id: {}, table_id:{}",
                        _stats.merged_rows, _stats.filtered_rows, missed_rows_size,
                        _tablet->tablet_id(), _tablet->table_id());
                if (config::enable_mow_compaction_correctness_check_core) {
                    CHECK(false) << err_msg;
                } else {
                    DCHECK(false) << err_msg;
                }
                LOG(WARNING) << err_msg;
            }
        }

        if (location_map) {
            RETURN_IF_ERROR(tablet()->check_rowid_conversion(_output_rowset, *location_map));
            location_map->clear();
        }

        {
            std::lock_guard<std::mutex> wrlock_(tablet()->get_rowset_update_lock());
            std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
            SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);

            // Here we will calculate all the rowsets delete bitmaps which are committed but not published to reduce the calculation pressure
            // of publish phase.
            // All rowsets which need to recalculate have been published so we don't need to acquire lock.
            // Step1: collect this tablet's all committed rowsets' delete bitmaps
            CommitTabletTxnInfoVec commit_tablet_txn_info_vec {};
            _engine.txn_manager()->get_all_commit_tablet_txn_info_by_tablet(
                    *tablet(), &commit_tablet_txn_info_vec);

            // Step2: calculate all rowsets' delete bitmaps which are published during compaction.
            for (auto& it : commit_tablet_txn_info_vec) {
                if (!_check_if_includes_input_rowsets(it.rowset_ids)) {
                    // When calculating the delete bitmap of all committed rowsets relative to the compaction,
                    // there may be cases where the compacted rowsets are newer than the committed rowsets.
                    // At this time, row number conversion cannot be performed, otherwise data will be missing.
                    // Therefore, we need to check if every committed rowset has calculated delete bitmap for
                    // all compaction input rowsets.
                    continue;
                }
                DeleteBitmap txn_output_delete_bitmap(_tablet->tablet_id());
                tablet()->calc_compaction_output_rowset_delete_bitmap(
                        _input_rowsets, _rowid_conversion, 0, UINT64_MAX, missed_rows.get(),
                        location_map.get(), *it.delete_bitmap.get(), &txn_output_delete_bitmap);
                if (config::enable_merge_on_write_correctness_check) {
                    RowsetIdUnorderedSet rowsetids;
                    rowsetids.insert(_output_rowset->rowset_id());
                    _tablet->add_sentinel_mark_to_delete_bitmap(&txn_output_delete_bitmap,
                                                                rowsetids);
                }
                it.delete_bitmap->merge(txn_output_delete_bitmap);
                // Step3: write back updated delete bitmap and tablet info.
                it.rowset_ids.insert(_output_rowset->rowset_id());
                _engine.txn_manager()->set_txn_related_delete_bitmap(
                        it.partition_id, it.transaction_id, _tablet->tablet_id(),
                        tablet()->tablet_uid(), true, it.delete_bitmap, it.rowset_ids,
                        it.partial_update_info);
            }

            // Convert the delete bitmap of the input rowsets to output rowset for
            // incremental data.
            tablet()->calc_compaction_output_rowset_delete_bitmap(
                    _input_rowsets, _rowid_conversion, version.second, UINT64_MAX,
                    missed_rows.get(), location_map.get(), _tablet->tablet_meta()->delete_bitmap(),
                    &output_rowset_delete_bitmap);

            if (missed_rows) {
                DCHECK_EQ(missed_rows->size(), missed_rows_size);
                if (missed_rows->size() != missed_rows_size) {
                    LOG(WARNING) << "missed rows don't match, before: " << missed_rows_size
                                 << " after: " << missed_rows->size();
                }
            }

            if (location_map) {
                RETURN_IF_ERROR(tablet()->check_rowid_conversion(_output_rowset, *location_map));
            }

            tablet()->merge_delete_bitmap(output_rowset_delete_bitmap);
            RETURN_IF_ERROR(tablet()->modify_rowsets(output_rowsets, _input_rowsets, true));
        }
    } else {
        std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        RETURN_IF_ERROR(tablet()->modify_rowsets(output_rowsets, _input_rowsets, true));
    }

    if (config::tablet_rowset_stale_sweep_by_size &&
        _tablet->tablet_meta()->all_stale_rs_metas().size() >=
                config::tablet_rowset_stale_sweep_threshold_size) {
        tablet()->delete_expired_stale_rowset();
    }

    int64_t cur_max_version = 0;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        cur_max_version = _tablet->max_version_unlocked();
        tablet()->save_meta();
    }
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        auto st = TabletMetaManager::remove_old_version_delete_bitmap(
                tablet()->data_dir(), _tablet->tablet_id(), cur_max_version);
        if (!st.ok()) {
            LOG(WARNING) << "failed to remove old version delete bitmap, st: " << st;
        }
    }
    return Status::OK();
}

bool CompactionMixin::_check_if_includes_input_rowsets(
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

Status Compaction::check_correctness() {
    // 1. check row number
    if (_input_row_num != _output_rowset->num_rows() + _stats.merged_rows + _stats.filtered_rows) {
        return Status::Error<CHECK_LINES_ERROR>(
                "row_num does not match between cumulative input and output! tablet={}, "
                "input_row_num={}, merged_row_num={}, filtered_row_num={}, output_row_num={}",
                _tablet->tablet_id(), _input_row_num, _stats.merged_rows, _stats.filtered_rows,
                _output_rowset->num_rows());
    }
    return Status::OK();
}

int64_t CompactionMixin::get_compaction_permits() {
    int64_t permits = 0;
    for (auto&& rowset : _input_rowsets) {
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

void CloudCompactionMixin::build_basic_info() {
    _output_version =
            Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());

    _newest_write_timestamp = _input_rowsets.back()->newest_write_timestamp();

    std::vector<RowsetMetaSharedPtr> rowset_metas(_input_rowsets.size());
    std::transform(_input_rowsets.begin(), _input_rowsets.end(), rowset_metas.begin(),
                   [](const RowsetSharedPtr& rowset) { return rowset->rowset_meta(); });
    _cur_tablet_schema = _tablet->tablet_schema_with_merged_max_schema_version(rowset_metas);
}

int64_t CloudCompactionMixin::get_compaction_permits() {
    int64_t permits = 0;
    for (auto&& rowset : _input_rowsets) {
        permits += rowset->rowset_meta()->get_compaction_score();
    }
    return permits;
}

CloudCompactionMixin::CloudCompactionMixin(CloudStorageEngine& engine, CloudTabletSPtr tablet,
                                           const std::string& label)
        : Compaction(tablet, label), _engine(engine) {}

Status CloudCompactionMixin::execute_compact_impl(int64_t permits) {
    OlapStopWatch watch;

    build_basic_info();

    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->tablet_id()
              << ", output_version=" << _output_version << ", permits: " << permits;

    RETURN_IF_ERROR(merge_input_rowsets());

    RETURN_IF_ERROR(do_inverted_index_compaction());

    RETURN_IF_ERROR(_engine.meta_mgr().commit_rowset(*_output_rowset->rowset_meta().get()));

    // 4. modify rowsets in memory
    RETURN_IF_ERROR(modify_rowsets());

    return Status::OK();
}

Status CloudCompactionMixin::execute_compact() {
    TEST_INJECTION_POINT("Compaction::do_compaction");
    int64_t permits = get_compaction_permits();
    HANDLE_EXCEPTION_IF_CATCH_EXCEPTION(execute_compact_impl(permits),
                                        [&](const doris::Exception& ex) { garbage_collection(); });
    _load_segment_to_cache();
    return Status::OK();
}

Status CloudCompactionMixin::modify_rowsets() {
    return Status::OK();
}

Status CloudCompactionMixin::construct_output_rowset_writer(RowsetWriterContext& ctx) {
    // only do index compaction for dup_keys and unique_keys with mow enabled
    if (config::inverted_index_compaction_enable &&
        (((_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
           _tablet->enable_unique_key_merge_on_write()) ||
          _tablet->keys_type() == KeysType::DUP_KEYS)) &&
        _cur_tablet_schema->get_inverted_index_storage_format() ==
                InvertedIndexStorageFormatPB::V1) {
        construct_skip_inverted_index(ctx);
    }

    // Use the storage resource of the previous rowset
    ctx.storage_resource =
            *DORIS_TRY(_input_rowsets.back()->rowset_meta()->remote_storage_resource());

    ctx.txn_id = boost::uuids::hash_value(UUIDGenerator::instance()->next_uuid()) &
                 std::numeric_limits<int64_t>::max(); // MUST be positive
    ctx.txn_expiration = _expiration;

    ctx.version = _output_version;
    ctx.rowset_state = VISIBLE;
    ctx.segments_overlap = NONOVERLAPPING;
    ctx.tablet_schema = _cur_tablet_schema;
    ctx.newest_write_timestamp = _newest_write_timestamp;
    ctx.write_type = DataWriteType::TYPE_COMPACTION;

    auto compaction_policy = _tablet->tablet_meta()->compaction_policy();
    if (_tablet->tablet_meta()->time_series_compaction_level_threshold() >= 2) {
        ctx.compaction_level = _engine.cumu_compaction_policy(compaction_policy)
                                       ->new_compaction_level(_input_rowsets);
    }

    ctx.write_file_cache = compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION;
    ctx.file_cache_ttl_sec = _tablet->ttl_seconds();
    _output_rs_writer = DORIS_TRY(_tablet->create_rowset_writer(ctx, _is_vertical));
    RETURN_IF_ERROR(_engine.meta_mgr().prepare_rowset(*_output_rs_writer->rowset_meta().get()));
    return Status::OK();
}

void CloudCompactionMixin::garbage_collection() {
    if (!config::enable_file_cache) {
        return;
    }
    if (_output_rs_writer) {
        auto* beta_rowset_writer = dynamic_cast<BaseBetaRowsetWriter*>(_output_rs_writer.get());
        DCHECK(beta_rowset_writer);
        for (const auto& [_, file_writer] : beta_rowset_writer->get_file_writers()) {
            auto file_key = io::BlockFileCache::hash(file_writer->path().filename().native());
            auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
            file_cache->remove_if_cached(file_key);
        }
    }
}

} // namespace doris
