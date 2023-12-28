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

#include "olap/rowset/beta_rowset_writer.h"

#include <assert.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <stdio.h>

#include <ctime> // time
#include <filesystem>
#include <memory>
#include <sstream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segcompaction.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "runtime/thread_context.h"
#include "util/debug_points.h"
#include "util/slice.h"
#include "util/time.h"
#include "vec/columns/column.h"
#include "vec/common/schema_util.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
using namespace ErrorCode;

namespace {

bool is_segment_overlapping(const std::vector<KeyBoundsPB>& segments_encoded_key_bounds) {
    std::string_view last;
    for (auto&& segment_encode_key : segments_encoded_key_bounds) {
        auto&& cur_min = segment_encode_key.min_key();
        auto&& cur_max = segment_encode_key.max_key();
        if (cur_min <= last) {
            return true;
        }
        last = cur_max;
    }
    return false;
}

void build_rowset_meta_with_spec_field(RowsetMeta& rowset_meta,
                                       const RowsetMeta& spec_rowset_meta) {
    rowset_meta.set_num_rows(spec_rowset_meta.num_rows());
    rowset_meta.set_total_disk_size(spec_rowset_meta.total_disk_size());
    rowset_meta.set_data_disk_size(spec_rowset_meta.total_disk_size());
    rowset_meta.set_index_disk_size(spec_rowset_meta.index_disk_size());
    // TODO write zonemap to meta
    rowset_meta.set_empty(spec_rowset_meta.num_rows() == 0);
    rowset_meta.set_creation_time(time(nullptr));
    rowset_meta.set_num_segments(spec_rowset_meta.num_segments());
    rowset_meta.set_segments_overlap(spec_rowset_meta.segments_overlap());
    rowset_meta.set_rowset_state(spec_rowset_meta.rowset_state());

    std::vector<KeyBoundsPB> segments_key_bounds;
    spec_rowset_meta.get_segments_key_bounds(&segments_key_bounds);
    rowset_meta.set_segments_key_bounds(segments_key_bounds);
}

} // namespace

BaseBetaRowsetWriter::BaseBetaRowsetWriter()
        : _rowset_meta(nullptr),
          _num_segment(0),
          _segment_start_id(0),
          _num_rows_written(0),
          _total_data_size(0),
          _total_index_size(0) {}

BetaRowsetWriter::BetaRowsetWriter(StorageEngine& engine)
        : _engine(engine), _segcompaction_worker(std::make_shared<SegcompactionWorker>(this)) {}

BaseBetaRowsetWriter::~BaseBetaRowsetWriter() {
    // TODO(lingbin): Should wrapper exception logic, no need to know file ops directly.
    if (!_already_built) { // abnormal exit, remove all files generated
        WARN_IF_ERROR(_segment_creator.close(),
                      "close segment creator failed"); // ensure all files are closed
        const auto& fs = _rowset_meta->fs();
        if (!fs || !_rowset_meta->is_local()) { // Remote fs will delete them asynchronously
            return;
        }
        for (int i = _segment_start_id; i < _segment_creator.next_segment_id(); ++i) {
            std::string seg_path =
                    BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, i);
            // Even if an error is encountered, these files that have not been cleaned up
            // will be cleaned up by the GC background. So here we only print the error
            // message when we encounter an error.
            WARN_IF_ERROR(fs->delete_file(seg_path),
                          fmt::format("Failed to delete file={}", seg_path));
        }
    }
}

BetaRowsetWriter::~BetaRowsetWriter() {
    /* Note that segcompaction is async and in parallel with load job. So we should handle carefully
     * when the job is cancelled. Although it is meaningless to continue segcompaction when the job
     * is cancelled, the objects involved in the job should be preserved during segcompaction to
     * avoid crashs for memory issues. */
    WARN_IF_ERROR(_wait_flying_segcompaction(), "segment compaction failed");
}

Status BaseBetaRowsetWriter::init(const RowsetWriterContext& rowset_writer_context) {
    _context = rowset_writer_context;
    _rowset_meta.reset(new RowsetMeta);
    _rowset_meta->set_fs(_context.fs);
    _rowset_meta->set_rowset_id(_context.rowset_id);
    _rowset_meta->set_partition_id(_context.partition_id);
    _rowset_meta->set_tablet_id(_context.tablet_id);
    _rowset_meta->set_tablet_schema_hash(_context.tablet_schema_hash);
    _rowset_meta->set_rowset_type(_context.rowset_type);
    _rowset_meta->set_rowset_state(_context.rowset_state);
    _rowset_meta->set_segments_overlap(_context.segments_overlap);
    if (_context.rowset_state == PREPARED || _context.rowset_state == COMMITTED) {
        _is_pending = true;
        _rowset_meta->set_txn_id(_context.txn_id);
        _rowset_meta->set_load_id(_context.load_id);
    } else {
        _rowset_meta->set_version(_context.version);
        _rowset_meta->set_newest_write_timestamp(_context.newest_write_timestamp);
    }
    _rowset_meta->set_tablet_uid(_context.tablet_uid);
    _rowset_meta->set_tablet_schema(_context.tablet_schema);
    _context.segment_collector = std::make_shared<SegmentCollectorT<BaseBetaRowsetWriter>>(this);
    _context.file_writer_creator = std::make_shared<FileWriterCreatorT<BaseBetaRowsetWriter>>(this);
    RETURN_IF_ERROR(_segment_creator.init(_context));
    return Status::OK();
}

Status BaseBetaRowsetWriter::add_block(const vectorized::Block* block) {
    return _segment_creator.add_block(block);
}

Status BetaRowsetWriter::_generate_delete_bitmap(int32_t segment_id) {
    SCOPED_RAW_TIMER(&_delete_bitmap_ns);
    if (!_context.tablet->enable_unique_key_merge_on_write() ||
        (_context.partial_update_info && _context.partial_update_info->is_partial_update)) {
        return Status::OK();
    }
    auto rowset = _build_tmp();
    auto beta_rowset = reinterpret_cast<BetaRowset*>(rowset.get());
    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_IF_ERROR(beta_rowset->load_segments(segment_id, segment_id + 1, &segments));
    std::vector<RowsetSharedPtr> specified_rowsets;
    auto tablet = static_cast<Tablet*>(_context.tablet.get());
    {
        std::shared_lock meta_rlock(tablet->get_header_lock());
        specified_rowsets = tablet->get_rowset_by_ids(&_context.mow_context->rowset_ids);
    }
    OlapStopWatch watch;
    RETURN_IF_ERROR(tablet->calc_delete_bitmap(rowset, segments, specified_rowsets,
                                               _context.mow_context->delete_bitmap,
                                               _context.mow_context->max_version, nullptr));
    size_t total_rows = std::accumulate(
            segments.begin(), segments.end(), 0,
            [](size_t sum, const segment_v2::SegmentSharedPtr& s) { return sum += s->num_rows(); });
    LOG(INFO) << "[Memtable Flush] construct delete bitmap tablet: " << _context.tablet->tablet_id()
              << ", rowset_ids: " << _context.mow_context->rowset_ids.size()
              << ", cur max_version: " << _context.mow_context->max_version
              << ", transaction_id: " << _context.mow_context->txn_id
              << ", cost: " << watch.get_elapse_time_us() << "(us), total rows: " << total_rows;
    return Status::OK();
}

Status BetaRowsetWriter::_load_noncompacted_segment(segment_v2::SegmentSharedPtr& segment,
                                                    int32_t segment_id) {
    DCHECK(_rowset_meta->is_local());
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>(
                "BetaRowsetWriter::_load_noncompacted_segment _rowset_meta->fs get failed");
    }
    auto path = BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, segment_id);
    io::FileReaderOptions reader_options {
            .cache_type = config::enable_file_cache ? io::FileCachePolicy::FILE_BLOCK_CACHE
                                                    : io::FileCachePolicy::NO_CACHE,
            .is_doris_table = true};
    auto s = segment_v2::Segment::open(fs, path, segment_id, rowset_id(), _context.tablet_schema,
                                       reader_options, &segment);
    if (!s.ok()) {
        LOG(WARNING) << "failed to open segment. " << path << ":" << s;
        return s;
    }
    return Status::OK();
}

/* policy of segcompaction target selection:
 *  1. skip big segments
 *  2. if the consecutive smalls end up with a big, compact the smalls, except
 *     single small
 *  3. if the consecutive smalls end up with small, compact the smalls if the
 *     length is beyond (config::segcompaction_batch_size / 2)
 */
Status BetaRowsetWriter::_find_longest_consecutive_small_segment(
        SegCompactionCandidatesSharedPtr& segments) {
    segments = std::make_shared<SegCompactionCandidates>();
    // skip last (maybe active) segment
    int32_t last_segment = _num_segment - 1;
    size_t task_bytes = 0;
    uint32_t task_rows = 0;
    int32_t segid;
    for (segid = _segcompacted_point;
         segid < last_segment && segments->size() < config::segcompaction_batch_size; segid++) {
        segment_v2::SegmentSharedPtr segment;
        RETURN_IF_ERROR(_load_noncompacted_segment(segment, segid));
        const auto segment_rows = segment->num_rows();
        const auto segment_bytes = segment->file_reader()->size();
        bool is_large_segment = segment_rows > config::segcompaction_candidate_max_rows ||
                                segment_bytes > config::segcompaction_candidate_max_bytes;
        if (is_large_segment) {
            if (segid == _segcompacted_point) {
                // skip large segments at the front
                RETURN_IF_ERROR(_rename_compacted_segment_plain(_segcompacted_point++));
                continue;
            } else {
                // stop because we need consecutive segments
                break;
            }
        }
        bool is_task_full = task_rows + segment_rows > config::segcompaction_task_max_rows ||
                            task_bytes + segment_bytes > config::segcompaction_task_max_bytes;
        if (is_task_full) {
            break;
        }
        segments->push_back(segment);
        task_rows += segment->num_rows();
        task_bytes += segment->file_reader()->size();
    }
    size_t s = segments->size();
    if (segid == last_segment && s <= (config::segcompaction_batch_size / 2)) {
        // we didn't collect enough segments, better to do it in next
        // round to compact more at once
        segments->clear();
        return Status::OK();
    }
    if (s == 1) { // poor bachelor, let it go
        VLOG_DEBUG << "only one candidate segment";
        RETURN_IF_ERROR(_rename_compacted_segment_plain(_segcompacted_point++));
        segments->clear();
        return Status::OK();
    }
    if (VLOG_DEBUG_IS_ON) {
        vlog_buffer.clear();
        for (auto& segment : (*segments.get())) {
            fmt::format_to(vlog_buffer, "[id:{} num_rows:{}]", segment->id(), segment->num_rows());
        }
        VLOG_DEBUG << "candidate segments num:" << s
                   << " list of candidates:" << fmt::to_string(vlog_buffer);
    }
    return Status::OK();
}

Status BetaRowsetWriter::_rename_compacted_segments(int64_t begin, int64_t end) {
    int ret;
    auto src_seg_path = BetaRowset::local_segment_path_segcompacted(_context.rowset_dir,
                                                                    _context.rowset_id, begin, end);
    auto dst_seg_path = BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id,
                                                      _num_segcompacted);
    ret = rename(src_seg_path.c_str(), dst_seg_path.c_str());
    if (ret) {
        return Status::Error<ROWSET_RENAME_FILE_FAILED>(
                "failed to rename {} to {}. ret:{}, errno:{}", src_seg_path, dst_seg_path, ret,
                errno);
    }

    // rename inverted index files
    RETURN_IF_ERROR(_rename_compacted_indices(begin, end, 0));

    _num_segcompacted++;
    return Status::OK();
}

void BetaRowsetWriter::_clear_statistics_for_deleting_segments_unsafe(uint64_t begin,
                                                                      uint64_t end) {
    VLOG_DEBUG << "_segid_statistics_map clear record segid range from:" << begin << " to:" << end;
    for (int i = begin; i <= end; ++i) {
        _segid_statistics_map.erase(i);
    }
}

Status BetaRowsetWriter::_rename_compacted_segment_plain(uint64_t seg_id) {
    if (seg_id == _num_segcompacted) {
        ++_num_segcompacted;
        return Status::OK();
    }

    auto src_seg_path =
            BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, seg_id);
    auto dst_seg_path = BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id,
                                                      _num_segcompacted);
    VLOG_DEBUG << "segcompaction skip this segment. rename " << src_seg_path << " to "
               << dst_seg_path;
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        DCHECK_EQ(_segid_statistics_map.find(seg_id) == _segid_statistics_map.end(), false);
        DCHECK_EQ(_segid_statistics_map.find(_num_segcompacted) == _segid_statistics_map.end(),
                  true);
        auto org = _segid_statistics_map[seg_id];
        _segid_statistics_map.emplace(_num_segcompacted, org);
        _clear_statistics_for_deleting_segments_unsafe(seg_id, seg_id);
    }
    int ret = rename(src_seg_path.c_str(), dst_seg_path.c_str());
    if (ret) {
        return Status::Error<ROWSET_RENAME_FILE_FAILED>(
                "failed to rename {} to {}. ret:{}, errno:{}", src_seg_path, dst_seg_path, ret,
                errno);
    }
    // rename remaining inverted index files
    RETURN_IF_ERROR(_rename_compacted_indices(-1, -1, seg_id));

    ++_num_segcompacted;
    return Status::OK();
}

Status BetaRowsetWriter::_rename_compacted_indices(int64_t begin, int64_t end, uint64_t seg_id) {
    int ret;
    // rename remaining inverted index files
    for (auto column : _context.tablet_schema->columns()) {
        if (_context.tablet_schema->has_inverted_index(column)) {
            auto index_info = _context.tablet_schema->get_inverted_index(column);
            auto index_id = index_info->index_id();
            auto src_idx_path =
                    begin < 0 ? InvertedIndexDescriptor::inverted_index_file_path(
                                        _context.rowset_dir, _context.rowset_id, seg_id, index_id,
                                        index_info->get_index_suffix())
                              : InvertedIndexDescriptor::local_inverted_index_path_segcompacted(
                                        _context.rowset_dir, _context.rowset_id, begin, end,
                                        index_id, index_info->get_index_suffix());
            auto dst_idx_path = InvertedIndexDescriptor::inverted_index_file_path(
                    _context.rowset_dir, _context.rowset_id, _num_segcompacted, index_id,
                    index_info->get_index_suffix());
            VLOG_DEBUG << "segcompaction skip this index. rename " << src_idx_path << " to "
                       << dst_idx_path;
            ret = rename(src_idx_path.c_str(), dst_idx_path.c_str());
            if (ret) {
                return Status::Error<INVERTED_INDEX_RENAME_FILE_FAILED>(
                        "failed to rename {} to {}. ret:{}, errno:{}", src_idx_path, dst_idx_path,
                        ret, errno);
            }
            // Erase the origin index file cache
            static_cast<void>(InvertedIndexSearcherCache::instance()->erase(src_idx_path));
            static_cast<void>(InvertedIndexSearcherCache::instance()->erase(dst_idx_path));
        }
    }
    return Status::OK();
}

// return true if there isn't any flying segcompaction, otherwise return false
bool BetaRowsetWriter::_check_and_set_is_doing_segcompaction() {
    return !_is_doing_segcompaction.exchange(true);
}

Status BetaRowsetWriter::_segcompaction_if_necessary() {
    Status status = Status::OK();
    // leave _check_and_set_is_doing_segcompaction as the last condition
    // otherwise _segcompacting_cond will never get notified
    if (!config::enable_segcompaction || !_context.enable_segcompaction ||
        !_context.tablet_schema->cluster_key_idxes().empty() ||
        !_check_and_set_is_doing_segcompaction()) {
        return status;
    }
    if (_segcompaction_status.load() != OK) {
        status = Status::Error<SEGCOMPACTION_FAILED>(
                "BetaRowsetWriter::_segcompaction_if_necessary meet invalid state");
    } else if ((_num_segment - _segcompacted_point) >= config::segcompaction_batch_size) {
        SegCompactionCandidatesSharedPtr segments;
        status = _find_longest_consecutive_small_segment(segments);
        if (LIKELY(status.ok()) && (!segments->empty())) {
            LOG(INFO) << "submit segcompaction task, tablet_id:" << _context.tablet_id
                      << " rowset_id:" << _context.rowset_id << " segment num:" << _num_segment
                      << ", segcompacted_point:" << _segcompacted_point;
            status = _engine.submit_seg_compaction_task(_segcompaction_worker, segments);
            if (status.ok()) {
                return status;
            }
        }
    }
    {
        std::lock_guard lk(_is_doing_segcompaction_lock);
        _is_doing_segcompaction = false;
        _segcompacting_cond.notify_all();
    }
    return status;
}

Status BetaRowsetWriter::_segcompaction_rename_last_segments() {
    DCHECK_EQ(_is_doing_segcompaction, false);
    if (!config::enable_segcompaction) {
        return Status::OK();
    }
    if (_segcompaction_status.load() != OK) {
        return Status::Error<SEGCOMPACTION_FAILED>(
                "BetaRowsetWriter::_segcompaction_rename_last_segments meet invalid state");
    }
    if (!_is_segcompacted() || _segcompacted_point == _num_segment) {
        // no need if never segcompact before or all segcompacted
        return Status::OK();
    }
    // currently we only rename remaining segments to reduce wait time
    // so that transaction can be committed ASAP
    VLOG_DEBUG << "segcompaction last few segments";
    for (int32_t segid = _segcompacted_point; segid < _num_segment; segid++) {
        RETURN_IF_ERROR(_rename_compacted_segment_plain(_segcompacted_point++));
    }
    return Status::OK();
}

Status BaseBetaRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    assert(rowset->rowset_meta()->rowset_type() == BETA_ROWSET);
    RETURN_IF_ERROR(rowset->link_files_to(_context.rowset_dir, _context.rowset_id));
    _num_rows_written += rowset->num_rows();
    _total_data_size += rowset->rowset_meta()->data_disk_size();
    _total_index_size += rowset->rowset_meta()->index_disk_size();
    _num_segment += rowset->num_segments();
    // append key_bounds to current rowset
    RETURN_IF_ERROR(rowset->get_segments_key_bounds(&_segments_encoded_key_bounds));
    // TODO update zonemap
    if (rowset->rowset_meta()->has_delete_predicate()) {
        _rowset_meta->set_delete_predicate(rowset->rowset_meta()->delete_predicate());
    }
    // Update the tablet schema in the rowset metadata if the tablet schema contains a variant.
    // During the build process, _context.tablet_schema will be used as the rowset schema.
    // This situation may arise in the event of a linked schema change. If this schema is not set,
    // the subcolumns of the variant will be lost.
    if (_context.tablet_schema->num_variant_columns() > 0 && rowset->tablet_schema() != nullptr) {
        _context.tablet_schema = rowset->tablet_schema();
    }
    return Status::OK();
}

Status BaseBetaRowsetWriter::add_rowset_for_linked_schema_change(RowsetSharedPtr rowset) {
    // TODO use schema_mapping to transfer zonemap
    return add_rowset(rowset);
}

Status BaseBetaRowsetWriter::flush() {
    return _segment_creator.flush();
}

Status BaseBetaRowsetWriter::flush_memtable(vectorized::Block* block, int32_t segment_id,
                                            int64_t* flush_size) {
    if (block->rows() == 0) {
        return Status::OK();
    }

    {
        SCOPED_RAW_TIMER(&_segment_writer_ns);
        RETURN_IF_ERROR(_segment_creator.flush_single_block(block, segment_id, flush_size));
    }
    return Status::OK();
}

Status BaseBetaRowsetWriter::flush_single_block(const vectorized::Block* block) {
    return _segment_creator.flush_single_block(block);
}

Status BetaRowsetWriter::_wait_flying_segcompaction() {
    std::unique_lock<std::mutex> l(_is_doing_segcompaction_lock);
    uint64_t begin_wait = GetCurrentTimeMicros();
    while (_is_doing_segcompaction) {
        // change sync wait to async?
        _segcompacting_cond.wait(l);
    }
    uint64_t elapsed = GetCurrentTimeMicros() - begin_wait;
    if (elapsed >= MICROS_PER_SEC) {
        LOG(INFO) << "wait flying segcompaction finish time:" << elapsed << "us";
    }
    if (_segcompaction_status.load() != OK) {
        return Status::Error<SEGCOMPACTION_FAILED>("BetaRowsetWriter meet invalid state.");
    }
    return Status::OK();
}

RowsetSharedPtr BaseBetaRowsetWriter::manual_build(const RowsetMetaSharedPtr& spec_rowset_meta) {
    if (_rowset_meta->newest_write_timestamp() == -1) {
        _rowset_meta->set_newest_write_timestamp(UnixSeconds());
    }

    build_rowset_meta_with_spec_field(*_rowset_meta, *spec_rowset_meta);
    RowsetSharedPtr rowset;
    auto status = RowsetFactory::create_rowset(_context.tablet_schema, _context.rowset_dir,
                                               _rowset_meta, &rowset);
    if (!status.ok()) {
        LOG(WARNING) << "rowset init failed when build new rowset, res=" << status;
        return nullptr;
    }
    _already_built = true;
    return rowset;
}

Status BaseBetaRowsetWriter::_close_file_writers() {
    // TODO(lingbin): move to more better place, or in a CreateBlockBatch?
    for (auto& file_writer : _file_writers) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                file_writer->close(),
                fmt::format("failed to close file writer, path={}", file_writer->path().string()));
    }
    RETURN_NOT_OK_STATUS_WITH_WARN(_segment_creator.close(),
                                   "failed to close segment creator when build new rowset");
    return Status::OK();
}

Status BetaRowsetWriter::_close_file_writers() {
    RETURN_IF_ERROR(BaseBetaRowsetWriter::_close_file_writers());
    // if _segment_start_id is not zero, that means it's a transient rowset writer for
    // MoW partial update, don't need to do segment compaction.
    if (_segment_start_id == 0) {
        if (_segcompaction_worker->cancel()) {
            std::lock_guard lk(_is_doing_segcompaction_lock);
            _is_doing_segcompaction = false;
            _segcompacting_cond.notify_all();
        } else {
            RETURN_NOT_OK_STATUS_WITH_WARN(_wait_flying_segcompaction(),
                                           "segcompaction failed when build new rowset");
        }
        RETURN_NOT_OK_STATUS_WITH_WARN(_segcompaction_rename_last_segments(),
                                       "rename last segments failed when build new rowset");
        if (_segcompaction_worker->get_file_writer()) {
            RETURN_NOT_OK_STATUS_WITH_WARN(_segcompaction_worker->get_file_writer()->close(),
                                           "close segment compaction worker failed");
        }
    }
    return Status::OK();
}

Status BaseBetaRowsetWriter::build(RowsetSharedPtr& rowset) {
    RETURN_IF_ERROR(_close_file_writers());

    RETURN_NOT_OK_STATUS_WITH_WARN(_check_segment_number_limit(),
                                   "too many segments when build new rowset");
    _build_rowset_meta(_rowset_meta);

    if (_rowset_meta->newest_write_timestamp() == -1) {
        _rowset_meta->set_newest_write_timestamp(UnixSeconds());
    }

    // update rowset meta tablet schema if tablet schema updated
    if (_context.tablet_schema->num_variant_columns() > 0) {
        _rowset_meta->set_tablet_schema(_context.tablet_schema);
    }

    RETURN_NOT_OK_STATUS_WITH_WARN(
            RowsetFactory::create_rowset(_context.tablet_schema, _context.rowset_dir, _rowset_meta,
                                         &rowset),
            "rowset init failed when build new rowset");
    _already_built = true;
    return Status::OK();
}

int64_t BaseBetaRowsetWriter::_num_seg() const {
    return _num_segment;
}

int64_t BetaRowsetWriter::_num_seg() const {
    return _is_segcompacted() ? _num_segcompacted : _num_segment;
}

// update tablet schema when meet variant columns, before commit_txn
// Eg. rowset schema:       A(int),    B(float),  C(int), D(int)
// _tabelt->tablet_schema:  A(bigint), B(double)
//  => update_schema:       A(bigint), B(double), C(int), D(int)
void BaseBetaRowsetWriter::update_rowset_schema(TabletSchemaSPtr flush_schema) {
    std::lock_guard<std::mutex> lock(*(_context.schema_lock));
    TabletSchemaSPtr update_schema;
    static_cast<void>(vectorized::schema_util::get_least_common_schema(
            {_context.tablet_schema, flush_schema}, nullptr, update_schema));
    CHECK_GE(update_schema->num_columns(), flush_schema->num_columns())
            << "Rowset merge schema columns count is " << update_schema->num_columns()
            << ", but flush_schema is larger " << flush_schema->num_columns()
            << " update_schema: " << update_schema->dump_structure()
            << " flush_schema: " << flush_schema->dump_structure();
    _context.tablet_schema.swap(update_schema);
    VLOG_DEBUG << "dump rs schema: " << _context.tablet_schema->dump_structure();
}

void BaseBetaRowsetWriter::_build_rowset_meta(std::shared_ptr<RowsetMeta> rowset_meta) {
    int64_t num_rows_written = 0;
    int64_t total_data_size = 0;
    int64_t total_index_size = 0;
    std::vector<KeyBoundsPB> segments_encoded_key_bounds;
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        for (const auto& itr : _segid_statistics_map) {
            num_rows_written += itr.second.row_num;
            total_data_size += itr.second.data_size;
            total_index_size += itr.second.index_size;
            segments_encoded_key_bounds.push_back(itr.second.key_bounds);
        }
    }
    for (auto itr = _segments_encoded_key_bounds.begin(); itr != _segments_encoded_key_bounds.end();
         ++itr) {
        segments_encoded_key_bounds.push_back(*itr);
    }
    // segment key bounds are empty in old version(before version 1.2.x). So we should not modify
    // the overlap property when key bounds are empty.
    if (!segments_encoded_key_bounds.empty() &&
        !is_segment_overlapping(segments_encoded_key_bounds)) {
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
    }

    rowset_meta->set_num_segments(_num_seg());
    // TODO(zhangzhengyu): key_bounds.size() should equal num_seg, but currently not always
    rowset_meta->set_num_rows(num_rows_written + _num_rows_written);
    rowset_meta->set_total_disk_size(total_data_size + _total_data_size);
    rowset_meta->set_data_disk_size(total_data_size + _total_data_size);
    rowset_meta->set_index_disk_size(total_index_size + _total_index_size);
    rowset_meta->set_segments_key_bounds(segments_encoded_key_bounds);
    // TODO write zonemap to meta
    rowset_meta->set_empty((num_rows_written + _num_rows_written) == 0);
    rowset_meta->set_creation_time(time(nullptr));

    if (_is_pending) {
        rowset_meta->set_rowset_state(COMMITTED);
    } else {
        rowset_meta->set_rowset_state(VISIBLE);
    }
}

RowsetSharedPtr BaseBetaRowsetWriter::_build_tmp() {
    std::shared_ptr<RowsetMeta> rowset_meta_ = std::make_shared<RowsetMeta>();
    *rowset_meta_ = *_rowset_meta;
    _build_rowset_meta(rowset_meta_);

    RowsetSharedPtr rowset;
    auto status = RowsetFactory::create_rowset(_context.tablet_schema, _context.rowset_dir,
                                               rowset_meta_, &rowset);
    if (!status.ok()) {
        LOG(WARNING) << "rowset init failed when build new rowset, res=" << status;
        return nullptr;
    }
    return rowset;
}

Status BaseBetaRowsetWriter::_create_file_writer(std::string path, io::FileWriterPtr& file_writer) {
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }
    io::FileWriterOptions opts {
            .write_file_cache = _context.write_file_cache,
            .is_cold_data = _context.is_hot_data,
            .file_cache_expiration =
                    _context.file_cache_ttl_sec > 0 && _context.newest_write_timestamp > 0
                            ? _context.newest_write_timestamp + _context.file_cache_ttl_sec
                            : 0,
    };
    Status st = fs->create_file(path, &file_writer, &opts);
    if (!st.ok()) {
        LOG(WARNING) << "failed to create writable file. path=" << path << ", err: " << st;
        return st;
    }

    DCHECK(file_writer != nullptr);
    return Status::OK();
}

Status BaseBetaRowsetWriter::create_file_writer(uint32_t segment_id,
                                                io::FileWriterPtr& file_writer) {
    std::string path;
    path = BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, segment_id);
    return _create_file_writer(path, file_writer);
}

Status BetaRowsetWriter::_create_segment_writer_for_segcompaction(
        std::unique_ptr<segment_v2::SegmentWriter>* writer, int64_t begin, int64_t end) {
    DCHECK(begin >= 0 && end >= 0);
    std::string path = BetaRowset::local_segment_path_segcompacted(_context.rowset_dir,
                                                                   _context.rowset_id, begin, end);
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(_create_file_writer(path, file_writer));

    segment_v2::SegmentWriterOptions writer_options;
    writer_options.enable_unique_key_merge_on_write = _context.enable_unique_key_merge_on_write;
    writer_options.rowset_ctx = &_context;
    writer_options.write_type = _context.write_type;
    writer_options.write_type = DataWriteType::TYPE_COMPACTION;

    *writer = std::make_unique<segment_v2::SegmentWriter>(
            file_writer.get(), _num_segcompacted, _context.tablet_schema, _context.tablet,
            _context.data_dir, _context.max_rows_per_segment, writer_options, _context.mow_context);
    if (_segcompaction_worker->get_file_writer() != nullptr) {
        RETURN_IF_ERROR(_segcompaction_worker->get_file_writer()->close());
    }
    _segcompaction_worker->get_file_writer().reset(file_writer.release());

    return Status::OK();
}

Status BaseBetaRowsetWriter::_check_segment_number_limit() {
    size_t total_segment_num = _num_segment + 1;
    DBUG_EXECUTE_IF("BetaRowsetWriter._check_segment_number_limit_too_many_segments",
                    { total_segment_num = dp->param("segnum", 1024); });
    if (UNLIKELY(total_segment_num > config::max_segment_num_per_rowset)) {
        return Status::Error<TOO_MANY_SEGMENTS>(
                "too many segments in rowset. tablet_id:{}, rowset_id:{}, max:{}, "
                "_num_segment:{}, ",
                _context.tablet_id, _context.rowset_id.to_string(),
                config::max_segment_num_per_rowset, _num_segment);
    }
    return Status::OK();
}

Status BetaRowsetWriter::_check_segment_number_limit() {
    size_t total_segment_num = _num_segment - _segcompacted_point + 1 + _num_segcompacted;
    DBUG_EXECUTE_IF("BetaRowsetWriter._check_segment_number_limit_too_many_segments",
                    { total_segment_num = dp->param("segnum", 1024); });
    if (UNLIKELY(total_segment_num > config::max_segment_num_per_rowset)) {
        return Status::Error<TOO_MANY_SEGMENTS>(
                "too many segments in rowset. tablet_id:{}, rowset_id:{}, max:{}, _num_segment:{}, "
                "_segcompacted_point:{}, _num_segcompacted:{}",
                _context.tablet_id, _context.rowset_id.to_string(),
                config::max_segment_num_per_rowset, _num_segment, _segcompacted_point,
                _num_segcompacted);
    }
    return Status::OK();
}

Status BaseBetaRowsetWriter::add_segment(uint32_t segment_id, const SegmentStatistics& segstat,
                                         TabletSchemaSPtr flush_schema) {
    uint32_t segid_offset = segment_id - _segment_start_id;
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        CHECK_EQ(_segid_statistics_map.find(segment_id) == _segid_statistics_map.end(), true);
        _segid_statistics_map.emplace(segment_id, segstat);
        if (segment_id >= _segment_num_rows.size()) {
            _segment_num_rows.resize(segment_id + 1);
        }
        _segment_num_rows[segid_offset] = segstat.row_num;
    }
    VLOG_DEBUG << "_segid_statistics_map add new record. segment_id:" << segment_id
               << " row_num:" << segstat.row_num << " data_size:" << segstat.data_size
               << " index_size:" << segstat.index_size;

    {
        std::lock_guard<std::mutex> lock(_segment_set_mutex);
        _segment_set.add(segid_offset);
        while (_segment_set.contains(_num_segment)) {
            _num_segment++;
        }
    }
    // tablet schema updated
    if (flush_schema != nullptr) {
        update_rowset_schema(flush_schema);
    }
    if (_context.mow_context != nullptr) {
        RETURN_IF_ERROR(_generate_delete_bitmap(segment_id));
    }
    return Status::OK();
}

Status BetaRowsetWriter::add_segment(uint32_t segment_id, const SegmentStatistics& segstat,
                                     TabletSchemaSPtr flush_schema) {
    RETURN_IF_ERROR(
            BaseBetaRowsetWriter::add_segment(segment_id, segstat, std::move(flush_schema)));
    return _segcompaction_if_necessary();
}

Status BetaRowsetWriter::flush_segment_writer_for_segcompaction(
        std::unique_ptr<segment_v2::SegmentWriter>* writer, uint64_t index_size,
        KeyBoundsPB& key_bounds) {
    uint32_t segid = (*writer)->get_segment_id();
    uint32_t row_num = (*writer)->row_count();
    uint64_t segment_size;

    auto s = (*writer)->finalize_footer(&segment_size);
    if (!s.ok()) {
        return Status::Error<WRITER_DATA_WRITE_ERROR>("failed to finalize segment: {}",
                                                      s.to_string());
    }

    SegmentStatistics segstat;
    segstat.row_num = row_num;
    segstat.data_size = segment_size + (*writer)->get_inverted_index_file_size();
    segstat.index_size = index_size + (*writer)->get_inverted_index_file_size();
    segstat.key_bounds = key_bounds;
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        CHECK_EQ(_segid_statistics_map.find(segid) == _segid_statistics_map.end(), true);
        _segid_statistics_map.emplace(segid, segstat);
    }
    VLOG_DEBUG << "_segid_statistics_map add new record. segid:" << segid << " row_num:" << row_num
               << " data_size:" << segment_size << " index_size:" << index_size;

    writer->reset();

    return Status::OK();
}

} // namespace doris
