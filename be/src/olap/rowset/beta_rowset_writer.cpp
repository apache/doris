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
#include <stdio.h>

#include <ctime> // time
#include <filesystem>
#include <sstream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_reader_options.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/schema_change.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "runtime/thread_context.h"
#include "util/slice.h"
#include "util/time.h"
#include "vec/columns/column.h"
#include "vec/columns/column_object.h"
#include "vec/common/schema_util.h" // LocalSchemaChangeRecorder
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
using namespace ErrorCode;

BetaRowsetWriter::BetaRowsetWriter()
        : _rowset_meta(nullptr),
          _next_segment_id(0),
          _num_segment(0),
          _segment_start_id(0),
          _segcompacted_point(0),
          _num_segcompacted(0),
          _segment_writer(nullptr),
          _num_rows_written(0),
          _total_data_size(0),
          _total_index_size(0),
          _raw_num_rows_written(0),
          _segcompaction_worker(this),
          _is_doing_segcompaction(false) {
    _segcompaction_status.store(OK);
}

BetaRowsetWriter::~BetaRowsetWriter() {
    /* Note that segcompaction is async and in parallel with load job. So we should handle carefully
     * when the job is cancelled. Although it is meaningless to continue segcompaction when the job
     * is cancelled, the objects involved in the job should be preserved during segcompaction to
     * avoid crashs for memory issues. */
    wait_flying_segcompaction();

    // TODO(lingbin): Should wrapper exception logic, no need to know file ops directly.
    if (!_already_built) {       // abnormal exit, remove all files generated
        _segment_writer.reset(); // ensure all files are closed
        auto fs = _rowset_meta->fs();
        if (!fs) {
            return;
        }
        DCHECK_LE(_segment_start_id + _num_segment, _next_segment_id);
        for (int i = _segment_start_id; i < _next_segment_id; ++i) {
            std::string seg_path =
                    BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, i);
            // Even if an error is encountered, these files that have not been cleaned up
            // will be cleaned up by the GC background. So here we only print the error
            // message when we encounter an error.
            WARN_IF_ERROR(fs->delete_file(seg_path),
                          strings::Substitute("Failed to delete file=$0", seg_path));
        }
    }
}

Status BetaRowsetWriter::init(const RowsetWriterContext& rowset_writer_context) {
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
    _context.schema_change_recorder =
            std::make_shared<vectorized::schema_util::LocalSchemaChangeRecorder>();

    return Status::OK();
}

Status BetaRowsetWriter::add_block(const vectorized::Block* block) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    if (UNLIKELY(_segment_writer == nullptr)) {
        RETURN_IF_ERROR(_create_segment_writer(_segment_writer, allocate_segment_id()));
    }
    return _add_block(block, _segment_writer);
}

Status BetaRowsetWriter::_generate_delete_bitmap(int32_t segment_id) {
    SCOPED_RAW_TIMER(&_delete_bitmap_ns);
    if (!_context.tablet->enable_unique_key_merge_on_write() ||
        _context.tablet_schema->is_partial_update()) {
        return Status::OK();
    }
    auto rowset = _build_tmp();
    auto beta_rowset = reinterpret_cast<BetaRowset*>(rowset.get());
    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_IF_ERROR(beta_rowset->load_segments(segment_id, segment_id + 1, &segments));
    std::vector<RowsetSharedPtr> specified_rowsets;
    {
        std::shared_lock meta_rlock(_context.tablet->get_header_lock());
        specified_rowsets = _context.tablet->get_rowset_by_ids(&_context.mow_context->rowset_ids);
    }
    OlapStopWatch watch;
    RETURN_IF_ERROR(_context.tablet->calc_delete_bitmap(rowset, segments, specified_rowsets,
                                                        _context.mow_context->delete_bitmap,
                                                        _context.mow_context->max_version));
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

Status BetaRowsetWriter::_load_noncompacted_segments(
        std::vector<segment_v2::SegmentSharedPtr>* segments, size_t num) {
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>(
                "BetaRowsetWriter::_load_noncompacted_segments _rowset_meta->fs get failed");
    }
    for (int seg_id = _segcompacted_point; seg_id < num; ++seg_id) {
        auto seg_path =
                BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, seg_id);
        std::shared_ptr<segment_v2::Segment> segment;
        auto type = config::enable_file_cache ? config::file_cache_type : "";
        io::FileReaderOptions reader_options(io::cache_type_from_string(type),
                                             io::SegmentCachePathPolicy());
        auto s = segment_v2::Segment::open(fs, seg_path, seg_id, rowset_id(),
                                           _context.tablet_schema, reader_options, &segment);
        if (!s.ok()) {
            LOG(WARNING) << "failed to open segment. " << seg_path << ":" << s.to_string();
            return s;
        }
        segments->push_back(std::move(segment));
    }
    return Status::OK();
}

/* policy of segcompaction target selection:
 *  1. skip big segments
 *  2. if the consecutive smalls end up with a big, compact the smalls, except
 *     single small
 *  3. if the consecutive smalls end up with small, compact the smalls if the
 *     length is beyond (config::segcompaction_threshold_segment_num / 2)
 */
Status BetaRowsetWriter::_find_longest_consecutive_small_segment(
        SegCompactionCandidatesSharedPtr segments) {
    std::vector<segment_v2::SegmentSharedPtr> all_segments;
    // subtract one to skip last (maybe active) segment
    RETURN_IF_ERROR(_load_noncompacted_segments(&all_segments, _num_segment - 1));

    if (VLOG_DEBUG_IS_ON) {
        vlog_buffer.clear();
        for (auto& segment : all_segments) {
            fmt::format_to(vlog_buffer, "[id:{} num_rows:{}]", segment->id(), segment->num_rows());
        }
        VLOG_DEBUG << "all noncompacted segments num:" << all_segments.size()
                   << " list of segments:" << fmt::to_string(vlog_buffer);
    }

    bool is_terminated_by_big = false;
    bool let_big_terminate = false;
    size_t small_threshold = config::segcompaction_small_threshold;
    for (int64_t i = 0; i < all_segments.size(); ++i) {
        segment_v2::SegmentSharedPtr seg = all_segments[i];
        if (seg->num_rows() > small_threshold) {
            if (let_big_terminate) {
                is_terminated_by_big = true;
                break;
            } else {
                RETURN_IF_ERROR(_rename_compacted_segment_plain(_segcompacted_point++));
            }
        } else {
            let_big_terminate = true; // break if find a big after small
            segments->push_back(seg);
        }
    }
    size_t s = segments->size();
    if (!is_terminated_by_big && s <= (config::segcompaction_threshold_segment_num / 2)) {
        // start with big segments and end with small, better to do it in next
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

    int ret;
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
    ret = rename(src_seg_path.c_str(), dst_seg_path.c_str());
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
        if (_context.tablet_schema->has_inverted_index(column.unique_id())) {
            auto index_id =
                    _context.tablet_schema->get_inverted_index(column.unique_id())->index_id();
            auto src_idx_path =
                    begin < 0 ? InvertedIndexDescriptor::inverted_index_file_path(
                                        _context.rowset_dir, _context.rowset_id, seg_id, index_id)
                              : InvertedIndexDescriptor::local_inverted_index_path_segcompacted(
                                        _context.rowset_dir, _context.rowset_id, begin, end,
                                        index_id);
            auto dst_idx_path = InvertedIndexDescriptor::inverted_index_file_path(
                    _context.rowset_dir, _context.rowset_id, _num_segcompacted, index_id);
            VLOG_DEBUG << "segcompaction skip this index. rename " << src_idx_path << " to "
                       << dst_idx_path;
            ret = rename(src_idx_path.c_str(), dst_idx_path.c_str());
            if (ret) {
                return Status::Error<INVERTED_INDEX_RENAME_FILE_FAILED>(
                        "failed to rename {} to {}. ret:{}, errno:{}", src_idx_path, dst_idx_path,
                        ret, errno);
            }
            // Erase the origin index file cache
            InvertedIndexSearcherCache::instance()->erase(src_idx_path);
            InvertedIndexSearcherCache::instance()->erase(dst_idx_path);
        }
    }
    return Status::OK();
}

Status BetaRowsetWriter::_get_segcompaction_candidates(SegCompactionCandidatesSharedPtr& segments,
                                                       bool is_last) {
    if (is_last) {
        VLOG_DEBUG << "segcompaction last few segments";
        // currently we only rename remaining segments to reduce wait time
        // so that transaction can be committed ASAP
        RETURN_IF_ERROR(_load_noncompacted_segments(segments.get(), _num_segment));
        for (int i = 0; i < segments->size(); ++i) {
            RETURN_IF_ERROR(_rename_compacted_segment_plain(_segcompacted_point++));
        }
        segments->clear();
    } else {
        RETURN_IF_ERROR(_find_longest_consecutive_small_segment(segments));
    }
    return Status::OK();
}

bool BetaRowsetWriter::_check_and_set_is_doing_segcompaction() {
    std::lock_guard<std::mutex> l(_is_doing_segcompaction_lock);
    if (!_is_doing_segcompaction) {
        _is_doing_segcompaction = true;
        return true;
    } else {
        return false;
    }
}

Status BetaRowsetWriter::_segcompaction_if_necessary() {
    Status status = Status::OK();
    if (!config::enable_segcompaction || _context.tablet_schema->is_dynamic_schema() ||
        !_check_and_set_is_doing_segcompaction()) {
        return status;
    }
    if (_segcompaction_status.load() != OK) {
        status = Status::Error<SEGCOMPACTION_FAILED>(
                "BetaRowsetWriter::_segcompaction_if_necessary meet invalid state");
    } else if ((_num_segment - _segcompacted_point) >=
               config::segcompaction_threshold_segment_num) {
        SegCompactionCandidatesSharedPtr segments = std::make_shared<SegCompactionCandidates>();
        status = _get_segcompaction_candidates(segments, false);
        if (LIKELY(status.ok()) && (segments->size() > 0)) {
            LOG(INFO) << "submit segcompaction task, tablet_id:" << _context.tablet_id
                      << " rowset_id:" << _context.rowset_id << " segment num:" << _num_segment
                      << ", segcompacted_point:" << _segcompacted_point;
            status = StorageEngine::instance()->submit_seg_compaction_task(this, segments);
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

Status BetaRowsetWriter::_segcompaction_ramaining_if_necessary() {
    Status status = Status::OK();
    DCHECK_EQ(_is_doing_segcompaction, false);
    if (!config::enable_segcompaction) {
        return Status::OK();
    }
    if (_segcompaction_status.load() != OK) {
        return Status::Error<SEGCOMPACTION_FAILED>(
                "BetaRowsetWriter::_segcompaction_ramaining_if_necessary meet invalid state");
    }
    if (!_is_segcompacted() || _segcompacted_point == _num_segment) {
        // no need if never segcompact before or all segcompacted
        return Status::OK();
    }
    _is_doing_segcompaction = true;
    SegCompactionCandidatesSharedPtr segments = std::make_shared<SegCompactionCandidates>();
    status = _get_segcompaction_candidates(segments, true);
    if (LIKELY(status.ok()) && (segments->size() > 0)) {
        LOG(INFO) << "submit segcompaction remaining task, tablet_id:" << _context.tablet_id
                  << " rowset_id:" << _context.rowset_id << " segment num:" << _num_segment
                  << " segcompacted_point:" << _segcompacted_point;
        status = StorageEngine::instance()->submit_seg_compaction_task(this, segments);
        if (status.ok()) {
            return status;
        }
    }
    _is_doing_segcompaction = false;
    return status;
}

Status BetaRowsetWriter::_add_rows(const vectorized::Block* block,
                                   std::unique_ptr<segment_v2::SegmentWriter>& segment_writer,
                                   size_t row_offset, size_t input_row_num) {
    auto s = segment_writer->append_block(block, row_offset, input_row_num);
    if (UNLIKELY(!s.ok())) {
        return Status::Error<WRITER_DATA_WRITE_ERROR>("failed to append block: {}", s.to_string());
    }
    _raw_num_rows_written += input_row_num;
    return Status::OK();
}

Status BetaRowsetWriter::_add_block(const vectorized::Block* block,
                                    std::unique_ptr<segment_v2::SegmentWriter>& segment_writer) {
    size_t block_size_in_bytes = block->bytes();
    size_t block_row_num = block->rows();
    size_t row_avg_size_in_bytes = std::max((size_t)1, block_size_in_bytes / block_row_num);
    size_t row_offset = 0;

    do {
        auto max_row_add = segment_writer->max_row_to_add(row_avg_size_in_bytes);
        if (UNLIKELY(max_row_add < 1)) {
            // no space for another single row, need flush now
            RETURN_IF_ERROR(_flush_segment_writer(segment_writer));
            RETURN_IF_ERROR(_create_segment_writer(segment_writer, allocate_segment_id()));
            max_row_add = segment_writer->max_row_to_add(row_avg_size_in_bytes);
            DCHECK(max_row_add > 0);
        }
        size_t input_row_num = std::min(block_row_num - row_offset, size_t(max_row_add));
        RETURN_IF_ERROR(_add_rows(block, segment_writer, row_offset, input_row_num));
        row_offset += input_row_num;
    } while (row_offset < block_row_num);

    return Status::OK();
}

Status BetaRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    assert(rowset->rowset_meta()->rowset_type() == BETA_ROWSET);
    RETURN_IF_ERROR(rowset->link_files_to(_context.rowset_dir, _context.rowset_id));
    _num_rows_written += rowset->num_rows();
    _total_data_size += rowset->rowset_meta()->data_disk_size();
    _total_index_size += rowset->rowset_meta()->index_disk_size();
    _num_segment += rowset->num_segments();
    // _next_segment_id is not used in this code path,
    // just to make sure it matches with _num_segment
    _next_segment_id = _num_segment.load();
    // append key_bounds to current rowset
    rowset->get_segments_key_bounds(&_segments_encoded_key_bounds);
    // TODO update zonemap
    if (rowset->rowset_meta()->has_delete_predicate()) {
        _rowset_meta->set_delete_predicate(rowset->rowset_meta()->delete_predicate());
    }
    return Status::OK();
}

Status BetaRowsetWriter::add_rowset_for_linked_schema_change(RowsetSharedPtr rowset) {
    // TODO use schema_mapping to transfer zonemap
    return add_rowset(rowset);
}

Status BetaRowsetWriter::flush() {
    if (_segment_writer != nullptr) {
        RETURN_IF_ERROR(_flush_segment_writer(_segment_writer));
    }
    return Status::OK();
}

Status BetaRowsetWriter::flush_memtable(vectorized::Block* block, int32_t segment_id,
                                        int64_t* flush_size) {
    if (block->rows() == 0) {
        return Status::OK();
    }

    TabletSchemaSPtr flush_schema;
    if (_context.tablet_schema->is_dynamic_schema()) {
        // Unfold variant column
        RETURN_IF_ERROR(_unfold_variant_column(*block, flush_schema));
    }
    {
        SCOPED_RAW_TIMER(&_segment_writer_ns);
        RETURN_IF_ERROR(_flush_single_block(block, segment_id, flush_size, flush_schema));
    }
    RETURN_IF_ERROR(_generate_delete_bitmap(segment_id));
    RETURN_IF_ERROR(_segcompaction_if_necessary());
    return Status::OK();
}

Status BetaRowsetWriter::flush_single_block(const vectorized::Block* block) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    return _flush_single_block(block, allocate_segment_id());
}

Status BetaRowsetWriter::_flush_single_block(const vectorized::Block* block, int32_t segment_id,
                                             int64_t* flush_size, TabletSchemaSPtr flush_schema) {
    std::unique_ptr<segment_v2::SegmentWriter> writer;
    bool no_compression = block->bytes() <= config::segment_compression_threshold_kb * 1024;
    RETURN_IF_ERROR(_create_segment_writer(writer, segment_id, no_compression, flush_schema));
    RETURN_IF_ERROR(_add_rows(block, writer, 0, block->rows()));
    RETURN_IF_ERROR(_flush_segment_writer(writer, flush_size));
    return Status::OK();
}

Status BetaRowsetWriter::wait_flying_segcompaction() {
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

RowsetSharedPtr BetaRowsetWriter::manual_build(const RowsetMetaSharedPtr& spec_rowset_meta) {
    if (_rowset_meta->newest_write_timestamp() == -1) {
        _rowset_meta->set_newest_write_timestamp(UnixSeconds());
    }

    _build_rowset_meta_with_spec_field(_rowset_meta, spec_rowset_meta);
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

RowsetSharedPtr BetaRowsetWriter::build() {
    // make sure all segments are flushed
    DCHECK_EQ(_segment_start_id + _num_segment, _next_segment_id);
    // TODO(lingbin): move to more better place, or in a CreateBlockBatch?
    for (auto& file_writer : _file_writers) {
        Status status = file_writer->close();
        if (!status.ok()) {
            LOG(WARNING) << "failed to close file writer, path=" << file_writer->path()
                         << " res=" << status;
            return nullptr;
        }
    }
    Status status;
    // if _segment_start_id is not zero, that means it's a transient rowset writer for
    // MoW partial update, don't need to do segment compaction.
    if (_segment_start_id == 0) {
        status = wait_flying_segcompaction();
        if (!status.ok()) {
            LOG(WARNING) << "segcompaction failed when build new rowset 1st wait, res=" << status;
            return nullptr;
        }
        status = _segcompaction_ramaining_if_necessary();
        if (!status.ok()) {
            LOG(WARNING) << "segcompaction failed when build new rowset, res=" << status;
            return nullptr;
        }
        status = wait_flying_segcompaction();
        if (!status.ok()) {
            LOG(WARNING) << "segcompaction failed when build new rowset 2nd wait, res=" << status;
            return nullptr;
        }

        if (_segcompaction_worker.get_file_writer()) {
            _segcompaction_worker.get_file_writer()->close();
        }
    }
    // When building a rowset, we must ensure that the current _segment_writer has been
    // flushed, that is, the current _segment_writer is nullptr
    DCHECK(_segment_writer == nullptr) << "segment must be null when build rowset";
    _build_rowset_meta(_rowset_meta);

    if (_rowset_meta->newest_write_timestamp() == -1) {
        _rowset_meta->set_newest_write_timestamp(UnixSeconds());
    }

    // schema changed during this load
    if (_context.schema_change_recorder->has_extended_columns()) {
        DCHECK(_context.tablet_schema->is_dynamic_schema())
                << "Load can change local schema only in dynamic table";
        TabletSchemaSPtr new_schema = std::make_shared<TabletSchema>();
        new_schema->copy_from(*_context.tablet_schema);
        for (auto const& [_, col] : _context.schema_change_recorder->copy_extended_columns()) {
            new_schema->append_column(col);
        }
        new_schema->set_schema_version(_context.schema_change_recorder->schema_version());
        if (_context.schema_change_recorder->schema_version() >
            _context.tablet_schema->schema_version()) {
            _context.tablet->update_max_version_schema(new_schema);
        }
        _rowset_meta->set_tablet_schema(new_schema);
    }

    RowsetSharedPtr rowset;
    status = RowsetFactory::create_rowset(_context.tablet_schema, _context.rowset_dir, _rowset_meta,
                                          &rowset);
    if (!status.ok()) {
        LOG(WARNING) << "rowset init failed when build new rowset, res=" << status;
        return nullptr;
    }
    _already_built = true;
    return rowset;
}

bool BetaRowsetWriter::_is_segment_overlapping(
        const std::vector<KeyBoundsPB>& segments_encoded_key_bounds) {
    std::string last;
    for (auto segment_encode_key : segments_encoded_key_bounds) {
        auto cur_min = segment_encode_key.min_key();
        auto cur_max = segment_encode_key.max_key();
        if (cur_min <= last) {
            return true;
        }
        last = cur_max;
    }
    return false;
}

void BetaRowsetWriter::_build_rowset_meta_with_spec_field(
        RowsetMetaSharedPtr rowset_meta, const RowsetMetaSharedPtr& spec_rowset_meta) {
    rowset_meta->set_num_rows(spec_rowset_meta->num_rows());
    rowset_meta->set_total_disk_size(spec_rowset_meta->total_disk_size());
    rowset_meta->set_data_disk_size(spec_rowset_meta->total_disk_size());
    rowset_meta->set_index_disk_size(spec_rowset_meta->index_disk_size());
    // TODO write zonemap to meta
    rowset_meta->set_empty(spec_rowset_meta->num_rows() == 0);
    rowset_meta->set_creation_time(time(nullptr));
    rowset_meta->set_num_segments(spec_rowset_meta->num_segments());
    rowset_meta->set_segments_overlap(spec_rowset_meta->segments_overlap());
    rowset_meta->set_rowset_state(spec_rowset_meta->rowset_state());

    std::vector<KeyBoundsPB> segments_key_bounds;
    spec_rowset_meta->get_segments_key_bounds(&segments_key_bounds);
    rowset_meta->set_segments_key_bounds(segments_key_bounds);
}

void BetaRowsetWriter::_build_rowset_meta(std::shared_ptr<RowsetMeta> rowset_meta) {
    int64_t num_seg = _is_segcompacted() ? _num_segcompacted : _num_segment;
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
        !_is_segment_overlapping(segments_encoded_key_bounds)) {
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
    }

    rowset_meta->set_num_segments(num_seg);
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

RowsetSharedPtr BetaRowsetWriter::_build_tmp() {
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

Status BetaRowsetWriter::_create_file_writer(std::string path, io::FileWriterPtr& file_writer) {
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::Error<INIT_FAILED>("get fs failed");
    }
    Status st = fs->create_file(path, &file_writer);
    if (!st.ok()) {
        LOG(WARNING) << "failed to create writable file. path=" << path << ", err: " << st;
        return st;
    }

    DCHECK(file_writer != nullptr);
    return Status::OK();
}

Status BetaRowsetWriter::create_file_writer(uint32_t segment_id, io::FileWriterPtr& file_writer) {
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

    writer->reset(new segment_v2::SegmentWriter(file_writer.get(), _num_segcompacted,
                                                _context.tablet_schema, _context.tablet,
                                                _context.data_dir, _context.max_rows_per_segment,
                                                writer_options, _context.mow_context));
    if (_segcompaction_worker.get_file_writer() != nullptr) {
        _segcompaction_worker.get_file_writer()->close();
    }
    _segcompaction_worker.get_file_writer().reset(file_writer.release());

    return Status::OK();
}

Status BetaRowsetWriter::_create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                                int32_t segment_id, bool no_compression,
                                                TabletSchemaSPtr flush_schema) {
    RETURN_IF_ERROR(_check_segment_number_limit());
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(create_file_writer(segment_id, file_writer));

    segment_v2::SegmentWriterOptions writer_options;
    writer_options.enable_unique_key_merge_on_write = _context.enable_unique_key_merge_on_write;
    writer_options.rowset_ctx = &_context;
    writer_options.write_type = _context.write_type;
    if (no_compression) {
        writer_options.compression_type = NO_COMPRESSION;
    }

    const auto& tablet_schema = flush_schema ? flush_schema : _context.tablet_schema;
    writer.reset(new segment_v2::SegmentWriter(
            file_writer.get(), segment_id, tablet_schema, _context.tablet, _context.data_dir,
            _context.max_rows_per_segment, writer_options, _context.mow_context));
    {
        std::lock_guard<SpinLock> l(_lock);
        _file_writers.push_back(std::move(file_writer));
    }
    auto s = writer->init();
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        writer.reset();
        return s;
    }
    return Status::OK();
}

Status BetaRowsetWriter::_check_segment_number_limit() {
    size_t total_segment_num = _num_segment - _segcompacted_point + 1 + _num_segcompacted;
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

Status BetaRowsetWriter::_flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                               int64_t* flush_size) {
    uint32_t segid = writer->get_segment_id();
    uint32_t row_num = writer->num_rows_written();

    if (writer->num_rows_written() == 0) {
        return Status::OK();
    }
    uint64_t segment_size;
    uint64_t index_size;
    Status s = writer->finalize(&segment_size, &index_size);
    if (!s.ok()) {
        return Status::Error(s.code(), "failed to finalize segment: {}", s.to_string());
    }
    VLOG_DEBUG << "tablet_id:" << _context.tablet_id
               << " flushing filename: " << writer->get_data_dir()->path()
               << " rowset_id:" << _context.rowset_id << " segment num:" << _num_segment;

    KeyBoundsPB key_bounds;
    Slice min_key = writer->min_encoded_key();
    Slice max_key = writer->max_encoded_key();
    DCHECK_LE(min_key.compare(max_key), 0);
    key_bounds.set_min_key(min_key.to_string());
    key_bounds.set_max_key(max_key.to_string());

    SegmentStatistics segstat;
    segstat.row_num = row_num;
    segstat.data_size = segment_size + writer->get_inverted_index_file_size();
    segstat.index_size = index_size + writer->get_inverted_index_file_size();
    segstat.key_bounds = key_bounds;

    writer.reset();
    if (flush_size) {
        *flush_size = segment_size + index_size;
    }

    add_segment(segid, segstat);
    return Status::OK();
}

void BetaRowsetWriter::add_segment(uint32_t segid, SegmentStatistics& segstat) {
    uint32_t segid_offset = segid - _segment_start_id;
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        CHECK_EQ(_segid_statistics_map.find(segid) == _segid_statistics_map.end(), true);
        _segid_statistics_map.emplace(segid, segstat);
        _segment_num_rows.resize(_next_segment_id);
        _segment_num_rows[segid_offset] = segstat.row_num;
    }
    VLOG_DEBUG << "_segid_statistics_map add new record. segid:" << segid
               << " row_num:" << segstat.row_num << " data_size:" << segstat.data_size
               << " index_size:" << segstat.index_size;

    {
        std::lock_guard<std::mutex> lock(_segment_set_mutex);
        _segment_set.add(segid_offset);
        while (_segment_set.contains(_num_segment)) {
            _num_segment++;
        }
    }
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

Status BetaRowsetWriter::_unfold_variant_column(vectorized::Block& block,
                                                TabletSchemaSPtr& flush_schema) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // Sanitize block to match exactly from the same type of frontend meta
    vectorized::schema_util::FullBaseSchemaView schema_view;
    schema_view.table_id = _context.tablet_schema->table_id();
    vectorized::ColumnWithTypeAndName* variant_column =
            block.try_get_by_name(BeConsts::DYNAMIC_COLUMN_NAME);
    if (!variant_column) {
        return Status::OK();
    }
    auto base_column = variant_column->column;
    vectorized::ColumnObject& object_column =
            assert_cast<vectorized::ColumnObject&>(base_column->assume_mutable_ref());
    if (object_column.empty()) {
        block.erase(BeConsts::DYNAMIC_COLUMN_NAME);
        return Status::OK();
    }
    object_column.finalize();
    // Has extended columns
    RETURN_IF_ERROR(vectorized::schema_util::send_fetch_full_base_schema_view_rpc(&schema_view));
    // Dynamic Block consists of two parts, dynamic part of columns and static part of columns
    //  static   dynamic
    // | ----- | ------- |
    // The static ones are original _tablet_schame columns
    flush_schema = std::make_shared<TabletSchema>(*_context.tablet_schema);
    vectorized::Block flush_block(std::move(block));
    // The dynamic ones are auto generated and extended, append them the the orig_block
    for (auto& entry : object_column.get_subcolumns()) {
        const std::string& column_name = entry->path.get_path();
        auto column_iter = schema_view.column_name_to_column.find(column_name);
        if (UNLIKELY(column_iter == schema_view.column_name_to_column.end())) {
            // Column maybe dropped by light weight schema change DDL
            continue;
        }
        TabletColumn column(column_iter->second);
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                column, column.is_nullable());
        // Dynamic generated columns does not appear in original tablet schema
        if (_context.tablet_schema->field_index(column.name()) < 0) {
            flush_schema->append_column(column);
            flush_block.insert({data_type->create_column(), data_type, column.name()});
        }
    }

    // Ensure column are all present at this schema version.Otherwise there will be some senario:
    //  Load1 -> version(10) with schema [a, b, c, d, e], d & e is new added columns and schema version became 10
    //  Load2 -> version(10) with schema [a, b, c] and has no extended columns and fetched the schema at version 10
    //  Load2 will persist meta with [a, b, c] but Load1 will persist meta with [a, b, c, d, e]
    // So we should make sure that rowset at the same schema version alawys contain the same size of columns.
    // so that all columns at schema_version is in either _context.tablet_schema or schema_change_recorder
    for (const auto& [name, column] : schema_view.column_name_to_column) {
        if (_context.tablet_schema->field_index(name) == -1) {
            const auto& tcolumn = schema_view.column_name_to_column[name];
            TabletColumn new_column(tcolumn);
            _context.schema_change_recorder->add_extended_columns(column,
                                                                  schema_view.schema_version);
        }
    }

    // Last schema alignment before flush to disk, due to the schema maybe variant before this procedure
    // Eg. add columnA(INT) -> drop ColumnA -> add ColumnA(Double), then columnA could be type of `Double`,
    // unfold will cast to Double type
    RETURN_IF_ERROR(vectorized::schema_util::unfold_object(
            flush_block.get_position_by_name(BeConsts::DYNAMIC_COLUMN_NAME), flush_block, true));
    flush_block.erase(BeConsts::DYNAMIC_COLUMN_NAME);
    block.swap(flush_block);
    return Status::OK();
}

} // namespace doris
