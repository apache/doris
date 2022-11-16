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

#include <ctime> // time
#include <memory>
#include <sstream>

#include "common/config.h"
#include "common/logging.h"
#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_writer.h"
#include "olap/memtable.h"
#include "olap/olap_define.h"
#include "olap/row.h"        // ContiguousRow
#include "olap/row_cursor.h" // RowCursor
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/segment_writer.h"
//#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"

namespace doris {

class StorageEngine;

BetaRowsetWriter::BetaRowsetWriter()
        : _rowset_meta(nullptr),
          _num_segment(0),
          _segcompacted_point(0),
          _num_segcompacted(0),
          _segment_writer(nullptr),
          _num_rows_written(0),
          _total_data_size(0),
          _total_index_size(0),
          _raw_num_rows_written(0),
          _is_doing_segcompaction(false) {
    _segcompaction_status.store(OLAP_SUCCESS);
}

BetaRowsetWriter::~BetaRowsetWriter() {
    OLAP_UNUSED_ARG(_wait_flying_segcompaction());

    // TODO(lingbin): Should wrapper exception logic, no need to know file ops directly.
    if (!_already_built) {       // abnormal exit, remove all files generated
        _segment_writer.reset(); // ensure all files are closed
        auto fs = _rowset_meta->fs();
        if (!fs) {
            return;
        }
        for (int i = 0; i < _num_segment; ++i) {
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
    if (_context.fs == nullptr && _context.data_dir) {
        _rowset_meta->set_fs(_context.data_dir->fs());
    } else {
        _rowset_meta->set_fs(_context.fs);
    }
    if (_context.fs != nullptr && _context.fs->resource_id().size() > 0) {
        _rowset_meta->set_resource_id(_context.fs->resource_id());
    }
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
        _rowset_meta->set_oldest_write_timestamp(_context.oldest_write_timestamp);
        _rowset_meta->set_newest_write_timestamp(_context.newest_write_timestamp);
    }
    _rowset_meta->set_tablet_uid(_context.tablet_uid);
    _rowset_meta->set_tablet_schema(_context.tablet_schema);

    return Status::OK();
}

Status BetaRowsetWriter::add_block(const vectorized::Block* block) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    if (UNLIKELY(_segment_writer == nullptr)) {
        RETURN_NOT_OK(_create_segment_writer(&_segment_writer));
    }
    return _add_block(block, &_segment_writer);
}

vectorized::VMergeIterator* BetaRowsetWriter::_get_segcompaction_reader(
        SegCompactionCandidatesSharedPtr segments, std::shared_ptr<Schema> schema,
        OlapReaderStatistics* stat, uint64_t* merged_row_stat) {
    StorageReadOptions read_options;
    read_options.stats = stat;
    read_options.use_page_cache = false;
    read_options.tablet_schema = _context.tablet_schema;
    std::vector<std::unique_ptr<RowwiseIterator>> seg_iterators;
    for (auto& seg_ptr : *segments) {
        std::unique_ptr<RowwiseIterator> iter;
        auto s = seg_ptr->new_iterator(*schema, read_options, &iter);
        if (!s.ok()) {
            LOG(WARNING) << "failed to create iterator[" << seg_ptr->id() << "]: " << s.to_string();
            return nullptr;
        }
        seg_iterators.push_back(std::move(iter));
    }
    std::vector<RowwiseIterator*> iterators;
    for (auto& owned_it : seg_iterators) {
        // transfer ownership
        iterators.push_back(owned_it.release());
    }
    bool is_unique = (_context.tablet_schema->keys_type() == UNIQUE_KEYS);
    bool is_reverse = false;
    auto merge_itr =
            vectorized::new_merge_iterator(iterators, -1, is_unique, is_reverse, merged_row_stat);
    DCHECK(merge_itr);
    auto s = merge_itr->init(read_options);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init iterator: " << s.to_string();
        for (auto& itr : iterators) {
            delete itr;
        }
        return nullptr;
    }

    return (vectorized::VMergeIterator*)merge_itr;
}

std::unique_ptr<segment_v2::SegmentWriter> BetaRowsetWriter::_create_segcompaction_writer(
        uint64_t begin, uint64_t end) {
    Status status;
    std::unique_ptr<segment_v2::SegmentWriter> writer = nullptr;
    status = _create_segment_writer_for_segcompaction(&writer, begin, end);
    if (status != Status::OK() || writer == nullptr) {
        LOG(ERROR) << "failed to create segment writer for begin:" << begin << " end:" << end
                   << " path:" << writer->get_data_dir()->path() << " status:" << status;
        return nullptr;
    } else {
        return writer;
    }
}

Status BetaRowsetWriter::_delete_original_segments(uint32_t begin, uint32_t end) {
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
    }
    for (uint32_t i = begin; i <= end; ++i) {
        auto seg_path = BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, i);
        // Even if an error is encountered, these files that have not been cleaned up
        // will be cleaned up by the GC background. So here we only print the error
        // message when we encounter an error.
        RETURN_NOT_OK_LOG(fs->delete_file(seg_path),
                          strings::Substitute("Failed to delete file=$0", seg_path));
    }
    return Status::OK();
}

Status BetaRowsetWriter::_rename_compacted_segments(int64_t begin, int64_t end) {
    int ret;
    auto src_seg_path = BetaRowset::local_segment_path_segcompacted(_context.rowset_dir,
                                                                    _context.rowset_id, begin, end);
    auto dst_seg_path = BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id,
                                                      _num_segcompacted++);
    ret = rename(src_seg_path.c_str(), dst_seg_path.c_str());
    if (ret) {
        LOG(WARNING) << "failed to rename " << src_seg_path << " to " << dst_seg_path
                     << ". ret:" << ret << " errno:" << errno;
        return Status::OLAPInternalError(OLAP_ERR_ROWSET_RENAME_FILE_FAILED);
    }
    return Status::OK();
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
        Statistics org = _segid_statistics_map[seg_id];
        _segid_statistics_map.emplace(_num_segcompacted, org);
        _clear_statistics_for_deleting_segments_unsafe(seg_id, seg_id);
    }
    ++_num_segcompacted;
    ret = rename(src_seg_path.c_str(), dst_seg_path.c_str());
    if (ret) {
        LOG(WARNING) << "failed to rename " << src_seg_path << " to " << dst_seg_path
                     << ". ret:" << ret << " errno:" << errno;
        return Status::OLAPInternalError(OLAP_ERR_ROWSET_RENAME_FILE_FAILED);
    }
    return Status::OK();
}

void BetaRowsetWriter::_clear_statistics_for_deleting_segments_unsafe(uint64_t begin,
                                                                      uint64_t end) {
    VLOG_DEBUG << "_segid_statistics_map clear record segid range from:" << begin << " to:" << end;
    for (int i = begin; i <= end; ++i) {
        _segid_statistics_map.erase(i);
    }
}

Status BetaRowsetWriter::_check_correctness(std::unique_ptr<OlapReaderStatistics> stat,
                                            uint64_t merged_row_stat, uint64_t row_count,
                                            uint64_t begin, uint64_t end) {
    uint64_t stat_read_row = stat->raw_rows_read;
    uint64_t sum_target_row = 0;

    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        for (int i = begin; i <= end; ++i) {
            sum_target_row += _segid_statistics_map[i].row_num;
        }
    }

    if (sum_target_row != stat_read_row) {
        LOG(WARNING) << "read row_num does not match. expect read row:" << sum_target_row
                     << " actual read row:" << stat_read_row;
        return Status::OLAPInternalError(OLAP_ERR_CHECK_LINES_ERROR);
    }

    uint64_t total_row = row_count + merged_row_stat;
    if (stat_read_row != total_row) {
        LOG(WARNING) << "total row_num does not match. expect total row:" << total_row
                     << " actual total row:" << stat_read_row;
        return Status::OLAPInternalError(OLAP_ERR_CHECK_LINES_ERROR);
    }
    return Status::OK();
}

Status BetaRowsetWriter::_do_compact_segments(SegCompactionCandidatesSharedPtr segments) {
    SCOPED_CONSUME_MEM_TRACKER(StorageEngine::instance()->segcompaction_mem_tracker());
    // throttle segcompaction task if memory depleted.
    if (MemTrackerLimiter::sys_mem_exceed_limit_check(GB_EXCHANGE_BYTE)) {
        LOG(WARNING) << "skip segcompaction due to memory shortage";
        return Status::OLAPInternalError(OLAP_ERR_FETCH_MEMORY_EXCEEDED);
    }
    uint64_t begin = (*(segments->begin()))->id();
    uint64_t end = (*(segments->end() - 1))->id();
    uint64_t begin_time = GetCurrentTimeMicros();

    auto schema = std::make_shared<Schema>(_context.tablet_schema->columns(),
                                           _context.tablet_schema->columns().size());
    std::unique_ptr<OlapReaderStatistics> stat(new OlapReaderStatistics());
    uint64_t merged_row_stat = 0;
    vectorized::VMergeIterator* reader =
            _get_segcompaction_reader(segments, schema, stat.get(), &merged_row_stat);
    if (UNLIKELY(reader == nullptr)) {
        LOG(WARNING) << "failed to get segcompaction reader";
        return Status::OLAPInternalError(OLAP_ERR_SEGCOMPACTION_INIT_READER);
    }
    std::unique_ptr<vectorized::VMergeIterator> reader_ptr;
    reader_ptr.reset(reader);
    auto writer = _create_segcompaction_writer(begin, end);
    if (UNLIKELY(writer == nullptr)) {
        LOG(WARNING) << "failed to get segcompaction writer";
        return Status::OLAPInternalError(OLAP_ERR_SEGCOMPACTION_INIT_WRITER);
    }
    uint64_t row_count = 0;
    vectorized::Block block = _context.tablet_schema->create_block();
    while (true) {
        auto status = reader_ptr->next_batch(&block);
        row_count += block.rows();
        if (status != Status::OK()) {
            if (LIKELY(status.is_end_of_file())) {
                RETURN_NOT_OK_LOG(_add_block_for_segcompaction(&block, &writer),
                                  "write block failed");
                break;
            } else {
                LOG(WARNING) << "read block failed: " << status.to_string();
                return status;
            }
        }
        RETURN_NOT_OK_LOG(_add_block_for_segcompaction(&block, &writer), "write block failed");
        block.clear_column_data();
    }
    RETURN_NOT_OK_LOG(_check_correctness(std::move(stat), merged_row_stat, row_count, begin, end),
                      "check correctness failed");
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        _clear_statistics_for_deleting_segments_unsafe(begin, end);
    }
    RETURN_NOT_OK(_flush_segment_writer(&writer));

    if (_segcompaction_file_writer != nullptr) {
        _segcompaction_file_writer->close();
    }

    RETURN_NOT_OK(_delete_original_segments(begin, end));
    RETURN_NOT_OK(_rename_compacted_segments(begin, end));

    if (VLOG_DEBUG_IS_ON) {
        vlog_buffer.clear();
        for (const auto& entry : std::filesystem::directory_iterator(_context.rowset_dir)) {
            fmt::format_to(vlog_buffer, "[{}]", string(entry.path()));
        }
        VLOG_DEBUG << "tablet_id:" << _context.tablet_id << " rowset_id:" << _context.rowset_id
                   << "_segcompacted_point:" << _segcompacted_point
                   << " _num_segment:" << _num_segment << " _num_segcompacted:" << _num_segcompacted
                   << " list directory:" << fmt::to_string(vlog_buffer);
    }

    _segcompacted_point += (end - begin + 1);

    uint64_t elapsed = GetCurrentTimeMicros() - begin_time;
    LOG(INFO) << "segcompaction completed. tablet_id:" << _context.tablet_id
              << " rowset_id:" << _context.rowset_id << " elapsed time:" << elapsed
              << "us. update segcompacted_point:" << _segcompacted_point
              << " segment num:" << segments->size() << " begin:" << begin << " end:" << end;

    return Status::OK();
}

void BetaRowsetWriter::compact_segments(SegCompactionCandidatesSharedPtr segments) {
    Status status = _do_compact_segments(segments);
    if (!status.ok()) {
        int16_t errcode = status.precise_code();
        switch (errcode) {
        case OLAP_ERR_FETCH_MEMORY_EXCEEDED:
        case OLAP_ERR_SEGCOMPACTION_INIT_READER:
        case OLAP_ERR_SEGCOMPACTION_INIT_WRITER:
            LOG(WARNING) << "segcompaction failed, try next time:" << status;
            return;
        default:
            LOG(WARNING) << "segcompaction fatal, terminating the write job."
                         << " tablet_id:" << _context.tablet_id
                         << " rowset_id:" << _context.rowset_id << " status:" << status;
            // status will be checked by the next trigger of segcompaction or the final wait
            _segcompaction_status.store(OLAP_ERR_OTHER_ERROR);
        }
    }
    DCHECK_EQ(_is_doing_segcompaction, true);
    {
        std::lock_guard lk(_is_doing_segcompaction_lock);
        _is_doing_segcompaction = false;
        _segcompacting_cond.notify_all();
    }
}

Status BetaRowsetWriter::_load_noncompacted_segments(
        std::vector<segment_v2::SegmentSharedPtr>* segments) {
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
    }
    for (int seg_id = _segcompacted_point; seg_id < _num_segment; ++seg_id) {
        auto seg_path =
                BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, seg_id);
        auto cache_path =
                BetaRowset::segment_cache_path(_context.rowset_dir, _context.rowset_id, seg_id);
        std::shared_ptr<segment_v2::Segment> segment;
        auto s = segment_v2::Segment::open(fs, seg_path, cache_path, seg_id, rowset_id(),
                                           _context.tablet_schema, &segment);
        if (!s.ok()) {
            LOG(WARNING) << "failed to open segment. " << seg_path << ":" << s.to_string();
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_LOAD_FAILED);
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
    RETURN_NOT_OK(_load_noncompacted_segments(&all_segments));

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
                RETURN_NOT_OK(_rename_compacted_segment_plain(_segcompacted_point++));
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
        RETURN_NOT_OK(_rename_compacted_segment_plain(_segcompacted_point++));
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

Status BetaRowsetWriter::_get_segcompaction_candidates(SegCompactionCandidatesSharedPtr& segments,
                                                       bool is_last) {
    if (is_last) {
        VLOG_DEBUG << "segcompaction last few segments";
        // currently we only rename remaining segments to reduce wait time
        // so that transaction can be committed ASAP
        RETURN_NOT_OK(_load_noncompacted_segments(segments.get()));
        for (int i = 0; i < segments->size(); ++i) {
            RETURN_NOT_OK(_rename_compacted_segment_plain(_segcompacted_point++));
        }
        segments->clear();
    } else {
        RETURN_NOT_OK(_find_longest_consecutive_small_segment(segments));
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
    if (!config::enable_segcompaction || !config::enable_storage_vectorization ||
        !_check_and_set_is_doing_segcompaction()) {
        return status;
    }
    if (_segcompaction_status.load() != OLAP_SUCCESS) {
        status = Status::OLAPInternalError(OLAP_ERR_SEGCOMPACTION_FAILED);
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
    if (!config::enable_segcompaction || !config::enable_storage_vectorization) {
        return Status::OK();
    }
    if (_segcompaction_status.load() != OLAP_SUCCESS) {
        return Status::OLAPInternalError(OLAP_ERR_SEGCOMPACTION_FAILED);
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

Status BetaRowsetWriter::_add_block(const vectorized::Block* block,
                                    std::unique_ptr<segment_v2::SegmentWriter>* segment_writer) {
    size_t block_size_in_bytes = block->bytes();
    size_t block_row_num = block->rows();
    size_t row_avg_size_in_bytes = std::max((size_t)1, block_size_in_bytes / block_row_num);
    size_t row_offset = 0;

    do {
        auto max_row_add = (*segment_writer)->max_row_to_add(row_avg_size_in_bytes);
        if (UNLIKELY(max_row_add < 1)) {
            // no space for another signle row, need flush now
            RETURN_NOT_OK(_flush_segment_writer(segment_writer));
            RETURN_NOT_OK(_create_segment_writer(segment_writer));
            max_row_add = (*segment_writer)->max_row_to_add(row_avg_size_in_bytes);
            DCHECK(max_row_add > 0);
        }

        size_t input_row_num = std::min(block_row_num - row_offset, size_t(max_row_add));
        auto s = (*segment_writer)->append_block(block, row_offset, input_row_num);
        if (UNLIKELY(!s.ok())) {
            LOG(WARNING) << "failed to append block: " << s.to_string();
            return Status::OLAPInternalError(OLAP_ERR_WRITER_DATA_WRITE_ERROR);
        }
        row_offset += input_row_num;
    } while (row_offset < block_row_num);

    _raw_num_rows_written += block_row_num;
    return Status::OK();
}

Status BetaRowsetWriter::_add_block_for_segcompaction(
        const vectorized::Block* block,
        std::unique_ptr<segment_v2::SegmentWriter>* segment_writer) {
    auto s = (*segment_writer)->append_block(block, 0, block->rows());
    if (UNLIKELY(!s.ok())) {
        LOG(WARNING) << "failed to append block: " << s.to_string();
        return Status::OLAPInternalError(OLAP_ERR_WRITER_DATA_WRITE_ERROR);
    }
    return Status::OK();
}

template <typename RowType>
Status BetaRowsetWriter::_add_row(const RowType& row) {
    if (PREDICT_FALSE(_segment_writer == nullptr)) {
        RETURN_NOT_OK(_create_segment_writer(&_segment_writer));
    }
    // TODO update rowset zonemap
    auto s = _segment_writer->append_row(row);
    if (PREDICT_FALSE(!s.ok())) {
        LOG(WARNING) << "failed to append row: " << s.to_string();
        return Status::OLAPInternalError(OLAP_ERR_WRITER_DATA_WRITE_ERROR);
    }
    if (PREDICT_FALSE(_segment_writer->estimate_segment_size() >= MAX_SEGMENT_SIZE ||
                      _segment_writer->num_rows_written() >= _context.max_rows_per_segment)) {
        RETURN_NOT_OK(_flush_segment_writer(&_segment_writer));
    }
    ++_raw_num_rows_written;
    return Status::OK();
}

template Status BetaRowsetWriter::_add_row(const RowCursor& row);
template Status BetaRowsetWriter::_add_row(const ContiguousRow& row);

Status BetaRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    assert(rowset->rowset_meta()->rowset_type() == BETA_ROWSET);
    RETURN_NOT_OK(rowset->link_files_to(_context.rowset_dir, _context.rowset_id));
    _num_rows_written += rowset->num_rows();
    _total_data_size += rowset->rowset_meta()->data_disk_size();
    _total_index_size += rowset->rowset_meta()->index_disk_size();
    _num_segment += rowset->num_segments();
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
        RETURN_NOT_OK(_flush_segment_writer(&_segment_writer));
        RETURN_NOT_OK(_segcompaction_if_necessary());
    }
    return Status::OK();
}

Status BetaRowsetWriter::flush_single_memtable(MemTable* memtable, int64_t* flush_size) {
    int64_t size = 0;
    int64_t sum_size = 0;
    // Create segment writer for each memtable, so that
    // all memtables can be flushed in parallel.
    std::unique_ptr<segment_v2::SegmentWriter> writer;

    MemTable::Iterator it(memtable);
    for (it.seek_to_first(); it.valid(); it.next()) {
        if (PREDICT_FALSE(writer == nullptr)) {
            RETURN_NOT_OK(_segcompaction_if_necessary());
            RETURN_NOT_OK(_create_segment_writer(&writer));
        }
        ContiguousRow dst_row = it.get_current_row();
        auto s = writer->append_row(dst_row);
        if (PREDICT_FALSE(!s.ok())) {
            LOG(WARNING) << "failed to append row: " << s.to_string();
            return Status::OLAPInternalError(OLAP_ERR_WRITER_DATA_WRITE_ERROR);
        }

        if (PREDICT_FALSE(writer->estimate_segment_size() >= MAX_SEGMENT_SIZE ||
                          writer->num_rows_written() >= _context.max_rows_per_segment)) {
            auto s = _flush_segment_writer(&writer, &size);
            sum_size += size;
            if (OLAP_UNLIKELY(!s.ok())) {
                *flush_size = sum_size;
                return s;
            }
        }
    }

    if (writer != nullptr) {
        auto s = _flush_segment_writer(&writer, &size);
        sum_size += size;
        *flush_size = sum_size;
        if (OLAP_UNLIKELY(!s.ok())) {
            return s;
        }
    }

    return Status::OK();
}

Status BetaRowsetWriter::flush_single_memtable(const vectorized::Block* block) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    RETURN_NOT_OK(_segcompaction_if_necessary());
    std::unique_ptr<segment_v2::SegmentWriter> writer;
    RETURN_NOT_OK(_create_segment_writer(&writer));
    RETURN_NOT_OK(_add_block(block, &writer));
    RETURN_NOT_OK(_flush_segment_writer(&writer));
    return Status::OK();
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
    if (_segcompaction_status.load() != OLAP_SUCCESS) {
        return Status::OLAPInternalError(OLAP_ERR_SEGCOMPACTION_FAILED);
    }
    return Status::OK();
}

RowsetSharedPtr BetaRowsetWriter::manual_build(const RowsetMetaSharedPtr& spec_rowset_meta) {
    if (_rowset_meta->oldest_write_timestamp() == -1) {
        _rowset_meta->set_oldest_write_timestamp(UnixSeconds());
    }

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
    status = _wait_flying_segcompaction();
    if (!status.ok()) {
        LOG(WARNING) << "segcompaction failed when build new rowset 1st wait, res=" << status;
        return nullptr;
    }
    status = _segcompaction_ramaining_if_necessary();
    if (!status.ok()) {
        LOG(WARNING) << "segcompaction failed when build new rowset, res=" << status;
        return nullptr;
    }
    status = _wait_flying_segcompaction();
    if (!status.ok()) {
        LOG(WARNING) << "segcompaction failed when build new rowset 2nd wait, res=" << status;
        return nullptr;
    }

    if (_segcompaction_file_writer) {
        _segcompaction_file_writer->close();
    }
    // When building a rowset, we must ensure that the current _segment_writer has been
    // flushed, that is, the current _segment_writer is nullptr
    DCHECK(_segment_writer == nullptr) << "segment must be null when build rowset";
    _build_rowset_meta(_rowset_meta);

    if (_rowset_meta->oldest_write_timestamp() == -1) {
        _rowset_meta->set_oldest_write_timestamp(UnixSeconds());
    }

    if (_rowset_meta->newest_write_timestamp() == -1) {
        _rowset_meta->set_newest_write_timestamp(UnixSeconds());
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
        if (cur_min < last) {
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
    std::vector<uint32_t> segment_num_rows;
    std::vector<KeyBoundsPB> segments_encoded_key_bounds;
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        for (const auto& itr : _segid_statistics_map) {
            num_rows_written += itr.second.row_num;
            segment_num_rows.push_back(itr.second.row_num);
            total_data_size += itr.second.data_size;
            total_index_size += itr.second.index_size;
            segments_encoded_key_bounds.push_back(itr.second.key_bounds);
        }
    }
    for (auto itr = _segments_encoded_key_bounds.begin(); itr != _segments_encoded_key_bounds.end();
         ++itr) {
        segments_encoded_key_bounds.push_back(*itr);
    }
    if (!_is_segment_overlapping(segments_encoded_key_bounds)) {
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
    }

    rowset_meta->set_num_segments(num_seg);
    _segment_num_rows = segment_num_rows;
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

RowsetSharedPtr BetaRowsetWriter::build_tmp() {
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

Status BetaRowsetWriter::_do_create_segment_writer(
        std::unique_ptr<segment_v2::SegmentWriter>* writer, bool is_segcompaction, int64_t begin,
        int64_t end) {
    std::string path;
    int32_t segment_id = 0;
    if (is_segcompaction) {
        DCHECK(begin >= 0 && end >= 0);
        path = BetaRowset::local_segment_path_segcompacted(_context.rowset_dir, _context.rowset_id,
                                                           begin, end);
    } else {
        segment_id = _num_segment.fetch_add(1);
        path = BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, segment_id);
    }
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
    }
    io::FileWriterPtr file_writer;
    Status st = fs->create_file(path, &file_writer);
    if (!st.ok()) {
        LOG(WARNING) << "failed to create writable file. path=" << path
                     << ", err: " << st.get_error_msg();
        return st;
    }

    DCHECK(file_writer != nullptr);
    segment_v2::SegmentWriterOptions writer_options;
    writer_options.enable_unique_key_merge_on_write = _context.enable_unique_key_merge_on_write;

    if (is_segcompaction) {
        writer->reset(new segment_v2::SegmentWriter(file_writer.get(), _num_segcompacted,
                                                    _context.tablet_schema, _context.data_dir,
                                                    _context.max_rows_per_segment, writer_options));
        if (_segcompaction_file_writer != nullptr) {
            _segcompaction_file_writer->close();
        }
        _segcompaction_file_writer.reset(file_writer.release());
    } else {
        writer->reset(new segment_v2::SegmentWriter(file_writer.get(), segment_id,
                                                    _context.tablet_schema, _context.data_dir,
                                                    _context.max_rows_per_segment, writer_options));
        {
            std::lock_guard<SpinLock> l(_lock);
            _file_writers.push_back(std::move(file_writer));
        }
    }

    auto s = (*writer)->init();
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        writer->reset(nullptr);
        return s;
    }
    return Status::OK();
}

Status BetaRowsetWriter::_create_segment_writer(
        std::unique_ptr<segment_v2::SegmentWriter>* writer) {
    size_t total_segment_num = _num_segment - _segcompacted_point + 1 + _num_segcompacted;
    if (UNLIKELY(total_segment_num > config::max_segment_num_per_rowset)) {
        LOG(ERROR) << "too many segments in rowset."
                   << " tablet_id:" << _context.tablet_id << " rowset_id:" << _context.rowset_id
                   << " max:" << config::max_segment_num_per_rowset
                   << " _num_segment:" << _num_segment
                   << " _segcompacted_point:" << _segcompacted_point
                   << " _num_segcompacted:" << _num_segcompacted;
        return Status::OLAPInternalError(OLAP_ERR_TOO_MANY_SEGMENTS);
    } else {
        return _do_create_segment_writer(writer, false, -1, -1);
    }
}

Status BetaRowsetWriter::_create_segment_writer_for_segcompaction(
        std::unique_ptr<segment_v2::SegmentWriter>* writer, uint64_t begin, uint64_t end) {
    return _do_create_segment_writer(writer, true, begin, end);
}

Status BetaRowsetWriter::_flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer,
                                               int64_t* flush_size) {
    uint32_t segid = (*writer)->get_segment_id();
    uint32_t row_num = (*writer)->num_rows_written();

    if ((*writer)->num_rows_written() == 0) {
        return Status::OK();
    }
    uint64_t segment_size;
    uint64_t index_size;
    Status s = (*writer)->finalize(&segment_size, &index_size);
    if (!s.ok()) {
        LOG(WARNING) << "failed to finalize segment: " << s.to_string();
        return Status::OLAPInternalError(OLAP_ERR_WRITER_DATA_WRITE_ERROR);
    }
    KeyBoundsPB key_bounds;
    Slice min_key = (*writer)->min_encoded_key();
    Slice max_key = (*writer)->max_encoded_key();
    DCHECK_LE(min_key.compare(max_key), 0);
    key_bounds.set_min_key(min_key.to_string());
    key_bounds.set_max_key(max_key.to_string());

    Statistics segstat;
    segstat.row_num = row_num;
    segstat.data_size = segment_size;
    segstat.index_size = index_size;
    segstat.key_bounds = key_bounds;
    {
        std::lock_guard<std::mutex> lock(_segid_statistics_map_mutex);
        CHECK_EQ(_segid_statistics_map.find(segid) == _segid_statistics_map.end(), true);
        _segid_statistics_map.emplace(segid, segstat);
    }
    VLOG_DEBUG << "_segid_statistics_map add new record. segid:" << segid << " row_num:" << row_num
               << " data_size:" << segment_size << " index_size:" << index_size;

    writer->reset();
    if (flush_size) {
        *flush_size = segment_size + index_size;
    }
    return Status::OK();
}

} // namespace doris
