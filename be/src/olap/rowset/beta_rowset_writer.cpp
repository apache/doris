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
          _is_doing_segcompaction(false) {}

BetaRowsetWriter::~BetaRowsetWriter() {
    // TODO(lingbin): Should wrapper exception logic, no need to know file ops directly.
    if (!_already_built) {       // abnormal exit, remove all files generated
        _segment_writer.reset(); // ensure all files are closed
        auto fs = _rowset_meta->fs();
        if (!fs) {
            return;
        }
        // TODO(zhangzhengyu): segment compaction handling
        for (int i = 0; i < _num_segment; ++i) {
            auto seg_path =
                    BetaRowset::local_segment_path(_context.tablet_path, _context.rowset_id, i);
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
    if (_context.data_dir) {
        _rowset_meta->set_fs(_context.data_dir->fs());
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


#if 0
    std::vector<std::unique_ptr<RowwiseIterator>> seg_iterators;
    for (auto& seg_ptr : _segment_cache_handle.get_segments()) {
        std::unique_ptr<RowwiseIterator> iter;
        auto s = seg_ptr->new_iterator(*_input_schema, read_options, &iter);
        if (!s.ok()) {
            LOG(WARNING) << "failed to create iterator[" << seg_ptr->id() << "]: " << s.to_string();
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_READER_INIT);
        }
        seg_iterators.push_back(std::move(iter));
    }

    std::vector<RowwiseIterator*> iterators;
    for (auto& owned_it : seg_iterators) {
        // transfer ownership of segment iterator to `_iterator`
        iterators.push_back(owned_it.release());
    }

    // merge or union segment iterator
    RowwiseIterator* final_iterator;
    if (config::enable_storage_vectorization && read_context.is_vec) {
        if (read_context.need_ordered_result &&
            _rowset->rowset_meta()->is_segments_overlapping()) {
            final_iterator = vectorized::new_merge_iterator(
                    iterators, read_context.sequence_id_idx, read_context.is_unique,
                    read_context.read_orderby_key_reverse, read_context.merged_rows);
        }
    }
#endif
#if 0
    auto rowset = build_tmp();
    auto beta_rowset = reinterpret_cast<BetaRowset*>(rowset.get());

    RowsetReaderSharedPtr rowset_reader;
    auto s = beta_rowset->create_reader(&rowset_reader);
    EXPECT_EQ(Status::OK(), s);

    auto tablet_schema = std::make_shared<Schema>(_context.tablet_schema->columns(), _context.tablet_schema->columns().size());
    OlapReaderStatistics stats;
    RowsetReaderContext reader_context;
    reader_context.tablet_schema = tablet_schema;
    reader_context.need_ordered_result = true;
    reader_context.return_columns = &_context.tablet_schema->columns();
    reader_context.stats = &stats;

    s = rowset_reader->init(&reader_context);
    EXPECT_EQ(Status::OK(), s);

    return rowset_reader;
#endif

vectorized::VMergeIterator* BetaRowsetWriter::get_segcompaction_reader(SegCompactionCandidatesSharedPtr segments, std::shared_ptr<Schema> schema, OlapReaderStatistics* stat) {
    StorageReadOptions read_options;
    read_options.stats = stat;
    read_options.tablet_schema = _context.tablet_schema;
    std::vector<std::unique_ptr<RowwiseIterator>> seg_iterators;
    for (auto& seg_ptr : *segments) {
        std::unique_ptr<RowwiseIterator> iter;
        auto s = seg_ptr->new_iterator(*schema, read_options, &iter);
        if (!s.ok()) {
            LOG(WARNING) << "failed to create iterator[" << seg_ptr->id() << "]: " << s.to_string();
            //return Status::OLAPInternalError(OLAP_ERR_ROWSET_READER_INIT);
        }
        seg_iterators.push_back(std::move(iter));
    }
    std::vector<RowwiseIterator*> iterators;
    for (auto& owned_it : seg_iterators) {
        // transfer ownership
        iterators.push_back(owned_it.release());
    }
    bool is_unique = (_context.tablet_schema->keys_type() == UNIQUE_KEYS);
    bool is_reverse = false; // TODO: wtf?
    auto merge_itr = vectorized::new_merge_iterator(iterators, -1, is_unique, is_reverse, nullptr);
    merge_itr->init(read_options);

    return (vectorized::VMergeIterator*)merge_itr;

}

std::unique_ptr<segment_v2::SegmentWriter> BetaRowsetWriter::create_segcompaction_writer(
        uint64_t begin, uint64_t end) {
    Status status;
    std::unique_ptr<segment_v2::SegmentWriter> writer = nullptr;
    status = _create_segment_writer_for_segcompaction(&writer, begin, end);
    if (status != Status::OK()) {
        writer = nullptr;
        LOG(ERROR) << "failed to create segment writer for begin:" << begin << " end:" << end << " path:" << writer->get_data_dir()->path();
    }
    if (writer->get_data_dir())
        LOG(INFO) << "segcompaction segment writer created for begin:" << begin << " end:" << end << " path:" << writer->get_data_dir()->path();
    return writer;
}

Status  BetaRowsetWriter::delete_original_segments(uint32_t begin, uint32_t end) {
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
    }
    for (uint32_t i = begin; i <= end; ++i) {
        auto seg_path =
                BetaRowset::local_segment_path(_context.tablet_path, _context.rowset_id, i);
        // Even if an error is encountered, these files that have not been cleaned up
        // will be cleaned up by the GC background. So here we only print the error
        // message when we encounter an error.
        WARN_IF_ERROR(fs->delete_file(seg_path),
                      strings::Substitute("Failed to delete file=$0", seg_path));
    }
    return Status::OK();
}

void BetaRowsetWriter::rename_compacted_segments(int64_t begin, int64_t end) {
    int ret;
    auto src_seg_path =
            BetaRowset::local_segment_path_segcompacted(_context.tablet_path, _context.rowset_id, begin, end);
    auto dst_seg_path =
            BetaRowset::local_segment_path(_context.tablet_path, _context.rowset_id, _num_segcompacted++);
    ret = rename(src_seg_path.c_str(), dst_seg_path.c_str());
    DCHECK_EQ(ret, 0);
}

// todo: will rename only do the job? maybe need deep modification
void BetaRowsetWriter::rename_compacted_segment_plain(uint64_t seg_id) {
    int ret;
    auto src_seg_path =
            BetaRowset::local_segment_path(_context.tablet_path, _context.rowset_id, seg_id);
    auto dst_seg_path =
            BetaRowset::local_segment_path(_context.tablet_path, _context.rowset_id, _num_segcompacted++);
    LOG(INFO) << "segcompaction skip this segment. rename " << src_seg_path << " to " << dst_seg_path;
    if (src_seg_path.compare(dst_seg_path) != 0) {
        ret = rename(src_seg_path.c_str(), dst_seg_path.c_str());
        DCHECK_EQ(ret, 0);
    }
}

Status BetaRowsetWriter::do_segcompaction(SegCompactionCandidatesSharedPtr segments) {
    uint64_t begin = (*(segments->begin()))->id();
    uint64_t end =  (*(segments->end()-1))->id();
    LOG(INFO) << "BetaRowsetWriter:" << this << " do segcompaction at "
              << segments->size() << " segments. Begin:" << begin << " End:" << end;

    auto schema = std::make_shared<Schema>(_context.tablet_schema->columns(),
                                           _context.tablet_schema->columns().size());
    std::unique_ptr<OlapReaderStatistics> stat(new OlapReaderStatistics());
    vectorized::VMergeIterator* reader = get_segcompaction_reader(segments, schema, stat.get());
    auto writer = create_segcompaction_writer(begin, end);
    assert(writer != nullptr);
    vectorized::Block block = _context.tablet_schema->create_block();
    while (true) {
        auto status = reader->next_batch(&block);
        if (status != Status::OK()) {
            assert(status.is_end_of_file());
            break;
        }
        _add_block(&block, &writer);
        block.clear_column_data();
    }
    RETURN_NOT_OK(_flush_segment_writer(&writer));
    delete_original_segments(begin, end);
    rename_compacted_segments(begin, end);

    std::stringstream ss;
    for (const auto & entry : std::filesystem::directory_iterator(_context.tablet_path)) {
        ss  << "[" << entry.path() << "]";
    }
    LOG(INFO) << "_segcompacted_point:" << _segcompacted_point
              << " _num_segment:" << _num_segment
              << " _num_segcompacted:" << _num_segcompacted
              << " list directory:" << ss.str();
    CHECK_EQ(_is_doing_segcompaction, true);
    _is_doing_segcompaction = false;
    _segcompacting_cond.notify_all();

    return Status::OK();
}

Status BetaRowsetWriter::load_noncompacted_segments(std::vector<segment_v2::SegmentSharedPtr>* segments)
{
    auto fs = _rowset_meta->fs();
    if (!fs) {
        return Status::OLAPInternalError(OLAP_ERR_INIT_FAILED);
    }
    for (int seg_id = _segcompacted_point; seg_id < _num_segment; ++seg_id) {
        auto seg_path = BetaRowset::local_segment_path(_context.tablet_path, _context.rowset_id, seg_id);
        auto cache_path = BetaRowset::local_cache_path(_context.tablet_path, _context.rowset_id, seg_id);
        std::shared_ptr<segment_v2::Segment> segment;
        auto s = segment_v2::Segment::open(fs, seg_path, cache_path, seg_id, _context.tablet_schema, &segment);
        if (!s.ok()) {
            LOG(WARNING) << "failed to open segment. " << seg_path << ":" << s.to_string();
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_LOAD_FAILED);
        }
        segments->push_back(std::move(segment));
    }
    return Status::OK();
}

void BetaRowsetWriter::find_longest_consecutive_small_segment(SegCompactionCandidatesSharedPtr segments) {
    std::vector<segment_v2::SegmentSharedPtr> all_segments;
    load_noncompacted_segments(&all_segments);

    std::stringstream ss_all;
    for (auto& segment : all_segments) {
        ss_all << "[id:" << segment->id() << " num_rows:" << segment->num_rows() << "]";
    }
    LOG(INFO) << "all noncompacted segments num:" << all_segments.size()
              << " list of segments:" << ss_all.str();

    bool is_terminated_by_big = false;
    bool let_big_terminate = false;
    size_t small_threshold = config::segcompaction_small_threshold;
    for (int64_t i = 0; i < all_segments.size(); ++i)
    {
        segment_v2::SegmentSharedPtr seg = all_segments[i];
        if (seg->num_rows() > small_threshold) {
            if (let_big_terminate) {
                is_terminated_by_big = true;
                break;
            } else {
                rename_compacted_segment_plain(_segcompacted_point);
                ++_segcompacted_point;
            }
        } else {
            let_big_terminate = true; // break if find a big after small
            segments->push_back(seg);
            ++_segcompacted_point;
        }
    }
    size_t s = segments->size();
    if (!is_terminated_by_big && s <= (config::segcompaction_threshold_segment_num / 2)) {
        // start with big segments and end with small, better to do it in next
        // round to compact more at once
        _segcompacted_point -= s;
        segments->clear();
        LOG(INFO) << "candidate segments num too small:" << s;
        return;
    }
    if (s == 1) { // poor bachelor, let it go
        LOG(INFO) << "only one candidate segment";
        rename_compacted_segment_plain(_segcompacted_point-1);
        segments->clear();
        return;
    }
    std::stringstream ss;
    for (auto& segment : (*segments.get())) {
        ss << "[id:" << segment->id() << " num_rows:" << segment->num_rows() << "]";
    }
    LOG(INFO) << "candidate segments num:" << s << " list of candidates:" << ss.str();
}

SegCompactionCandidatesSharedPtr BetaRowsetWriter::get_segcompaction_candidates(bool is_last) {
    SegCompactionCandidatesSharedPtr segments = std::make_shared<SegCompactionCandidates>();
    if (is_last) {
        load_noncompacted_segments(segments.get());
        if (segments->size() == 1) {
            LOG(INFO) << "only one last candidate segment";
            rename_compacted_segment_plain(_segcompacted_point);
            segments->clear();
        }
    } else {
        find_longest_consecutive_small_segment(segments);
    }
    return segments;
}

void BetaRowsetWriter::segcompaction_if_necessary() {
    if (!config::enable_segcompaction) {
        return;
    }
    if (!_is_doing_segcompaction && ((_num_segment - _segcompacted_point) >= config::segcompaction_threshold_segment_num))
    {
        _is_doing_segcompaction = true;
        SegCompactionCandidatesSharedPtr segments = get_segcompaction_candidates(false);
        if (segments->size() > 0) {
            LOG(INFO) << "submit segcompaction task, segment num:" << _num_segment
                      << ", segcompacted_point:" << _segcompacted_point;
            StorageEngine::instance()->submit_seg_compaction_task(this, segments);
        } else {
            _is_doing_segcompaction = false;
        }
    }
}

void BetaRowsetWriter::segcompaction_ramaining_if_necessary() {
    if (!config::enable_segcompaction) {
        return;
    }
    if (_num_segcompacted == 0 || _segcompacted_point == _num_segment) {
        // no need if never segcompact before or all segcompacted
        return;
    }
    CHECK_EQ(_is_doing_segcompaction, false);
    _is_doing_segcompaction = true;
    SegCompactionCandidatesSharedPtr segments = get_segcompaction_candidates(true);
    if (segments->size() > 0) {
        LOG(INFO) << "submit segcompaction remaining task, segment num:" << _num_segment
                  << ", segcompacted_point:" << _segcompacted_point;
        StorageEngine::instance()->submit_seg_compaction_task(this, segments);
    } else {
        _is_doing_segcompaction = false;
    }
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

    _num_rows_written += block_row_num;
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
    ++_num_rows_written;
    return Status::OK();
}

template Status BetaRowsetWriter::_add_row(const RowCursor& row);
template Status BetaRowsetWriter::_add_row(const ContiguousRow& row);

Status BetaRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    assert(rowset->rowset_meta()->rowset_type() == BETA_ROWSET);
    RETURN_NOT_OK(rowset->link_files_to(_context.tablet_path, _context.rowset_id));
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
        segcompaction_if_necessary();
    }
    return Status::OK();
}

Status BetaRowsetWriter::flush_single_memtable(MemTable* memtable, int64_t* flush_size) {
    int64_t current_flush_size = _total_data_size + _total_index_size;
    // Create segment writer for each memtable, so that
    // all memtables can be flushed in parallel.
    std::unique_ptr<segment_v2::SegmentWriter> writer;

    MemTable::Iterator it(memtable);
    for (it.seek_to_first(); it.valid(); it.next()) {
        if (PREDICT_FALSE(writer == nullptr)) {
            segcompaction_if_necessary();
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
            RETURN_NOT_OK(_flush_segment_writer(&writer));
        }
        ++_num_rows_written;
    }

    if (writer != nullptr) {
        RETURN_NOT_OK(_flush_segment_writer(&writer));
    }

    *flush_size = (_total_data_size + _total_index_size) - current_flush_size;
    return Status::OK();
}

Status BetaRowsetWriter::flush_single_memtable(const vectorized::Block* block) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    segcompaction_if_necessary();
    std::unique_ptr<segment_v2::SegmentWriter> writer;
    RETURN_NOT_OK(_create_segment_writer(&writer));
    RETURN_NOT_OK(_add_block(block, &writer));
    RETURN_NOT_OK(_flush_segment_writer(&writer));
    return Status::OK();
}

void BetaRowsetWriter::wait_flying_segcompaction() {
    while (_is_doing_segcompaction) { // TODO: memory barrier?
        // change sync wait to async?
        std::unique_lock<std::mutex> l(_segcompacting_cond_lock);
        _segcompacting_cond.wait(l);
    }
}

RowsetSharedPtr BetaRowsetWriter::build() {
    if (_file_writer) {
        _file_writer->close();
    }
    wait_flying_segcompaction();
    segcompaction_ramaining_if_necessary();
    wait_flying_segcompaction();

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
    auto status = RowsetFactory::create_rowset(_context.tablet_schema, _context.tablet_path,
                                               _rowset_meta, &rowset);
    if (!status.ok()) {
        LOG(WARNING) << "rowset init failed when build new rowset, res=" << status;
        return nullptr;
    }
    _already_built = true;
    return rowset;
}

void BetaRowsetWriter::_build_rowset_meta(std::shared_ptr<RowsetMeta> rowset_meta) {
    rowset_meta->set_num_rows(_num_rows_written);
    rowset_meta->set_total_disk_size(_total_data_size);
    rowset_meta->set_data_disk_size(_total_data_size);
    rowset_meta->set_index_disk_size(_total_index_size);
    // TODO write zonemap to meta
    rowset_meta->set_empty(_num_rows_written == 0);
    rowset_meta->set_creation_time(time(nullptr));
    rowset_meta->set_num_segments(_num_segment);
    if (_num_segment <= 1) {
        rowset_meta->set_segments_overlap(NONOVERLAPPING);
    }
    if (_is_pending) {
        rowset_meta->set_rowset_state(COMMITTED);
    } else {
        rowset_meta->set_rowset_state(VISIBLE);
    }
    rowset_meta->set_segments_key_bounds(_segments_encoded_key_bounds);
}

RowsetSharedPtr BetaRowsetWriter::build_tmp() {
    std::shared_ptr<RowsetMeta> rowset_meta_ = std::make_shared<RowsetMeta>();
    *rowset_meta_ = *_rowset_meta;
    _build_rowset_meta(rowset_meta_);

    RowsetSharedPtr rowset;
    auto status = RowsetFactory::create_rowset(_context.tablet_schema, _context.tablet_path,
                                               rowset_meta_, &rowset);
    if (!status.ok()) {
        LOG(WARNING) << "rowset init failed when build new rowset, res=" << status;
        return nullptr;
    }
    return rowset;
}

Status BetaRowsetWriter::_do_create_segment_writer(
        std::unique_ptr<segment_v2::SegmentWriter>* writer, bool is_segcompaction,
        int64_t begin, int64_t end) {
    std::string path;
    if (is_segcompaction) {
        assert(begin >= 0 && end >= 0);
        path = BetaRowset::local_segment_path_segcompacted(_context.tablet_path, _context.rowset_id,
                                               begin, end);
    } else {
        path = BetaRowset::local_segment_path(_context.tablet_path, _context.rowset_id,
                                               _num_segment++);
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
        writer->reset(new segment_v2::SegmentWriter(file_writer.get(), _num_segcompacted + 1, //TODO: +1 or not?
                                                _context.tablet_schema, _context.data_dir,
                                                _context.max_rows_per_segment, writer_options));
        if (_segcompaction_file_writer != nullptr) {
            _segcompaction_file_writer->close();
        }
        _segcompaction_file_writer.reset(file_writer.release());
    } else {
        writer->reset(new segment_v2::SegmentWriter(file_writer.get(), _num_segment,
                                                _context.tablet_schema, _context.data_dir,
                                                _context.max_rows_per_segment, writer_options));
        if (_file_writer != nullptr) {
            _file_writer->close();
        }
        _file_writer.reset(file_writer.release());
    }

    auto s = (*writer)->init(config::push_write_mbytes_per_sec);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        writer->reset(nullptr);
        return s;
    }
    return Status::OK();
}

Status BetaRowsetWriter::_create_segment_writer(
        std::unique_ptr<segment_v2::SegmentWriter>* writer) {
    return _do_create_segment_writer(writer, false, -1, -1);
}

Status BetaRowsetWriter::_create_segment_writer_for_segcompaction(
        std::unique_ptr<segment_v2::SegmentWriter>* writer,
        uint64_t begin, uint64_t end) {
    return _do_create_segment_writer(writer, true, begin, end);
}

Status BetaRowsetWriter::_flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>* writer) {
    _segment_num_rows.push_back((*writer)->num_rows_written());
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
    _total_data_size += segment_size;
    _total_index_size += index_size;
    KeyBoundsPB key_bounds;
    Slice min_key = (*writer)->min_encoded_key();
    Slice max_key = (*writer)->max_encoded_key();
    DCHECK_LE(min_key.compare(max_key), 0);
    key_bounds.set_min_key(min_key.to_string());
    key_bounds.set_max_key(max_key.to_string());
    _segments_encoded_key_bounds.emplace_back(key_bounds);
    writer->reset();
    return Status::OK();
}

} // namespace doris
