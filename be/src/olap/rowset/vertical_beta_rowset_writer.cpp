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

#include "olap/rowset/vertical_beta_rowset_writer.h"

#include "olap/rowset/beta_rowset.h"

namespace doris {

VerticalBetaRowsetWriter::~VerticalBetaRowsetWriter() {
    if (!_already_built) {
        auto fs = _rowset_meta->fs();
        if (!fs) {
            return;
        }
        for (auto& segment_writer : _segment_writers) {
            segment_writer.reset();
        }
        for (int i = 0; i < _num_segment; ++i) {
            auto path = BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, i);
            // Even if an error is encountered, these files that have not been cleaned up
            // will be cleaned up by the GC background. So here we only print the error
            // message when we encounter an error.
            WARN_IF_ERROR(fs->delete_file(path),
                          strings::Substitute("Failed to delete file=$0", path));
        }
    }
}

Status VerticalBetaRowsetWriter::add_columns(const vectorized::Block* block,
                                             const std::vector<uint32_t>& col_ids, bool is_key,
                                             uint32_t max_rows_per_segment) {
    VLOG_NOTICE << "VerticalBetaRowsetWriter::add_columns, columns: " << block->columns();
    size_t num_rows = block->rows();
    if (num_rows == 0) {
        return Status::OK();
    }
    if (UNLIKELY(max_rows_per_segment > _context.max_rows_per_segment)) {
        max_rows_per_segment = _context.max_rows_per_segment;
    }
    if (_segment_writers.empty()) {
        // it must be key columns
        DCHECK(is_key);
        std::unique_ptr<segment_v2::SegmentWriter> writer;
        RETURN_IF_ERROR(_create_segment_writer(col_ids, is_key, &writer));
        _segment_writers.emplace_back(std::move(writer));
        _cur_writer_idx = 0;
        RETURN_IF_ERROR(_segment_writers[_cur_writer_idx]->append_block(block, 0, num_rows));
    } else if (is_key) {
        if (_segment_writers[_cur_writer_idx]->num_rows_written() > max_rows_per_segment) {
            // segment is full, need flush columns and create new segment writer
            RETURN_IF_ERROR(_flush_columns(&_segment_writers[_cur_writer_idx], true));
            std::unique_ptr<segment_v2::SegmentWriter> writer;
            RETURN_IF_ERROR(_create_segment_writer(col_ids, is_key, &writer));
            _segment_writers.emplace_back(std::move(writer));
            ++_cur_writer_idx;
        }
        RETURN_IF_ERROR(_segment_writers[_cur_writer_idx]->append_block(block, 0, num_rows));
    } else {
        // value columns
        uint32_t num_rows_written = _segment_writers[_cur_writer_idx]->num_rows_written();
        VLOG_NOTICE << "num_rows_written: " << num_rows_written
                    << ", _cur_writer_idx: " << _cur_writer_idx;
        // init if it's first value column write in current segment
        if (_cur_writer_idx == 0 && num_rows_written == 0) {
            VLOG_NOTICE << "init first value column segment writer";
            RETURN_IF_ERROR(_segment_writers[_cur_writer_idx]->init(col_ids, is_key));
        }
        if (num_rows_written > max_rows_per_segment) {
            RETURN_IF_ERROR(_flush_columns(&_segment_writers[_cur_writer_idx]));
            // switch to next writer
            ++_cur_writer_idx;
            VLOG_NOTICE << "init next value column segment writer: " << _cur_writer_idx;
            RETURN_IF_ERROR(_segment_writers[_cur_writer_idx]->init(col_ids, is_key));
        }
        RETURN_IF_ERROR(_segment_writers[_cur_writer_idx]->append_block(block, 0, num_rows));
    }
    if (is_key) {
        _num_rows_written += num_rows;
    }
    return Status::OK();
}

Status VerticalBetaRowsetWriter::_flush_columns(
        std::unique_ptr<segment_v2::SegmentWriter>* segment_writer, bool is_key) {
    uint64_t index_size = 0;
    VLOG_NOTICE << "flush columns index: " << _cur_writer_idx;
    RETURN_IF_ERROR((*segment_writer)->finalize_columns(&index_size));
    if (is_key) {
        // record segment key bound
        KeyBoundsPB key_bounds;
        Slice min_key = (*segment_writer)->min_encoded_key();
        Slice max_key = (*segment_writer)->max_encoded_key();
        DCHECK_LE(min_key.compare(max_key), 0);
        key_bounds.set_min_key(min_key.to_string());
        key_bounds.set_max_key(max_key.to_string());
        _segments_encoded_key_bounds.emplace_back(key_bounds);
    }
    _total_index_size += static_cast<int64_t>(index_size);
    return Status::OK();
}

Status VerticalBetaRowsetWriter::flush_columns() {
    if (_segment_writers.empty()) {
        return Status::OK();
    }

    DCHECK(_segment_writers[_cur_writer_idx]);
    RETURN_IF_ERROR(_flush_columns(&_segment_writers[_cur_writer_idx]));
    _cur_writer_idx = 0;
    return Status::OK();
}

Status VerticalBetaRowsetWriter::_create_segment_writer(
        const std::vector<uint32_t>& column_ids, bool is_key,
        std::unique_ptr<segment_v2::SegmentWriter>* writer) {
    auto path =
            BetaRowset::segment_file_path(_context.rowset_dir, _context.rowset_id, _num_segment++);
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
    writer->reset(new segment_v2::SegmentWriter(file_writer.get(), _num_segment,
                                                _context.tablet_schema, _context.data_dir,
                                                _context.max_rows_per_segment, writer_options));
    {
        std::lock_guard<SpinLock> l(_lock);
        _file_writers.push_back(std::move(file_writer));
    }

    auto s = (*writer)->init(column_ids, is_key);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        writer->reset(nullptr);
        return s;
    }
    return Status::OK();
}

Status VerticalBetaRowsetWriter::final_flush() {
    for (auto& segment_writer : _segment_writers) {
        uint64_t segment_size = 0;
        //uint64_t footer_position = 0;
        auto st = segment_writer->finalize_footer(&segment_size);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to finalize segment footer, " << st;
            return st;
        }
        _total_data_size += segment_size;
        segment_writer.reset();
    }
    return Status::OK();
}

} // namespace doris
