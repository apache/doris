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

#include "olap/rowset/segment_creator.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep

#include <filesystem>
#include <sstream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "io/fs/file_writer.h"
#include "olap/rowset/beta_rowset_writer.h" // SegmentStatistics
#include "olap/rowset/segment_v2/segment_writer.h"
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

SegmentFlusher::SegmentFlusher() = default;

SegmentFlusher::~SegmentFlusher() = default;

Status SegmentFlusher::init(const RowsetWriterContext& rowset_writer_context) {
    _context = rowset_writer_context;
    return Status::OK();
}

Status SegmentFlusher::flush_single_block(const vectorized::Block* block, int32_t segment_id,
                                          int64_t* flush_size, TabletSchemaSPtr flush_schema) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    std::unique_ptr<segment_v2::SegmentWriter> writer;
    bool no_compression = block->bytes() <= config::segment_compression_threshold_kb * 1024;
    RETURN_IF_ERROR(_create_segment_writer(writer, segment_id, no_compression, flush_schema));
    RETURN_IF_ERROR(_add_rows(writer, block, 0, block->rows()));
    RETURN_IF_ERROR(_flush_segment_writer(writer, flush_size));
    return Status::OK();
}

Status SegmentFlusher::close() {
    std::lock_guard<SpinLock> l(_lock);
    for (auto& file_writer : _file_writers) {
        Status status = file_writer->close();
        if (!status.ok()) {
            LOG(WARNING) << "failed to close file writer, path=" << file_writer->path()
                         << " res=" << status;
            return status;
        }
    }
    return Status::OK();
}

Status SegmentFlusher::_add_rows(std::unique_ptr<segment_v2::SegmentWriter>& segment_writer,
                                 const vectorized::Block* block, size_t row_offset,
                                 size_t row_num) {
    auto s = segment_writer->append_block(block, row_offset, row_num);
    if (UNLIKELY(!s.ok())) {
        return Status::Error<WRITER_DATA_WRITE_ERROR>("failed to append block: {}", s.to_string());
    }
    _num_rows_written += row_num;
    return Status::OK();
}

Status SegmentFlusher::_create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                              int32_t segment_id, bool no_compression,
                                              TabletSchemaSPtr flush_schema) {
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(_context.file_writer_creator->create(segment_id, file_writer));

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

Status SegmentFlusher::_flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                             int64_t* flush_size) {
    uint32_t row_num = writer->num_rows_written();
    _num_rows_filtered += writer->num_rows_filtered();

    if (row_num == 0) {
        return Status::OK();
    }
    uint64_t segment_size;
    uint64_t index_size;
    Status s = writer->finalize(&segment_size, &index_size);
    if (!s.ok()) {
        return Status::Error(s.code(), "failed to finalize segment: {}", s.to_string());
    }
    VLOG_DEBUG << "tablet_id:" << _context.tablet_id
               << " flushing rowset_dir: " << _context.rowset_dir
               << " rowset_id:" << _context.rowset_id;

    KeyBoundsPB key_bounds;
    Slice min_key = writer->min_encoded_key();
    Slice max_key = writer->max_encoded_key();
    DCHECK_LE(min_key.compare(max_key), 0);
    key_bounds.set_min_key(min_key.to_string());
    key_bounds.set_max_key(max_key.to_string());

    uint32_t segment_id = writer->get_segment_id();
    SegmentStatistics segstat;
    segstat.row_num = row_num;
    segstat.data_size = segment_size + writer->get_inverted_index_file_size();
    segstat.index_size = index_size + writer->get_inverted_index_file_size();
    segstat.key_bounds = key_bounds;

    if (!_indicator_maps) {
        _indicator_maps.reset(new IndicatorMaps);
    }
    auto indicator_maps = writer->get_indicator_maps();
    if (indicator_maps) {
        _indicator_maps->merge(*indicator_maps);
    }

    writer.reset();

    RETURN_IF_ERROR(_context.segment_collector->add(segment_id, segstat));

    if (flush_size) {
        *flush_size = segment_size + index_size;
    }
    return Status::OK();
}

Status SegmentFlusher::create_writer(std::unique_ptr<SegmentFlusher::Writer>& writer,
                                     uint32_t segment_id) {
    std::unique_ptr<segment_v2::SegmentWriter> segment_writer;
    RETURN_IF_ERROR(_create_segment_writer(segment_writer, segment_id));
    DCHECK(segment_writer != nullptr);
    writer.reset(new SegmentFlusher::Writer(this, segment_writer));
    return Status::OK();
}

SegmentFlusher::Writer::Writer(SegmentFlusher* flusher,
                               std::unique_ptr<segment_v2::SegmentWriter>& segment_writer)
        : _flusher(flusher), _writer(std::move(segment_writer)) {};

SegmentFlusher::Writer::~Writer() = default;

Status SegmentFlusher::Writer::flush() {
    return _flusher->_flush_segment_writer(_writer);
}

int64_t SegmentFlusher::Writer::max_row_to_add(size_t row_avg_size_in_bytes) {
    return _writer->max_row_to_add(row_avg_size_in_bytes);
}

Status SegmentCreator::init(const RowsetWriterContext& rowset_writer_context) {
    static_cast<void>(_segment_flusher.init(rowset_writer_context));
    return Status::OK();
}

Status SegmentCreator::add_block(const vectorized::Block* block) {
    if (block->rows() == 0) {
        return Status::OK();
    }

    size_t block_size_in_bytes = block->bytes();
    size_t block_row_num = block->rows();
    size_t row_avg_size_in_bytes = std::max((size_t)1, block_size_in_bytes / block_row_num);
    size_t row_offset = 0;

    if (_flush_writer == nullptr) {
        RETURN_IF_ERROR(_segment_flusher.create_writer(_flush_writer, allocate_segment_id()));
    }

    do {
        auto max_row_add = _flush_writer->max_row_to_add(row_avg_size_in_bytes);
        if (UNLIKELY(max_row_add < 1)) {
            // no space for another single row, need flush now
            RETURN_IF_ERROR(flush());
            RETURN_IF_ERROR(_segment_flusher.create_writer(_flush_writer, allocate_segment_id()));
            max_row_add = _flush_writer->max_row_to_add(row_avg_size_in_bytes);
            DCHECK(max_row_add > 0);
        }
        size_t input_row_num = std::min(block_row_num - row_offset, size_t(max_row_add));
        RETURN_IF_ERROR(_flush_writer->add_rows(block, row_offset, input_row_num));
        row_offset += input_row_num;
    } while (row_offset < block_row_num);

    return Status::OK();
}

Status SegmentCreator::flush() {
    if (_flush_writer == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_flush_writer->flush());
    _flush_writer.reset();
    return Status::OK();
}

Status SegmentCreator::flush_single_block(const vectorized::Block* block, int32_t segment_id,
                                          int64_t* flush_size, TabletSchemaSPtr flush_schema) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    RETURN_IF_ERROR(
            _segment_flusher.flush_single_block(block, segment_id, flush_size, flush_schema));
    return Status::OK();
}

Status SegmentCreator::close() {
    RETURN_IF_ERROR(flush());
    RETURN_IF_ERROR(_segment_flusher.close());
    return Status::OK();
}

} // namespace doris
