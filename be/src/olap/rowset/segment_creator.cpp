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
#include <memory>
#include <sstream>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_writer.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset_writer.h" // SegmentStatistics
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/rowset/segment_v2/vertical_segment_writer.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_object.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h" // variant column
#include "vec/core/block.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris {
using namespace ErrorCode;

SegmentFlusher::SegmentFlusher(RowsetWriterContext& context, SegmentFileCollection& seg_files)
        : _context(context), _seg_files(seg_files) {}

SegmentFlusher::~SegmentFlusher() = default;

Status SegmentFlusher::flush_single_block(const vectorized::Block* block, int32_t segment_id,
                                          int64_t* flush_size) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    vectorized::Block flush_block(*block);
    if (_context.write_type != DataWriteType::TYPE_COMPACTION &&
        _context.tablet_schema->num_variant_columns() > 0) {
        RETURN_IF_ERROR(_parse_variant_columns(flush_block));
    }
    bool no_compression = flush_block.bytes() <= config::segment_compression_threshold_kb * 1024;
    if (config::enable_vertical_segment_writer &&
        _context.tablet_schema->cluster_key_idxes().empty()) {
        std::unique_ptr<segment_v2::VerticalSegmentWriter> writer;
        RETURN_IF_ERROR(_create_segment_writer(writer, segment_id, no_compression));
        RETURN_IF_ERROR(_add_rows(writer, &flush_block, 0, flush_block.rows()));
        RETURN_IF_ERROR(_flush_segment_writer(writer, writer->flush_schema(), flush_size));
    } else {
        std::unique_ptr<segment_v2::SegmentWriter> writer;
        RETURN_IF_ERROR(_create_segment_writer(writer, segment_id, no_compression));
        RETURN_IF_ERROR(_add_rows(writer, &flush_block, 0, flush_block.rows()));
        RETURN_IF_ERROR(_flush_segment_writer(writer, writer->flush_schema(), flush_size));
    }
    return Status::OK();
}

Status SegmentFlusher::_parse_variant_columns(vectorized::Block& block) {
    size_t num_rows = block.rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    std::vector<int> variant_column_pos;
    for (int i = 0; i < block.columns(); ++i) {
        const auto& entry = block.get_by_position(i);
        if (vectorized::is_variant_type(remove_nullable(entry.type))) {
            variant_column_pos.push_back(i);
        }
    }

    if (variant_column_pos.empty()) {
        return Status::OK();
    }

    vectorized::schema_util::ParseContext ctx;
    ctx.record_raw_json_column = _context.tablet_schema->has_row_store_for_all_columns();
    RETURN_IF_ERROR(vectorized::schema_util::parse_variant_columns(block, variant_column_pos, ctx));
    return Status::OK();
}

Status SegmentFlusher::close() {
    return _seg_files.close();
}

bool SegmentFlusher::need_buffering() {
    // buffering variants for schema change
    return _context.write_type == DataWriteType::TYPE_SCHEMA_CHANGE &&
           _context.tablet_schema->num_variant_columns() > 0;
}

Status SegmentFlusher::_add_rows(std::unique_ptr<segment_v2::SegmentWriter>& segment_writer,
                                 const vectorized::Block* block, size_t row_offset,
                                 size_t row_num) {
    RETURN_IF_ERROR(segment_writer->append_block(block, row_offset, row_num));
    _num_rows_written += row_num;
    return Status::OK();
}

Status SegmentFlusher::_add_rows(std::unique_ptr<segment_v2::VerticalSegmentWriter>& segment_writer,
                                 const vectorized::Block* block, size_t row_offset,
                                 size_t row_num) {
    RETURN_IF_ERROR(segment_writer->batch_block(block, row_offset, row_num));
    RETURN_IF_ERROR(segment_writer->write_batch());
    _num_rows_written += row_num;
    return Status::OK();
}

Status SegmentFlusher::_create_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                              int32_t segment_id, bool no_compression) {
    io::FileWriterPtr segment_file_writer;
    RETURN_IF_ERROR(_context.file_writer_creator->create(segment_id, segment_file_writer));

    io::FileWriterPtr inverted_file_writer;
    if (_context.tablet_schema->has_inverted_index() &&
        _context.tablet_schema->get_inverted_index_storage_format() >=
                InvertedIndexStorageFormatPB::V2 &&
        _context.memtable_on_sink_support_index_v2) {
        RETURN_IF_ERROR(_context.file_writer_creator->create(segment_id, inverted_file_writer,
                                                             FileType::INVERTED_INDEX_FILE));
    }

    segment_v2::SegmentWriterOptions writer_options;
    writer_options.enable_unique_key_merge_on_write = _context.enable_unique_key_merge_on_write;
    writer_options.rowset_ctx = &_context;
    writer_options.write_type = _context.write_type;
    if (no_compression) {
        writer_options.compression_type = NO_COMPRESSION;
    }

    writer = std::make_unique<segment_v2::SegmentWriter>(
            segment_file_writer.get(), segment_id, _context.tablet_schema, _context.tablet,
            _context.data_dir, _context.max_rows_per_segment, writer_options, _context.mow_context,
            std::move(inverted_file_writer));
    RETURN_IF_ERROR(_seg_files.add(segment_id, std::move(segment_file_writer)));
    auto s = writer->init();
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        writer.reset();
        return s;
    }
    return Status::OK();
}

Status SegmentFlusher::_create_segment_writer(
        std::unique_ptr<segment_v2::VerticalSegmentWriter>& writer, int32_t segment_id,
        bool no_compression) {
    io::FileWriterPtr segment_file_writer;
    RETURN_IF_ERROR(_context.file_writer_creator->create(segment_id, segment_file_writer));

    io::FileWriterPtr inverted_file_writer;
    if (_context.tablet_schema->has_inverted_index() &&
        _context.tablet_schema->get_inverted_index_storage_format() >=
                InvertedIndexStorageFormatPB::V2 &&
        _context.memtable_on_sink_support_index_v2) {
        RETURN_IF_ERROR(_context.file_writer_creator->create(segment_id, inverted_file_writer,
                                                             FileType::INVERTED_INDEX_FILE));
    }

    segment_v2::VerticalSegmentWriterOptions writer_options;
    writer_options.enable_unique_key_merge_on_write = _context.enable_unique_key_merge_on_write;
    writer_options.rowset_ctx = &_context;
    writer_options.write_type = _context.write_type;
    if (no_compression) {
        writer_options.compression_type = NO_COMPRESSION;
    }

    writer = std::make_unique<segment_v2::VerticalSegmentWriter>(
            segment_file_writer.get(), segment_id, _context.tablet_schema, _context.tablet,
            _context.data_dir, _context.max_rows_per_segment, writer_options, _context.mow_context,
            std::move(inverted_file_writer));
    RETURN_IF_ERROR(_seg_files.add(segment_id, std::move(segment_file_writer)));
    auto s = writer->init();
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        writer.reset();
        return s;
    }

    VLOG_DEBUG << "create new segment writer, tablet_id:" << _context.tablet_id
               << " segment id: " << segment_id << " filename: " << writer->data_dir_path()
               << " rowset_id:" << _context.rowset_id;
    return Status::OK();
}

Status SegmentFlusher::_flush_segment_writer(
        std::unique_ptr<segment_v2::VerticalSegmentWriter>& writer, TabletSchemaSPtr flush_schema,
        int64_t* flush_size) {
    uint32_t row_num = writer->num_rows_written();
    _num_rows_updated += writer->num_rows_updated();
    _num_rows_deleted += writer->num_rows_deleted();
    _num_rows_new_added += writer->num_rows_new_added();
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
               << " flushing filename: " << writer->data_dir_path()
               << " rowset_id:" << _context.rowset_id;

    KeyBoundsPB key_bounds;
    Slice min_key = writer->min_encoded_key();
    Slice max_key = writer->max_encoded_key();
    DCHECK_LE(min_key.compare(max_key), 0);
    key_bounds.set_min_key(min_key.to_string());
    key_bounds.set_max_key(max_key.to_string());

    uint32_t segment_id = writer->segment_id();
    SegmentStatistics segstat;
    segstat.row_num = row_num;
    segstat.data_size = segment_size + writer->inverted_index_file_size();
    segstat.index_size = index_size + writer->inverted_index_file_size();
    segstat.key_bounds = key_bounds;

    writer.reset();

    RETURN_IF_ERROR(_context.segment_collector->add(segment_id, segstat, flush_schema));

    if (flush_size) {
        *flush_size = segment_size + index_size;
    }
    return Status::OK();
}

Status SegmentFlusher::_flush_segment_writer(std::unique_ptr<segment_v2::SegmentWriter>& writer,
                                             TabletSchemaSPtr flush_schema, int64_t* flush_size) {
    uint32_t row_num = writer->num_rows_written();
    _num_rows_updated += writer->num_rows_updated();
    _num_rows_deleted += writer->num_rows_deleted();
    _num_rows_new_added += writer->num_rows_new_added();
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
               << " flushing rowset_dir: " << _context.tablet_path
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

    writer.reset();

    RETURN_IF_ERROR(_context.segment_collector->add(segment_id, segstat, flush_schema));

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

SegmentCreator::SegmentCreator(RowsetWriterContext& context, SegmentFileCollection& seg_files)
        : _segment_flusher(context, seg_files) {}

Status SegmentCreator::add_block(const vectorized::Block* block) {
    if (block->rows() == 0) {
        return Status::OK();
    }

    size_t block_size_in_bytes = block->bytes();
    size_t block_row_num = block->rows();
    size_t row_avg_size_in_bytes = std::max((size_t)1, block_size_in_bytes / block_row_num);
    size_t row_offset = 0;
    if (_segment_flusher.need_buffering()) {
        RETURN_IF_ERROR(_buffer_block.merge(*block));
        if (_buffer_block.allocated_bytes() > config::write_buffer_size) {
            LOG(INFO) << "directly flush a single block " << _buffer_block.rows() << " rows"
                      << ", block size " << _buffer_block.bytes() << " block allocated_size "
                      << _buffer_block.allocated_bytes();
            vectorized::Block block = _buffer_block.to_block();
            RETURN_IF_ERROR(flush_single_block(&block));
            _buffer_block.clear();
        }
        return Status::OK();
    }

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
    if (_buffer_block.rows() > 0) {
        vectorized::Block block = _buffer_block.to_block();
        LOG(INFO) << "directly flush a single block " << block.rows() << " rows"
                  << ", block size " << block.bytes() << " block allocated_size "
                  << block.allocated_bytes();
        RETURN_IF_ERROR(flush_single_block(&block));
        _buffer_block.clear();
    }
    if (_flush_writer == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_flush_writer->flush());
    _flush_writer.reset();
    return Status::OK();
}

Status SegmentCreator::flush_single_block(const vectorized::Block* block, int32_t segment_id,
                                          int64_t* flush_size) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_segment_flusher.flush_single_block(block, segment_id, flush_size));
    return Status::OK();
}

Status SegmentCreator::close() {
    RETURN_IF_ERROR(flush());
    RETURN_IF_ERROR(_segment_flusher.close());
    return Status::OK();
}

} // namespace doris
