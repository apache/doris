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
#include "common/status.h"
#include "io/fs/file_writer.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset_writer.h" // SegmentStatistics
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column_object.h"
#include "vec/common/schema_util.h" // variant column
#include "vec/core/block.h"

namespace doris {
using namespace ErrorCode;

SegmentFlusher::SegmentFlusher() = default;

SegmentFlusher::~SegmentFlusher() = default;

Status SegmentFlusher::init(RowsetWriterContext& rowset_writer_context) {
    _context = &rowset_writer_context;
    return Status::OK();
}

Status SegmentFlusher::flush_single_block(const vectorized::Block* block, int32_t segment_id,
                                          int64_t* flush_size) {
    if (block->rows() == 0) {
        return Status::OK();
    }
    TabletSchemaSPtr flush_schema = nullptr;
    // Expand variant columns
    vectorized::Block flush_block(*block);
    if (_context->write_type != DataWriteType::TYPE_COMPACTION &&
        _context->tablet_schema->num_variant_columns() > 0) {
        RETURN_IF_ERROR(_expand_variant_to_subcolumns(flush_block, flush_schema));
    }
    std::unique_ptr<segment_v2::SegmentWriter> writer;
    bool no_compression = flush_block.bytes() <= config::segment_compression_threshold_kb * 1024;
    RETURN_IF_ERROR(_create_segment_writer(writer, segment_id, no_compression, flush_schema));
    RETURN_IF_ERROR(_add_rows(writer, &flush_block, 0, flush_block.rows()));
    RETURN_IF_ERROR(_flush_segment_writer(writer, flush_size));
    return Status::OK();
}

Status SegmentFlusher::_expand_variant_to_subcolumns(vectorized::Block& block,
                                                     TabletSchemaSPtr& flush_schema) {
    size_t num_rows = block.rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    std::vector<int> variant_column_pos;
    if (_context->tablet_schema->is_partial_update()) {
        // check columns that used to do partial updates should not include variant
        for (int i : _context->tablet_schema->get_update_cids()) {
            if (_context->tablet_schema->columns()[i].is_variant_type()) {
                return Status::InvalidArgument("Not implement partial updates for variant");
            }
        }
    } else {
        for (int i = 0; i < _context->tablet_schema->columns().size(); ++i) {
            if (_context->tablet_schema->columns()[i].is_variant_type()) {
                variant_column_pos.push_back(i);
            }
        }
    }

    if (variant_column_pos.empty()) {
        return Status::OK();
    }

    try {
        // Parse each variant column from raw string column
        vectorized::schema_util::parse_variant_columns(block, variant_column_pos);
        vectorized::schema_util::finalize_variant_columns(block, variant_column_pos,
                                                          false /*not ingore sparse*/);
        vectorized::schema_util::encode_variant_sparse_subcolumns(block, variant_column_pos);
    } catch (const doris::Exception& e) {
        // TODO more graceful, max_filter_ratio
        LOG(WARNING) << "encounter execption " << e.to_string();
        return Status::InternalError(e.to_string());
    }

    // Dynamic Block consists of two parts, dynamic part of columns and static part of columns
    //     static     extracted
    // | --------- | ----------- |
    // The static ones are original _tablet_schame columns
    flush_schema = std::make_shared<TabletSchema>();
    flush_schema->copy_from(*_context->tablet_schema);

    vectorized::Block flush_block(std::move(block));
    // If column already exist in original tablet schema, then we pick common type
    // and cast column to common type, and modify tablet column to common type,
    // otherwise it's a new column, we should add to frontend
    auto append_column = [&](const TabletColumn& parent_variant, auto& column_entry_from_object) {
        const std::string& column_name =
                parent_variant.name_lower_case() + "." + column_entry_from_object->path.get_path();
        const vectorized::DataTypePtr& final_data_type_from_object =
                column_entry_from_object->data.get_least_common_type();
        TabletColumn tablet_column;
        vectorized::PathInDataBuilder full_path_builder;
        auto full_path = full_path_builder.append(parent_variant.name_lower_case(), false)
                                 .append(column_entry_from_object->path.get_parts(), false)
                                 .build();
        vectorized::schema_util::get_column_by_type(
                final_data_type_from_object, column_name, tablet_column,
                vectorized::schema_util::ExtraInfo {.unique_id = parent_variant.unique_id(),
                                                    .parent_unique_id = parent_variant.unique_id(),
                                                    .path_info = full_path});
        flush_schema->append_column(std::move(tablet_column));

        flush_block.insert({column_entry_from_object->data.get_finalized_column_ptr()->get_ptr(),
                            final_data_type_from_object, column_name});
    };

    // 1. Flatten variant column into flat columns, append flatten columns to the back of original Block and TabletSchema
    // those columns are extracted columns, leave none extracted columns remain in original variant column, which is
    // JSONB format at present.
    // 2. Collect columns that need to be added or modified when data type changes or new columns encountered
    for (size_t i = 0; i < variant_column_pos.size(); ++i) {
        size_t variant_pos = variant_column_pos[i];
        vectorized::ColumnObject& object_column = assert_cast<vectorized::ColumnObject&>(
                flush_block.get_by_position(variant_pos).column->assume_mutable_ref());
        const TabletColumn& parent_column = _context->tablet_schema->columns()[variant_pos];
        CHECK(object_column.is_finalized());
        std::shared_ptr<vectorized::ColumnObject::Subcolumns::Node> root;
        for (auto& entry : object_column.get_subcolumns()) {
            if (entry->path.empty()) {
                // root
                root = entry;
                continue;
            }
            append_column(parent_column, entry);
        }
        // Create new variant column and set root column
        auto obj = vectorized::ColumnObject::create(true, false);
        // '{}' indicates a root path
        static_cast<vectorized::ColumnObject*>(obj.get())->add_sub_column(
                {}, root->data.get_finalized_column_ptr()->assume_mutable(),
                root->data.get_least_common_type());
        flush_block.get_by_position(variant_pos).column = obj->get_ptr();
        vectorized::PathInDataBuilder full_root_path_builder;
        auto full_root_path =
                full_root_path_builder.append(parent_column.name_lower_case(), false).build();
        flush_schema->mutable_columns()[variant_pos].set_path_info(full_root_path);
        VLOG_DEBUG << "set root_path : " << full_root_path.get_path();
    }

    vectorized::schema_util::inherit_tablet_index(flush_schema);

    {
        // Update rowset schema, tablet's tablet schema will be updated when build Rowset
        // Eg. flush schema:    A(int),    B(float),  C(int), D(int)
        // ctx.tablet_schema:  A(bigint), B(double)
        // => update_schema:   A(bigint), B(double), C(int), D(int)
        std::lock_guard<std::mutex> lock(*(_context->schema_lock));
        TabletSchemaSPtr update_schema = vectorized::schema_util::get_least_common_schema(
                {_context->tablet_schema, flush_schema}, nullptr);
        CHECK_GE(update_schema->num_columns(), flush_schema->num_columns())
                << "Rowset merge schema columns count is " << update_schema->num_columns()
                << ", but flush_schema is larger " << flush_schema->num_columns()
                << " update_schema: " << update_schema->dump_structure()
                << " flush_schema: " << flush_schema->dump_structure();
        _context->tablet_schema.swap(update_schema);
        VLOG_DEBUG << "dump rs schema: " << _context->tablet_schema->dump_structure();
    }

    block.swap(flush_block);
    VLOG_DEBUG << "dump block: " << block.dump_data();
    VLOG_DEBUG << "dump flush schema: " << flush_schema->dump_structure();
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

bool SegmentFlusher::need_buffering() {
    // buffering variants for schema change
    return _context->write_type == DataWriteType::TYPE_SCHEMA_CHANGE &&
           _context->tablet_schema->num_variant_columns() > 0;
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
    RETURN_IF_ERROR(_context->file_writer_creator->create(segment_id, file_writer));

    segment_v2::SegmentWriterOptions writer_options;
    writer_options.enable_unique_key_merge_on_write = _context->enable_unique_key_merge_on_write;
    writer_options.rowset_ctx = _context;
    writer_options.write_type = _context->write_type;
    if (no_compression) {
        writer_options.compression_type = NO_COMPRESSION;
    }

    const auto& tablet_schema = flush_schema ? flush_schema : _context->tablet_schema;
    writer.reset(new segment_v2::SegmentWriter(
            file_writer.get(), segment_id, tablet_schema, _context->tablet, _context->data_dir,
            _context->max_rows_per_segment, writer_options, _context->mow_context));
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
    VLOG_DEBUG << "tablet_id:" << _context->tablet_id
               << " flushing filename: " << writer->get_data_dir()->path()
               << " rowset_id:" << _context->rowset_id;

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

    RETURN_IF_ERROR(_context->segment_collector->add(segment_id, segstat));

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

Status SegmentCreator::init(RowsetWriterContext& rowset_writer_context) {
    _segment_flusher.init(rowset_writer_context);
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

    if (_segment_flusher.need_buffering()) {
        constexpr static int MAX_BUFFER_SIZE = 1024 * 1024 * 400; // 400M
        if (_buffer_block.allocated_bytes() > MAX_BUFFER_SIZE) {
            vectorized::Block block = _buffer_block.to_block();
            RETURN_IF_ERROR(flush_single_block(&block));
        } else {
            RETURN_IF_ERROR(_buffer_block.merge(*block));
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
        RETURN_IF_ERROR(flush_single_block(&block));
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
