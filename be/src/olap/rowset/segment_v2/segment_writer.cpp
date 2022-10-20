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

#include "olap/rowset/segment_v2/segment_writer.h"

#include "common/logging.h" // LOG
#include "env/env.h"        // Env
#include "io/fs/file_writer.h"
#include "olap/data_dir.h"
#include "olap/primary_key_index.h"
#include "olap/row.h"                             // ContiguousRow
#include "olap/row_cursor.h"                      // RowCursor
#include "olap/rowset/segment_v2/column_writer.h" // ColumnWriter
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/schema.h"
#include "olap/short_key_index.h"
#include "runtime/memory/mem_tracker.h"
#include "util/crc32c.h"
#include "util/faststring.h"
#include "util/key_util.h"

namespace doris {
namespace segment_v2 {

const char* k_segment_magic = "D0R1";
const uint32_t k_segment_magic_length = 4;

SegmentWriter::SegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                             TabletSchemaSPtr tablet_schema, DataDir* data_dir,
                             uint32_t max_row_per_segment, const SegmentWriterOptions& opts)
        : _segment_id(segment_id),
          _tablet_schema(tablet_schema),
          _data_dir(data_dir),
          _max_row_per_segment(max_row_per_segment),
          _opts(opts),
          _file_writer(file_writer),
          _mem_tracker(std::make_unique<MemTracker>("SegmentWriter:Segment-" +
                                                    std::to_string(segment_id))),
          _olap_data_convertor(tablet_schema.get()) {
    CHECK_NOTNULL(file_writer);
    if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
        _num_key_columns = _tablet_schema->num_key_columns();
    } else {
        _num_key_columns = _tablet_schema->num_short_key_columns();
    }
    for (size_t cid = 0; cid < _num_key_columns; ++cid) {
        const auto& column = _tablet_schema->column(cid);
        _key_coders.push_back(get_key_coder(column.type()));
        _key_index_size.push_back(column.index_length());
    }
    // encode the sequence id into the primary key index
    if (_tablet_schema->has_sequence_col() && _tablet_schema->keys_type() == UNIQUE_KEYS &&
        _opts.enable_unique_key_merge_on_write) {
        const auto& column = _tablet_schema->column(_tablet_schema->sequence_col_idx());
        _key_coders.push_back(get_key_coder(column.type()));
    }
}

SegmentWriter::~SegmentWriter() {
    _mem_tracker->release(_mem_tracker->consumption());
}

void SegmentWriter::init_column_meta(ColumnMetaPB* meta, uint32_t* column_id,
                                     const TabletColumn& column, TabletSchemaSPtr tablet_schema) {
    // TODO(zc): Do we need this column_id??
    meta->set_column_id((*column_id)++);
    meta->set_unique_id(column.unique_id());
    meta->set_type(column.type());
    meta->set_length(column.length());
    meta->set_encoding(DEFAULT_ENCODING);
    meta->set_compression(tablet_schema->compression_type());
    meta->set_is_nullable(column.is_nullable());
    for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
        init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i),
                         tablet_schema);
    }
}

Status SegmentWriter::init() {
    uint32_t column_id = 0;
    _column_writers.reserve(_tablet_schema->columns().size());
    for (auto& column : _tablet_schema->columns()) {
        ColumnWriterOptions opts;
        opts.meta = _footer.add_columns();

        init_column_meta(opts.meta, &column_id, column, _tablet_schema);

        // now we create zone map for key columns in AGG_KEYS or all column in UNIQUE_KEYS or DUP_KEYS
        // and not support zone map for array type and jsonb type.
        opts.need_zone_map = column.is_key() || _tablet_schema->keys_type() != KeysType::AGG_KEYS;
        opts.need_bloom_filter = column.is_bf_column();
        opts.need_bitmap_index = column.has_bitmap_index();
        if (column.type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            opts.need_zone_map = false;
            if (opts.need_bloom_filter) {
                return Status::NotSupported("Do not support bloom filter for array type");
            }
            if (opts.need_bitmap_index) {
                return Status::NotSupported("Do not support bitmap index for array type");
            }
        }
        if (column.type() == FieldType::OLAP_FIELD_TYPE_JSONB) {
            opts.need_zone_map = false;
            if (opts.need_bloom_filter) {
                return Status::NotSupported("Do not support bloom filter for jsonb type");
            }
            if (opts.need_bitmap_index) {
                return Status::NotSupported("Do not support bitmap index for jsonb type");
            }
        }

        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(ColumnWriter::create(opts, &column, _file_writer, &writer));
        RETURN_IF_ERROR(writer->init());
        _column_writers.push_back(std::move(writer));
    }

    // we don't need the short key index for unique key merge on write table.
    if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
        size_t seq_col_length = 0;
        if (_tablet_schema->has_sequence_col()) {
            seq_col_length =
                    _tablet_schema->column(_tablet_schema->sequence_col_idx()).length() + 1;
        }
        _primary_key_index_builder.reset(new PrimaryKeyIndexBuilder(_file_writer, seq_col_length));
        RETURN_IF_ERROR(_primary_key_index_builder->init());
    } else {
        _short_key_index_builder.reset(
                new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
    }
    return Status::OK();
}

Status SegmentWriter::append_block(const vectorized::Block* block, size_t row_pos,
                                   size_t num_rows) {
    assert(block && num_rows > 0 && row_pos + num_rows <= block->rows() &&
           block->columns() == _column_writers.size());
    _olap_data_convertor.set_source_content(block, row_pos, num_rows);

    // find all row pos for short key indexes
    std::vector<size_t> short_key_pos;
    // We build a short key index every `_opts.num_rows_per_block` rows. Specifically, we
    // build a short key index using 1st rows for first block and `_short_key_row_pos - _row_count`
    // for next blocks.
    // Ensure we build a short key index using 1st rows only for the first block (ISSUE-9766).
    if (UNLIKELY(_short_key_row_pos == 0 && _row_count == 0)) {
        short_key_pos.push_back(0);
    }
    while (_short_key_row_pos + _opts.num_rows_per_block < _row_count + num_rows) {
        _short_key_row_pos += _opts.num_rows_per_block;
        short_key_pos.push_back(_short_key_row_pos - _row_count);
    }

    // convert column data from engine format to storage layer format
    std::vector<vectorized::IOlapColumnDataAccessor*> key_columns;
    for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
        auto converted_result = _olap_data_convertor.convert_column_data(cid);
        if (converted_result.first != Status::OK()) {
            return converted_result.first;
        }
        if (cid < _num_key_columns ||
            (_tablet_schema->has_sequence_col() && _tablet_schema->keys_type() == UNIQUE_KEYS &&
             _opts.enable_unique_key_merge_on_write && cid == _tablet_schema->sequence_col_idx())) {
            key_columns.push_back(converted_result.second);
        }
        RETURN_IF_ERROR(_column_writers[cid]->append(converted_result.second->get_nullmap(),
                                                     converted_result.second->get_data(),
                                                     num_rows));
    }

    if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
        // create primary indexes
        for (size_t pos = 0; pos < num_rows; pos++) {
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(_encode_keys(key_columns, pos)));
        }
    } else {
        // create short key indexes
        for (const auto pos : short_key_pos) {
            RETURN_IF_ERROR(_short_key_index_builder->add_item(_encode_keys(key_columns, pos)));
        }
    }

    _row_count += num_rows;
    _olap_data_convertor.clear_source_content();
    return Status::OK();
}

int64_t SegmentWriter::max_row_to_add(size_t row_avg_size_in_bytes) {
    auto segment_size = estimate_segment_size();
    if (PREDICT_FALSE(segment_size >= MAX_SEGMENT_SIZE || _row_count >= _max_row_per_segment)) {
        return 0;
    }
    int64_t size_rows = ((int64_t)MAX_SEGMENT_SIZE - (int64_t)segment_size) / row_avg_size_in_bytes;
    int64_t count_rows = (int64_t)_max_row_per_segment - _row_count;

    return std::min(size_rows, count_rows);
}

std::string SegmentWriter::_encode_keys(
        const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t pos,
        bool null_first) {
    if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write &&
        _tablet_schema->has_sequence_col()) {
        assert(key_columns.size() == _num_key_columns + 1 &&
               _key_coders.size() == _num_key_columns + 1 &&
               _key_index_size.size() == _num_key_columns);
    } else {
        assert(key_columns.size() == _num_key_columns && _key_coders.size() == _num_key_columns &&
               _key_index_size.size() == _num_key_columns);
    }

    std::string encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        auto field = column->get_data_at(pos);
        if (UNLIKELY(!field)) {
            if (null_first) {
                encoded_keys.push_back(KEY_NULL_FIRST_MARKER);
            } else {
                encoded_keys.push_back(KEY_NULL_LAST_MARKER);
            }
            continue;
        }
        encoded_keys.push_back(KEY_NORMAL_MARKER);
        if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
            _key_coders[cid]->full_encode_ascending(field, &encoded_keys);
        } else {
            _key_coders[cid]->encode_ascending(field, _key_index_size[cid], &encoded_keys);
        }
        ++cid;
    }
    return encoded_keys;
}

template <typename RowType>
Status SegmentWriter::append_row(const RowType& row) {
    for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
        auto cell = row.cell(cid);
        RETURN_IF_ERROR(_column_writers[cid]->append(cell));
    }

    if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
        std::string encoded_key;
        encode_key<RowType, true, true>(&encoded_key, row, _num_key_columns);
        if (_tablet_schema->has_sequence_col()) {
            encoded_key.push_back(KEY_NORMAL_MARKER);
            auto cid = _tablet_schema->sequence_col_idx();
            auto cell = row.cell(cid);
            row.schema()->column(cid)->full_encode_ascending(cell.cell_ptr(), &encoded_key);
        }
        RETURN_IF_ERROR(_primary_key_index_builder->add_item(encoded_key));
    } else {
        // At the beginning of one block, so add a short key index entry
        if ((_row_count % _opts.num_rows_per_block) == 0) {
            std::string encoded_key;
            encode_key(&encoded_key, row, _num_key_columns);
            RETURN_IF_ERROR(_short_key_index_builder->add_item(encoded_key));
        }
    }
    ++_row_count;
    return Status::OK();
}

template Status SegmentWriter::append_row(const RowCursor& row);
template Status SegmentWriter::append_row(const ContiguousRow& row);

// TODO(lingbin): Currently this function does not include the size of various indexes,
// We should make this more precise.
// NOTE: This function will be called when any row of data is added, so we need to
// make this function efficient.
uint64_t SegmentWriter::estimate_segment_size() {
    // footer_size(4) + checksum(4) + segment_magic(4)
    uint64_t size = 12;
    for (auto& column_writer : _column_writers) {
        size += column_writer->estimate_buffer_size();
    }
    if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
        size += _primary_key_index_builder->size();
    } else {
        size += _short_key_index_builder->size();
    }

    // update the mem_tracker of segment size
    _mem_tracker->consume(size - _mem_tracker->consumption());
    return size;
}

Status SegmentWriter::finalize(uint64_t* segment_file_size, uint64_t* index_size) {
    // check disk capacity
    if (_data_dir != nullptr && _data_dir->reach_capacity_limit((int64_t)estimate_segment_size())) {
        return Status::InternalError("disk {} exceed capacity limit.", _data_dir->path_hash());
    }
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->finish());
    }
    RETURN_IF_ERROR(_write_data());
    uint64_t index_offset = _file_writer->bytes_appended();
    RETURN_IF_ERROR(_write_ordinal_index());
    RETURN_IF_ERROR(_write_zone_map());
    RETURN_IF_ERROR(_write_bitmap_index());
    RETURN_IF_ERROR(_write_bloom_filter_index());
    if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
        RETURN_IF_ERROR(_write_primary_key_index());
    } else {
        RETURN_IF_ERROR(_write_short_key_index());
    }
    *index_size = _file_writer->bytes_appended() - index_offset;
    RETURN_IF_ERROR(_write_footer());
    RETURN_IF_ERROR(_file_writer->finalize());
    *segment_file_size = _file_writer->bytes_appended();
    return Status::OK();
}

// write column data to file one by one
Status SegmentWriter::_write_data() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_data());
    }
    return Status::OK();
}

// write ordinal index after data has been written
Status SegmentWriter::_write_ordinal_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status SegmentWriter::_write_zone_map() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_zone_map());
    }
    return Status::OK();
}

Status SegmentWriter::_write_bitmap_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_bitmap_index());
    }
    return Status::OK();
}

Status SegmentWriter::_write_bloom_filter_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_bloom_filter_index());
    }
    return Status::OK();
}

Status SegmentWriter::_write_short_key_index() {
    std::vector<Slice> body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_short_key_index_builder->finalize(_row_count, &body, &footer));
    PagePointer pp;
    // short key index page is not compressed right now
    RETURN_IF_ERROR(PageIO::write_page(_file_writer, body, footer, &pp));
    pp.to_proto(_footer.mutable_short_key_index_page());
    return Status::OK();
}

Status SegmentWriter::_write_primary_key_index() {
    CHECK(_primary_key_index_builder->num_rows() == _row_count);
    return _primary_key_index_builder->finalize(_footer.mutable_primary_key_index_meta());
}

Status SegmentWriter::_write_footer() {
    _footer.set_num_rows(_row_count);

    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string footer_buf;
    if (!_footer.SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize segment footer");
    }

    faststring fixed_buf;
    // footer's size
    put_fixed32_le(&fixed_buf, footer_buf.size());
    // footer's checksum
    uint32_t checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    put_fixed32_le(&fixed_buf, checksum);
    // Append magic number. we don't write magic number in the header because
    // that will need an extra seek when reading
    fixed_buf.append(k_segment_magic, k_segment_magic_length);

    std::vector<Slice> slices {footer_buf, fixed_buf};
    return _write_raw_data(slices);
}

Status SegmentWriter::_write_raw_data(const std::vector<Slice>& slices) {
    RETURN_IF_ERROR(_file_writer->appendv(&slices[0], slices.size()));
    return Status::OK();
}

Slice SegmentWriter::min_encoded_key() {
    return (_primary_key_index_builder == nullptr) ? Slice()
                                                   : _primary_key_index_builder->min_key();
}
Slice SegmentWriter::max_encoded_key() {
    return (_primary_key_index_builder == nullptr) ? Slice()
                                                   : _primary_key_index_builder->max_key();
}

} // namespace segment_v2
} // namespace doris
