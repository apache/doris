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
#include "olap/data_dir.h"
#include "olap/fs/block_manager.h"
#include "olap/row.h"                             // ContiguousRow
#include "olap/row_cursor.h"                      // RowCursor
#include "olap/rowset/segment_v2/column_writer.h" // ColumnWriter
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/schema.h"
#include "olap/short_key_index.h"
#include "runtime/mem_tracker.h"
#include "util/crc32c.h"
#include "util/faststring.h"

namespace doris {
namespace segment_v2 {

const char* k_segment_magic = "D0R1";
const uint32_t k_segment_magic_length = 4;

SegmentWriter::SegmentWriter(fs::WritableBlock* wblock, uint32_t segment_id,
                             const TabletSchema* tablet_schema, DataDir* data_dir,
                             uint32_t max_row_per_segment, const SegmentWriterOptions& opts)
        : _segment_id(segment_id),
          _tablet_schema(tablet_schema),
          _data_dir(data_dir),
          _max_row_per_segment(max_row_per_segment),
          _opts(opts),
          _wblock(wblock),
          _mem_tracker(MemTracker::create_virtual_tracker(
                  -1, "SegmentWriter:Segment-" + std::to_string(segment_id))),
          _olap_data_convertor(tablet_schema) {
    CHECK_NOTNULL(_wblock);
    size_t num_short_key_column = _tablet_schema->num_short_key_columns();
    for (size_t cid = 0; cid < num_short_key_column; ++cid) {
        const auto& column = _tablet_schema->column(cid);
        _short_key_coders.push_back(get_key_coder(column.type()));
        _short_key_index_size.push_back(column.index_length());
    }
}

SegmentWriter::~SegmentWriter() {
    _mem_tracker->release(_mem_tracker->consumption());
};

void SegmentWriter::init_column_meta(ColumnMetaPB* meta, uint32_t* column_id,
                                     const TabletColumn& column) {
    // TODO(zc): Do we need this column_id??
    meta->set_column_id((*column_id)++);
    meta->set_unique_id(column.unique_id());
    meta->set_type(column.type());
    meta->set_length(column.length());
    meta->set_encoding(DEFAULT_ENCODING);
    meta->set_compression(LZ4F);
    meta->set_is_nullable(column.is_nullable());
    for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
        init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i));
    }
}

Status SegmentWriter::init(uint32_t write_mbytes_per_sec __attribute__((unused))) {
    uint32_t column_id = 0;
    _column_writers.reserve(_tablet_schema->columns().size());
    for (auto& column : _tablet_schema->columns()) {
        ColumnWriterOptions opts;
        opts.meta = _footer.add_columns();

        init_column_meta(opts.meta, &column_id, column);

        // now we create zone map for key columns in AGG_KEYS or all column in UNIQUE_KEYS or DUP_KEYS
        // and not support zone map for array type.
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

        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(ColumnWriter::create(opts, &column, _wblock, &writer));
        RETURN_IF_ERROR(writer->init());
        _column_writers.push_back(std::move(writer));
    }
    _index_builder.reset(new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
    return Status::OK();
}

Status SegmentWriter::append_block(const vectorized::Block* block, size_t row_pos,
                                   size_t num_rows) {
    assert(block && num_rows > 0 && row_pos + num_rows <= block->rows() &&
           block->columns() == _column_writers.size());
    _olap_data_convertor.set_source_content(block, row_pos, num_rows);

    // find all row pos for short key indexes
    std::vector<size_t> short_key_pos;
    if (UNLIKELY(_short_key_row_pos == 0)) {
        short_key_pos.push_back(0);
    }
    while (_short_key_row_pos + _opts.num_rows_per_block < _row_count + num_rows) {
        _short_key_row_pos += _opts.num_rows_per_block;
        short_key_pos.push_back(_short_key_row_pos - _row_count);
    }

    // convert column data from engine format to storage layer format
    std::vector<vectorized::IOlapColumnDataAccessorSPtr> short_key_columns;
    size_t num_key_columns = _tablet_schema->num_short_key_columns();
    for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
        auto converted_result = _olap_data_convertor.convert_column_data(cid);
        if (converted_result.first != Status::OK()) {
            return converted_result.first;
        }
        if (cid < num_key_columns) {
            short_key_columns.push_back(converted_result.second);
        }
        _column_writers[cid]->append(converted_result.second->get_nullmap(),
                                     converted_result.second->get_data(), num_rows);
    }

    // create short key indexes
    std::vector<const void*> key_column_fields;
    for (const auto pos : short_key_pos) {
        for (const auto& column : short_key_columns) {
            key_column_fields.push_back(column->get_data_at(pos));
        }
        std::string encoded_key = encode_short_keys(key_column_fields);
        RETURN_IF_ERROR(_index_builder->add_item(encoded_key));
        key_column_fields.clear();
    }

    _row_count += num_rows;
    _olap_data_convertor.clear_source_content();
    return Status::OK();
}

int64_t SegmentWriter::max_row_to_add(size_t row_avg_size_in_bytes) {
    int64_t size_rows =
            ((int64_t)MAX_SEGMENT_SIZE - (int64_t)estimate_segment_size()) / row_avg_size_in_bytes;
    int64_t count_rows = (int64_t)_max_row_per_segment - _row_count;

    return std::min(size_rows, count_rows);
}

std::string SegmentWriter::encode_short_keys(const std::vector<const void*> key_column_fields,
                                             bool null_first) {
    size_t num_key_columns = _tablet_schema->num_short_key_columns();
    assert(key_column_fields.size() == num_key_columns &&
           _short_key_coders.size() == num_key_columns &&
           _short_key_index_size.size() == num_key_columns);

    std::string encoded_keys;
    for (size_t cid = 0; cid < num_key_columns; ++cid) {
        auto field = key_column_fields[cid];
        if (UNLIKELY(!field)) {
            if (null_first) {
                encoded_keys.push_back(KEY_NULL_FIRST_MARKER);
            } else {
                encoded_keys.push_back(KEY_NULL_LAST_MARKER);
            }
            continue;
        }
        encoded_keys.push_back(KEY_NORMAL_MARKER);
        _short_key_coders[cid]->encode_ascending(field, _short_key_index_size[cid], &encoded_keys);
    }
    return encoded_keys;
}

template <typename RowType>
Status SegmentWriter::append_row(const RowType& row) {
    for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
        auto cell = row.cell(cid);
        RETURN_IF_ERROR(_column_writers[cid]->append(cell));
    }

    // At the begin of one block, so add a short key index entry
    if ((_row_count % _opts.num_rows_per_block) == 0) {
        std::string encoded_key;
        encode_key(&encoded_key, row, _tablet_schema->num_short_key_columns());
        RETURN_IF_ERROR(_index_builder->add_item(encoded_key));
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
    size += _index_builder->size();

    // update the mem_tracker of segment size
    _mem_tracker->consume(size - _mem_tracker->consumption());
    return size;
}

Status SegmentWriter::finalize(uint64_t* segment_file_size, uint64_t* index_size) {
    // check disk capacity
    if (_data_dir != nullptr && _data_dir->reach_capacity_limit((int64_t)estimate_segment_size())) {
        return Status::InternalError(
                fmt::format("disk {} exceed capacity limit.", _data_dir->path_hash()));
    }
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->finish());
    }
    RETURN_IF_ERROR(_write_data());
    uint64_t index_offset = _wblock->bytes_appended();
    RETURN_IF_ERROR(_write_ordinal_index());
    RETURN_IF_ERROR(_write_zone_map());
    RETURN_IF_ERROR(_write_bitmap_index());
    RETURN_IF_ERROR(_write_bloom_filter_index());
    RETURN_IF_ERROR(_write_short_key_index());
    *index_size = _wblock->bytes_appended() - index_offset;
    RETURN_IF_ERROR(_write_footer());
    RETURN_IF_ERROR(_wblock->finalize());
    *segment_file_size = _wblock->bytes_appended();
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
    RETURN_IF_ERROR(_index_builder->finalize(_row_count, &body, &footer));
    PagePointer pp;
    // short key index page is not compressed right now
    RETURN_IF_ERROR(PageIO::write_page(_wblock, body, footer, &pp));
    pp.to_proto(_footer.mutable_short_key_index_page());
    return Status::OK();
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
    RETURN_IF_ERROR(_wblock->appendv(&slices[0], slices.size()));
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris