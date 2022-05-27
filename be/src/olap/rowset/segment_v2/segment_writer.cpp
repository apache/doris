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
                             const SegmentWriterOptions& opts, std::shared_ptr<MemTracker> parent)
        : _segment_id(segment_id), _tablet_schema(tablet_schema), _data_dir(data_dir),
          _opts(opts), _wblock(wblock), _mem_tracker(MemTracker::CreateTracker(
                -1, "Segment-" + std::to_string(segment_id), parent, false)) {
    CHECK_NOTNULL(_wblock);
}

SegmentWriter::~SegmentWriter() {
    _mem_tracker->Release(_mem_tracker->consumption());
};

void SegmentWriter::init_column_meta(ColumnMetaPB* meta, uint32_t* column_id,
                                     const TabletColumn& column,
                                     const TabletSchema* tablet_schema) {
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

Status SegmentWriter::init(uint32_t write_mbytes_per_sec __attribute__((unused))) {
    uint32_t column_id = 0;
    _column_writers.reserve(_tablet_schema->columns().size());
    for (auto& column : _tablet_schema->columns()) {
        ColumnWriterOptions opts;
        opts.meta = _footer.add_columns();

        init_column_meta(opts.meta, &column_id, column, _tablet_schema);

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
        opts.parent = _mem_tracker;

        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(ColumnWriter::create(opts, &column, _wblock, &writer));
        RETURN_IF_ERROR(writer->init());
        _column_writers.push_back(std::move(writer));
    }
    _index_builder.reset(new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
    return Status::OK();
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
    _mem_tracker->Consume(size - _mem_tracker->consumption());
    return size;
}

Status SegmentWriter::finalize(uint64_t* segment_file_size, uint64_t* index_size) {
    // check disk capacity
    if (_data_dir != nullptr && _data_dir->reach_capacity_limit((int64_t) estimate_segment_size())) {
        return Status::InternalError(fmt::format("disk {} exceed capacity limit.", _data_dir->path_hash()));
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

    std::vector<Slice> slices{footer_buf, fixed_buf};
    return _write_raw_data(slices);
}

Status SegmentWriter::_write_raw_data(const std::vector<Slice>& slices) {
    RETURN_IF_ERROR(_wblock->appendv(&slices[0], slices.size()));
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
