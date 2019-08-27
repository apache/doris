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

#include "env/env.h" // Env
#include "olap/row.h" // ContiguousRow
#include "olap/row_block.h" // RowBlock
#include "olap/row_cursor.h" // RowCursor
#include "olap/rowset/segment_v2/column_writer.h" // ColumnWriter
#include "olap/short_key_index.h"
#include "util/hash_util.hpp"

namespace doris {
namespace segment_v2 {

const char* k_segment_magic = "D0R1";
const uint32_t k_segment_magic_length = 4;

SegmentWriter::SegmentWriter(std::string fname, uint32_t segment_id,
                             const TabletSchema* tablet_schema,
                             const SegmentWriterOptions& opts)
        : _fname(std::move(fname)),
        _segment_id(segment_id),
        _tablet_schema(tablet_schema),
        _opts(opts) {
}

SegmentWriter::~SegmentWriter() {
    for (auto writer : _column_writers) {
        delete writer;
    }
}

Status SegmentWriter::init(uint32_t write_mbytes_per_sec) {
    // create for write
    RETURN_IF_ERROR(Env::Default()->new_writable_file(_fname, &_output_file));

    uint32_t column_id = 0;
    for (auto& column : _tablet_schema->columns()) {
        ColumnMetaPB* column_meta = _footer.add_columns();
        // TODO(zc): Do we need this column_id??
        column_meta->set_column_id(column_id++);
        column_meta->set_unique_id(column.unique_id());
        bool is_nullable = column.is_nullable();
        column_meta->set_is_nullable(is_nullable);

        // TODO(zc): we can add type_info into TabletColumn?
        const TypeInfo* type_info = get_type_info(column.type());
        DCHECK(type_info != nullptr);

        ColumnWriterOptions opts;
        opts.compression_type = segment_v2::CompressionTypePB::LZ4F;
        // now we create zone map for key columns
        if (column.is_key()) {
            opts.need_zone_map = true;
        }
        std::unique_ptr<ColumnWriter> writer(new ColumnWriter(opts, type_info, is_nullable, _output_file.get()));
        RETURN_IF_ERROR(writer->init());
        _column_writers.push_back(writer.release());
    }
    _index_builder.reset(new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
    return Status::OK();
}

template<typename RowType>
Status SegmentWriter::append_row(const RowType& row) {
    for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
        auto cell = row.cell(cid);
        RETURN_IF_ERROR(_column_writers[cid]->append(cell));
    }

    if ((_row_count % _opts.num_rows_per_block) == 0) {
        std::string encoded_key;
        encode_key(&encoded_key, row, _tablet_schema->num_short_key_columns());
        RETURN_IF_ERROR(_index_builder->add_item(encoded_key));
        _block_count++;
    }
    _row_count++;
    return Status::OK();
}

template Status SegmentWriter::append_row(const RowCursor& row);
template Status SegmentWriter::append_row(const ContiguousRow& row);

uint64_t SegmentWriter::estimate_segment_size() {
    return 0;
}

Status SegmentWriter::finalize(uint32_t* segment_file_size) {
    for (auto column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->finish());
    }
    RETURN_IF_ERROR(_write_raw_data({k_segment_magic}));
    RETURN_IF_ERROR(_write_data());
    RETURN_IF_ERROR(_write_ordinal_index());
    RETURN_IF_ERROR(_write_zone_map());
    RETURN_IF_ERROR(_write_short_key_index());
    RETURN_IF_ERROR(_write_footer());
    return Status::OK();
}

// write column data to file one by one
Status SegmentWriter::_write_data() {
    for (auto column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_data());
    }
    return Status::OK();
}

// write ordinal index after data has been written
Status SegmentWriter::_write_ordinal_index() {
    for (auto column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status SegmentWriter::_write_zone_map() {
    for (auto column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_zone_map());
    }
    return Status::OK();
}

Status SegmentWriter::_write_short_key_index() {
    std::vector<Slice> slices;
    // TODO(zc): we should get segment_size
    RETURN_IF_ERROR(_index_builder->finalize(_row_count * 100, _row_count, &slices));

    uint64_t offset = _output_file->size();
    RETURN_IF_ERROR(_write_raw_data(slices));
    uint32_t written_bytes = _output_file->size() - offset;

    _footer.mutable_short_key_index_page()->set_offset(offset);
    _footer.mutable_short_key_index_page()->set_size(written_bytes);
    return Status::OK();
}

Status SegmentWriter::_write_footer() {
    _footer.set_num_rows(_row_count);
    // collect all 
    for (int i = 0; i < _column_writers.size(); ++i) {
        _column_writers[i]->write_meta(_footer.mutable_columns(i));
    }

    // write footer
    std::string footer_buf;
    if (!_footer.SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize segment footer");
    }

    std::string footer_info_buf;
    // put footer's size
    put_fixed32_le(&footer_info_buf, footer_buf.size());
    // compute footer's checksum
    uint32_t checksum = HashUtil::crc_hash(footer_buf.data(), footer_buf.size(), 0);
    put_fixed32_le(&footer_info_buf, checksum);

    // I think we don't need to put a tail magic.

    std::vector<Slice> slices{footer_buf, footer_info_buf};
    // write offset and length
    RETURN_IF_ERROR(_write_raw_data(slices));
    return Status::OK();
}

Status SegmentWriter::_write_raw_data(const std::vector<Slice>& slices) {
    RETURN_IF_ERROR(_output_file->appendv(&slices[0], slices.size()));
    return Status::OK();
}

}
}
