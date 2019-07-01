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
#include "olap/row_block.h" // RowBlock
#include "olap/row_cursor.h" // RowCursor
#include "olap/rowset/segment_v2/column_writer.h" // ColumnWriter
#include "olap/short_key_index.h"

namespace doris {
namespace segment_v2 {

const char* k_segment_magic = "D0R1";
const uint32_t k_segment_magic_length = 4;

SegmentWriter::SegmentWriter(
        std::string fname, const std::vector<ColumnSchemaV2>& schema,
        size_t num_short_keys, uint32_t segment_id,
        uint32_t num_rows_per_block, bool delete_flag)
    : _fname(std::move(fname)),
    _schema(schema),
    _num_short_keys(num_short_keys),
    _segment_id(segment_id),
    _num_rows_per_block(num_rows_per_block),
    _delete_flag(delete_flag) {
}

SegmentWriter::~SegmentWriter() {
    for (auto writer : _column_writers) {
        delete writer;
    }
}

Status SegmentWriter::init(uint32_t write_mbytes_per_sec) {
    // create for write
    RETURN_IF_ERROR(Env::Default()->new_writable_file(_fname, &_output_file));

    for (size_t cid = 0; cid < _schema.size(); ++cid) {
        auto& column_schema = _schema[cid];

        ColumnMetaPB* column_meta = _footer.add_columns();
        column_meta->set_column_id(cid);
        column_meta->set_unique_id(column_schema.field_info().unique_id);
        bool is_nullable = column_schema.field_info().is_allow_null;
        column_meta->set_is_nullable(is_nullable);

        // 
        const TypeInfo* type_info = column_schema.type_info();

        ColumnWriterOptions opts;
        std::unique_ptr<ColumnWriter> writer(new ColumnWriter(opts, type_info, is_nullable, _output_file.get()));
        RETURN_IF_ERROR(writer->init());
        _column_writers.push_back(writer.release());
    }

    std::vector<ColumnSchemaV2> short_keys;
    for (size_t i = 0; i < _num_short_keys; ++i) {
        short_keys.push_back(_schema[i]);
    }
    _index_builder.reset(
        new ShortKeyIndexBuilder(short_keys, _segment_id, _num_rows_per_block, _delete_flag));
    RETURN_IF_ERROR(_index_builder->init());
    return Status::OK();
}

Status SegmentWriter::write_batch(RowBlock* block, RowCursor* cursor, bool is_finalize) {
    DCHECK_GT(block->row_num(), 0) << "Try to write emtpy block";
    // TODO(zc): make it better
    for (size_t row = 0; row < block->row_num(); ++row) {
        block->get_row(row, cursor);
        for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
            const Field* field = cursor->get_field_by_index(cid);

            bool is_null = field->is_null(cursor->get_buf());
            char* buf = field->get_field_ptr(cursor->get_buf());
            // buf + 1 to skip its 
            RETURN_IF_ERROR(_column_writers[cid]->append(is_null, buf + 1));
        }
    }
    // update key
    block->get_row(0, cursor);
    LOG(INFO) << "input cursor=" << cursor->to_string() << ", block_count=" << _block_count;
    RETURN_IF_ERROR(_index_builder->add_item(*cursor, _block_count));
    
    _row_count += block->row_num();
    ++_block_count;
    return Status::OK();
}

uint64_t SegmentWriter::estimate_segment_size() {
    return 0;
}

Status SegmentWriter::finalize(uint32_t* segment_file_size) {
    for (auto column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->finish());
    }
    RETURN_IF_ERROR(_write_header());
    RETURN_IF_ERROR(_write_data());
    RETURN_IF_ERROR(_write_ordinal_index());
    RETURN_IF_ERROR(_write_short_key_index());
    RETURN_IF_ERROR(_write_footer());
    return Status::OK();
}

// write header
Status SegmentWriter::_write_header() {
    std::string header_buf;
    SegmentHeaderPB header;
    if (!header.SerializeToString(&header_buf)) {
        return Status::InternalError("failed to serialize segment header");
    }

    // header length
    std::string magic_and_len_buf;
    magic_and_len_buf.append(k_segment_magic);
    put_fixed32_le(&magic_and_len_buf, header_buf.size());

    // checksum
    uint8_t checksum_buf[4];
    // TODO(zc): add checksum

    std::vector<Slice> slices = {magic_and_len_buf, header_buf, {checksum_buf, 4}};
    RETURN_IF_ERROR(_write_raw_data(slices));
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
    std::string magic_and_len_buf;
    magic_and_len_buf.append(k_segment_magic, k_segment_magic_length);
    put_fixed32_le(&magic_and_len_buf, footer_buf.size());

    char checksum_buf[4];
    // TODO(zc): compute checsum
    std::vector<Slice> slices{{checksum_buf, 4}, footer_buf, magic_and_len_buf};
    // write offset and length
    RETURN_IF_ERROR(_write_raw_data(slices));
    // magic
    return Status::OK();
}

Status SegmentWriter::_write_raw_data(const std::vector<Slice>& slices) {
    RETURN_IF_ERROR(_output_file->appendv(&slices[0], slices.size()));
    return Status::OK();
}

}
}
