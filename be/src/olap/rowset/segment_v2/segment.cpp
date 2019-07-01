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

#include "olap/rowset/segment_v2/segment.h"

#include "common/logging.h" // LOG
#include "env/env.h" // RandomAccessFile
#include "olap/field_info.h" // FieldInfo
#include "olap/rowset/segment_v2/column_reader.h" // ColumnReader
#include "olap/rowset/segment_v2/segment_writer.h" // k_segment_magic_length
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "util/slice.h" // Slice
#include "runtime/vectorized_row_batch.h" // VectorizedRowBatch
#include "olap/short_key_index.h"
#include "olap/olap_index.h"

namespace doris {
namespace segment_v2 {

Segment::Segment(
        std::string fname, const std::vector<ColumnSchemaV2>& schema,
        size_t num_short_keys, uint32_t segment_id)
        : _fname(std::move(fname)),
        _schema(schema),
        _num_short_keys(num_short_keys),
        _segment_id(segment_id) {
}

Segment::~Segment() {
    for (auto reader : _column_readers) {
        delete reader;
    }
}

Status Segment::open() {
    RETURN_IF_ERROR(Env::Default()->new_random_access_file(
            _fname, &_input_file));
    RETURN_IF_ERROR(_input_file->size(&_file_size));
    // 24: 2 * magic + 2 * checksum + 2 * footer/header length
    if (_file_size < 24) {
        return Status::Corruption("Bad segment, file size is too small");
    }
    // parse footer to get meta
    RETURN_IF_ERROR(_parse_footer());
    // parse footer to get meta
    RETURN_IF_ERROR(_parse_header());
    // parse short key index
    RETURN_IF_ERROR(_parse_index());
    // initial all column reader
    RETURN_IF_ERROR(_initial_column_readers());
    return Status::OK();
}

Status Segment::new_iterator(std::unique_ptr<SegmentIterator>* output) {
    output->reset(new SegmentIterator(this->shared_from_this()));
    return Status::OK();
}

// read data at offset of input file, and parse data into magic and
// header/footer's length. currently return header/footer's length
Status Segment::_parse_magic_and_len(uint64_t offset, uint32_t* length) {
    // read magic and length
    uint8_t buf[8];
    Slice slice(buf, 8);
    RETURN_IF_ERROR(_input_file->read_at(offset, slice));

    if (memcmp(slice.data, k_segment_magic, k_segment_magic_length) != 0) {
        LOG(WARNING) << "Magic don't match, magic=" << std::string((char*)buf, 4);
        return Status::Corruption("Bad segment, file magic don't match");
    }
    *length = decode_fixed32_le((uint8_t*)slice.data + 4);
    return Status::OK();
}

Status Segment::_parse_footer() {
    uint64_t offset = _file_size - 8;
    uint32_t footer_length = 0;
    RETURN_IF_ERROR(_parse_magic_and_len(offset, &footer_length));

    // check file size footer + checksum
    if (offset < footer_length + 4) {
        LOG(WARNING) << "Segment file is too small, size=" << _file_size << ", footer_size=" << footer_length;
        return Status::Corruption("Bad segment, file size is too small");
    }
    offset -= footer_length + 4;

    uint8_t checksum_buf[4];
    std::string footer_buf;
    footer_buf.resize(footer_length);

    std::vector<Slice> slices = {{checksum_buf, 4}, {footer_buf.data(), footer_length}};

    RETURN_IF_ERROR(_input_file->readv_at(offset, &slices[0], 2));

    // TOOD(zc): check footer's checksum 
    if (!_footer.ParseFromString(footer_buf)) {
        return Status::Corruption("Bad segment, parse footer from PB failed");
    }

    return Status::OK();
}

Status Segment::_parse_header() {
    uint64_t offset = 0;
    uint32_t header_length = 0;
    RETURN_IF_ERROR(_parse_magic_and_len(offset, &header_length));

    offset += 8;
    if ((offset + header_length + 4) > _file_size) {
        LOG(WARNING) << "Segment file is too small, size=" << _file_size << ", header_size=" << header_length;
        return Status::Corruption("Bad segment, file too small");
    }
    std::string header_buf;
    header_buf.resize(header_length);
    uint8_t checksum_buf[4];
    std::vector<Slice> slices{{header_buf.data(), header_length}, {checksum_buf, 4}};
    RETURN_IF_ERROR(_input_file->readv_at(offset, &slices[0], 2));

    // TODO(zc): check checksum
    if (!_header.ParseFromString(header_buf)) {
        return Status::Corruption("Bad segment, parse header from PB failed");
    }
    return Status::OK();
}

// load and parse short key index
Status Segment::_parse_index() {
    _short_key_length = 0;
    _new_short_key_length = 0;

    for (size_t i = 0; i < _num_short_keys; ++i) {
        auto& field_info = _schema[i].field_info();

        _field_infos.push_back(field_info);

        _short_key_length += field_info.index_length + 1;
        if (field_info.type == OLAP_FIELD_TYPE_CHAR ||
            field_info.type == OLAP_FIELD_TYPE_VARCHAR) {
            _new_short_key_length += sizeof(Slice) + 1;
        } else {
            _new_short_key_length += field_info.index_length + 1;
        }
    }

    faststring buf;
    buf.resize(_footer.short_key_index_page().size());
    Slice slice(buf.data(), buf.size());
    RETURN_IF_ERROR(_input_file->read_at(_footer.short_key_index_page().offset(), slice));

    ShortKeyIndexDecoder decoder(slice);
    RETURN_IF_ERROR(decoder.parse());
    _mem_index.reset(new MemIndex());
    auto olap_st = _mem_index->init(_short_key_length, _new_short_key_length, _num_short_keys, &_field_infos);
    if (olap_st != OLAP_SUCCESS) {
        return Status::InternalError("Failed to init short key index");
    }
    RETURN_IF_ERROR(_mem_index->load_segment(decoder.header(), decoder.index_data()));
    return Status::OK();
}

Status Segment::_initial_column_readers() {
    // TODO(zc): Lazy init()?
    // There may be too many columns, majority of them would not be used
    // in query, so we should not init them here.
    for (int cid = 0; cid < _schema.size(); ++cid) {
        ColumnReaderOptions opts;
        std::unique_ptr<ColumnReader> reader(new ColumnReader(opts, _footer.columns(cid), _input_file.get()));
        RETURN_IF_ERROR(reader->init());

        _column_readers.emplace_back(reader.release());
    }
    return Status::OK();
}

Status Segment::new_column_iterator(uint32_t cid, ColumnIterator** iter) {
    return _column_readers[cid]->new_iterator(iter);
}

ShortKeyIndexIterator* Segment::new_short_key_index_iterator() {
    return new ShortKeyIndexIterator(_mem_index.get());
}

}
}
