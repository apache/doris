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
#include "gutil/strings/substitute.h"
#include "olap/rowset/segment_v2/column_reader.h" // ColumnReader
#include "olap/rowset/segment_v2/segment_writer.h" // k_segment_magic_length
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "util/slice.h" // Slice
#include "olap/tablet_schema.h"
#include "util/hash_util.hpp"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Segment::Segment(
        std::string fname, uint32_t segment_id,
        const TabletSchema* tablet_schema)
        : _fname(std::move(fname)),
        _segment_id(segment_id),
        _tablet_schema(tablet_schema) {
}

Segment::~Segment() {
    for (auto reader : _column_readers) {
        delete reader;
    }
}

Status Segment::open() {
    RETURN_IF_ERROR(Env::Default()->new_random_access_file(_fname, &_input_file));
    RETURN_IF_ERROR(_input_file->size(&_file_size));

    // 24: 1 * magic + 1 * checksum + 1 * footer length
    if (_file_size < 12) {
        return Status::Corruption(
            Substitute("Bad segment, file size is too small, real=$0 vs need=$1",
                       _file_size, 12));
    }

    // check header's magic
    RETURN_IF_ERROR(_check_magic(0));

    // parse footer to get meta
    RETURN_IF_ERROR(_parse_footer());
    // parse short key index
    RETURN_IF_ERROR(_parse_index());
    // initial all column reader
    RETURN_IF_ERROR(_initial_column_readers());
    return Status::OK();
}

std::unique_ptr<SegmentIterator> Segment::new_iterator(const Schema& schema, const StorageReadOptions& read_options) {
    auto it = std::unique_ptr<SegmentIterator>(new SegmentIterator(this->shared_from_this(), schema));
    it->init(read_options);
    return it;
}

// Read data at offset of input file, check if the file content match the magic
Status Segment::_check_magic(uint64_t offset) {
    // read magic and length
    uint8_t buf[k_segment_magic_length];
    Slice slice(buf, k_segment_magic_length);
    RETURN_IF_ERROR(_input_file->read_at(offset, slice));

    if (memcmp(slice.data, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption(
            Substitute("Bad segment, file magic don't match, magic=$0 vs need=$1",
                       std::string((char*)buf, k_segment_magic_length), k_segment_magic));
    }
    return Status::OK();
}

Status Segment::_parse_footer() {
    uint64_t offset = _file_size - 8;
    // read footer's length and checksum
    uint8_t buf[8];
    Slice slice(buf, 8);
    RETURN_IF_ERROR(_input_file->read_at(offset, slice));

    // check file size footer
    uint32_t footer_length = decode_fixed32_le((uint8_t*)slice.data);
    if (offset < footer_length) {
        return Status::Corruption(
            Substitute("Bad segment, file size is too small, file_size=$0 vs footer_size=$1",
                       _file_size, footer_length));
    }
    offset -= footer_length;

    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(_input_file->read_at(offset, footer_buf));

    uint32_t expect_checksum = decode_fixed32_le((uint8_t*)slice.data + 4);
    uint32_t actual_checksum = HashUtil::crc_hash(footer_buf.data(), footer_buf.size(), 0);
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
            Substitute("Bad segment, segment footer checksum not match, actual=$0 vs expect=$1",
                       actual_checksum, expect_checksum));
    }

    if (!_footer.ParseFromString(footer_buf)) {
        return Status::Corruption("Bad segment, parse footer from PB failed");
    }

    return Status::OK();
}

// load and parse short key index
Status Segment::_parse_index() {
    // read short key index content
    _sk_index_buf.resize(_footer.short_key_index_page().size());
    Slice slice(_sk_index_buf.data(), _sk_index_buf.size());
    RETURN_IF_ERROR(_input_file->read_at(_footer.short_key_index_page().offset(), slice));

    // Parse short key index
    _sk_index_decoder.reset(new ShortKeyIndexDecoder(_sk_index_buf));
    RETURN_IF_ERROR(_sk_index_decoder->parse());
    return Status::OK();
}

Status Segment::_initial_column_readers() {
    // Map from column unique id to column ordinal in footer's ColumnMetaPB
    // If we can't find unique id, it means this segment is created
    // with an old schema. So we should create a DefaultValueIterator
    // for this column.
    std::unordered_map<uint32_t, uint32_t> unique_id_to_ordinal;
    for (uint32_t ordinal = 0; ordinal < _footer.columns().size(); ++ordinal) {
        auto& column_pb = _footer.columns(ordinal);
        unique_id_to_ordinal.emplace(column_pb.unique_id(), ordinal);
    }
    // TODO(zc): Lazy init()?
    // There may be too many columns, majority of them would not be used
    // in query, so we should not init them here.
    _column_readers.resize(_tablet_schema->columns().size(), nullptr);

    for (uint32_t ordinal = 0; ordinal < _tablet_schema->num_columns(); ++ordinal) {
        auto& column = _tablet_schema->columns()[ordinal];
        auto iter = unique_id_to_ordinal.find(column.unique_id());
        if (iter == unique_id_to_ordinal.end()) {
            continue;
        }

        ColumnReaderOptions opts;
        std::unique_ptr<ColumnReader> reader(
            new ColumnReader(opts, _footer.columns(iter->second), _footer.num_rows(), _input_file.get()));
        RETURN_IF_ERROR(reader->init());

        _column_readers[ordinal] = reader.release();
    }
    return Status::OK();
}

Status Segment::new_column_iterator(uint32_t cid, ColumnIterator** iter) {
    if (_column_readers[cid] == nullptr) {
        // TODO(zc): create a DefaultValueIterator for this column
        // create
    }
    return _column_readers[cid]->new_iterator(iter);
}

}
}
