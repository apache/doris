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

#include <utility>

#include "common/logging.h" // LOG
#include "gutil/strings/substitute.h"
#include "olap/fs/fs_util.h"
#include "olap/rowset/segment_v2/column_reader.h" // ColumnReader
#include "olap/rowset/segment_v2/empty_segment_iterator.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/rowset/segment_v2/segment_writer.h" // k_segment_magic_length
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "util/crc32c.h"
#include "util/slice.h" // Slice

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status Segment::open(const FilePathDesc& path_desc, uint32_t segment_id,
                     const TabletSchema* tablet_schema, std::shared_ptr<Segment>* output) {
    std::shared_ptr<Segment> segment(new Segment(path_desc, segment_id, tablet_schema));
    if (!path_desc.is_remote()) {
        RETURN_IF_ERROR(segment->_open());
    }
    output->swap(segment);
    return Status::OK();
}

Segment::Segment(const FilePathDesc& path_desc, uint32_t segment_id,
                 const TabletSchema* tablet_schema)
        : _path_desc(path_desc), _segment_id(segment_id), _tablet_schema(tablet_schema) {
#ifndef BE_TEST
    _mem_tracker = StorageEngine::instance()->tablet_mem_tracker();
#else
    _mem_tracker = MemTracker::get_process_tracker();
#endif
}

Segment::~Segment() {
    _mem_tracker->release(_mem_tracker->consumption());
}

Status Segment::_open() {
    RETURN_IF_ERROR(_parse_footer());
    RETURN_IF_ERROR(_create_column_readers());
    _is_open = true;
    return Status::OK();
}

Status Segment::new_iterator(const Schema& schema, const StorageReadOptions& read_options,
                             std::unique_ptr<RowwiseIterator>* iter) {
    if (!_is_open) {
        RETURN_IF_ERROR(_open());
    }
    read_options.stats->total_segment_number++;
    // trying to prune the current segment by segment-level zone map
    if (read_options.conditions != nullptr) {
        for (auto& column_condition : read_options.conditions->columns()) {
            int32_t column_id = column_condition.first;
            if (_column_readers[column_id] == nullptr ||
                !_column_readers[column_id]->has_zone_map()) {
                continue;
            }
            if (!_column_readers[column_id]->match_condition(column_condition.second)) {
                // any condition not satisfied, return.
                iter->reset(new EmptySegmentIterator(schema));
                read_options.stats->filtered_segment_number++;
                return Status::OK();
            }
        }
    }

    RETURN_IF_ERROR(_load_index());
    iter->reset(new SegmentIterator(this->shared_from_this(), schema));
    iter->get()->init(read_options);
    return Status::OK();
}

Status Segment::_parse_footer() {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::unique_ptr<fs::ReadableBlock> rblock;
    fs::BlockManager* block_mgr = fs::fs_util::block_manager(_path_desc);
    RETURN_IF_ERROR(block_mgr->open_block(_path_desc, &rblock));

    uint64_t file_size;
    RETURN_IF_ERROR(rblock->size(&file_size));

    if (file_size < 12) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < 12",
                                                      _path_desc.filepath, file_size));
    }

    uint8_t fixed_buf[12];
    RETURN_IF_ERROR(rblock->read(file_size - 12, Slice(fixed_buf, 12)));

    // validate magic number
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: magic number not match",
                                                      _path_desc.filepath));
    }

    // read footer PB
    uint32_t footer_length = decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < $2",
                                                      _path_desc.filepath, file_size,
                                                      12 + footer_length));
    }
    _mem_tracker->consume(footer_length);

    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(rblock->read(file_size - 12 - footer_length, footer_buf));

    // validate footer PB's checksum
    uint32_t expect_checksum = decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(strings::Substitute(
                "Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2",
                _path_desc.filepath, actual_checksum, expect_checksum));
    }

    // deserialize footer PB
    if (!_footer.ParseFromString(footer_buf)) {
        return Status::Corruption(strings::Substitute(
                "Bad segment file $0: failed to parse SegmentFooterPB", _path_desc.filepath));
    }
    return Status::OK();
}

Status Segment::_load_index() {
    return _load_index_once.call([this] {
        // read and parse short key index page
        std::unique_ptr<fs::ReadableBlock> rblock;
        fs::BlockManager* block_mgr = fs::fs_util::block_manager(_path_desc);
        RETURN_IF_ERROR(block_mgr->open_block(_path_desc, &rblock));

        PageReadOptions opts;
        opts.rblock = rblock.get();
        opts.page_pointer = PagePointer(_footer.short_key_index_page());
        opts.codec = nullptr; // short key index page uses NO_COMPRESSION for now
        OlapReaderStatistics tmp_stats;
        opts.stats = &tmp_stats;
        opts.type = INDEX_PAGE;

        Slice body;
        PageFooterPB footer;
        RETURN_IF_ERROR(PageIO::read_and_decompress_page(opts, &_sk_index_handle, &body, &footer));
        DCHECK_EQ(footer.type(), SHORT_KEY_PAGE);
        DCHECK(footer.has_short_key_page_footer());

        _mem_tracker->consume(body.get_size());
        _sk_index_decoder.reset(new ShortKeyIndexDecoder);
        return _sk_index_decoder->parse(body, footer.short_key_page_footer());
    });
}

Status Segment::_create_column_readers() {
    for (uint32_t ordinal = 0; ordinal < _footer.columns().size(); ++ordinal) {
        auto& column_pb = _footer.columns(ordinal);
        _column_id_to_footer_ordinal.emplace(column_pb.unique_id(), ordinal);
    }

    _column_readers.resize(_tablet_schema->columns().size());
    for (uint32_t ordinal = 0; ordinal < _tablet_schema->num_columns(); ++ordinal) {
        auto& column = _tablet_schema->columns()[ordinal];
        auto iter = _column_id_to_footer_ordinal.find(column.unique_id());
        if (iter == _column_id_to_footer_ordinal.end()) {
            continue;
        }

        ColumnReaderOptions opts;
        opts.kept_in_memory = _tablet_schema->is_in_memory();
        std::unique_ptr<ColumnReader> reader;
        RETURN_IF_ERROR(ColumnReader::create(opts, _footer.columns(iter->second),
                                             _footer.num_rows(), _path_desc, &reader));
        _column_readers[ordinal] = std::move(reader);
    }
    return Status::OK();
}

Status Segment::new_column_iterator(uint32_t cid, ColumnIterator** iter) {
    if (_column_readers[cid] == nullptr) {
        const TabletColumn& tablet_column = _tablet_schema->column(cid);
        if (!tablet_column.has_default_value() && !tablet_column.is_nullable()) {
            return Status::InternalError("invalid nonexistent column without default value.");
        }
        auto type_info = get_type_info(&tablet_column);
        std::unique_ptr<DefaultValueColumnIterator> default_value_iter(
                new DefaultValueColumnIterator(
                        tablet_column.has_default_value(), tablet_column.default_value(),
                        tablet_column.is_nullable(), std::move(type_info), tablet_column.length()));
        ColumnIteratorOptions iter_opts;

        RETURN_IF_ERROR(default_value_iter->init(iter_opts));
        *iter = default_value_iter.release();
        return Status::OK();
    }
    return _column_readers[cid]->new_iterator(iter);
}

Status Segment::new_bitmap_index_iterator(uint32_t cid, BitmapIndexIterator** iter) {
    if (_column_readers[cid] != nullptr && _column_readers[cid]->has_bitmap_index()) {
        return _column_readers[cid]->new_bitmap_index_iterator(iter);
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
