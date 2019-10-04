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
#include "util/crc32c.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Segment::Segment(
        std::string fname, uint32_t segment_id,
        const TabletSchema* tablet_schema)
        : _fname(std::move(fname)),
        _segment_id(segment_id),
        _tablet_schema(tablet_schema),
        _index_loaded(false) {
}

Segment::~Segment() {
    for (auto reader : _column_readers) {
        delete reader;
    }
}

Status Segment::open() {
    RETURN_IF_ERROR(Env::Default()->new_random_access_file(_fname, &_input_file));
    // parse footer to get meta
    RETURN_IF_ERROR(_parse_footer());

    return Status::OK();
}

Status Segment::new_iterator(
        const Schema& schema,
        const StorageReadOptions& read_options,
        std::unique_ptr<segment_v2::SegmentIterator>* iter) {

    if (read_options.conditions != nullptr) {
        for(auto& column_condition : read_options.conditions->columns()) {
            int32_t column_id = column_condition.first;
            auto entry = _column_id_to_footer_ordinal.find(column_id);
            if (entry == _column_id_to_footer_ordinal.end()) {
                continue;
            }
            ColumnMetaPB c_meta = _footer.columns(entry->second);
            if (!c_meta.has_zone_map()) {
                continue;
            }
            ZoneMapPB c_zone_map = c_meta.zone_map();
            if (!c_zone_map.has_not_null() && !c_zone_map.has_null()) {
                // no data
                iter->reset(new EmptySegmentIterator(this->shared_from_this(), schema));
                return Status::OK();
            }
            // TODO Logic here and the similar logic in ColumnReader::_get_filtered_pages should be unified.
            TypeInfo* type_info = get_type_info((FieldType)c_meta.type());
            if (type_info == nullptr) {
                return Status::NotSupported(Substitute("unsupported typeinfo, type=$0", c_meta.type()));
            }
            FieldType type = type_info->type();
            std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type));
            std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type));
            if (c_zone_map.has_not_null()) {
                min_value->from_string(c_zone_map.min());
                max_value->from_string(c_zone_map.max());
            }
            if (c_zone_map.has_null()) {
                min_value->set_null();
                if (!c_zone_map.has_not_null()) {
                    max_value->set_null();
                }
            }
            if (!column_condition.second->eval({min_value.get(), max_value.get()})) {
                // any condition not satisfied, return.
                iter->reset(new EmptySegmentIterator(this->shared_from_this(), schema));
                return Status::OK();
            }
        }
    }


    if(!_index_loaded) {
        // parse short key index
        RETURN_IF_ERROR(_parse_index());
        // initial all column reader
        RETURN_IF_ERROR(_initial_column_readers());
        _index_loaded = true;
    }
    iter->reset(new SegmentIterator(this->shared_from_this(), schema));
    iter->get()->init(read_options);
    return Status::OK();
}

Status Segment::_parse_footer() {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    uint64_t file_size;
    RETURN_IF_ERROR(_input_file->size(&file_size));

    if (file_size < 12) {
        return Status::Corruption(Substitute("Bad segment file $0: file size $1 < 12", _fname, file_size));
    }

    uint8_t fixed_buf[12];
    RETURN_IF_ERROR(_input_file->read_at(file_size - 12, Slice(fixed_buf, 12)));

    // validate magic number
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption(Substitute("Bad segment file $0: magic number not match", _fname));
    }

    // read footer PB
    uint32_t footer_length = decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption(
            Substitute("Bad segment file $0: file size $1 < $2", _fname, file_size, 12 + footer_length));
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(_input_file->read_at(file_size - 12 - footer_length, footer_buf));

    // validate footer PB's checksum
    uint32_t expect_checksum = decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
            Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2",
                       _fname, actual_checksum, expect_checksum));
    }

    // deserialize footer PB
    if (!_footer.ParseFromString(footer_buf)) {
        return Status::Corruption(Substitute("Bad segment file $0: failed to parse SegmentFooterPB", _fname));
    }

    for (uint32_t ordinal = 0; ordinal < _footer.columns().size(); ++ordinal) {
        auto& column_pb = _footer.columns(ordinal);
        _column_id_to_footer_ordinal.emplace(column_pb.unique_id(), ordinal);
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
    // If we can't find unique id, it means this segment is created
    // with an old schema. So we should create a DefaultValueIterator
    // for this column.
    // TODO(zc): Lazy init()?
    // There may be too many columns, majority of them would not be used
    // in query, so we should not init them here.
    _column_readers.resize(_tablet_schema->columns().size(), nullptr);

    for (uint32_t ordinal = 0; ordinal < _tablet_schema->num_columns(); ++ordinal) {
        auto& column = _tablet_schema->columns()[ordinal];
        auto iter = _column_id_to_footer_ordinal.find(column.unique_id());
        if (iter == _column_id_to_footer_ordinal.end()) {
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
        const TabletColumn& tablet_column = _tablet_schema->column(cid);
        if (!tablet_column.has_default_value()) {
            return Status::InternalError("invalid nonexistent column without default value.");
        }
        std::unique_ptr<DefaultValueColumnIterator> default_value_iter(
                new DefaultValueColumnIterator(tablet_column.default_value(),
                tablet_column.is_nullable(), tablet_column.type()));
        RETURN_IF_ERROR(default_value_iter->init());
        *iter = default_value_iter.release();
        return Status::OK();
    }
    return _column_readers[cid]->new_iterator(iter);
}

}
}
