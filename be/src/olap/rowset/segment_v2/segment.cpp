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
#include "olap/rowset/segment_v2/empty_segment_iterator.h"
#include "util/slice.h" // Slice
#include "olap/tablet_schema.h"
#include "util/crc32c.h"
#include "util/file_manager.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status Segment::open(std::string filename,
                     uint32_t segment_id,
                     const TabletSchema* tablet_schema,
                     std::shared_ptr<Segment>* output) {
    std::shared_ptr<Segment> segment(new Segment(std::move(filename), segment_id, tablet_schema));
    RETURN_IF_ERROR(segment->_open());
    output->swap(segment);
    return Status::OK();
}

Segment::Segment(
        std::string fname, uint32_t segment_id,
        const TabletSchema* tablet_schema)
        : _fname(std::move(fname)),
        _segment_id(segment_id),
        _tablet_schema(tablet_schema) {
}

Segment::~Segment() = default;

Status Segment::_open() {
    RETURN_IF_ERROR(_parse_footer());
    RETURN_IF_ERROR(_create_column_readers());
    return Status::OK();
}

Status Segment::new_iterator(const Schema& schema,
                             const StorageReadOptions& read_options,
                             std::unique_ptr<RowwiseIterator>* iter) {
    DCHECK_NOTNULL(read_options.stats);
    // trying to prune the current segment by segment-level zone map
    if (read_options.conditions != nullptr) {
        for (auto& column_condition : read_options.conditions->columns()) {
            int32_t column_id = column_condition.first;
            auto entry = _column_id_to_footer_ordinal.find(column_id);
            if (entry == _column_id_to_footer_ordinal.end()) {
                continue;
            }
            auto& c_meta = _footer.columns(entry->second);
            if (!c_meta.has_zone_map()) {
                continue;
            }
            auto& c_zone_map = c_meta.zone_map();
            if (!c_zone_map.has_not_null() && !c_zone_map.has_null()) {
                // no data
                iter->reset(new EmptySegmentIterator(schema));
                return Status::OK();
            }
            // TODO Logic here and the similar logic in ColumnReader::_get_filtered_pages should be unified.
            TypeInfo* type_info = get_type_info((FieldType)c_meta.type());
            if (type_info == nullptr) {
                return Status::NotSupported(Substitute("unsupported typeinfo, type=$0", c_meta.type()));
            }
            FieldType type = type_info->type();
            const Field* field = schema.column(column_id);
            int32_t var_length = field->length();
            std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, var_length));
            std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, var_length));
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
                iter->reset(new EmptySegmentIterator(schema));
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
    OpenedFileHandle<RandomAccessFile> file_handle;
    RETURN_IF_ERROR(FileManager::instance()->open_file(_fname, &file_handle));
    RandomAccessFile* input_file = file_handle.file();
    uint64_t file_size;
    RETURN_IF_ERROR(input_file->size(&file_size));

    if (file_size < 12) {
        return Status::Corruption(Substitute("Bad segment file $0: file size $1 < 12", _fname, file_size));
    }

    uint8_t fixed_buf[12];
    RETURN_IF_ERROR(input_file->read_at(file_size - 12, Slice(fixed_buf, 12)));

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
    RETURN_IF_ERROR(input_file->read_at(file_size - 12 - footer_length, footer_buf));

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
    return Status::OK();
}

Status Segment::_load_index() {
    return _load_index_once.call([this] {
        // read short key index content
        OpenedFileHandle<RandomAccessFile> file_handle;
        RETURN_IF_ERROR(FileManager::instance()->open_file(_fname, &file_handle));
        RandomAccessFile* input_file = file_handle.file();
        _sk_index_buf.resize(_footer.short_key_index_page().size());
        Slice slice(_sk_index_buf.data(), _sk_index_buf.size());
        RETURN_IF_ERROR(input_file->read_at(_footer.short_key_index_page().offset(), slice));

        // Parse short key index
        _sk_index_decoder.reset(new ShortKeyIndexDecoder(_sk_index_buf));
        RETURN_IF_ERROR(_sk_index_decoder->parse());
        return Status::OK();
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
        std::unique_ptr<ColumnReader> reader;
        // pass Descriptor<RandomAccessFile>* to column reader
        RETURN_IF_ERROR(ColumnReader::create(
            opts, _footer.columns(iter->second), _footer.num_rows(), _fname, &reader));
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
        std::unique_ptr<DefaultValueColumnIterator> default_value_iter(
                new DefaultValueColumnIterator(tablet_column.has_default_value(),
                tablet_column.default_value(),
                tablet_column.is_nullable(),
                tablet_column.type(),
                tablet_column.length()));
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

}
}
