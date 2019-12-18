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

#include "olap/rowset/segment_v2/column_writer.h"

#include <cstddef>

#include "common/logging.h" // for LOG
#include "env/env.h" // for LOG
#include "gutil/strings/substitute.h" // for Substitute
#include "olap/rowset/segment_v2/bitmap_index_writer.h"
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/options.h" // for PageBuilderOptions
#include "olap/rowset/segment_v2/ordinal_page_index.h" // for OrdinalPageIndexBuilder
#include "olap/rowset/segment_v2/page_builder.h" // for PageBuilder
#include "olap/rowset/segment_v2/page_compression.h"
#include "olap/rowset/segment_v2/bloom_filter_index_writer.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/types.h" // for TypeInfo
#include "util/crc32c.h"
#include "util/faststring.h" // for fastring
#include "util/rle_encoding.h" // for RleEncoder
#include "util/block_compression.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

class NullBitmapBuilder {
public:
    NullBitmapBuilder() : _bitmap_buf(512), _rle_encoder(&_bitmap_buf, 1) {
    }

    explicit NullBitmapBuilder(size_t reserve_bits)
        : _bitmap_buf(BitmapSize(reserve_bits)), _rle_encoder(&_bitmap_buf, 1) {
    }

    void add_run(bool value, size_t run) {
        _rle_encoder.Put(value, run);
    }

    OwnedSlice finish() {
        _rle_encoder.Flush();
        return _bitmap_buf.build();
    }

    void reset() {
        _rle_encoder.Clear();
    }

    uint64_t size() {
        return _bitmap_buf.size();
    }
private:
    faststring _bitmap_buf;
    RleEncoder<bool> _rle_encoder;
};

ColumnWriter::ColumnWriter(const ColumnWriterOptions& opts,
                           std::unique_ptr<Field> field,
                           bool is_nullable,
                           WritableFile* output_file)
        : _opts(opts),
        _is_nullable(is_nullable),
        _output_file(output_file),
        _field(std::move(field)) {
}

ColumnWriter::~ColumnWriter() {
    // delete all page
    Page* page = _pages.head;
    while (page != nullptr) {
        Page* next_page = page->next;
        delete page;
        page = next_page;
    }
}

Status ColumnWriter::init() {
    RETURN_IF_ERROR(EncodingInfo::get(_field->type_info(), _opts.encoding_type, &_encoding_info));
    if (_opts.compression_type != NO_COMPRESSION) {
        RETURN_IF_ERROR(get_block_compression_codec(_opts.compression_type, &_compress_codec));
    }

    // create page builder
    PageBuilder* page_builder = nullptr;
    PageBuilderOptions opts;
    opts.data_page_size = _opts.data_page_size;
    RETURN_IF_ERROR(_encoding_info->create_page_builder(opts, &page_builder));
    if (page_builder == nullptr) {
        return Status::NotSupported(
            Substitute("Failed to create page builder for type $0 and encoding $1",
                       _field->type(), _opts.encoding_type));
    }
    _page_builder.reset(page_builder);
    // create ordinal builder
    _ordinal_index_builder.reset(new OrdinalPageIndexBuilder());
    // create null bitmap builder
    if (_is_nullable) {
        _null_bitmap_builder.reset(new NullBitmapBuilder());
    }
    if (_opts.need_zone_map) {
        _column_zone_map_builder.reset(new ColumnZoneMapBuilder(_field.get()));
    }
    if (_opts.need_bitmap_index) {
        RETURN_IF_ERROR(BitmapIndexWriter::create(_field->type_info(), &_bitmap_index_builder));
    }
    if (_opts.need_bloom_filter) {
        RETURN_IF_ERROR(BloomFilterIndexWriter::create(BloomFilterOptions(),
                _field->type_info(), &_bloom_filter_index_builder));
    }
    return Status::OK();
}

Status ColumnWriter::append_nulls(size_t num_rows) {
    _null_bitmap_builder->add_run(true, num_rows);
    _next_rowid += num_rows;
    if (_opts.need_zone_map) {
        RETURN_IF_ERROR(_column_zone_map_builder->add(nullptr, 1));
    }
    if (_opts.need_bitmap_index) {
        _bitmap_index_builder->add_nulls(num_rows);
    }
    if (_opts.need_bloom_filter) {
        _bloom_filter_index_builder->add_nulls(num_rows);
    }
    return Status::OK();
}

Status ColumnWriter::append(const void* data, size_t num_rows) {
    return _append_data((const uint8_t**)&data, num_rows);
}

// append data to page builder. this function will make sure that
// num_rows must be written before return. And ptr will be modified
// to next data should be written
Status ColumnWriter::_append_data(const uint8_t** ptr, size_t num_rows) {
    size_t remaining = num_rows;
    while (remaining > 0) {
        size_t num_written = remaining;
        RETURN_IF_ERROR(_page_builder->add(*ptr, &num_written));
        if (_opts.need_zone_map) {
            RETURN_IF_ERROR(_column_zone_map_builder->add(*ptr, num_written));
        }
        if (_opts.need_bitmap_index) {
            _bitmap_index_builder->add_values(*ptr, num_written);
        }
        if (_opts.need_bloom_filter) {
            _bloom_filter_index_builder->add_values(*ptr, num_written);
        }

        bool is_page_full = (num_written < remaining);
        remaining -= num_written;
        _next_rowid += num_written;
        *ptr += _field->size() * num_written;
        // we must write null bits after write data, because we don't
        // know how many rows can be written into current page
        if (_is_nullable) {
            _null_bitmap_builder->add_run(false, num_written);
        }

        if (is_page_full) {
            RETURN_IF_ERROR(_finish_current_page());
        }
    }
    return Status::OK();
}

Status ColumnWriter::append_nullable(
        const uint8_t* is_null_bits, const void* data, size_t num_rows) {
    const uint8_t* ptr = (const uint8_t*)data;
    BitmapIterator null_iter(is_null_bits, num_rows);
    bool is_null = false;
    size_t this_run = 0;
    while ((this_run = null_iter.Next(&is_null)) > 0) {
        if (is_null) {
            _null_bitmap_builder->add_run(true, this_run);
            _next_rowid += this_run;
            if (_opts.need_zone_map) {
                RETURN_IF_ERROR(_column_zone_map_builder->add(nullptr, 1));
            }
            if (_opts.need_bitmap_index) {
                _bitmap_index_builder->add_nulls(this_run);
            }
            if (_opts.need_bloom_filter) {
                _bloom_filter_index_builder->add_nulls(this_run);
            }
        } else {
            RETURN_IF_ERROR(_append_data(&ptr, this_run));
        }
    }
    return Status::OK();
}

uint64_t ColumnWriter::estimate_buffer_size() {
    uint64_t size = 0;
    Page* page = _pages.head;
    while (page != nullptr) {
        size += page->data.slice().size;
        if (_is_nullable) {
            size += page->null_bitmap.slice().get_size();
        }
        page = page->next;
    }
    size += _page_builder->size();
    if (_is_nullable) {
        size += _null_bitmap_builder->size();
    }
    size += _ordinal_index_builder->size();
    if (_opts.need_zone_map) {
        size += _column_zone_map_builder->size();
    }
    if (_opts.need_bitmap_index) {
        size += _bitmap_index_builder->size();
    }
    if (_opts.need_bloom_filter) {
        size += _bloom_filter_index_builder->size();
    }
    return size;
}

Status ColumnWriter::finish() {
    return _finish_current_page();
}

Status ColumnWriter::write_data() {
    Page* page = _pages.head;
    while (page != nullptr) {
        RETURN_IF_ERROR(_write_data_page(page));
        page = page->next;
    }
    // write column dict
    if (_encoding_info->encoding() == DICT_ENCODING) {
        OwnedSlice dict_page;
        _page_builder->get_dictionary_page(&dict_page);
        std::vector<Slice> origin_data;
        origin_data.push_back(dict_page.slice());
        RETURN_IF_ERROR(_write_physical_page(&origin_data, &_dict_page_pp));
    }
    return Status::OK();
}

Status ColumnWriter::write_ordinal_index() {
    Slice data = _ordinal_index_builder->finish();
    std::vector<Slice> slices{data};
    return _write_physical_page(&slices, &_ordinal_index_pp);
}

Status ColumnWriter::write_zone_map() {
    if (_opts.need_zone_map) {
        OwnedSlice data = _column_zone_map_builder->finish();
        std::vector<Slice> slices{data.slice()};
        return _write_physical_page(&slices, &_zone_map_pp);
    }
    return Status::OK();
}

Status ColumnWriter::write_bitmap_index() {
    if (!_opts.need_bitmap_index) {
        return Status::OK();
    }
    return _bitmap_index_builder->finish(_output_file, &_bitmap_index_meta);
}

Status ColumnWriter::write_bloom_filter_index() {
    if (!_opts.need_bloom_filter) {
        return Status::OK();
    }
    return _bloom_filter_index_builder->finish(_output_file, &_bloom_filter_index_meta);
}

void ColumnWriter::write_meta(ColumnMetaPB* meta) {
    meta->set_type(_field->type());
    meta->set_encoding(_encoding_info->encoding());
    // should store more concrete encoding type instead of DEFAULT_ENCODING
    // because the default encoding of a data type can be changed in the future
    DCHECK_NE(meta->encoding(), DEFAULT_ENCODING);
    meta->set_compression(_opts.compression_type);
    meta->set_is_nullable(_is_nullable);
    _ordinal_index_pp.to_proto(meta->mutable_ordinal_index_page());
    if (_opts.need_zone_map) {
        _zone_map_pp.to_proto(meta->mutable_zone_map_page());
        _column_zone_map_builder->fill_segment_zone_map(meta->mutable_zone_map());
    }
    if (_encoding_info->encoding() == DICT_ENCODING) {
        _dict_page_pp.to_proto(meta->mutable_dict_page());
    }
    if (_opts.need_bitmap_index) {
        meta->mutable_bitmap_index()->CopyFrom(_bitmap_index_meta);
    }
    if (_opts.need_bloom_filter) {
        meta->mutable_bloom_filter_index()->CopyFrom(_bloom_filter_index_meta);
    }
}

// write a page into file and update ordinal index
// this function will call _write_physical_page to write data
Status ColumnWriter::_write_data_page(Page* page) {
    std::vector<Slice> origin_data;
    faststring header;
    // 1. first rowid
    put_varint32(&header, page->first_rowid);
    // 2. row count
    put_varint32(&header, page->num_rows);
    if (_is_nullable) {
        put_varint32(&header, page->null_bitmap.slice().get_size());
    }
    origin_data.emplace_back(header.data(), header.size());
    if (_is_nullable) {
        origin_data.push_back(page->null_bitmap.slice());
    }
    origin_data.push_back(page->data.slice());
    // TODO(zc): upadte page's statistics

    PagePointer pp;
    RETURN_IF_ERROR(_write_physical_page(&origin_data, &pp));
    _ordinal_index_builder->append_entry(page->first_rowid, pp);
    return Status::OK();
}

// write a physical page in to files
Status ColumnWriter::_write_physical_page(std::vector<Slice>* origin_data, PagePointer* pp) {
    std::vector<Slice>* output_data = origin_data;
    std::vector<Slice> compressed_data;

    // Put compressor out of if block, because we will use compressor's
    // content until this function finished.
    PageCompressor compressor(_compress_codec);
    if (_compress_codec != nullptr) {
        RETURN_IF_ERROR(compressor.compress(*origin_data, &compressed_data));
        output_data = &compressed_data;
    }

    // checksum
    uint8_t checksum_buf[sizeof(uint32_t)];
    uint32_t checksum = crc32c::Value(*output_data);
    encode_fixed32_le(checksum_buf, checksum);
    output_data->emplace_back(checksum_buf, sizeof(uint32_t));

    // remember the offset
    pp->offset = _output_file->size();
    // write content to file
    size_t bytes_written = 0;
    RETURN_IF_ERROR(_write_raw_data(*output_data, &bytes_written));
    pp->size = bytes_written;

    return Status::OK();
}

// write raw data into file, this is the only place to write data
Status ColumnWriter::_write_raw_data(const std::vector<Slice>& data, size_t* bytes_written) {
    auto file_size = _output_file->size();
    auto st = _output_file->appendv(&data[0], data.size());
    if (!st.ok()) {
        LOG(WARNING) << "failed to append data to file, st=" << st.to_string();
        return st;
    }
    *bytes_written = _output_file->size() - file_size;
    _written_size += *bytes_written;
    return Status::OK();
}

Status ColumnWriter::_finish_current_page() {
    if (_next_rowid == _last_first_rowid) {
        return Status::OK();
    }
    Page* page = new Page();
    page->first_rowid = _last_first_rowid;
    page->num_rows = _next_rowid - _last_first_rowid;
    page->data = _page_builder->finish();
    _page_builder->reset();
    if (_is_nullable) {
        page->null_bitmap = _null_bitmap_builder->finish();
        _null_bitmap_builder->reset();
    }
    // update last first rowid
    _last_first_rowid = _next_rowid;

    _push_back_page(page);
    if (_opts.need_zone_map) {
        RETURN_IF_ERROR(_column_zone_map_builder->flush());
    }

    if (_opts.need_bloom_filter) {
        RETURN_IF_ERROR(_bloom_filter_index_builder->flush());
    }
    return Status::OK();
}

}
}
