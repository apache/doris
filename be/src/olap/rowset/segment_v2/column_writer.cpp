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
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/options.h" // for PageBuilderOptions
#include "olap/rowset/segment_v2/ordinal_page_index.h" // for OrdinalPageIndexBuilder
#include "olap/rowset/segment_v2/page_builder.h" // for PageBuilder
#include "olap/types.h" // for TypeInfo
#include "util/faststring.h" // for fastring
#include "util/rle_encoding.h" // for RleEncoder

namespace doris {
namespace segment_v2 {

using strings::Substitute;

class NullBitmapBuilder {
public:
    NullBitmapBuilder() : _offset(0), _bitmap_buf(512), _rle_encoder(&_bitmap_buf, 1) { }
    NullBitmapBuilder(size_t reserve_bits)
        : _offset(0), _bitmap_buf(BitmapSize(reserve_bits)), _rle_encoder(&_bitmap_buf, 1) { }
    void add_run(bool value, size_t run) {
        _rle_encoder.Put(value, run);
        _offset += run;
    }
    Slice finish() {
        auto len = _rle_encoder.Flush();
        return Slice(_bitmap_buf.data(), len);
    }
    void reset() {
        _offset = 0;
        _rle_encoder.Clear();
    }
    // slice returned by finish should be deleted by caller
    void release() {
        size_t capacity = _bitmap_buf.capacity();
        _bitmap_buf.release();
        _bitmap_buf.reserve(capacity);
    }
private:
    size_t _offset;
    faststring _bitmap_buf;
    RleEncoder<bool> _rle_encoder;
};

ColumnWriter::ColumnWriter(const ColumnWriterOptions& opts,
                           const TypeInfo* typeinfo,
                           bool is_nullable,
                           WritableFile* output_file)
        : _opts(opts),
        _type_info(typeinfo),
        _is_nullable(is_nullable),
        _output_file(output_file) {
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
    RETURN_IF_ERROR(EncodingInfo::get(_type_info, _opts.encoding_type, &_encoding_info));
    if (_opts.compression_type != NO_COMPRESSION) {
        // TODO(zc):
    }

    // create page builder
    PageBuilder* page_builder = nullptr;
    PageBuilderOptions opts;
    opts.data_page_size = _opts.data_page_size;
    RETURN_IF_ERROR(_encoding_info->create_page_builder(opts, &page_builder));
    if (page_builder == nullptr) {
        return Status::NotSupported(
            Substitute("Failed to create page builder for type $0 and encoding $1",
                       _type_info->type(), _opts.encoding_type));
    }
    _page_builder.reset(page_builder);
    // create ordinal builder
    _ordinal_index_builer.reset(new OrdinalPageIndexBuilder());
    // create null bitmap builder
    if (_is_nullable) {
        _null_bitmap_builder.reset(new NullBitmapBuilder());
    }
    return Status::OK();
}

Status ColumnWriter::append_nulls(size_t num_rows) {
    _null_bitmap_builder->add_run(true, num_rows);
    _next_rowid += num_rows;
    return Status::OK();
}

Status ColumnWriter::append(const void* data, size_t num_rows) {
    return _append_data((const uint8_t**)&data, num_rows);
}

// append data to page builder. this funciton will make sure that
// num_rows must be written before return. And ptr will be modifed
// to next data should be written
Status ColumnWriter::_append_data(const uint8_t** ptr, size_t num_rows) {
    size_t remaining = num_rows;
    while (remaining > 0) {
        size_t num_written = remaining;
        RETURN_IF_ERROR(_page_builder->add(*ptr, &num_written));

        bool is_page_full = (num_written < remaining);
        remaining -= num_written;
        _next_rowid += num_written;
        *ptr += _type_info->size() * num_written;
        // we must write null bits after write data, because we don't
        // know how many rows can be written into current page
        if (_is_nullable) {
            _null_bitmap_builder->add_run(false, num_written);
        }

        // TODO(zc): update statistics for this page

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
        } else {
            RETURN_IF_ERROR(_append_data(&ptr, this_run));
        }
    }
    return Status::OK();
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
    // write ordinal index
    // auto slice = _ordinal_index_builer->finish();
    // file->append
    return Status::OK();
}

Status ColumnWriter::write_ordinal_index() {
    Slice data = _ordinal_index_builer->finish();
    std::vector<Slice> slices{data};
    return _write_physical_page(&slices, &_ordinal_index_pp);
}

void ColumnWriter::write_meta(ColumnMetaPB* meta) {
    meta->set_type(_type_info->type());
    meta->set_encoding(_opts.encoding_type);
    meta->set_compression(_opts.compression_type);
    meta->set_is_nullable(_is_nullable);
    meta->set_has_checksum(_opts.need_checksum);
    _ordinal_index_pp.to_proto(meta->mutable_ordinal_index_page());
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
        put_varint32(&header, page->null_bitmap.size);
    }
    origin_data.emplace_back(header.data(), header.size());
    if (_is_nullable) {
        origin_data.push_back(page->null_bitmap);
    }
    origin_data.push_back(page->data);
    // TODO(zc): upadte page's statistics

    PagePointer pp;
    RETURN_IF_ERROR(_write_physical_page(&origin_data, &pp));

    _ordinal_index_builer->append_entry(page->first_rowid, pp);
    return Status::OK();
}

// write a physical page in to files
Status ColumnWriter::_write_physical_page(std::vector<Slice>* origin_data, PagePointer* pp) {
    std::vector<Slice>* output_data = origin_data;
    std::vector<Slice> compressed_data;
    // TODO(zc): support compress
    // if (_need_compress) {
    //     output_data = &compressed_data;
    // }

    // checksum
    uint8_t checksum_buf[sizeof(uint32_t)];
    if (_opts.need_checksum) {
        uint32_t checksum = _compute_checksum(*output_data);
        encode_fixed32_le(checksum_buf, checksum);
        output_data->emplace_back(checksum_buf, sizeof(uint32_t));
    }

    // remember the offset
    pp->offset = _output_file->size();
    // write content to file
    size_t bytes_written = 0;
    RETURN_IF_ERROR(_write_raw_data(*output_data, &bytes_written));
    pp->size = bytes_written;

    return Status::OK();
}

uint32_t ColumnWriter::_compute_checksum(const std::vector<Slice>& data) {
    // TODO(zc): compute checksum
    return 0;
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
    _page_builder->release();
    _page_builder->reset();
    if (_is_nullable) {
        page->null_bitmap = _null_bitmap_builder->finish();
        _null_bitmap_builder->release();
        _null_bitmap_builder->reset();
    }
    // update last first rowid
    _last_first_rowid = _next_rowid;

    _push_back_page(page);

    return Status::OK();
}

}
}
