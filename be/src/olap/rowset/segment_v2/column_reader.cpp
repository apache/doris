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

#include "olap/rowset/segment_v2/column_reader.h"

#include "env/env.h" // for RandomAccessFile
#include "gutil/strings/substitute.h" // for Substitute
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/page_decoder.h" // for PagePointer
#include "olap/rowset/segment_v2/page_handle.h" // for PageHandle
#include "olap/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "olap/rowset/segment_v2/options.h" // for PageDecoderOptions
#include "olap/types.h" // for TypeInfo
#include "olap/column_block.h" // for ColumnBlockView
#include "util/coding.h" // for get_varint32
#include "util/rle_encoding.h" // for RleDecoder

namespace doris {
namespace segment_v2 {

using strings::Substitute;

// This contains information when one page is loaded, and ready for read
// This struct can be reused, client should call reset first before reusing
// this object
struct ParsedPage {
    ParsedPage() { }
    ~ParsedPage() {
        delete data_decoder;
    }

    PagePointer page_pointer;
    PageHandle page_handle;

    Slice null_bitmap;
    RleDecoder<bool> null_decoder;
    PageDecoder* data_decoder = nullptr;

    // first rowid for this page
    rowid_t first_rowid = 0;

    // number of rows including nulls and not-nulls
    uint32_t num_rows = 0;

    // current offset when read this page
    // this means next row we will read
    uint32_t offset_in_page = 0;
    
    bool contains(rowid_t rid) { return rid >= first_rowid && rid < (first_rowid + num_rows); }
    rowid_t last_rowid() { return first_rowid + num_rows - 1; }
    bool has_remaining() const { return offset_in_page < num_rows; }
    size_t remaining() const { return num_rows - offset_in_page; }
};

ColumnReader::ColumnReader(const ColumnReaderOptions& opts,
                           const ColumnMetaPB& meta,
                           RandomAccessFile* file)
        : _opts(opts),
        _meta(meta),
        _file(file) {
}

ColumnReader::~ColumnReader() {
}

Status ColumnReader::init() {
    _type_info = get_type_info((FieldType)_meta.type());
    if (_type_info == nullptr) {
        return Status::NotSupported(Substitute("unsupported typeinfo, type=$0", _meta.type()));
    }
    RETURN_IF_ERROR(EncodingInfo::get(_type_info, _meta.encoding(), &_encoding_info));

    // TODO(zc): do with compress type
    RETURN_IF_ERROR(_init_ordinal_index());

    return Status::OK();
}

Status ColumnReader::new_iterator(ColumnIterator** iterator) {
    *iterator = new FileColumnIterator(this);
    return Status::OK();
}

Status ColumnReader::read_page(const PagePointer& pp, PageHandle* handle) {
    // Now we read this from file. we 
    size_t data_size = pp.size;
    if (has_checksum() && data_size < sizeof(uint32_t)) {
        return Status::Corruption("Bad page, page size is too small");
    }
    if (has_checksum()) {
        data_size -= sizeof(uint32_t);
    }
    uint8_t* buf = new uint8_t[data_size];
    Slice data(buf, data_size);

    uint8_t checksum_buf[sizeof(uint32_t)];
    Slice slices[2] = { data, Slice(checksum_buf, 4) };

    bool verify_checksum = has_checksum() && _opts.verify_checksum;
    RETURN_IF_ERROR(_file->readv_at(pp.offset, slices, verify_checksum ? 2 : 1));

    if (verify_checksum) {
        // TODO(zc): verify checksum
    }

    // TODO(zc): compress
    
    *handle = PageHandle::create_from_slice(data);

    return Status::OK();
}

// initial ordinal index
Status ColumnReader::_init_ordinal_index() {
    PagePointer pp = _meta.ordinal_index_page();
    PageHandle ph;
    RETURN_IF_ERROR(read_page(pp, &ph));

    _ordinal_index.reset(new OrdinalPageIndex(ph.data()));
    RETURN_IF_ERROR(_ordinal_index->load());

    return Status::OK();
}

Status ColumnReader::seek_to_first(OrdinalPageIndexIterator* iter) {
    *iter = _ordinal_index->begin();
    if (!iter->valid()) {
        return Status::NotFound("Failed to seek to first rowid");
    }
    return Status::OK();
}

Status ColumnReader::seek_at_or_before(rowid_t rowid, OrdinalPageIndexIterator* iter) {
    *iter = _ordinal_index->seek_at_or_before(rowid);
    if (!iter->valid()) {
        return Status::NotFound(Substitute("Failed to seek to rowid $0, ", rowid));
    }
    return Status::OK();
}

FileColumnIterator::FileColumnIterator(ColumnReader* reader) : _reader(reader) {
}

FileColumnIterator::~FileColumnIterator() {
}

Status FileColumnIterator::seek_to_first() {
    RETURN_IF_ERROR(_reader->seek_to_first(&_page_iter));

    _page.reset(new ParsedPage());
    RETURN_IF_ERROR(_read_page(_page_iter, _page.get()));

    _seek_to_pos_in_page(_page.get(), 0);
    _current_rowid = 0;

    return Status::OK();
}

Status FileColumnIterator::seek_to_ordinal(rowid_t rid) {
    if (_page != nullptr && _page->contains(rid)) {
        // current page contains this row, we just
    } else {
        // we need to seek to 
        RETURN_IF_ERROR(_reader->seek_at_or_before(rid, &_page_iter));
        _page.reset(new ParsedPage());
        RETURN_IF_ERROR(_read_page(_page_iter, _page.get()));
    }

    _seek_to_pos_in_page(_page.get(), rid - _page->first_rowid);
    _current_rowid = rid;
    return Status::OK();
}

void FileColumnIterator::_seek_to_pos_in_page(ParsedPage* page, uint32_t offset_in_page) {
    if (page->offset_in_page == offset_in_page) {
        // fast path, do nothing
        return;
    }

    uint32_t pos_in_data = offset_in_page;
    if (_reader->is_nullable()) {
        rowid_t offset_in_data = 0;
        rowid_t skips = offset_in_page;

        if (offset_in_page > page->offset_in_page) {
            // forward, reuse null bitmap
            skips = offset_in_page - page->offset_in_page;
            offset_in_data = page->data_decoder->current_index();
        } else {
            // rewind null bitmap, and
            page->null_decoder = RleDecoder<bool>((const uint8_t*)page->null_bitmap.data, page->null_bitmap.size, 1);
        }

        auto skip_nulls = page->null_decoder.Skip(skips);
        pos_in_data = offset_in_data + skips - skip_nulls;
    }

    page->data_decoder->seek_to_position_in_page(pos_in_data);
    page->offset_in_page = offset_in_page;
}

Status FileColumnIterator::next_batch(size_t* n, ColumnBlock* dst) {
    ColumnBlockView column_view(dst);
    size_t remaining = *n;
    while (remaining > 0) {
        if (!_page->has_remaining()) {
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos));
            if (eos) {
                break;
            }
        }

        // number of rows to be read from this page
        size_t nrows_in_page = std::min(remaining, _page->remaining());
        size_t nrows_to_read = nrows_in_page;
        if (_reader->is_nullable()) {
            // when this column is nullable we read data in some runs
            // first we read null bits in the same value, if this is null, we 
            // don't need to read value from page.
            // If this is not null, we read data from page in batch.
            // This would be bad in case that data is arranged one by one, which
            // will lead too many function calls to PageDecoder
            while (nrows_to_read > 0) {
                bool is_null = false;
                size_t this_run = _page->null_decoder.GetNextRun(&is_null, nrows_to_read);

                // we use num_rows only for CHECK
                size_t num_rows = this_run;
                if (!is_null) {
                    RETURN_IF_ERROR(_page->data_decoder->next_batch(&num_rows, &column_view));
                    DCHECK_EQ(this_run, num_rows);
                }

                // set null bits
                column_view.set_null_bits(this_run, is_null);

                nrows_to_read -= this_run;
                _page->offset_in_page += this_run;
                column_view.advance(this_run);
                _current_rowid += this_run;
            }
        } else {
            RETURN_IF_ERROR(_page->data_decoder->next_batch(&nrows_to_read, &column_view));
            DCHECK_EQ(nrows_to_read, nrows_in_page);

            if (column_view.is_nullable()) {
                column_view.set_null_bits(nrows_to_read, false);
            }

            // set null bits to
            _page->offset_in_page += nrows_to_read;
            column_view.advance(nrows_to_read);
            _current_rowid += nrows_to_read;
        }
        remaining -= nrows_in_page;
    }
    *n -= remaining;
    return Status::OK();
}

Status FileColumnIterator::_load_next_page(bool* eos) {
    _page_iter.next();
    if (!_page_iter.valid()) {
        *eos = true;
        return Status::OK();
    }
    _page.reset(new ParsedPage());
    RETURN_IF_ERROR(_read_page(_page_iter, _page.get()));
    _seek_to_pos_in_page(_page.get(), 0);
    *eos = false;
    return Status::OK();
}

// read one page from file and parse this page to make
// it ready to read
Status FileColumnIterator::_read_page(const OrdinalPageIndexIterator& iter, ParsedPage* page) {
    page->page_pointer = iter.page();
    RETURN_IF_ERROR(_reader->read_page(page->page_pointer, &page->page_handle));
    // TODO(zc): read page from file
    Slice data = page->page_handle.data();

    // decode first rowid
    if (!get_varint32(&data, &page->first_rowid)) {
        return Status::Corruption("Bad page, failed to decode first rowid");
    }
    // decode number rows
    if (!get_varint32(&data, &page->num_rows)) {
        return Status::Corruption("Bad page, failed to decode rows count");
    }
    if (_reader->is_nullable()) {
        uint32_t null_bitmap_size = 0;
        if (!get_varint32(&data, &null_bitmap_size)) {
            return Status::Corruption("Bad page, failed to decode null bitmap size");
        }
        if (null_bitmap_size > data.size) {
            return Status::Corruption(
                Substitute("Bad page, null bitmap too large $0 vs $1", null_bitmap_size, data.size));
        }
        page->null_decoder = RleDecoder<bool>((uint8_t*)data.data, null_bitmap_size, 1);
        page->null_bitmap = Slice(data.data, null_bitmap_size);

        // remove null bitmap
        data.remove_prefix(null_bitmap_size);
    }

    // create page data decoder
    PageDecoderOptions options;
    RETURN_IF_ERROR(_reader->encoding_info()->create_page_decoder(data, options, &page->data_decoder));
    RETURN_IF_ERROR(page->data_decoder->init());

    page->offset_in_page = 0;

    return Status::OK();
}

}
}
