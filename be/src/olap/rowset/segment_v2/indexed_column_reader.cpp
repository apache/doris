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

#include "olap/rowset/segment_v2/indexed_column_reader.h"

#include "env/env.h" // for RandomAccessFile
#include "gutil/strings/substitute.h" // for Substitute
#include "olap/key_coder.h"
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/index_page.h" // for IndexPageReader
#include "olap/rowset/segment_v2/options.h" // for PageDecoderOptions
#include "olap/rowset/segment_v2/page_compression.h"
#include "olap/rowset/segment_v2/page_decoder.h" // for PagePointer
#include "util/crc32c.h"
#include "util/rle_encoding.h" // for RleDecoder
#include "util/file_manager.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status IndexedColumnReader::load() {
    _type_info = get_type_info((FieldType)_meta.data_type());
    if (_type_info == nullptr) {
        return Status::NotSupported(Substitute("unsupported typeinfo, type=$0", _meta.data_type()));
    }
    RETURN_IF_ERROR(EncodingInfo::get(_type_info, _meta.encoding(), &_encoding_info));
    RETURN_IF_ERROR(get_block_compression_codec(_meta.compression(), &_compress_codec));
    _validx_key_coder = get_key_coder(_type_info->type());

    OpenedFileHandle<RandomAccessFile> file_handle;
    RETURN_IF_ERROR(FileManager::instance()->open_file(_file_name, &file_handle));
    RandomAccessFile* input_file = file_handle.file();
    // read and parse ordinal index page when exists
    if (_meta.has_ordinal_index_meta()) {
        if (_meta.ordinal_index_meta().is_root_data_page()) {
            _sole_data_page = PagePointer(_meta.ordinal_index_meta().root_page());
        } else {
            RETURN_IF_ERROR(read_page(input_file, _meta.ordinal_index_meta().root_page(), &_ordinal_index_page_handle));
            RETURN_IF_ERROR(_ordinal_index_reader.parse(_ordinal_index_page_handle.data()));
            _has_index_page = true;
        }
    }

    // read and parse value index page when exists
    if (_meta.has_value_index_meta()) {
        if (_meta.value_index_meta().is_root_data_page()) {
            _sole_data_page = PagePointer(_meta.value_index_meta().root_page());
        } else {
            RETURN_IF_ERROR(read_page(input_file, _meta.value_index_meta().root_page(), &_value_index_page_handle));
            RETURN_IF_ERROR(_value_index_reader.parse(_value_index_page_handle.data()));
            _has_index_page = true;
        }
    }
    _num_values = _meta.num_values();
    return Status::OK();
}

Status IndexedColumnReader::read_page(RandomAccessFile* file, const PagePointer& pp, PageHandle* handle) const {
    auto cache = StoragePageCache::instance();
    PageCacheHandle cache_handle;
    StoragePageCache::CacheKey cache_key(file->file_name(), pp.offset);
    // column index only load once, so we use global config to decide
    if (!config::disable_storage_page_cache && cache->lookup(cache_key, &cache_handle)) {
        // we find page in cache, use it
        *handle = PageHandle(std::move(cache_handle));
        return Status::OK();
    }
    // Now we read this from file.
    size_t page_size = pp.size;
    if (page_size < sizeof(uint32_t)) {
        return Status::Corruption(Substitute("Bad page, page size is too small, size=$0", page_size));
    }

    // Now we use this buffer to store page from storage, if this page is compressed
    // this buffer will assigned uncompressed page, and origin content will be freed.
    std::unique_ptr<uint8_t[]> page(new uint8_t[page_size]);
    Slice page_slice(page.get(), page_size);
    RETURN_IF_ERROR(file->read_at(pp.offset, page_slice));

    size_t data_size = page_size - 4;
    if (_verify_checksum) {
        uint32_t expect = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
        uint32_t actual = crc32c::Value(page_slice.data, page_slice.size - 4);
        if (expect != actual) {
            return Status::Corruption(
                Substitute("Page checksum mismatch, actual=$0 vs expect=$1", actual, expect));
        }
    }

    // remove page's suffix
    page_slice.size = data_size;
    if (_compress_codec != nullptr) {
        PageDecompressor decompressor(page_slice, _compress_codec);

        Slice uncompressed_page;
        RETURN_IF_ERROR(decompressor.decompress_to(&uncompressed_page));

        // If decompressor create new heap memory for uncompressed data,
        // assign this uncompressed page to page and page slice
        if (uncompressed_page.data != page_slice.data) {
            page.reset((uint8_t*)uncompressed_page.data);
        }
        page_slice = uncompressed_page;
    }
    if (!config::disable_storage_page_cache) {
        // insert this into cache and return the cache handle
        cache->insert(cache_key, page_slice, &cache_handle, _cache_in_memory);
        *handle = PageHandle(std::move(cache_handle));
    } else {
        *handle = PageHandle(page_slice);
    }

    page.release();
    return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////

Status IndexedColumnIterator::_read_data_page(const PagePointer& page_pointer, ParsedPage* page) {
    RETURN_IF_ERROR(_reader->read_page(_file, page_pointer, &page->page_handle));
    Slice data = page->page_handle.data();

    // decode first rowid
    if (!get_varint32(&data, &page->first_rowid)) {
        return Status::Corruption("Bad page, failed to decode first rowid");
    }

    // decode number rows
    if (!get_varint32(&data, &page->num_rows)) {
        return Status::Corruption("Bad page, failed to decode rows count");
    }

    // create page data decoder
    PageDecoderOptions options;
    RETURN_IF_ERROR(_reader->encoding_info()->create_page_decoder(data, options, &page->data_decoder));
    RETURN_IF_ERROR(page->data_decoder->init());

    page->offset_in_page = 0;
    return Status::OK();
}

Status IndexedColumnIterator::seek_to_ordinal(rowid_t idx) {
    DCHECK(idx >= 0 && idx <= _reader->num_values());

    if (!_reader->support_ordinal_seek()) {
        return Status::NotSupported("no ordinal index");
    }

    // it's ok to seek past the last value
    if (idx == _reader->num_values()) {
        _current_rowid = idx;
        _seeked = true;
        return Status::OK();
    }

    if (_data_page == nullptr || !_data_page->contains(idx)) {
        // need to read the data page containing row at idx
        _data_page.reset(new ParsedPage());
        if (_reader->_has_index_page) {
            std::string key;
            KeyCoderTraits<OLAP_FIELD_TYPE_UNSIGNED_INT>::full_encode_ascending(&idx, &key);
            RETURN_IF_ERROR(_ordinal_iter.seek_at_or_before(key));
            RETURN_IF_ERROR(_read_data_page(_ordinal_iter.current_page_pointer(), _data_page.get()));
            _current_iter = &_ordinal_iter;
        } else {
            RETURN_IF_ERROR(_read_data_page(_reader->_sole_data_page, _data_page.get()));
        }
    }

    rowid_t offset_in_page = idx - _data_page->first_rowid;
    RETURN_IF_ERROR(_data_page->data_decoder->seek_to_position_in_page(offset_in_page));
    DCHECK(offset_in_page == _data_page->data_decoder->current_index());
    _data_page->offset_in_page = offset_in_page;
    _current_rowid = idx;
    _seeked = true;
    return Status::OK();
}

Status IndexedColumnIterator::seek_at_or_after(const void* key, bool* exact_match) {
    if (!_reader->support_value_seek()) {
        return Status::NotSupported("no value index");
    }

    if (_reader->num_values() == 0) {
        return Status::NotFound("value index is empty ");
    }

    bool load_data_page = false;
    PagePointer data_page_pp;
    if (_reader->_has_index_page) {
        // seek index to determine the data page to seek
        std::string encoded_key;
        _reader->_validx_key_coder->full_encode_ascending(key, &encoded_key);
        RETURN_IF_ERROR(_value_iter.seek_at_or_before(encoded_key));
        data_page_pp = _value_iter.current_page_pointer();
        _current_iter = &_value_iter;
        if (_data_page == nullptr || _data_page->page_pointer != data_page_pp) {
            // load when it's not the same with the current
            load_data_page = true;
        }
    } else if (!_data_page) {
        // no index page, load data page for the first time
        load_data_page = true;
        data_page_pp = PagePointer(_reader->_sole_data_page);
    }

    if (load_data_page) {
        _data_page.reset(new ParsedPage());
        RETURN_IF_ERROR(_read_data_page(data_page_pp, _data_page.get()));
    }

    // seek inside data page
    RETURN_IF_ERROR(_data_page->data_decoder->seek_at_or_after_value(key, exact_match));
    _data_page->offset_in_page = _data_page->data_decoder->current_index();
    _current_rowid = _data_page->first_rowid + _data_page->offset_in_page;
    DCHECK(_data_page->contains(_current_rowid));
    _seeked = true;
    return Status::OK();
}

rowid_t IndexedColumnIterator::get_current_ordinal() const {
    DCHECK(_seeked);
    return _current_rowid;
}

Status IndexedColumnIterator::next_batch(size_t* n, ColumnBlockView* column_view) {
    DCHECK(_seeked);
    if (_current_rowid == _reader->num_values()) {
        *n = 0;
        return Status::OK();
    }

    size_t remaining = *n;
    while (remaining > 0) {
        if (!_data_page->has_remaining()) {
            // trying to read next data page
            if (!_reader->_has_index_page) {
                break; // no more data page
            }
            bool has_next = _current_iter->move_next();
            if (!has_next) {
                break; // no more data page
            }
            _data_page.reset(new ParsedPage());
            RETURN_IF_ERROR(_read_data_page(_current_iter->current_page_pointer(), _data_page.get()));
        }

        size_t rows_to_read = std::min(_data_page->remaining(), remaining);
        size_t rows_read = rows_to_read;
        RETURN_IF_ERROR(_data_page->data_decoder->next_batch(&rows_read, column_view));
        DCHECK(rows_to_read == rows_read);

        _data_page->offset_in_page += rows_read;
        _current_rowid += rows_read;
        column_view->advance(rows_read);
        remaining -= rows_read;
    }
    *n -= remaining;
    _seeked = false;
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris