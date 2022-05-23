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

#include "gutil/strings/substitute.h" // for Substitute
#include "olap/key_coder.h"
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/page_io.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

Status IndexedColumnReader::load(bool use_page_cache, bool kept_in_memory) {
    _use_page_cache = use_page_cache;
    _kept_in_memory = kept_in_memory;

    _type_info = get_scalar_type_info((FieldType)_meta.data_type());
    if (_type_info == nullptr) {
        return Status::NotSupported(
                strings::Substitute("unsupported typeinfo, type=$0", _meta.data_type()));
    }
    RETURN_IF_ERROR(EncodingInfo::get(_type_info, _meta.encoding(), &_encoding_info));
    _value_key_coder = get_key_coder(_type_info->type());

    std::unique_ptr<fs::ReadableBlock> rblock;
    fs::BlockManager* block_mgr = fs::fs_util::block_manager(_path_desc.storage_medium);
    RETURN_IF_ERROR(block_mgr->open_block(_path_desc, &rblock));
    // read and parse ordinal index page when exists
    if (_meta.has_ordinal_index_meta()) {
        if (_meta.ordinal_index_meta().is_root_data_page()) {
            _sole_data_page = PagePointer(_meta.ordinal_index_meta().root_page());
        } else {
            RETURN_IF_ERROR(load_index_page(rblock.get(), _meta.ordinal_index_meta().root_page(),
                                            &_ordinal_index_page_handle, &_ordinal_index_reader));
            _has_index_page = true;
        }
    }

    // read and parse value index page when exists
    if (_meta.has_value_index_meta()) {
        if (_meta.value_index_meta().is_root_data_page()) {
            _sole_data_page = PagePointer(_meta.value_index_meta().root_page());
        } else {
            RETURN_IF_ERROR(load_index_page(rblock.get(), _meta.value_index_meta().root_page(),
                                            &_value_index_page_handle, &_value_index_reader));
            _has_index_page = true;
        }
    }
    _num_values = _meta.num_values();
    return Status::OK();
}

Status IndexedColumnReader::load_index_page(fs::ReadableBlock* rblock, const PagePointerPB& pp,
                                            PageHandle* handle, IndexPageReader* reader) {
    Slice body;
    PageFooterPB footer;
    std::unique_ptr<BlockCompressionCodec> local_compress_codec;
    RETURN_IF_ERROR(get_block_compression_codec(_meta.compression(), local_compress_codec));
    RETURN_IF_ERROR(read_page(rblock, PagePointer(pp), handle, &body, &footer, INDEX_PAGE,
                              local_compress_codec.get()));
    RETURN_IF_ERROR(reader->parse(body, footer.index_page_footer()));
    return Status::OK();
}

Status IndexedColumnReader::read_page(fs::ReadableBlock* rblock, const PagePointer& pp,
                                      PageHandle* handle, Slice* body, PageFooterPB* footer,
                                      PageTypePB type, BlockCompressionCodec* codec) const {
    PageReadOptions opts;
    opts.rblock = rblock;
    opts.page_pointer = pp;
    opts.codec = codec;
    OlapReaderStatistics tmp_stats;
    opts.stats = &tmp_stats;
    opts.use_page_cache = _use_page_cache;
    opts.kept_in_memory = _kept_in_memory;
    opts.type = type;

    return PageIO::read_and_decompress_page(opts, handle, body, footer);
}

///////////////////////////////////////////////////////////////////////////////

Status IndexedColumnIterator::_read_data_page(const PagePointer& pp) {
    // there is not init() for IndexedColumnIterator, so do it here
    if (!_compress_codec.get())
        RETURN_IF_ERROR(get_block_compression_codec(_reader->get_compression(), _compress_codec));

    PageHandle handle;
    Slice body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_reader->read_page(_rblock.get(), pp, &handle, &body, &footer, DATA_PAGE,
                                       _compress_codec.get()));
    // parse data page
    // note that page_index is not used in IndexedColumnIterator, so we pass 0
    return ParsedPage::create(std::move(handle), body, footer.data_page_footer(),
                              _reader->encoding_info(), pp, 0, &_data_page);
}

Status IndexedColumnIterator::seek_to_ordinal(ordinal_t idx) {
    DCHECK(idx >= 0 && idx <= _reader->num_values());

    if (!_reader->support_ordinal_seek()) {
        return Status::NotSupported("no ordinal index");
    }

    // it's ok to seek past the last value
    if (idx == _reader->num_values()) {
        _current_ordinal = idx;
        _seeked = true;
        return Status::OK();
    }

    if (!_data_page || !_data_page.contains(idx)) {
        // need to read the data page containing row at idx
        if (_reader->_has_index_page) {
            std::string key;
            KeyCoderTraits<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>::full_encode_ascending(&idx, &key);
            RETURN_IF_ERROR(_ordinal_iter.seek_at_or_before(key));
            RETURN_IF_ERROR(_read_data_page(_ordinal_iter.current_page_pointer()));
            _current_iter = &_ordinal_iter;
        } else {
            RETURN_IF_ERROR(_read_data_page(_reader->_sole_data_page));
        }
    }

    ordinal_t offset_in_page = idx - _data_page.first_ordinal;
    RETURN_IF_ERROR(_data_page.data_decoder->seek_to_position_in_page(offset_in_page));
    DCHECK(offset_in_page == _data_page.data_decoder->current_index());
    _data_page.offset_in_page = offset_in_page;
    _current_ordinal = idx;
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
        _reader->_value_key_coder->full_encode_ascending(key, &encoded_key);
        Status st = _value_iter.seek_at_or_before(encoded_key);
        if (st.is_not_found()) {
            // all keys in page is greater than `encoded_key`, point to the first page.
            // otherwise, we may missing some pages.
            // For example, the predicate is `col1 > 2`, and the index page is [3,5,7].
            // so the `seek_at_or_before(2)` will return Status::NotFound().
            // But actually, we expect it to point to page `3`.
            _value_iter.seek_to_first();
        } else if (!st.ok()) {
            return st;
        }
        data_page_pp = _value_iter.current_page_pointer();
        _current_iter = &_value_iter;
        if (!_data_page || _data_page.page_pointer != data_page_pp) {
            // load when it's not the same with the current
            load_data_page = true;
        }
    } else if (!_data_page) {
        // no index page, load data page for the first time
        load_data_page = true;
        data_page_pp = PagePointer(_reader->_sole_data_page);
    }

    if (load_data_page) {
        RETURN_IF_ERROR(_read_data_page(data_page_pp));
    }

    // seek inside data page
    RETURN_IF_ERROR(_data_page.data_decoder->seek_at_or_after_value(key, exact_match));
    _data_page.offset_in_page = _data_page.data_decoder->current_index();
    _current_ordinal = _data_page.first_ordinal + _data_page.offset_in_page;
    DCHECK(_data_page.contains(_current_ordinal));
    _seeked = true;
    return Status::OK();
}

Status IndexedColumnIterator::next_batch(size_t* n, ColumnBlockView* column_view) {
    DCHECK(_seeked);
    if (_current_ordinal == _reader->num_values()) {
        *n = 0;
        return Status::OK();
    }

    size_t remaining = *n;
    while (remaining > 0) {
        if (!_data_page.has_remaining()) {
            // trying to read next data page
            if (!_reader->_has_index_page) {
                break; // no more data page
            }
            bool has_next = _current_iter->move_next();
            if (!has_next) {
                break; // no more data page
            }
            RETURN_IF_ERROR(_read_data_page(_current_iter->current_page_pointer()));
        }

        size_t rows_to_read = std::min(_data_page.remaining(), remaining);
        size_t rows_read = rows_to_read;
        RETURN_IF_ERROR(_data_page.data_decoder->next_batch(&rows_read, column_view));
        DCHECK(rows_to_read == rows_read);

        _data_page.offset_in_page += rows_read;
        _current_ordinal += rows_read;
        column_view->advance(rows_read);
        remaining -= rows_read;
    }
    *n -= remaining;
    _seeked = false;
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
