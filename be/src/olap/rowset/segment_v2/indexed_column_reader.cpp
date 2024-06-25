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

#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>

#include "common/status.h"
#include "gutil/strings/substitute.h" // for Substitute
#include "io/io_common.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/page_decoder.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/types.h"
#include "util/block_compression.h"
#include "util/bvar_helper.h"

namespace doris {
using namespace ErrorCode;
namespace segment_v2 {

static bvar::Adder<uint64_t> g_index_reader_bytes("doris_pk", "index_reader_bytes");
static bvar::Adder<uint64_t> g_index_reader_compressed_bytes("doris_pk",
                                                             "index_reader_compressed_bytes");
static bvar::PerSecond<bvar::Adder<uint64_t>> g_index_reader_bytes_per_second(
        "doris_pk", "index_reader_bytes_per_second", &g_index_reader_bytes, 60);
static bvar::Adder<uint64_t> g_index_reader_pages("doris_pk", "index_reader_pages");
static bvar::PerSecond<bvar::Adder<uint64_t>> g_index_reader_pages_per_second(
        "doris_pk", "index_reader_pages_per_second", &g_index_reader_pages, 60);
static bvar::Adder<uint64_t> g_index_reader_cached_pages("doris_pk", "index_reader_cached_pages");
static bvar::PerSecond<bvar::Adder<uint64_t>> g_index_reader_cached_pages_per_second(
        "doris_pk", "index_reader_cached_pages_per_second", &g_index_reader_cached_pages, 60);
static bvar::Adder<uint64_t> g_index_reader_seek_count("doris_pk", "index_reader_seek_count");
static bvar::PerSecond<bvar::Adder<uint64_t>> g_index_reader_seek_per_second(
        "doris_pk", "index_reader_seek_per_second", &g_index_reader_seek_count, 60);
static bvar::Adder<uint64_t> g_index_reader_pk_pages("doris_pk", "index_reader_pk_pages");
static bvar::PerSecond<bvar::Adder<uint64_t>> g_index_reader_pk_bytes_per_second(
        "doris_pk", "index_reader_pk_pages_per_second", &g_index_reader_pk_pages, 60);

static bvar::Adder<uint64_t> g_index_reader_memory_bytes("doris_index_reader_memory_bytes");

using strings::Substitute;

Status IndexedColumnReader::load(bool use_page_cache, bool kept_in_memory) {
    _use_page_cache = use_page_cache;
    _kept_in_memory = kept_in_memory;

    _type_info = get_scalar_type_info((FieldType)_meta.data_type());
    if (_type_info == nullptr) {
        return Status::NotSupported("unsupported typeinfo, type={}", _meta.data_type());
    }
    RETURN_IF_ERROR(EncodingInfo::get(_type_info, _meta.encoding(), &_encoding_info));
    _value_key_coder = get_key_coder(_type_info->type());

    // read and parse ordinal index page when exists
    if (_meta.has_ordinal_index_meta()) {
        if (_meta.ordinal_index_meta().is_root_data_page()) {
            _sole_data_page = PagePointer(_meta.ordinal_index_meta().root_page());
        } else {
            RETURN_IF_ERROR(load_index_page(_meta.ordinal_index_meta().root_page(),
                                            &_ordinal_index_page_handle, &_ordinal_index_reader));
            _has_index_page = true;
        }
    }

    // read and parse value index page when exists
    if (_meta.has_value_index_meta()) {
        if (_meta.value_index_meta().is_root_data_page()) {
            _sole_data_page = PagePointer(_meta.value_index_meta().root_page());
        } else {
            RETURN_IF_ERROR(load_index_page(_meta.value_index_meta().root_page(),
                                            &_value_index_page_handle, &_value_index_reader));
            _has_index_page = true;
        }
    }
    _num_values = _meta.num_values();

    g_index_reader_memory_bytes << sizeof(*this);
    return Status::OK();
}

Status IndexedColumnReader::load_index_page(const PagePointerPB& pp, PageHandle* handle,
                                            IndexPageReader* reader) {
    Slice body;
    PageFooterPB footer;
    BlockCompressionCodec* local_compress_codec;
    RETURN_IF_ERROR(get_block_compression_codec(_meta.compression(), &local_compress_codec));
    RETURN_IF_ERROR(read_page(PagePointer(pp), handle, &body, &footer, INDEX_PAGE,
                              local_compress_codec, false));
    RETURN_IF_ERROR(reader->parse(body, footer.index_page_footer()));
    _mem_size += body.get_size();
    return Status::OK();
}

Status IndexedColumnReader::read_page(const PagePointer& pp, PageHandle* handle, Slice* body,
                                      PageFooterPB* footer, PageTypePB type,
                                      BlockCompressionCodec* codec, bool pre_decode) const {
    OlapReaderStatistics tmp_stats;
    PageReadOptions opts {
            .use_page_cache = _use_page_cache,
            .kept_in_memory = _kept_in_memory,
            .pre_decode = pre_decode,
            .type = type,
            .file_reader = _file_reader.get(),
            .page_pointer = pp,
            .codec = codec,
            .stats = &tmp_stats,
            .encoding_info = _encoding_info,
            .io_ctx = io::IOContext {.is_index_data = true},
    };
    if (_is_pk_index) {
        opts.type = PRIMARY_KEY_INDEX_PAGE;
    }
    auto st = PageIO::read_and_decompress_page(opts, handle, body, footer);
    g_index_reader_compressed_bytes << pp.size;
    g_index_reader_bytes << footer->uncompressed_size();
    g_index_reader_pages << 1;
    g_index_reader_cached_pages << tmp_stats.cached_pages_num;
    return st;
}

IndexedColumnReader::~IndexedColumnReader() {
    g_index_reader_memory_bytes << -sizeof(*this);
}

///////////////////////////////////////////////////////////////////////////////

Status IndexedColumnIterator::_read_data_page(const PagePointer& pp) {
    Status status;
    // there is not init() for IndexedColumnIterator, so do it here
    if (!_compress_codec) {
        RETURN_IF_ERROR(get_block_compression_codec(_reader->get_compression(), &_compress_codec));
    }

    PageHandle handle;
    Slice body;
    PageFooterPB footer;
    RETURN_IF_ERROR(
            _reader->read_page(pp, &handle, &body, &footer, DATA_PAGE, _compress_codec, true));
    // parse data page
    // note that page_index is not used in IndexedColumnIterator, so we pass 0
    PageDecoderOptions opts;
    opts.need_check_bitmap = false;
    status = ParsedPage::create(std::move(handle), body, footer.data_page_footer(),
                                _reader->encoding_info(), pp, 0, &_data_page, opts);
    DCHECK(_reader->_meta.ordinal_index_meta().is_root_data_page()
                   ? _reader->_meta.num_values() == _data_page.num_rows
                   : true);
    return status;
}

Status IndexedColumnIterator::seek_to_ordinal(ordinal_t idx) {
    DCHECK(idx <= _reader->num_values());

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
            KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>::full_encode_ascending(&idx,
                                                                                              &key);
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
        return Status::Error<ErrorCode::ENTRY_NOT_FOUND>("value index is empty ");
    }

    g_index_reader_seek_count << 1;

    bool load_data_page = false;
    PagePointer data_page_pp;
    if (_reader->_has_index_page) {
        // seek index to determine the data page to seek
        std::string encoded_key;
        _reader->_value_key_coder->full_encode_ascending(key, &encoded_key);
        Status st = _value_iter.seek_at_or_before(encoded_key);
        if (st.is<ENTRY_NOT_FOUND>()) {
            // all keys in page is greater than `encoded_key`, point to the first page.
            // otherwise, we may missing some pages.
            // For example, the predicate is `col1 > 2`, and the index page is [3,5,7].
            // so the `seek_at_or_before(2)` will return Status::Error<ENTRY_NOT_FOUND>().
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
    Status st = _data_page.data_decoder->seek_at_or_after_value(key, exact_match);
    // return the first row of next page when not found
    if (st.is<ENTRY_NOT_FOUND>() && _reader->_has_index_page) {
        if (_value_iter.has_next()) {
            _seeked = true;
            *exact_match = false;
            _current_ordinal = _data_page.first_ordinal + _data_page.num_rows;
            // move offset to the end of the page
            _data_page.offset_in_page = _data_page.num_rows;
            return Status::OK();
        }
    }
    RETURN_IF_ERROR(st);
    _data_page.offset_in_page = _data_page.data_decoder->current_index();
    _current_ordinal = _data_page.first_ordinal + _data_page.offset_in_page;
    DCHECK(_data_page.contains(_current_ordinal));
    _seeked = true;
    return Status::OK();
}

Status IndexedColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
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
        RETURN_IF_ERROR(_data_page.data_decoder->next_batch(&rows_read, dst));
        DCHECK(rows_to_read == rows_read);

        _data_page.offset_in_page += rows_read;
        _current_ordinal += rows_read;
        remaining -= rows_read;
    }
    *n -= remaining;
    _seeked = false;
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
