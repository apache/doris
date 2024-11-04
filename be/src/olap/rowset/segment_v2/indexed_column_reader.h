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

#pragma once

#include <gen_cpp/segment_v2.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <string>
#include <utility>

#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/index_page.h"
#include "olap/rowset/segment_v2/page_handle.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "olap/rowset/segment_v2/parsed_page.h"
#include "util/slice.h"
#include "vec/data_types/data_type.h"

namespace doris {

class KeyCoder;
class TypeInfo;
class BlockCompressionCodec;

namespace segment_v2 {

class EncodingInfo;

// thread-safe reader for IndexedColumn (see comments of `IndexedColumnWriter` to understand what IndexedColumn is)
class IndexedColumnReader : public MetadataAdder<IndexedColumnReader> {
public:
    explicit IndexedColumnReader(io::FileReaderSPtr file_reader, const IndexedColumnMetaPB& meta)
            : _file_reader(std::move(file_reader)), _meta(meta) {}

    ~IndexedColumnReader();

    Status load(bool use_page_cache, bool kept_in_memory);

    // read a page specified by `pp' from `file' into `handle'
    Status read_page(const PagePointer& pp, PageHandle* handle, Slice* body, PageFooterPB* footer,
                     PageTypePB type, BlockCompressionCodec* codec, bool pre_decode) const;

    int64_t num_values() const { return _num_values; }
    const EncodingInfo* encoding_info() const { return _encoding_info; }
    const TypeInfo* type_info() const { return _type_info; }
    bool support_ordinal_seek() const { return _meta.has_ordinal_index_meta(); }
    bool support_value_seek() const { return _meta.has_value_index_meta(); }

    CompressionTypePB get_compression() const { return _meta.compression(); }
    uint64_t get_memory_size() const { return _mem_size; }
    void set_is_pk_index(bool is_pk) { _is_pk_index = is_pk; }

private:
    Status load_index_page(const PagePointerPB& pp, PageHandle* handle, IndexPageReader* reader);

    int64_t get_metadata_size() const override;

    friend class IndexedColumnIterator;

    io::FileReaderSPtr _file_reader;
    IndexedColumnMetaPB _meta;

    bool _use_page_cache;
    bool _kept_in_memory;
    int64_t _num_values = 0;
    // whether this column contains any index page.
    // could be false when the column contains only one data page.
    bool _has_index_page = false;
    // valid only when the column contains only one data page
    PagePointer _sole_data_page;
    IndexPageReader _ordinal_index_reader;
    IndexPageReader _value_index_reader;
    PageHandle _ordinal_index_page_handle;
    PageHandle _value_index_page_handle;

    const TypeInfo* _type_info = nullptr;
    const EncodingInfo* _encoding_info = nullptr;
    const KeyCoder* _value_key_coder = nullptr;
    uint64_t _mem_size = 0;
    bool _is_pk_index = false;
};

class IndexedColumnIterator {
public:
    explicit IndexedColumnIterator(const IndexedColumnReader* reader)
            : _reader(reader),
              _ordinal_iter(&reader->_ordinal_index_reader),
              _value_iter(&reader->_value_index_reader) {}

    // Seek to the given ordinal entry. Entry 0 is the first entry.
    // Return Status::Error<ENTRY_NOT_FOUND> if provided seek point is past the end.
    // Return NotSupported for column without ordinal index.
    Status seek_to_ordinal(ordinal_t idx);

    // Seek the index to the given key, or to the index entry immediately
    // before it. Then seek the data block to the value matching value or to
    // the value immediately after it.
    //
    // Sets *exact_match to indicate whether the seek found the exact
    // key requested.
    //
    // Return Status::Error<ENTRY_NOT_FOUND> if the given key is greater than all keys in this column.
    // Return NotSupported for column without value index.
    Status seek_at_or_after(const void* key, bool* exact_match);
    Status seek_at_or_after(const std::string* key, bool* exact_match) {
        Slice slice(key->data(), key->size());
        return seek_at_or_after(static_cast<const void*>(&slice), exact_match);
    }

    // Get the ordinal index that the iterator is currently pointed to.
    ordinal_t get_current_ordinal() const {
        DCHECK(_seeked);
        return _current_ordinal;
    }

    // After one seek, we can only call this function once to read data
    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst);

private:
    Status _read_data_page(const PagePointer& pp);

    const IndexedColumnReader* _reader = nullptr;
    // iterator for ordinal index page
    IndexPageIterator _ordinal_iter;
    // iterator for value index page
    IndexPageIterator _value_iter;

    bool _seeked = false;
    // current in-use index iterator, could be `&_ordinal_iter` or `&_value_iter` or null
    IndexPageIterator* _current_iter = nullptr;
    // seeked data page, containing value at `_current_ordinal`
    ParsedPage _data_page;
    // next_batch() will read from this position
    ordinal_t _current_ordinal = 0;
    // iterator owned compress codec, should NOT be shared by threads, initialized before used
    BlockCompressionCodec* _compress_codec = nullptr;
};

} // namespace segment_v2
} // namespace doris
