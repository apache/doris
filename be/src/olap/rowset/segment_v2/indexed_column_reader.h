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

#include <memory>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "olap/column_block.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/index_page.h"
#include "olap/rowset/segment_v2/page_handle.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "olap/rowset/segment_v2/parsed_page.h"
#include "util/block_compression.h"
#include "util/slice.h"
#include "util/file_cache.h"
#include "util/file_manager.h"

namespace doris {

class KeyCoder;
class RandomAccessFile;
class TypeInfo;

namespace segment_v2 {

class EncodingInfo;
class IndexedColumnIterator;

// thread-safe reader for IndexedColumn (see comments of `IndexedColumnWriter` to understand what IndexedColumn is)
class IndexedColumnReader {
public:
    explicit IndexedColumnReader(const std::string& file_name, const IndexedColumnMetaPB& meta)
        : _file_name(file_name), _meta(meta) {};

    Status load();

    // read a page from file into a page handle
    // use file(usually is RandomAccessFile*) to read page
    Status read_page(RandomAccessFile* file, const PagePointer& pp, PageHandle* handle) const;

    int64_t num_values() const { return _num_values; }

    const EncodingInfo* encoding_info() const { return _encoding_info; }

    const TypeInfo* type_info() const { return _type_info; }

    bool support_ordinal_seek() const { return _meta.has_ordinal_index_meta(); }

    bool support_value_seek() const { return _meta.has_value_index_meta(); }

private:
    friend class IndexedColumnIterator;

    std::string _file_name;
    IndexedColumnMetaPB _meta;
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

    bool _verify_checksum = true;
    const TypeInfo* _type_info = nullptr;
    const EncodingInfo* _encoding_info = nullptr;
    const BlockCompressionCodec* _compress_codec = nullptr;
    const KeyCoder* _validx_key_coder = nullptr;
};

class IndexedColumnIterator {
public:
    explicit IndexedColumnIterator(const IndexedColumnReader* reader)
        : _reader(reader),
          _ordinal_iter(&reader->_ordinal_index_reader),
          _value_iter(&reader->_value_index_reader),
          _file(nullptr) {
        auto st = FileManager::instance()->open_file(_reader->_file_name, &_file_handle);
        DCHECK(st.ok());
        WARN_IF_ERROR(st, "open file failed:" + _reader->_file_name);
        _file = _file_handle.file();
    }

    // Seek to the given ordinal entry. Entry 0 is the first entry.
    // Return NotFound if provided seek point is past the end.
    // Return NotSupported for column without ordinal index.
    Status seek_to_ordinal(rowid_t idx);

    // Seek the index to the given key, or to the index entry immediately
    // before it. Then seek the data block to the value matching value or to
    // the value immediately after it.
    //
    // Sets *exact_match to indicate whether the seek found the exact
    // key requested.
    //
    // Return NotFound if the given key is greater than all keys in this column.
    // Return NotSupported for column without value index.
    Status seek_at_or_after(const void* key, bool* exact_match);

    // Get the ordinal index that the iterator is currently pointed to.
    rowid_t get_current_ordinal() const;

    // After one seek, we can only call this function once to read data
    // into ColumnBlock. when read string type data, memory will allocated
    // from Arena
    Status next_batch(size_t* n, ColumnBlockView* column_view);
private:
    Status _read_data_page(const PagePointer& page_pointer, ParsedPage* page);

    const IndexedColumnReader* _reader;
    // iterator for ordinal index page
    IndexPageIterator _ordinal_iter;
    // iterator for value index page
    IndexPageIterator _value_iter;

    bool _seeked = false;
    // current in-use index iterator, could be `&_ordinal_iter` or `&_value_iter` or null
    IndexPageIterator* _current_iter = nullptr;
    // seeked data page, containing value at `_current_rowid`
    std::unique_ptr<ParsedPage> _data_page;
    // next_batch() will read from this position
    rowid_t _current_rowid = 0;
    // open file handle
    OpenedFileHandle<RandomAccessFile> _file_handle;
    // file to read
    RandomAccessFile* _file;
};

} // namespace segment_v2
} // namespace doris