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

#include <cstdint>
#include <string>

#include "common/status.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "util/coding.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

// this class encode ordinal page index
// the binary format is like that
// Header | Content
// Header: 
//      number of pages (4 Bytes)
// Content:
//      array of index_pair
// index_pair:
//      Ordinal (4 Bytes)
//      PagePointer (8 Bytes)

static const uint32_t ORDINAL_PAGE_INDEX_HEADER_SIZE = 4;

class OrdinalPageIndexBuilder {
public:
    OrdinalPageIndexBuilder() : _num_pages(0) {
        _buffer.reserve(4 * 1024);
        // reserve space for number of pages
        _buffer.resize(ORDINAL_PAGE_INDEX_HEADER_SIZE);
    }

    void append_entry(rowid_t rid, const PagePointer& page) {
        // rid
        put_varint32(&_buffer, rid);
        // page pointer
        page.encode_to(&_buffer);
        _num_pages++;
    }

    uint64_t size() {
        return _buffer.size();
    }

    Slice finish() {
        // encoded number of pages
        encode_fixed32_le((uint8_t*)_buffer.data(), _num_pages);
        return Slice(_buffer);
    }

private:
    std::string _buffer;
    uint32_t _num_pages;
};

class OrdinalPageIndex;
class OrdinalPageIndexIterator {
public:
    OrdinalPageIndexIterator() : _index(nullptr), _cur_idx(-1) { }
    OrdinalPageIndexIterator(OrdinalPageIndex* index) : _index(index), _cur_idx(0) { }
    OrdinalPageIndexIterator(OrdinalPageIndex* index, int cur_idx) : _index(index), _cur_idx(cur_idx) { }
    inline bool valid() const;
    inline void next();
    inline rowid_t rowid() const;
    inline int32_t cur_idx() const;
    inline const PagePointer& page() const;
    inline rowid_t cur_page_first_row_id() const;
    inline rowid_t cur_page_last_row_id() const;
private:
    OrdinalPageIndex* _index;
    int32_t _cur_idx;
};

// Page index 
class OrdinalPageIndex {
public:
    OrdinalPageIndex(const Slice& data, uint64_t num_rows)
        : _data(data), _num_rows(num_rows), _num_pages(0), _rowids(nullptr), _pages(nullptr) {
    }
    ~OrdinalPageIndex();
    
    Status load();

    OrdinalPageIndexIterator seek_at_or_before(rowid_t rid);
    OrdinalPageIndexIterator begin() {
        return OrdinalPageIndexIterator(this);
    }
    OrdinalPageIndexIterator end() {
        return OrdinalPageIndexIterator(this, _num_pages);
    }
    rowid_t get_first_row_id(int page_index) const {
        return _rowids[page_index];
    }

    rowid_t get_last_row_id(int page_index) const {
        // because add additional number of rows as the last rowid
        // so just return next_page_first_id - 1
        int next_page_index = page_index + 1;
        return get_first_row_id(next_page_index) - 1;
    }

    int32_t num_pages() const {
        return _num_pages;
    }

private:
    uint32_t _header_size() const { return ORDINAL_PAGE_INDEX_HEADER_SIZE; }

private:
    friend OrdinalPageIndexIterator;

    Slice _data;
    uint64_t _num_rows;

    // valid after laod
    int32_t _num_pages;
    // the last row id is additional, set to number of rows
    rowid_t* _rowids;
    PagePointer* _pages;
};

inline bool OrdinalPageIndexIterator::valid() const {
    return _cur_idx < _index->_num_pages;
}

inline void OrdinalPageIndexIterator::next() {
    DCHECK_LT(_cur_idx, _index->_num_pages);
    _cur_idx++;
}

inline rowid_t OrdinalPageIndexIterator::rowid() const {
    return _index->_rowids[_cur_idx];
}

int32_t OrdinalPageIndexIterator::cur_idx() const {
    return _cur_idx;
}

inline const PagePointer& OrdinalPageIndexIterator::page() const {
    return _index->_pages[_cur_idx];
}

rowid_t OrdinalPageIndexIterator::cur_page_first_row_id() const {
    return _index->get_first_row_id(_cur_idx);
}

rowid_t OrdinalPageIndexIterator::cur_page_last_row_id() const {
    return _index->get_last_row_id(_cur_idx);
}

}
}
