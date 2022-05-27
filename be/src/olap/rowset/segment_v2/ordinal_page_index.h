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
#include <memory>
#include <string>

#include "common/status.h"
#include "env/env.h"
#include "gutil/macros.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/index_page.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "util/coding.h"
#include "util/slice.h"

namespace doris {

namespace fs {
class WritableBlock;
}

namespace segment_v2 {

// Ordinal index is implemented by one IndexPage that stores the first value ordinal
// and file pointer for each data page.
// But if there is only one data page, there is no need for index page. So we store
// the file pointer to that data page directly in index meta (OrdinalIndexPB).
class OrdinalIndexWriter {
public:
    OrdinalIndexWriter() : _page_builder(new IndexPageBuilder(0, true)) {}

    void append_entry(ordinal_t ordinal, const PagePointer& data_pp);

    uint64_t size() { return _page_builder->size(); }

    Status finish(fs::WritableBlock* wblock, ColumnIndexMetaPB* meta);

private:
    DISALLOW_COPY_AND_ASSIGN(OrdinalIndexWriter);
    std::unique_ptr<IndexPageBuilder> _page_builder;
    PagePointer _last_pp;
};

class OrdinalPageIndexIterator;

class OrdinalIndexReader {
public:
    explicit OrdinalIndexReader(const FilePathDesc& path_desc, const OrdinalIndexPB* index_meta,
                                ordinal_t num_values)
            : _path_desc(path_desc), _index_meta(index_meta), _num_values(num_values) {}

    // load and parse the index page into memory
    Status load(bool use_page_cache, bool kept_in_memory);

    // the returned iter points to the largest element which is less than `ordinal`,
    // or points to the first element if all elements are greater than `ordinal`,
    // or points to "end" if all elements are smaller than `ordinal`.
    OrdinalPageIndexIterator seek_at_or_before(ordinal_t ordinal);
    inline OrdinalPageIndexIterator begin();
    inline OrdinalPageIndexIterator end();
    ordinal_t get_first_ordinal(int page_index) const { return _ordinals[page_index]; }

    ordinal_t get_last_ordinal(int page_index) const {
        return get_first_ordinal(page_index + 1) - 1;
    }

    // for test
    int32_t num_data_pages() const { return _num_pages; }

private:
    friend OrdinalPageIndexIterator;

    FilePathDesc _path_desc;
    const OrdinalIndexPB* _index_meta;
    // total number of values (including NULLs) in the indexed column,
    // equals to 1 + 'last ordinal of last data pages'
    ordinal_t _num_values;

    // valid after load
    int _num_pages = 0;
    // _ordinals[i] = first ordinal of the i-th data page,
    std::vector<ordinal_t> _ordinals;
    // _pages[i] = page pointer to the i-th data page
    std::vector<PagePointer> _pages;
};

class OrdinalPageIndexIterator {
public:
    OrdinalPageIndexIterator() : _index(nullptr), _cur_idx(-1) {}
    OrdinalPageIndexIterator(OrdinalIndexReader* index) : _index(index), _cur_idx(0) {}
    OrdinalPageIndexIterator(OrdinalIndexReader* index, int cur_idx)
            : _index(index), _cur_idx(cur_idx) {}
    bool valid() const { return _cur_idx < _index->_num_pages; }
    void next() {
        DCHECK_LT(_cur_idx, _index->_num_pages);
        _cur_idx++;
    }
    int32_t page_index() const { return _cur_idx; };
    const PagePointer& page() const { return _index->_pages[_cur_idx]; };
    ordinal_t first_ordinal() const { return _index->get_first_ordinal(_cur_idx); }
    ordinal_t last_ordinal() const { return _index->get_last_ordinal(_cur_idx); }

private:
    OrdinalIndexReader* _index;
    int32_t _cur_idx;
};

OrdinalPageIndexIterator OrdinalIndexReader::begin() {
    return OrdinalPageIndexIterator(this);
}

OrdinalPageIndexIterator OrdinalIndexReader::end() {
    return OrdinalPageIndexIterator(this, _num_pages);
}

} // namespace segment_v2
} // namespace doris
