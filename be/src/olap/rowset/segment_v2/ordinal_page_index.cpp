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

#include "olap/rowset/segment_v2/ordinal_page_index.h"

namespace doris {
namespace segment_v2 {

OrdinalPageIndex::~OrdinalPageIndex() {
    delete _rowids;
    delete _pages;
}

Status OrdinalPageIndex::load() {
    DCHECK_GE(_data.size, _header_size()) << "block size must greate than header";
    const uint8_t* ptr = (const uint8_t*)_data.data;
    const uint8_t* limit = (const uint8_t*)_data.data + _data.size;

    _num_pages = decode_fixed32_le(ptr);
    ptr += 4;

    _rowids = new rowid_t[_num_pages];
    _pages = new PagePointer[_num_pages];
    for (int i = 0; i < _num_pages; ++i) {
        ptr = decode_varint32_ptr(ptr, limit, &_rowids[i]);
        if (ptr == nullptr) {
            return Status::InternalError("Data corruption");
        }
        ptr = _pages[i].decode_from(ptr, limit);
        if (ptr == nullptr) {
            return Status::InternalError("Data corruption");
        }
    }
    return Status::OK();
}

OrdinalPageIndexIterator OrdinalPageIndex::seek_at_or_before(rowid_t rid) {
    int32_t left = 0;
    int32_t right = _num_pages - 1;
    while (left < right) {
        int32_t mid = (left + right + 1) / 2;

        if (_rowids[mid] < rid) {
            left = mid;
        } else if (_rowids[mid] > rid) {
            right = mid - 1;
        } else {
            left = mid;
            break;
        }
    }
    if (_rowids[left] > rid) {
        return OrdinalPageIndexIterator(this, _num_pages);
    }
    return OrdinalPageIndexIterator(this, left);
}

}
}
