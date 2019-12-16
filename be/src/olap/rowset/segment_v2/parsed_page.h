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

#include "olap/rowset/segment_v2/page_decoder.h" // for PagePointer
#include "util/rle_encoding.h" // for RleDecoder

namespace doris {
namespace segment_v2 {

class PageHandle;	
struct PagePointer;

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

    uint32_t page_index = 0;

    bool contains(rowid_t rid) { return rid >= first_rowid && rid < (first_rowid + num_rows); }
    rowid_t last_rowid() { return first_rowid + num_rows - 1; }
    bool has_remaining() const { return offset_in_page < num_rows; }
    size_t remaining() const { return num_rows - offset_in_page; }
};

}
}