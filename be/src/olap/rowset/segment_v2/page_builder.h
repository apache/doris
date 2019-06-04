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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_PAGE_BUILDER_H
#define DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_PAGE_BUILDER_H

#include <stdint.h>
#include <vector>

#include "util/slice.h"

namespace doris {

namespace segment_v2 {

// PageBuilder is used to build page
// Page is a data management unit, including:
// 1. Data Page: store encoded and compressed data
// 2. BloomFilter Page: store bloom filter of data
// 3. Ordinal Index Page: store ordinal index of data
// 4. Short Key Index Page: store short key index of data
// 5. Bitmap Index Page: store bitmap index of data
class PageBuilder {
public:
    virtual ~PageBuilder() { }

    // Used by column writer to determine whether the current page is full.
    // Column writer depends on the result to decide whether to flush current page.
    virtual bool is_page_full() = 0;

    // Add a sequence of values to the page.
    // Returns the number of values actually added, which may be less
    // than requested if the page is full.
    //
    // vals size should be decided according to the page build type
    virtual int add(const uint8_t* vals, size_t count) = 0;

    // Get the dictionary page for under dictionary encoding mode column.
    virtual bool get_dictionary_page(doris::Slice* dictionary_page);

    // Get the bitmap page for under bitmap indexed column.
    virtual bool get_bitmap_page(doris::Slice* bitmap_page);

    // Return a Slice which represents the encoded data of current page.
    //
    // This Slice points to internal data of this builder.
    virtual Slice finish(rowid_t page_first_rowid) = 0;

    // Reset the internal state of the page builder.
    //
    // Any data previously returned by finish may be invalidated by this call.
    virtual void reset() = 0;

    // Return the number of entries that have been added to the page.
    virtual size_t count() const = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(PageBuilder);
};

} // namespace segment_v2

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_PAGE_BUILDER_H
