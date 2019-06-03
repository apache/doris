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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_DFILE_PAGE_BUILDER_H
#define DORIS_BE_SRC_OLAP_ROWSET_DFILE_PAGE_BUILDER_H

#include <stdint.h>
#include <vector>

#include "util/slice.h"

namespace doris {

namespace dfile {

class PageBuilder {
public:
    virtual ~PageBuilder() { }

    // Used by column writer to determine whether the current page is full.
    // Column writer depends on the result to decide whether to flush current page.
    virtual bool is_page_full() = 0;

    // Get the dictionary page for under dictionary encoding mode column.
    virtual bool get_dictionary_page(Slice* dictionary_page);

    // Get the bloom filter page for under bloom filter indexed column.
    virtual bool get_bloom_filter_page(std::vector<Slice*>* bf_page);

    // Get the bitmap page for under bitmap indexed column.
    virtual bool get_bitmap_page(Slice* bitmap_page);

    // Add a sequence of values to the page.
    // Returns the number of values actually added, which may be less
    // than requested if the page is full.
    //
    // vals size should be decided according to the page build type
    virtual int add(const uint8_t* vals, size_t count) = 0;

    // Return a Slice which represents the encoded data of current page,
    // And the page pointer to the page. The offset is relative to the current column.
    // The offset of pointer should be revised in column writer.
    //
    // This Slice points to internal data of this builder.
    virtual Slice finish(rowid_t first_page_rowid) = 0;

    // Reset the internal state of the page builder.
    //
    // Any data previously returned by finish may be invalidated by this call.
    virtual void reset() = 0;

    // Return the number of entries that have been added to the page.
    virtual size_t count() const = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(PageBuilder);
};

}  // namespace dfile

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_DFILE_PAGE_BUILDER_H
