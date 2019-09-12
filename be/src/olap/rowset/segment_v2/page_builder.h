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

#include <stdint.h>
#include <vector>

#include "gutil/macros.h"
#include "util/slice.h"
#include "common/status.h"
#include "olap/rowset/segment_v2/common.h"

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
    PageBuilder() { }

    virtual ~PageBuilder() { }

    // Used by column writer to determine whether the current page is full.
    // Column writer depends on the result to decide whether to flush current page.
    virtual bool is_page_full() = 0;

    // Add a sequence of values to the page.
    // The number of values actually added will be returned through count, which may be less
    // than requested if the page is full.
    //
    // vals size should be decided according to the page build type
    virtual doris::Status add(const uint8_t* vals, size_t* count) = 0;

    // Get the dictionary page for dictionary encoding mode column.
    virtual Status get_dictionary_page(Slice* dictionary_page) {
        return Status::NotSupported("get_dictionary_page not implemented");
    }

    // Return a Slice which represents the encoded data of current page.
    //
    // This Slice points to internal data of this builder.
    virtual Slice finish() = 0;

    // Reset the internal state of the page builder.
    //
    // Any data previously returned by finish may be invalidated by this call.
    virtual void reset() = 0;

    // Return the number of entries that have been added to the page.
    virtual size_t count() const = 0;

    // Return the total bytes of pageBuilder that have been added to the page.
    virtual uint64_t size() const = 0;

    // This api is for release the resource owned by builder
    // It means it will transfer the ownership of some resource to other.
    // This api is always called after finish
    // and should be followed by reset() before reuse the builder
    virtual void release() = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(PageBuilder);
};

} // namespace segment_v2
} // namespace doris
