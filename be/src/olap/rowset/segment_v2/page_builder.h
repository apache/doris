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

#include <memory>
#include <vector>

#include "common/status.h"
#include "gutil/macros.h"
#include "olap/rowset/segment_v2/common.h"
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
    PageBuilder() {}

    virtual ~PageBuilder() {}

    // Init the internal state of the page builder.
    virtual Status init() = 0;

    // Used by column writer to determine whether the current page is full.
    // Column writer depends on the result to decide whether to flush current page.
    virtual bool is_page_full() = 0;

    // Add a sequence of values to the page.
    // The number of values actually added will be returned through count, which may be less
    // than requested if the page is full.

    // check page if full before truly add, return ok when page is full so that column write
    // will switch to next page
    // vals size should be decided according to the page build type
    // TODO make sure vals is naturally-aligned to its type so that impls can use aligned load
    // instead of memcpy to copy values.
    virtual Status add(const uint8_t* vals, size_t* count) = 0;

    // Finish building the current page, return the encoded data.
    // This api should be followed by reset() before reusing the builder
    // It will return error status when memory allocated failed during finish
    virtual Status finish(OwnedSlice* owned_slice) = 0;

    // Get the dictionary page for dictionary encoding mode column.
    virtual Status get_dictionary_page(OwnedSlice* dictionary_page) {
        return Status::NotSupported("get_dictionary_page not implemented");
    }

    // Reset the internal state of the page builder.
    //
    // Any data previously returned by finish may be invalidated by this call.
    virtual Status reset() = 0;

    // Return the number of entries that have been added to the page.
    virtual size_t count() const = 0;

    // Return the total bytes of pageBuilder that have been added to the page.
    virtual uint64_t size() const = 0;

    // Return the first value in this page.
    // This method could only be called between finish() and reset().
    // Status::Error<ENTRY_NOT_FOUND> if no values have been added.
    virtual Status get_first_value(void* value) const = 0;

    // Return the last value in this page.
    // This method could only be called between finish() and reset().
    // Status::Error<ENTRY_NOT_FOUND> if no values have been added.
    virtual Status get_last_value(void* value) const = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(PageBuilder);
};

template <typename Derived>
class PageBuilderHelper : public PageBuilder {
public:
    template <typename... Args>
    static Status create(PageBuilder** builder, Args&&... args) {
        std::unique_ptr<PageBuilder> builder_uniq_ptr(new Derived(std::forward<Args>(args)...));
        RETURN_IF_ERROR(builder_uniq_ptr->init());
        *builder = builder_uniq_ptr.release();
        return Status::OK();
    }
};

} // namespace segment_v2
} // namespace doris
