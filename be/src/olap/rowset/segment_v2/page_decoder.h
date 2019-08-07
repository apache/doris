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

#include "olap/column_block.h" // for ColumnBlockView
#include "olap/rowset/segment_v2/common.h" // for rowid_t
#include "common/status.h" // for Status

namespace doris {
namespace segment_v2 {

// PageDecoder is used to decode page page.
class PageDecoder {
public:
    PageDecoder() { }

    virtual ~PageDecoder() { }

    // Call this to do some preparation for decoder.
    // eg: parse data page header
    virtual Status init() = 0;

    // Seek the decoder to the given positional index of the page.
    // For example, seek_to_position_in_page(0) seeks to the first
    // stored entry.
    //
    // It is an error to call this with a value larger than Count().
    // Doing so has undefined results.
    virtual Status seek_to_position_in_page(size_t pos) = 0;

    // Seek the decoder forward by a given number of rows, or to the end
    // of the page. This is primarily used to skip over data.
    //
    // Return the step skipped.
    virtual size_t seek_forward(size_t n) {
        size_t step = std::min(n, count() - current_index());
        DCHECK_GE(step, 0);
        seek_to_position_in_page(current_index() + step);
        return step;
    }

    // Fetch the next vector of values from the page into 'column_vector_view'.
    // The output vector must have space for up to n cells.
    //
    // Return the size of read entries .
    //
    // In the case that the values are themselves references
    // to other memory (eg Slices), the referred-to memory is
    // allocated in the column_vector_view's mem_pool.
    virtual Status next_batch(size_t* n, ColumnBlockView* dst) = 0;

    // Return the number of elements in this page.
    virtual size_t count() const = 0;

    // Return the position within the page of the currently seeked
    // entry (ie the entry that will next be returned by next_vector())
    virtual size_t current_index() const = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(PageDecoder);
};

} // namespace segment_v2
} // namespace doris
