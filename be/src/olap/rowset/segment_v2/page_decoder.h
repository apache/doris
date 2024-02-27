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

#include "common/status.h" // for Status
#include "vec/columns/column.h"

namespace doris {
namespace segment_v2 {

// PageDecoder is used to decode page.
class PageDecoder {
public:
    PageDecoder() {}

    virtual ~PageDecoder() {}

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

    // Seek the decoder to the given value in the page, or the
    // lowest value which is greater than the given value.
    //
    // If the decoder was able to locate an exact match, then
    // sets *exact_match to true. Otherwise sets *exact_match to
    // false, to indicate that the seeked value is _after_ the
    // requested value.
    //
    // If the given value is less than the lowest value in the page,
    // seeks to the start of the page. If it is higher than the highest
    // value in the page, then returns Status::Error<ENTRY_NOT_FOUND>
    //
    // This will only return valid results when the data page
    // consists of values in sorted order.
    virtual Status seek_at_or_after_value(const void* value, bool* exact_match) {
        return Status::NotSupported("seek_at_or_after_value"); // FIXME
    }

    // Seek the decoder forward by a given number of rows, or to the end
    // of the page. This is primarily used to skip over data.
    //
    // Return the step skipped.
    virtual size_t seek_forward(size_t n) {
        size_t step = std::min(n, count() - current_index());
        DCHECK_GE(step, 0);
        static_cast<void>(seek_to_position_in_page(current_index() + step));
        return step;
    }

    virtual Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst) = 0;

    virtual Status read_by_rowids(const rowid_t* rowids, ordinal_t page_first_ordinal, size_t* n,
                                  vectorized::MutableColumnPtr& dst) {
        return Status::NotSupported("not implement vec op now");
    }

    // Same as `next_batch` except for not moving forward the cursor.
    // When read array's ordinals in `ArrayFileColumnIterator`, we want to read one extra ordinal
    // but do not want to move forward the cursor.
    virtual Status peek_next_batch(size_t* n, vectorized::MutableColumnPtr& dst) {
        return Status::NotSupported("not implement vec op now");
    }

    // Return the number of elements in this page.
    virtual size_t count() const = 0;

    // Return the position within the page of the currently seeked
    // entry (ie the entry that will next be returned by next_vector())
    virtual size_t current_index() const = 0;

    bool has_remaining() const { return current_index() < count(); }

private:
    DISALLOW_COPY_AND_ASSIGN(PageDecoder);
};

} // namespace segment_v2
} // namespace doris
