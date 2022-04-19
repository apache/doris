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
// This file is copied from
// https://github.com/apache/impala/blob/branch-3.0.0/be/src/runtime/buffered-tuple-stream.inline.h
// and modified by Doris

#ifndef DORIS_BE_RUNTIME_BUFFERED_TUPLE_STREAM_INLINE_H
#define DORIS_BE_RUNTIME_BUFFERED_TUPLE_STREAM_INLINE_H

#include "runtime/buffered_tuple_stream3.h"
#include "runtime/descriptors.h"
#include "runtime/tuple_row.h"
#include "util/bit_util.h"

namespace doris {

inline int BufferedTupleStream3::NullIndicatorBytesPerRow() const {
    DCHECK(has_nullable_tuple_);
    return BitUtil::RoundUpNumBytes(fixed_tuple_sizes_.size());
}

inline uint8_t* BufferedTupleStream3::AddRowCustomBegin(int64_t size, Status* status) {
    DCHECK(!closed_);
    DCHECK(has_write_iterator());
    if (UNLIKELY(write_page_ == nullptr || write_ptr_ + size > write_end_ptr_)) {
        return AddRowCustomBeginSlow(size, status);
    }
    DCHECK(write_page_ != nullptr);
    DCHECK(write_page_->is_pinned());
    DCHECK_LE(write_ptr_ + size, write_end_ptr_);
    ++num_rows_;
    ++write_page_->num_rows;

    uint8_t* data = write_ptr_;
    write_ptr_ += size;
    return data;
}

inline void BufferedTupleStream3::AddRowCustomEnd(int64_t size) {
    if (UNLIKELY(size > default_page_len_)) AddLargeRowCustomEnd(size);
}
} // namespace doris

#endif
