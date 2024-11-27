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

#include <string.h>

#include <cstddef>
#include <memory>

#include "common/logging.h"
#include "common/status.h"
#include "runtime/thread_context.h"
#include "vec/common/allocator.h"
#include "vec/common/allocator_fwd.h"

namespace doris {

struct ByteBuffer;
using ByteBufferPtr = std::shared_ptr<ByteBuffer>;

struct ByteBuffer : private Allocator<false> {
    static Status allocate(const size_t size, ByteBufferPtr* ptr) {
        RETURN_IF_CATCH_EXCEPTION({ *ptr = ByteBufferPtr(new ByteBuffer(size)); });
        return Status::OK();
    }

    ~ByteBuffer() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(mem_tracker_);
        Allocator<false>::free(ptr, capacity);
    }

    void put_bytes(const char* data, size_t size) {
        memcpy(ptr + pos, data, size);
        pos += size;
    }

    void get_bytes(char* data, size_t size) {
        memcpy(data, ptr + pos, size);
        pos += size;
        DCHECK(pos <= limit);
    }

    void flip() {
        limit = pos;
        pos = 0;
    }

    size_t remaining() const { return limit - pos; }
    bool has_remaining() const { return limit > pos; }

    char* ptr;
    size_t pos;
    size_t limit;
    size_t capacity;

private:
    ByteBuffer(size_t capacity_)
            : pos(0),
              limit(capacity_),
              capacity(capacity_),
              mem_tracker_(doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()) {
        ptr = reinterpret_cast<char*>(Allocator<false>::alloc(capacity_));
    }

    std::shared_ptr<MemTrackerLimiter> mem_tracker_;
};

} // namespace doris
