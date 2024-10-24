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

#include "vec/exec/format/parquet/arrow_memory_pool.h"

#include "glog/logging.h"

namespace doris::vectorized {

// A static piece of memory for 0-size allocations, so as to return
// an aligned non-null pointer.  Note the correct value for DebugAllocator
// checks is hardcoded.
alignas(kDefaultBufferAlignment) int64_t zero_size_area[1] = {kDebugXorSuffix};

arrow::Status ArrowAllocator::allocate_aligned(int64_t size, int64_t alignment, uint8_t** out) {
    if (size == 0) {
        *out = kZeroSizeArea;
        return arrow::Status::OK();
    }
    *out = reinterpret_cast<uint8_t*>(_allocator.alloc(size, alignment));
    if (*out == nullptr) {
        return arrow::Status::OutOfMemory("malloc of size ", size, " failed");
    }
    return arrow::Status::OK();
}

arrow::Status ArrowAllocator::reallocate_aligned(int64_t old_size, int64_t new_size,
                                                 int64_t alignment, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == kZeroSizeArea) {
        DCHECK_EQ(old_size, 0);
        return allocate_aligned(new_size, alignment, ptr);
    }
    if (new_size == 0) {
        deallocate_aligned(previous_ptr, old_size, alignment);
        *ptr = kZeroSizeArea;
        return arrow::Status::OK();
    }
    *ptr = reinterpret_cast<uint8_t*>(_allocator.realloc(*ptr, static_cast<size_t>(old_size),
                                                         static_cast<size_t>(new_size), alignment));
    if (*ptr == nullptr) {
        *ptr = previous_ptr;
        return arrow::Status::OutOfMemory("realloc of size ", new_size, " failed");
    }
    return arrow::Status::OK();
}

void ArrowAllocator::deallocate_aligned(uint8_t* ptr, int64_t size, int64_t alignment) {
    if (ptr == kZeroSizeArea) {
        DCHECK_EQ(size, 0);
    } else {
        _allocator.free(ptr, static_cast<size_t>(size));
    }
}

void ArrowAllocator::release_unused() {
    _allocator.release_unused();
}

} // namespace doris::vectorized
