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

#include "util/arena.h"
#include <assert.h>

namespace doris {

static const int kBlockSize = 4096;

Arena::Arena() : memory_usage_(0) {
    alloc_ptr_ = NULL;  // First allocation will allocate a block
    alloc_bytes_remaining_ = 0;
}

Arena::~Arena() {
    for (size_t i = 0; i < blocks_.size(); i++) {
        delete[] blocks_[i];
    }
}

char* Arena::AllocateFallback(size_t bytes) {
    if (bytes > kBlockSize / 4) {
        // Object is more than a quarter of our block size.  Allocate it separately
        // to avoid wasting too much space in leftover bytes.
        char* result = AllocateNewBlock(bytes);
        return result;
    }

    // We waste the remaining space in the current block.
    alloc_ptr_ = AllocateNewBlock(kBlockSize);
    alloc_bytes_remaining_ = kBlockSize;

    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

char* Arena::AllocateAligned(size_t bytes) {
    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed = bytes + slop;
    char* result;
    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    } else {
        // AllocateFallback always returned aligned memory
        result = AllocateFallback(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
    return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
    char* result = new char[block_bytes];
    blocks_.push_back(result);
    memory_usage_.store(MemoryUsage() + block_bytes + sizeof(char*),
            std::memory_order_relaxed);
    return result;
}

}  // namespace doris
