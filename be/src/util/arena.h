// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef DORIS_BE_SRC_COMMON_UTIL_ARENA_H
#define DORIS_BE_SRC_COMMON_UTIL_ARENA_H

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <vector>

#include "common/compiler_util.h"

namespace doris {

class Arena {
public:
    Arena();
    ~Arena();

    // Return a pointer to a newly allocated memory block of "bytes" bytes.
    char* Allocate(size_t bytes);

    // Allocate memory with the normal alignment guarantees provided by malloc
    char* AllocateAligned(size_t bytes);

    // Returns an estimate of the total memory usage of data allocated
    // by the arena.
    size_t MemoryUsage() const {
        //return reinterpret_cast<uintptr_t>(memory_usage_.NoBarrier_Load());
        return memory_usage_.load(std::memory_order_relaxed);
    }

private:
    char* AllocateFallback(size_t bytes);
    char* AllocateNewBlock(size_t block_bytes);

    // Allocation state
    char* alloc_ptr_;
    size_t alloc_bytes_remaining_;

    // Array of new[] allocated memory blocks
    std::vector<char*> blocks_;

    // Total memory usage of the arena.
    std::atomic<size_t> memory_usage_;

    // No copying allowed
    Arena(const Arena&);
    void operator=(const Arena&);
};

inline char* Arena::Allocate(size_t bytes) {
    if (UNLIKELY(bytes == 0)) { return nullptr; }
    if (bytes <= alloc_bytes_remaining_) {
        char* result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_bytes_remaining_ -= bytes;
        return result;
    }
    return AllocateFallback(bytes);
}

}  // namespace doris

#endif  // DORIS_BE_SRC_COMMON_UTIL_ARENA_H
