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

#ifndef DORIS_BE_RUNTIME_BUFFERPOOL_FREE_LIST_H
#define DORIS_BE_RUNTIME_BUFFERPOOL_FREE_LIST_H

#include <algorithm>
#include <cstdint>
#include <vector>

#include "common/logging.h"
#include "gutil/macros.h"
#include "runtime/bufferpool/buffer_pool.h"

namespace doris {

using BufferHandle = BufferPool::BufferHandle;

/// A non-threadsafe list of free buffers.
///
/// Buffers are allocated by the caller and can be added to the list for later retrieval
/// with AddFreeBuffer(). If the list is non-empty, calling PopFreeBuffer() will return
/// one of the buffers previously added to the list. FreeList is agnostic about the size
/// or other properties of the buffers added to it.
///
/// Buffers in the list can be freed at any point, e.g. if the list is storing too many
/// free buffers (according to some policy). The caller is responsible for implementing
/// the policy and calling FreeBuffers() or FreeAll() at the appropriate times.
///
/// Address space fragmentation
/// ---------------------------
/// To reduce memory fragmentation, the free list hands out buffers with lower memory
/// addresses first and frees buffers with higher memory address first. If buffers were
/// handed out by a policy that didn't take memory address into account, over time the
/// distribution of free buffers within the address space would become essentially
/// random. If free buffers were then unmapped, there would be many holes in the virtual
/// memory map, which can cause difficulties for the OS in some cases, e.g. exceeding the
/// maximum number of mmapped() regions (vm.max_map_count) in Linux. Using this approach
/// will tend to consolidate free buffers in higher parts of the address space, allowing
/// coalescing of the holes in most cases.
class FreeList {
public:
    FreeList() {}

    /// Gets a free buffer. If the list is non-empty, returns true and sets 'buffer' to
    /// one of the buffers previously added with AddFreeBuffer(). Otherwise returns false.
    bool PopFreeBuffer(BufferHandle* buffer) {
        if (free_list_.empty()) return false;
        std::pop_heap(free_list_.begin(), free_list_.end(), HeapCompare);
        *buffer = std::move(free_list_.back());
        free_list_.pop_back();
        return true;
    }

    /// Adds a free buffer to the list.
    void AddFreeBuffer(BufferHandle&& buffer) {
        buffer.Poison();
        free_list_.emplace_back(std::move(buffer));
        std::push_heap(free_list_.begin(), free_list_.end(), HeapCompare);
    }

    /// Get the 'num_buffers' buffers with the highest memory address from the list to
    /// free. The average time complexity is n log n, where n is the current size of the
    /// list.
    std::vector<BufferHandle> GetBuffersToFree(int64_t num_buffers) {
        std::vector<BufferHandle> buffers;
        DCHECK_LE(num_buffers, free_list_.size());
        // Sort the list so we can free the buffers with higher memory addresses.
        // Note that the sorted list is still a valid min-heap.
        std::sort(free_list_.begin(), free_list_.end(), SortCompare);

        for (int64_t i = 0; i < num_buffers; ++i) {
            buffers.emplace_back(std::move(free_list_.back()));
            free_list_.pop_back();
        }
        return buffers;
    }

    /// Returns the number of buffers currently in the list.
    int64_t Size() const { return free_list_.size(); }

private:
    friend class FreeListTest;

    DISALLOW_COPY_AND_ASSIGN(FreeList);

    /// Compare function that orders by memory address.
    static bool SortCompare(const BufferHandle& b1, const BufferHandle& b2) {
        return b1.data() < b2.data();
    }

    /// Compare function that orders by memory address. Needs to be inverse of SortCompare()
    /// because C++ provides a max-heap.
    static bool HeapCompare(const BufferHandle& b1, const BufferHandle& b2) {
        return SortCompare(b2, b1);
    }

    /// List of free memory buffers. Maintained as a min-heap ordered by the memory address
    /// of the buffer.
    std::vector<BufferHandle> free_list_;
};
} // namespace doris

#endif
