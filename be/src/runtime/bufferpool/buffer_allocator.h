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

#ifndef DORIS_BE_RUNTIME_BUFFER_ALLOCATOR_H
#define DORIS_BE_RUNTIME_BUFFER_ALLOCATOR_H

#include "runtime/bufferpool/buffer_pool_internal.h"
#include "runtime/bufferpool/free_list.h"
#include "runtime/mem_tracker.h"
#include "util/aligned_new.h"

namespace doris {

/// The internal buffer allocator used by BufferPool to allocator power-of-two sized
/// buffers. BufferAllocator builds on top of SystemAllocator by adding caching of
/// free buffers and clean pages where the memory is not currently in use by a client
/// but has not yet been released to SystemAllocator.
///
/// The allocator is optimised for the common case where an allocation can be served
/// by reclaiming a buffer of the request size from the current core's arena. In this
/// case there is no contention for locks between concurrently-running threads. If this
/// fails, progressively more expensive approaches to allocate memory are tried until
/// the allocation eventually success (see AllocateInternal() for details).
///
/// Buffer Reservations
/// ===================
/// The implementation of the BufferAllocator relies on the BufferPool's reservation
/// tracking system. The allocator is given a hard limit ('system_bytes_limit'), above
/// which all allocations will fail. Allocations up to 'system_bytes_limit' are
/// guaranteed to succeed unless an unexpected system error occurs (e.g. we can't allocate
/// all of the required memory from the OS). Reservations must be set up so that the total
/// of all reservations does not exceed 'system_bytes_limit', thus ensuring that
/// BufferAllocator can always find memory to fulfill reservations.
///
/// +========================+
/// | IMPLEMENTATION NOTES   |
/// +========================+
///
/// Memory
/// ======
/// Memory managed by BufferAllocator comes in four forms:
/// 1. Buffers returned to the client (corresponding to a used reservation)
/// 2. Free buffers cached in the BufferAllocator's free lists.
/// 3. Buffers attached to clean unpinned pages in the BufferAllocator's clean page lists.
/// 4. Bytes that are not allocated from the system: 'system_bytes_remaining_'.
/// Together these always add up to 'system_bytes_limit', which allows BufferAllocator
/// to always fulfill reservations via some combination of memory in forms 2, 3 or 4.
///
/// The BufferAllocator code is careful not to make memory inaccessible to concurrently
/// executing threads that are entitled to it. E.g. if one thread is entitled to allocate
/// a 1MB buffer from the BufferAllocator's free or clean page lists but needs to release
/// a 2MB buffer to the system to free up enough memory, it must add 1MB to
/// 'system_bytes_remaining_' in the same critical section in which it freed the 2MB
/// buffer. Otherwise a concurrent thread that had a reservation for 1MB of memory might
/// not be able to find it.
///
/// Arenas
/// ======
/// The buffer allocator's data structures are broken up into arenas, with an arena per
/// core. Within each arena, each buffer or page is stored in a list with buffers and
/// pages of the same size: there is a separate list for every power-of-two size. Each
/// arena is protected by a separate lock, so in the common case where threads are able
/// to fulfill allocations from their own arena, there will be no lock contention.
///
struct BufferPool::BufferAllocator {
    BufferAllocator(BufferPool* pool, int64_t min_buffer_len, int64_t system_bytes_limit,
                    int64_t clean_page_bytes_limit);
    ~BufferAllocator();

    /// Allocate a buffer with a power-of-two length 'len'. This function may acquire
    /// 'FreeBufferArena::lock_' and Page::lock so no locks lower in the lock acquisition
    /// order (see buffer-pool-internal.h) should be held by the caller.
    ///
    /// Always succeeds on allocating memory up to 'system_bytes_limit', unless the system
    /// is unable to give us 'system_bytes_limit' of memory or an internal bug: if all
    /// clients write out enough dirty pages to stay within their reservation, then there
    /// should always be enough free buffers and clean pages to reclaim.
    Status Allocate(ClientHandle* client, int64_t len,
                    BufferPool::BufferHandle* buffer) WARN_UNUSED_RESULT;

    /// Frees 'buffer', which must be open before calling. Closes 'buffer' and updates
    /// internal state but does not release to any reservation.
    void Free(BufferPool::BufferHandle&& buffer);

    /// Adds a clean page 'page' to a clean page list. Caller must hold the page's
    /// client's lock via 'client_lock' so that moving the page between the client list and
    /// the free page list is atomic. Caller must not hold 'FreeBufferArena::lock_' or any
    /// Page::lock.
    void AddCleanPage(const std::unique_lock<std::mutex>& client_lock, Page* page);

    /// Removes a clean page 'page' from a clean page list and returns true, if present in
    /// one of the lists. Returns true if it was present. If 'claim_buffer' is true, the
    /// caller must have reservation for the buffer, which is returned along with the page.
    /// Otherwise the buffer is moved directly to the free buffer list. Caller must hold
    /// the page's client's lock via 'client_lock' so that moving the page between the
    /// client list and the free page list is atomic. Caller must not hold
    /// 'FreeBufferArena::lock_' or any Page::lock.
    bool RemoveCleanPage(const std::unique_lock<std::mutex>& client_lock, bool claim_buffer,
                         Page* page);

    /// Periodically called to release free buffers back to the SystemAllocator. Releases
    /// buffers based on recent allocation patterns, trying to minimise the number of
    /// excess buffers retained in each list above the minimum required to avoid going
    /// to the system allocator.
    void Maintenance();

    /// Try to release at least 'bytes_to_free' bytes of memory to the system allocator.
    void ReleaseMemory(int64_t bytes_to_free);

    int64_t system_bytes_limit() const { return system_bytes_limit_; }

    /// Return the amount of memory currently allocated from the system.
    int64_t GetSystemBytesAllocated() const {
        return system_bytes_limit_ - system_bytes_remaining_.load();
    }

    /// Return the total number of free buffers in the allocator.
    int64_t GetNumFreeBuffers() const;

    /// Return the total bytes of free buffers in the allocator.
    int64_t GetFreeBufferBytes() const;

    /// Return the limit on bytes of clean pages in the allocator.
    int64_t GetCleanPageBytesLimit() const;

    /// Return the total number of clean pages in the allocator.
    int64_t GetNumCleanPages() const;

    /// Return the total bytes of clean pages in the allocator.
    int64_t GetCleanPageBytes() const;

    std::string DebugString();

protected:
    friend class BufferAllocatorTest;
    friend class BufferPoolTest;
    friend class FreeBufferArena;

    /// Test helper: gets the current size of the free list for buffers of 'len' bytes
    /// on core 'core'.
    int GetFreeListSize(int core, int64_t len);

    /// Test helper: reduce the number of scavenge attempts so backend tests can force
    /// use of the "locked" scavenging code path.
    void set_max_scavenge_attempts(int val) {
        DCHECK_GE(val, 1);
        max_scavenge_attempts_ = val;
    }

private:
    /// Compute the maximum power-of-two buffer length that could be allocated based on the
    /// amount of memory available 'system_bytes_limit'. The value is always at least
    /// 'min_buffer_len' so that there is at least one valid buffer size.
    static int64_t CalcMaxBufferLen(int64_t min_buffer_len, int64_t system_bytes_limit);

    /// Same as Allocate() but leaves 'buffer->client_' nullptr and does not update counters.
    Status AllocateInternal(int64_t len, BufferPool::BufferHandle* buffer) WARN_UNUSED_RESULT;

    /// Tries to reclaim enough memory from various sources so that the caller can allocate
    /// a buffer of 'target_bytes' from the system allocator. Scavenges buffers from the
    /// free buffer and clean page lists of all cores and frees them with
    /// 'system_allocator_'. Also tries to decrement 'system_bytes_remaining_'.
    /// 'current_core' is the index of the current CPU core. Any bytes freed in excess of
    /// 'target_bytes' are added to 'system_bytes_remaining_.' If 'slow_but_sure' is true,
    /// this function uses a slower strategy that guarantees enough memory will be found
    /// but can block progress of other threads for longer. If 'slow_but_sure' is false,
    /// then this function optimistically tries to reclaim the memory but may not reclaim
    /// 'target_bytes' of memory. Returns the number of bytes reclaimed.
    int64_t ScavengeBuffers(bool slow_but_sure, int current_core, int64_t target_bytes);

    /// Helper to free a list of buffers to the system. Returns the number of bytes freed.
    int64_t FreeToSystem(std::vector<BufferHandle>&& buffers);

    /// Compute a sum over all arenas. Does not lock the arenas.
    int64_t SumOverArenas(std::function<int64_t(FreeBufferArena* arena)> compute_fn) const;

    /// The pool that this allocator is associated with.
    BufferPool* const pool_;

    /// System allocator that is ultimately used to allocate and free buffers.
    const std::unique_ptr<SystemAllocator> system_allocator_;

    /// The minimum power-of-two buffer length that can be allocated.
    const int64_t min_buffer_len_;

    /// The maximum power-of-two buffer length that can be allocated. Always >=
    /// 'min_buffer_len' so that there is at least one valid buffer size.
    const int64_t max_buffer_len_;

    /// The log2 of 'min_buffer_len_'.
    const int log_min_buffer_len_;

    /// The log2 of 'max_buffer_len_'.
    const int log_max_buffer_len_;

    /// The maximum physical memory in bytes that will be allocated from the system.
    const int64_t system_bytes_limit_;

    /// The remaining number of bytes of 'system_bytes_limit_' that can be used for
    /// allocating new buffers. Must be updated atomically before a new buffer is
    /// allocated or after an existing buffer is freed with the system allocator.
    std::atomic<int64_t> system_bytes_remaining_;

    /// The maximum bytes of clean pages that can accumulate across all arenas before
    /// they will be evicted.
    const int64_t clean_page_bytes_limit_;

    /// The number of bytes of 'clean_page_bytes_limit_' not used by clean pages. I.e.
    /// (clean_page_bytes_limit - bytes of clean pages in the BufferAllocator).
    /// 'clean_pages_bytes_limit_' is enforced by increasing this value before a
    /// clean page is added and decreasing it after a clean page is reclaimed or evicted.
    std::atomic<int64_t> clean_page_bytes_remaining_;

    /// Free and clean pages. One arena per core.
    std::vector<std::unique_ptr<FreeBufferArena>> per_core_arenas_;

    /// Default number of times to attempt scavenging.
    static const int MAX_SCAVENGE_ATTEMPTS = 3;

    /// Number of times to attempt scavenging. Usually MAX_SCAVENGE_ATTEMPTS but can be
    /// overridden by tests. The first max_scavenge_attempts_ - 1 attempts do not lock
    /// all arenas so may fail. The final attempt locks all arenas, which is expensive
    /// but is guaranteed to succeed.
    int max_scavenge_attempts_;

    std::shared_ptr<MemTracker> _mem_tracker;
};
} // namespace doris

#endif
