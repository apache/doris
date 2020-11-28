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

#ifndef IMPALA_RUNTIME_BUFFERPOOL_SUBALLOCATOR_H
#define IMPALA_RUNTIME_BUFFERPOOL_SUBALLOCATOR_H

#include <cstdint>
#include <memory>

#include "runtime/bufferpool/buffer_pool.h"

namespace doris {

class Suballocation;

/// Helper class to subdivide buffers from the buffer pool. Implements a buddy
/// allocation algorithm optimised for power-of-two allocations. At or above the
/// 'min_buffer_len' value, each allocation is backed by a power-of-two buffer from
/// a BufferPool. Below that threshold, each allocation is backed by a
/// 'min_buffer_len' buffer split recursively into equal-sized buddies until the
/// desired allocation size is reached. Every time an allocation is freed,
/// free buddies are coalesced eagerly and whole buffers are freed eagerly.
///
/// The algorithms used are asymptotically efficient: O(log(max allocation size)), but
/// the implementation's constant-factor overhead is not optimised. Thus, the allocator
/// is best suited for relatively large allocations where the constant CPU/memory
/// overhead per allocation is not paramount, e.g. bucket directories of hash tables.
/// All allocations less than MIN_ALLOCATION_BYTES are rounded up to that amount.
///
/// Methods of Suballocator are not thread safe.
///
/// Implementation:
/// ---------------
/// The allocator uses two key data structures: a number of binary trees representing
/// the buddy relationships between allocations and a set of free lists, one for each
/// power-of-two size.
///
/// Each buffer allocated from the buffer pool has a tree of Suballocations associated
/// with it that use the memory from that buffer. The root of the tree is the
/// Suballocation corresponding to the entire buffer. Each node has either zero children
/// (if it hasn't been split) or two children (if it has been split into two buddy
/// allocations). Each non-root Suballocation has pointers to its buddy and its parent
/// to enable coalescing the buddies into the parent when both are free.
///
/// Suballocations are eagerly coalesced when freed, so a Suballocation only has children
/// if one of its descendants is allocated.
///
/// The free lists are doubly-linked lists of free Suballocation objects that support
/// O(1) add and remove. The next and previous pointers are stored in the
/// Suballocation object so no auxiliary memory is required.
class Suballocator {
public:
    /// Constructs a suballocator that allocates memory from 'pool' with 'client'.
    /// Suballocations smaller than 'min_buffer_len' are handled by allocating a
    /// buffer of 'min_buffer_len' and recursively splitting it.
    Suballocator(BufferPool* pool, BufferPool::ClientHandle* client, int64_t min_buffer_len);

    ~Suballocator();
    /// Compute how many mem will be allocated from BufferPool. We will use it to try
    /// consume mem in BufferedBlockMgr.
    uint64_t ComputeAllocateBufferSize(int64_t bytes) const;
    /// Allocate bytes from BufferPool. The allocation is nullptr if unsuccessful because
    /// the client's reservation was insufficient. If an unexpected error is encountered,
    /// returns that status. The allocation size is rounded up to the next power-of-two.
    /// The caller must always free the allocation by calling Free() (otherwise destructing
    /// the returned 'result' will DCHECK on debug builds or otherwise misbehave on release
    /// builds).
    ///
    /// Allocate() will try to increase the client's buffer pool reservation to fulfill
    /// the requested allocation if needed.
    ///
    /// The memory returned is at least 8-byte aligned.
    Status Allocate(int64_t bytes, std::unique_ptr<Suballocation>* result);

    /// Free the allocation. Does nothing if allocation is nullptr (e.g. was the result of a
    /// failed Allocate() call). Return how many really release in BufferPool, release mem in BufferedBlockMgr.
    uint64_t Free(std::unique_ptr<Suballocation> allocation);

    /// Upper bounds on the max allocation size and the number of different
    /// power-of-two allocation sizes. Used to bound the number of free lists.
    static constexpr int LOG_MAX_ALLOCATION_BYTES = BufferPool::LOG_MAX_BUFFER_BYTES;
    static constexpr int64_t MAX_ALLOCATION_BYTES = BufferPool::MAX_BUFFER_BYTES;

    /// Don't support allocations less than 4kb to avoid high overhead.
    static constexpr int LOG_MIN_ALLOCATION_BYTES = 12;
    static constexpr int64_t MIN_ALLOCATION_BYTES = 1L << LOG_MIN_ALLOCATION_BYTES;

private:
    DISALLOW_COPY_AND_ASSIGN(Suballocator);

    /// Compute the index for allocations of size 'bytes' in 'free_lists_'. 'bytes' is
    /// rounded up to the next power-of-two if it is not already a power-of-two.
    int ComputeListIndex(int64_t bytes) const;

    /// Allocate a buffer of size 'bytes' < MAX_ALLOCATION_BYTES from the buffer pool and
    /// initialize 'result' with it. If the reservation is insufficient, try to increase
    /// the reservation to fit.
    Status AllocateBuffer(int64_t bytes, std::unique_ptr<Suballocation>* result);

    /// Split the free allocation until we get an allocation of 'target_bytes' rounded up
    /// to a power-of-two. This allocation is returned. The other allocations resulting
    /// from the splits are added to free lists. node->in_use must be false and 'node'
    /// must not be in any free list. Can fail if allocating memory for data structures
    /// fails.
    Status SplitToSize(std::unique_ptr<Suballocation> node, int64_t target_bytes,
                       std::unique_ptr<Suballocation>* result);

    // Add allocation to the free list with given index.
    void AddToFreeList(std::unique_ptr<Suballocation> node);

    // Remove allocation from its free list.
    std::unique_ptr<Suballocation> RemoveFromFreeList(Suballocation* node);

    // Get the allocation at the head of the free list at index 'list_idx'. Return nullptr
    // if list is empty.
    std::unique_ptr<Suballocation> PopFreeListHead(int list_idx);

    // Check list_idx of Free List whether is nullptr
    bool CheckFreeListHeadNotNull(int list_idx) const;

    /// Coalesce two free buddies, 'b1' and 'b2'. Frees 'b1' and 'b2' and marks the parent
    /// not in use.
    std::unique_ptr<Suballocation> CoalesceBuddies(std::unique_ptr<Suballocation> b1,
                                                   std::unique_ptr<Suballocation> b2);

    /// The pool and corresponding client to allocate buffers from.
    BufferPool* pool_;
    BufferPool::ClientHandle* client_;

    /// The minimum length of buffer to allocate. To serve allocations below this threshold,
    /// a larger buffer is allocated and split into multiple allocations.
    const int64_t min_buffer_len_;

    /// Track how much memory has been returned in allocations but not freed.
    int64_t allocated_;

    /// Free lists for each supported power-of-two size. Statically allocate the maximum
    /// possible number of lists for simplicity. Indexed by log2 of the allocation size
    /// minus log2 of the minimum allocation size, e.g. 16k allocations are at index 2.
    /// Each free list should only include one buddy of each pair: if both buddies are
    /// free, they should have been coalesced.
    ///
    /// Each free list is implemented as a doubly-linked list.
    static constexpr int NUM_FREE_LISTS = LOG_MAX_ALLOCATION_BYTES - LOG_MIN_ALLOCATION_BYTES + 1;
    std::unique_ptr<Suballocation> free_lists_[NUM_FREE_LISTS];
};

/// An allocation made by a Suballocator. Each allocation returned by Suballocator must
/// be freed with Suballocator::Free().
///
/// Unique_ptr is used to manage ownership of these Suballocations as a guard against
/// memory leaks. The owner of the unique_ptr is either:
/// - client code, if the suballocation is in use
/// - the free list array, if the suballocation is the head of a free list
/// - the previous free list entry, if the suballocation is a subsequent free list entry
/// - the suballocation's left child, if the suballocation is split
class Suballocation {
public:
    // Checks that the allocation is not in use (therefore not leaked).
    ~Suballocation() { DCHECK(!in_use_); }

    uint8_t* data() const { return data_; }
    int64_t len() const { return len_; }

private:
    friend class Suballocator;

    DISALLOW_COPY_AND_ASSIGN(Suballocation);

    /// Static constructor for Suballocation. Can fail if new fails to allocate memory.
    static Status Create(std::unique_ptr<Suballocation>* new_suballocation);

    // The actual constructor - Create() is used for its better error handling.
    Suballocation()
            : data_(nullptr), len_(-1), buddy_(nullptr), prev_free_(nullptr), in_use_(false) {}

    /// The allocation's data and its length.
    uint8_t* data_;
    int64_t len_;

    /// The buffer backing the Suballocation, if the Suballocation is backed by an entire
    /// buffer. Otherwise uninitialized. 'buffer_' is open iff 'buddy_' is nullptr.
    BufferPool::BufferHandle buffer_;

    /// If this is a left child, the parent of this and its buddy. The parent's allocation
    /// is the contiguous memory buffer comprised of the two allocations. We store the
    /// parent in only the left child so that it is uniquely owned.
    std::unique_ptr<Suballocation> parent_;

    /// The buddy allocation of this allocation. The buddy's memory buffer is the same
    /// size and adjacent in memory. Two buddy Suballocation objects have the same
    /// lifetime: they are created in SplitToSize() and destroyed in CoalesceBuddies().
    Suballocation* buddy_;

    /// If this is in a free list, the next element in the list. nullptr if this is the last
    /// element in the free list. This pointer owns the next element in the linked list,
    /// which itself stores a raw back-pointer.
    std::unique_ptr<Suballocation> next_free_;

    /// If this is in a free list, the previous element in the list. nullptr if this is the
    /// first element. If non-nullptr, this Suballocation is owned by 'prev_free_'.
    Suballocation* prev_free_;

    /// True if was returned from Allocate() and hasn't been freed yet, or if it has been
    /// split into two child Suballocations.
    bool in_use_;
};
} // namespace doris

#endif
