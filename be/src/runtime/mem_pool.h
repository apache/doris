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

#ifndef DORIS_BE_RUNTIME_MEM_POOL_H
#define DORIS_BE_RUNTIME_MEM_POOL_H

#include <stdio.h>

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "common/logging.h"
#include "gutil/dynamic_annotations.h"
#include "util/bit_util.h"

namespace doris {

class MemTracker;

/// A MemPool maintains a list of memory chunks from which it allocates memory in
/// response to Allocate() calls;
/// Chunks stay around for the lifetime of the mempool or until they are passed on to
/// another mempool.
//
/// The caller registers a MemTracker with the pool; chunk allocations are counted
/// against that tracker and all of its ancestors. If chunks get moved between pools
/// during acquire_data() calls, the respective MemTrackers are updated accordingly.
/// Chunks freed up in the d'tor are subtracted from the registered trackers.
//
/// An Allocate() call will attempt to allocate memory from the chunk that was most
/// recently added; if that chunk doesn't have enough memory to
/// satisfy the allocation request, the free chunks are searched for one that is
/// big enough otherwise a new chunk is added to the list.
/// In order to keep allocation overhead low, chunk sizes double with each new one
/// added, until they hit a maximum size.
///
/// Allocated chunks can be reused for new allocations if Clear() is called to free
/// all allocations or ReturnPartialAllocation() is called to return part of the last
/// allocation.
///
/// All chunks before 'current_chunk_idx_' have allocated memory, while all chunks
/// after 'current_chunk_idx_' are free. The chunk at 'current_chunk_idx_' may or may
/// not have allocated memory.
///
///     Example:
///     MemPool* p = new MemPool();
///     for (int i = 0; i < 1024; ++i) {
/// returns 8-byte aligned memory (effectively 24 bytes):
///       .. = p->Allocate(17);
///     }
/// at this point, 17K have been handed out in response to Allocate() calls and
/// 28K of chunks have been allocated (chunk sizes: 4K, 8K, 16K)
/// We track total and peak allocated bytes. At this point they would be the same:
/// 28k bytes.  A call to Clear will return the allocated memory so
/// total_allocated_bytes_ becomes 0.
///     p->Clear();
/// the entire 1st chunk is returned:
///     .. = p->Allocate(4 * 1024);
/// 4K of the 2nd chunk are returned:
///     .. = p->Allocate(4 * 1024);
/// a new 20K chunk is created
///     .. = p->Allocate(20 * 1024);
//
///      MemPool* p2 = new MemPool();
/// the new mempool receives all chunks containing data from p
///      p2->acquire_data(p, false);
/// At this point p.total_allocated_bytes_ would be 0.
/// The one remaining (empty) chunk is released:
///    delete p;
class MemPool {
public:

    /// 'tracker' tracks the amount of memory allocated by this pool. Must not be NULL.
    MemPool(MemTracker* mem_tracker)
            : current_chunk_idx_(-1),
            next_chunk_size_(INITIAL_CHUNK_SIZE),
            total_allocated_bytes_(0),
            total_reserved_bytes_(0),
            peak_allocated_bytes_(0),
            mem_tracker_(mem_tracker) {
        DCHECK(mem_tracker != nullptr);
    }

    /// Frees all chunks of memory and subtracts the total allocated bytes
    /// from the registered limits.
    ~MemPool();

    /// Allocates a section of memory of 'size' bytes with DEFAULT_ALIGNMENT at the end
    /// of the the current chunk. Creates a new chunk if there aren't any chunks
    /// with enough capacity.
    uint8_t* allocate(int64_t size) {
        return allocate<false>(size, DEFAULT_ALIGNMENT);
    }

    /// Same as Allocate() except the mem limit is checked before the allocation and
    /// this call will fail (returns NULL) if it does.
    /// The caller must handle the NULL case. This should be used for allocations
    /// where the size can be very big to bound the amount by which we exceed mem limits.
    uint8_t* try_allocate(int64_t size) {
        return allocate<true>(size, DEFAULT_ALIGNMENT);
    }

    /// Same as TryAllocate() except a non-default alignment can be specified. It
    /// should be a power-of-two in [1, alignof(std::max_align_t)].
    uint8_t* try_allocate_aligned(int64_t size, int alignment) {
        DCHECK_GE(alignment, 1);
        DCHECK_LE(alignment, config::memory_max_alignment);
        DCHECK_EQ(BitUtil::RoundUpToPowerOfTwo(alignment), alignment);
        return allocate<true>(size, alignment);
    }

    /// Same as TryAllocate() except returned memory is not aligned at all.
    uint8_t* try_allocate_unaligned(int64_t size) {
        // Call templated implementation directly so that it is inlined here and the
        // alignment logic can be optimised out.
        return allocate<true>(size, 1);
    }

    /// Makes all allocated chunks available for re-use, but doesn't delete any chunks.
    void clear();

    /// Deletes all allocated chunks. free_all() or acquire_data() must be called for
    /// each mem pool
    void free_all();

    /// Absorb all chunks that hold data from src. If keep_current is true, let src hold on
    /// to its last allocated chunk that contains data.
    /// All offsets handed out by calls to GetCurrentOffset() for 'src' become invalid.
    void acquire_data(MemPool* src, bool keep_current);

    // Exchange all chunks with input source, including reserved chunks.
    // This funciton will keep its own MemTracker, and upate it after exchange.
    // Why we need this other than std::swap? Because swap will swap MemTracker too, which would
    // lead error. We only has MemTracker's pointer, which can be invalid after swap.
    void exchange_data(MemPool* other);

    std::string debug_string();

    int64_t total_allocated_bytes() const { return total_allocated_bytes_; }
    int64_t total_reserved_bytes() const { return total_reserved_bytes_; }
    int64_t peak_allocated_bytes() const { return peak_allocated_bytes_;}

    MemTracker* mem_tracker() { return mem_tracker_; }

    /// TODO: make a macro for doing this
    /// For C++/IR interop, we need to be able to look up types by name.
    static const char* LLVM_CLASS_NAME;

    static const int DEFAULT_ALIGNMENT = 8;

private:
    friend class MemPoolTest;
    static const int INITIAL_CHUNK_SIZE = 4 * 1024;

    /// The maximum size of chunk that should be allocated. Allocations larger than this
    /// size will get their own individual chunk.
    static const int MAX_CHUNK_SIZE = 512 * 1024;

    struct ChunkInfo {
        uint8_t* data; // Owned by the ChunkInfo.
        int64_t size;  // in bytes

        /// bytes allocated via Allocate() in this chunk
        int64_t allocated_bytes;

        explicit ChunkInfo(int64_t size, uint8_t* buf);

        ChunkInfo()
            : data(NULL),
            size(0),
            allocated_bytes(0) {}
    };

    /// A static field used as non-NULL pointer for zero length allocations. NULL is
    /// reserved for allocation failures. It must be as aligned as max_align_t for
    /// TryAllocateAligned().
    static uint32_t k_zero_length_region_;

    /// Find or allocated a chunk with at least min_size spare capacity and update
    /// current_chunk_idx_. Also updates chunks_, chunk_sizes_ and allocated_bytes_
    /// if a new chunk needs to be created.
    /// If check_limits is true, this call can fail (returns false) if adding a
    /// new chunk exceeds the mem limits.
    bool find_chunk(size_t min_size, bool check_limits);

    /// Check integrity of the supporting data structures; always returns true but DCHECKs
    /// all invariants.
    /// If 'check_current_chunk_empty' is true, checks that the current chunk contains no
    /// data. Otherwise the current chunk can be either empty or full.
    bool check_integrity(bool check_current_chunk_empty);

    /// Return offset to unoccupied space in current chunk.
    int64_t get_free_offset() const {
        if (current_chunk_idx_ == -1) return 0;
        return chunks_[current_chunk_idx_].allocated_bytes;
    }

    template <bool CHECK_LIMIT_FIRST>
    uint8_t* ALWAYS_INLINE allocate(int64_t size, int alignment) {
        DCHECK_GE(size, 0);
        if (UNLIKELY(size == 0)) return reinterpret_cast<uint8_t*>(&k_zero_length_region_);

        if (current_chunk_idx_ != -1) {
            ChunkInfo& info = chunks_[current_chunk_idx_];
            int64_t aligned_allocated_bytes = BitUtil::RoundUpToPowerOf2(
                info.allocated_bytes, alignment);
            if (aligned_allocated_bytes + size <= info.size) {
                // Ensure the requested alignment is respected.
                int64_t padding = aligned_allocated_bytes - info.allocated_bytes;
                uint8_t* result = info.data + aligned_allocated_bytes;
                ASAN_UNPOISON_MEMORY_REGION(result, size);
                DCHECK_LE(info.allocated_bytes + size, info.size);
                info.allocated_bytes += padding + size;
                total_allocated_bytes_ += padding + size;
                DCHECK_LE(current_chunk_idx_, chunks_.size() - 1);
                return result;
            }
        }

        // If we couldn't allocate a new chunk, return NULL. malloc() guarantees alignment
        // of alignof(std::max_align_t), so we do not need to do anything additional to
        // guarantee alignment.
        //static_assert(
        //INITIAL_CHUNK_SIZE >= config::FLAGS_MEMORY_MAX_ALIGNMENT, "Min chunk size too low");
        if (UNLIKELY(!find_chunk(size, CHECK_LIMIT_FIRST))) return NULL;

        ChunkInfo& info = chunks_[current_chunk_idx_];
        uint8_t* result = info.data + info.allocated_bytes;
        ASAN_UNPOISON_MEMORY_REGION(result, size);
        DCHECK_LE(info.allocated_bytes + size, info.size);
        info.allocated_bytes += size;
        total_allocated_bytes_ += size;
        DCHECK_LE(current_chunk_idx_, chunks_.size() - 1);
        peak_allocated_bytes_ = std::max(total_allocated_bytes_, peak_allocated_bytes_);
        return result;
    }

private:
    /// chunk from which we served the last Allocate() call;
    /// always points to the last chunk that contains allocated data;
    /// chunks 0..current_chunk_idx_ - 1 are guaranteed to contain data
    /// (chunks_[i].allocated_bytes > 0 for i: 0..current_chunk_idx_ - 1);
    /// chunks after 'current_chunk_idx_' are "free chunks" that contain no data.
    /// -1 if no chunks present
    int current_chunk_idx_;

    /// The size of the next chunk to allocate.
    int next_chunk_size_;

    /// sum of allocated_bytes_
    int64_t total_allocated_bytes_;

    /// sum of all bytes allocated in chunks_
    int64_t total_reserved_bytes_;

    /// Maximum number of bytes allocated from this pool at one time.
    int64_t peak_allocated_bytes_;

    std::vector<ChunkInfo> chunks_;

    /// The current and peak memory footprint of this pool. This is different from
    /// total allocated_bytes_ since it includes bytes in chunks that are not used.
    MemTracker* mem_tracker_;
};

// Stamp out templated implementations here so they're included in IR module
template uint8_t* MemPool::allocate<false>(int64_t size, int alignment);
template uint8_t* MemPool::allocate<true>(int64_t size, int alignment);
}

#endif
