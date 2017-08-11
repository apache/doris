// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_RUNTIME_MEM_POOL_H
#define BDG_PALO_BE_RUNTIME_MEM_POOL_H

#include <stdio.h>
#include <stdint.h>

#include <cstddef>
#include <algorithm>
#include <vector>
#include <string>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "util/bit_util.h"
#include "util/debug_util.h"

namespace palo {

class MemTracker;

// A MemPool maintains a list of memory chunks from which it allocates memory in
// response to allocate() calls;
// Chunks stay around for the lifetime of the mempool or until they are passed on to
// another mempool.
//
// The caller registers a MemTrackers with the pool; chunk allocations are counted
// against that tracker and all of its ancestors. If chunks get moved between pools
// during AcquireData() calls, the respective MemTrackers are updated accordingly.
// Chunks freed up in the d'tor are subtracted from the registered limits.
//
// An allocate() call will attempt to allocate memory from the chunk that was most
// recently added; if that chunk doesn't have enough memory to
// satisfy the allocation request, the free chunks are searched for one that is
// big enough otherwise a new chunk is added to the list.
// The _current_chunk_idx always points to the last chunk with allocated memory.
// In order to keep allocation overhead low, chunk sizes double with each new one
// added, until they hit a maximum size.
//
//     Example:
//     MemPool* p = new MemPool();
//     for (int i = 0; i < 1024; ++i) {
// returns 8-byte aligned memory (effectively 24 bytes):
//       .. = p->allocate(17);
//     }
// at this point, 17K have been handed out in response to allocate() calls and
// 28K of chunks have been allocated (chunk sizes: 4K, 8K, 16K)
// We track total and peak allocated bytes. At this point they would be the same:
// 28k bytes.  A call to Clear will return the allocated memory so
// _total_allocate_bytes
// becomes 0 while _peak_allocate_bytes remains at 28k.
//     p->Clear();
// the entire 1st chunk is returned:
//     .. = p->allocate(4 * 1024);
// 4K of the 2nd chunk are returned:
//     .. = p->allocate(4 * 1024);
// a new 20K chunk is created
//     .. = p->allocate(20 * 1024);
//
//      MemPool* p2 = new MemPool();
// the new mempool receives all chunks containing data from p
//      p2->AcquireData(p, false);
// At this point p._total_allocated_bytes would be 0 while p._peak_allocated_bytes
// remains unchanged.
// The one remaining (empty) chunk is released:
//    delete p;

class MemPool {
public:
    // Allocates mempool with fixed-size chunks of size 'chunk_size'.
    // Chunk_size must be >= 0; 0 requests automatic doubling of chunk sizes,
    // up to a limit.
    // 'tracker' tracks the amount of memory allocated by this pool. Must not be NULL.
    MemPool(MemTracker* mem_tracker, int chunk_size);
    MemPool(MemTracker* mem_tracker) : MemPool::MemPool(mem_tracker, 0) {}

    // Frees all chunks of memory and subtracts the total allocated bytes
    // from the registered limits.
    ~MemPool();

    // Allocates 8-byte aligned section of memory of 'size' bytes at the end
    // of the the current chunk. Creates a new chunk if there aren't any chunks
    // with enough capacity.
    uint8_t* allocate(int size) {
        return allocate<false>(size);
    }

    // Same as Allocate() except the mem limit is checked before the allocation and
    // this call will fail (returns NULL) if it does.
    // The caller must handle the NULL case. This should be used for allocations
    // where the size can be very big to bound the amount by which we exceed mem limits.
    uint8_t* try_allocate(int size) {
        return allocate<true>(size);
    }

    // Returns 'byte_size' to the current chunk back to the mem pool. This can
    // only be used to return either all or part of the previous allocation returned
    // by Allocate().
    void return_partial_allocation(int byte_size) {
        DCHECK_GE(byte_size, 0);
        DCHECK(_current_chunk_idx != -1);
        ChunkInfo& info = _chunks[_current_chunk_idx];
        DCHECK_GE(info.allocated_bytes, byte_size);
        info.allocated_bytes -= byte_size;
        _total_allocated_bytes -= byte_size;
    }

    // Makes all allocated chunks available for re-use, but doesn't delete any chunks.
    void clear() {
        _current_chunk_idx = -1;
        for (std::vector<ChunkInfo>::iterator chunk = _chunks.begin();
                chunk != _chunks.end(); ++chunk) {
            chunk->cumulative_allocated_bytes = 0;
            chunk->allocated_bytes = 0;
        }
        _total_allocated_bytes = 0;
        DCHECK(check_integrity(false));
    }

    // Deletes all allocated chunks. FreeAll() or AcquireData() must be called for
    // each mem pool
    void free_all();

    // Absorb all chunks that hold data from src. If keep_current is true, let src hold on
    // to its last allocated chunk that contains data.
    // All offsets handed out by calls to get_offset()/get_current_offset() for 'src'
    // become invalid.
    // All offsets handed out by calls to GetCurrentOffset() for 'src' become invalid.
    void acquire_data(MemPool* src, bool keep_current);

    // Diagnostic to check if memory is allocated from this mempool.
    // Inputs:
    //   ptr: start of memory block.
    //   size: size of memory block.
    // Returns true if memory block is in one of the chunks in this mempool.
    bool contains(uint8_t* ptr, int size);

    std::string debug_string();

    int64_t total_allocated_bytes() const {
        return _total_allocated_bytes;
    }
    // int64_t total_chunk_bytes() const {
    //     return _total_chunk_bytes;
    // }
    int64_t peak_allocated_bytes() const {
        return _peak_allocated_bytes;
    }
    int64_t total_reserved_bytes() const {
        return _total_reserved_bytes;
    }
    MemTracker* mem_tracker() {
        return _mem_tracker;
    }

    // Return sum of _chunk_sizes.
    int64_t get_total_chunk_sizes() const;

    // Return logical offset of memory returned by next call to allocate()
    // into allocated data.
    int get_current_offset() const {
        return _total_allocated_bytes;
    }

    // Return (data ptr, allocated bytes) pairs for all chunks owned by this mempool.
    void get_chunk_info(std::vector<std::pair<uint8_t*, int> >* chunk_info);

    // Print allocated bytes from all chunks.
    std::string debug_print();

    // TODO: make a macro for doing this
    // For C++/IR interop, we need to be able to look up types by name.
    static const char* _s_llvm_class_name;

private:
    static const int DEFAULT_INITIAL_CHUNK_SIZE = 4 * 1024;
    static const int64_t MAX_CHUNK_SIZE = 512 * 1024;

    struct ChunkInfo {
        bool owns_data;  // true if we eventually need to dealloc data
        uint8_t* data;
        int64_t size;  // in bytes

        // number of bytes allocated via allocate() up to but excluding this chunk;
        // *not* valid for chunks > _current_chunk_idx (because that would create too
        // much maintenance work if we have trailing unoccupied chunks)
        int64_t cumulative_allocated_bytes;

        // bytes allocated via allocate() in this chunk
        int64_t allocated_bytes;

        // explicit ChunkInfo(int size);
        explicit ChunkInfo(int64_t size, uint8_t* buf);

        ChunkInfo()
            : owns_data(true),
              data(NULL),
              size(0),
              cumulative_allocated_bytes(0),
              allocated_bytes(0) {}
    };

    // static uint32_t _s_zero_length_region alignas(max_align_t);
    static uint32_t _s_zero_length_region;

    // chunk from which we served the last allocate() call;
    // always points to the last chunk that contains allocated data;
    // chunks 0.._current_chunk_idx are guaranteed to contain data
    // (_chunks[i].allocated_bytes > 0 for i: 0.._current_chunk_idx);
    // -1 if no chunks present
    int _current_chunk_idx;

    // chunk where last offset conversion (get_offset() or get_data_ptr()) took place;
    // -1 if those functions have never been called
    int _last_offset_conversion_chunk_idx;

    int _chunk_size;  // if != 0, use this size for new chunks

    // sum of _allocated_bytes
    int64_t _total_allocated_bytes;

    // sum of _total_chunk_bytes
    // int64_t _total_chunk_bytes;

    // Maximum number of bytes allocated from this pool at one time.
    int64_t _peak_allocated_bytes;

    // sum of all bytes allocated in _chunks
    int64_t _total_reserved_bytes;

    std::vector<ChunkInfo> _chunks;

    // std::vector<MemTracker*> _limits;

    // // true if one of the registered limits was exceeded during an allocate()
    // // call
    // bool _exceeded_limit;

    // The current and peak memory footprint of this pool. This is different from
    // total _allocated_bytes since it includes bytes in chunks that are not used.
    MemTracker* _mem_tracker;

    // Find or allocated a chunk with at least min_size spare capacity and update
    // _current_chunk_idx. Also updates _chunks, _chunk_sizes and _allocated_bytes
    // if a new chunk needs to be created.
    // If check_limits is true, this call can fail (returns false) if adding a
    // new chunk exceeds the mem limits.
    // bool find_chunk(int min_size);
    bool find_chunk(int64_t min_size, bool check_limits);

    // Check integrity of the supporting data structures; always returns true but DCHECKs
    // all invariants.
    // If 'current_chunk_empty' is false, checks that the current chunk contains data.
    bool check_integrity(bool current_chunk_empty);

    int get_offset_helper(uint8_t* data);
    uint8_t* get_data_ptr_helper(int offset);

    // Return offset to unoccpied space in current chunk.
    int get_free_offset() const {
        if (_current_chunk_idx == -1) {
            return 0;
        }
        return _chunks[_current_chunk_idx].allocated_bytes;
    }

    template <bool CHECK_LIMIT_FIRST>
    uint8_t* allocate(int size) {
        if (size == 0) {
            return (uint8_t*)&_s_zero_length_region;
        }

        // round up to nearest 8 bytes
        // e.g. if size between 1 and 7, num_bytes will be 8
        // int64_t num_bytes = (size + 8LL - 1) / 8 * 8;
        int64_t num_bytes = BitUtil::round_up(size, 8);
        if (_current_chunk_idx == -1
                || (num_bytes + _chunks[_current_chunk_idx].allocated_bytes)
                    > _chunks[_current_chunk_idx].size) {
            // If we couldn't allocate a new chunk, return NULL.
            if (UNLIKELY(!find_chunk(num_bytes, CHECK_LIMIT_FIRST))) {
                return NULL;
            }
        }
        ChunkInfo& info = _chunks[_current_chunk_idx];
        uint8_t* result = info.data + info.allocated_bytes;
        DCHECK_LE(info.allocated_bytes + num_bytes, info.size);
        info.allocated_bytes += num_bytes;
        _total_allocated_bytes += num_bytes;
        DCHECK_LE(_current_chunk_idx, _chunks.size() - 1);
        _peak_allocated_bytes = std::max(_total_allocated_bytes, _peak_allocated_bytes);
        return result;
    }
};

} // end namespace palo

#endif
