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

#include "runtime/memory/chunk_allocator.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/bit_util.h"
#include "util/doris_metrics.h"

#include <algorithm>
#include <stdio.h>
#include <sstream>

#include "common/names.h"

namespace doris {

#define MEM_POOL_POISON (0x66aa77bb)

const int MemPool::INITIAL_CHUNK_SIZE;
const int MemPool::MAX_CHUNK_SIZE;

const int MemPool::DEFAULT_ALIGNMENT;
uint32_t MemPool::k_zero_length_region_ alignas(std::max_align_t) = MEM_POOL_POISON;

MemPool::ChunkInfo::ChunkInfo(const Chunk& chunk_)
        : chunk(chunk_),
        allocated_bytes(0) {
    DorisMetrics::instance()->memory_pool_bytes_total.increment(chunk.size);
}

MemPool::~MemPool() {
    int64_t total_bytes_released = 0;
    for (auto& chunk : chunks_) {
        total_bytes_released += chunk.chunk.size;
        ChunkAllocator::instance()->free(chunk.chunk);
    }
    mem_tracker_->release(total_bytes_released);
    DorisMetrics::instance()->memory_pool_bytes_total.increment(-total_bytes_released);
}

void MemPool::clear() {
    current_chunk_idx_ = -1;
    for (auto& chunk: chunks_) {
        chunk.allocated_bytes = 0;
        ASAN_POISON_MEMORY_REGION(chunk.chunk.data, chunk.chunk.size);
    }
    total_allocated_bytes_ = 0;
    DCHECK(check_integrity(false));
}

void MemPool::free_all() {
    int64_t total_bytes_released = 0;
    for (auto& chunk: chunks_) {
        total_bytes_released += chunk.chunk.size;
        ChunkAllocator::instance()->free(chunk.chunk);
    }
    chunks_.clear();
    next_chunk_size_ = INITIAL_CHUNK_SIZE;
    current_chunk_idx_ = -1;
    total_allocated_bytes_ = 0;
    total_reserved_bytes_ = 0;

    mem_tracker_->release(total_bytes_released);
    DorisMetrics::instance()->memory_pool_bytes_total.increment(-total_bytes_released);
}

bool MemPool::find_chunk(size_t min_size, bool check_limits) {
    // Try to allocate from a free chunk. We may have free chunks after the current chunk
    // if Clear() was called. The current chunk may be free if ReturnPartialAllocation()
    // was called. The first free chunk (if there is one) can therefore be either the
    // current chunk or the chunk immediately after the current chunk.
    int first_free_idx;
    if (current_chunk_idx_ == -1) {
        first_free_idx = 0;
    } else {
        DCHECK_GE(current_chunk_idx_, 0);
        first_free_idx = current_chunk_idx_ +
            (chunks_[current_chunk_idx_].allocated_bytes > 0);
    }
    for (int idx = current_chunk_idx_ + 1; idx < chunks_.size(); ++idx) {
        // All chunks after 'current_chunk_idx_' should be free.
        DCHECK_EQ(chunks_[idx].allocated_bytes, 0);
        if (chunks_[idx].chunk.size >= min_size) {
            // This chunk is big enough. Move it before the other free chunks.
            if (idx != first_free_idx) std::swap(chunks_[idx], chunks_[first_free_idx]);
            current_chunk_idx_ = first_free_idx;
            DCHECK(check_integrity(true));
            return true;
        }
    }

    // Didn't find a big enough free chunk - need to allocate new chunk.
    size_t chunk_size = 0;
    DCHECK_LE(next_chunk_size_, MAX_CHUNK_SIZE);

    if (config::disable_mem_pools) {
        // Disable pooling by sizing the chunk to fit only this allocation.
        // Make sure the alignment guarantees are respected.
        chunk_size = std::max<size_t>(min_size, alignof(max_align_t));
    } else {
        DCHECK_GE(next_chunk_size_, INITIAL_CHUNK_SIZE);
        chunk_size = max<size_t>(min_size, next_chunk_size_);
    }

    chunk_size = BitUtil::RoundUpToPowerOfTwo(chunk_size);
    if (check_limits) {
        if (!mem_tracker_->try_consume(chunk_size)) return false;
    } else {
        mem_tracker_->consume(chunk_size);
    }

    // Allocate a new chunk. Return early if allocate fails.
    Chunk chunk;
    if (!ChunkAllocator::instance()->allocate(chunk_size, &chunk)) {
        mem_tracker_->release(chunk_size);
        return false;
    }
    ASAN_POISON_MEMORY_REGION(chunk.data, chunk_size);
    // Put it before the first free chunk. If no free chunks, it goes at the end.
    if (first_free_idx == static_cast<int>(chunks_.size())) {
        chunks_.emplace_back(chunk);
    } else {
        chunks_.insert(chunks_.begin() + first_free_idx, ChunkInfo(chunk));
    }
    current_chunk_idx_ = first_free_idx;
    total_reserved_bytes_ += chunk_size;
    // Don't increment the chunk size until the allocation succeeds: if an attempted
    // large allocation fails we don't want to increase the chunk size further.
    next_chunk_size_ = static_cast<int>(min<int64_t>(chunk_size * 2, MAX_CHUNK_SIZE));

    DCHECK(check_integrity(true));
    return true;
}

void MemPool::acquire_data(MemPool* src, bool keep_current) {
    DCHECK(src->check_integrity(false));
    int num_acquired_chunks;
    if (keep_current) {
        num_acquired_chunks = src->current_chunk_idx_;
    } else if (src->get_free_offset() == 0) {
        // nothing in the last chunk
        num_acquired_chunks = src->current_chunk_idx_;
    } else {
        num_acquired_chunks = src->current_chunk_idx_ + 1;
    }

    if (num_acquired_chunks <= 0) {
        if (!keep_current) src->free_all();
        return;
    }

    auto end_chunk = src->chunks_.begin() + num_acquired_chunks;
    int64_t total_transfered_bytes = 0;
    for (auto i = src->chunks_.begin(); i != end_chunk; ++i) {
        total_transfered_bytes += i->chunk.size;
    }
    src->total_reserved_bytes_ -= total_transfered_bytes;
    total_reserved_bytes_ += total_transfered_bytes;

    // Skip unnecessary atomic ops if the mem_trackers are the same.
    if (src->mem_tracker_ != mem_tracker_) {
        src->mem_tracker_->release(total_transfered_bytes);
        mem_tracker_->consume(total_transfered_bytes);
    }

    // insert new chunks after current_chunk_idx_
    auto insert_chunk = chunks_.begin() + current_chunk_idx_ + 1;
    chunks_.insert(insert_chunk, src->chunks_.begin(), end_chunk);
    src->chunks_.erase(src->chunks_.begin(), end_chunk);
    current_chunk_idx_ += num_acquired_chunks;

    if (keep_current) {
        src->current_chunk_idx_ = 0;
        DCHECK(src->chunks_.size() == 1 || src->chunks_[1].allocated_bytes == 0);
        total_allocated_bytes_ += src->total_allocated_bytes_ - src->get_free_offset();
        src->total_allocated_bytes_ = src->get_free_offset();
    } else {
        src->current_chunk_idx_ = -1;
        total_allocated_bytes_ += src->total_allocated_bytes_;
        src->total_allocated_bytes_ = 0;
    }

    peak_allocated_bytes_ = std::max(total_allocated_bytes_, peak_allocated_bytes_);

    if (!keep_current) src->free_all();
    DCHECK(src->check_integrity(false));
    DCHECK(check_integrity(false));
}

void MemPool::exchange_data(MemPool* other) {
    int64_t delta_size = other->total_reserved_bytes_ - total_reserved_bytes_;

    std::swap(current_chunk_idx_, other->current_chunk_idx_);
    std::swap(next_chunk_size_, other->next_chunk_size_);
    std::swap(total_allocated_bytes_, other->total_allocated_bytes_);
    std::swap(total_reserved_bytes_, other->total_reserved_bytes_);
    std::swap(peak_allocated_bytes_, other->peak_allocated_bytes_);
    std::swap(chunks_, other->chunks_);

    // update MemTracker
    mem_tracker_->consume(delta_size);
    other->mem_tracker_->release(delta_size);
}

string MemPool::debug_string() {
    stringstream out;
    char str[16];
    out << "MemPool(#chunks=" << chunks_.size() << " [";
    for (int i = 0; i < chunks_.size(); ++i) {
        sprintf(str, "0x%lx=", reinterpret_cast<size_t>(chunks_[i].chunk.data));
        out << (i > 0 ? " " : "")
            << str
            << chunks_[i].chunk.size
            << "/" << chunks_[i].allocated_bytes;
    }
    out << "] current_chunk=" << current_chunk_idx_
        << " total_sizes=" << total_reserved_bytes_
        << " total_alloc=" << total_allocated_bytes_
        << ")";
    return out.str();
}

bool MemPool::check_integrity(bool check_current_chunk_empty) {
    DCHECK_LT(current_chunk_idx_, static_cast<int>(chunks_.size()));

    // Without pooling, there are way too many chunks and this takes too long.
    if (config::disable_mem_pools) return true;

    // check that current_chunk_idx_ points to the last chunk with allocated data
    int64_t total_allocated = 0;
    for (int i = 0; i < chunks_.size(); ++i) {
        DCHECK_GT(chunks_[i].chunk.size, 0);
        if (i < current_chunk_idx_) {
            DCHECK_GT(chunks_[i].allocated_bytes, 0);
        } else if (i == current_chunk_idx_) {
            DCHECK_GE(chunks_[i].allocated_bytes, 0);
            if (check_current_chunk_empty) DCHECK_EQ(chunks_[i].allocated_bytes, 0);
        } else {
            DCHECK_EQ(chunks_[i].allocated_bytes, 0);
        }
        total_allocated += chunks_[i].allocated_bytes;
    }
    DCHECK_EQ(total_allocated, total_allocated_bytes_);
    return true;
}

}
