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

#include "snii/writer/compact_posting_pool.h"

#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace snii::writer {

// Gentle (~1.5x) many-level payload-capacity schedule. Starting at 5 bytes with a
// slow ramp keeps the over-allocated FINAL slice small for the millions of low-df
// terms (the dominant arena-overhead source) while still reaching multi-KiB slices
// for high-df chains in a bounded number of hops (so the per-slice 4-byte forward
// pointer stays a small fraction of a large chain's bytes).
const uint32_t CompactPostingPool::kSliceSizes[kLevelCount] = {
        5, 8, 12, 18, 27, 40, 60, 90, 135, 202, 303, 455, 683, 1024, 1536, 2304};
const uint8_t CompactPostingPool::kNextLevel[kLevelCount] = {1, 2,  3,  4,  5,  6,  7,  8,
                                                             9, 10, 11, 12, 13, 14, 15, 15};

CompactPostingPool::CompactPostingPool() = default;

uint32_t CompactPostingPool::kSliceSizes_level0() {
    return kSliceSizes[0];
}

uint32_t CompactPostingPool::kSliceSize_at(int level) {
    return kSliceSizes[level];
}

uint8_t CompactPostingPool::kNextLevel_at(int level) {
    return kNextLevel[level];
}

void CompactPostingPool::reset() {
    std::vector<std::vector<uint8_t>>().swap(blocks_);
    next_offset_ = 0;
    payload_bytes_ = 0;
}

uint32_t CompactPostingPool::alloc_run(uint32_t bytes) {
    const uint32_t in_block = next_offset_ & kBlockMask;
    // A fresh block is needed when (a) there is no tail block yet, (b) the run does
    // not fit in the current tail block's remaining space, or (c) next_offset_ sits
    // exactly on a block boundary whose block has not been allocated (a previous run
    // that exactly filled the tail leaves next_offset_ == blocks_.size()*kBlockSize,
    // so in_block == 0 must NOT be mistaken for an empty fresh block).
    const bool tail_exists = (next_offset_ >> kBlockShift) < blocks_.size();
    const bool need_block = !tail_exists || in_block + bytes > kBlockSize;
    // Hard invariant (see arena_bytes()): the uint32 offset must never wrap. The spimi
    // accumulator force-spills below 4 GiB, but enforce it here too -- in release as
    // well as debug -- so any direct user of the pool fails loudly instead of silently
    // aliasing block 0. We are a library: throw and let the caller decide how to
    // handle it, rather than aborting the process. The run starts either in the
    // current tail or at a new block's base; compute that start in 64 bits before the
    // uint32 arithmetic can wrap.
    const uint64_t run_start =
            need_block ? static_cast<uint64_t>(blocks_.size()) * kBlockSize : next_offset_;
    if (run_start + bytes > UINT32_MAX) {
        throw std::overflow_error(
                "snii: CompactPostingPool arena exceeded the 4 GiB uint32 offset limit; "
                "the caller must spill before this point");
    }
    if (need_block) {
        blocks_.emplace_back(kBlockSize, 0);
        next_offset_ = static_cast<uint32_t>((blocks_.size() - 1) * kBlockSize);
    }
    const uint32_t off = next_offset_;
    next_offset_ += bytes;
    return off;
}

uint32_t CompactPostingPool::alloc_slice(int level, uint32_t* slice_end) {
    const uint32_t cap = kSliceSizes[level];
    const uint32_t first = alloc_run(cap + kPtrBytes);
    *slice_end = first + cap;
    // Zero the forward pointer so a not-yet-extended tail slice reads next_head == 0.
    std::memset(at(*slice_end), 0, kPtrBytes);
    return first;
}

uint32_t CompactPostingPool::read_ptr(uint32_t slice_end) const {
    uint32_t v;
    std::memcpy(&v, at(slice_end), sizeof(v));
    return v;
}

void CompactPostingPool::write_ptr(uint32_t slice_end, uint32_t next_head) {
    std::memcpy(at(slice_end), &next_head, sizeof(next_head));
}

uint32_t CompactPostingPool::start_chain(SliceWriter* w, uint8_t* level) {
    *level = 0;
    const uint32_t head = alloc_slice(0, &w->slice_end);
    w->cur = head;
    return head;
}

void CompactPostingPool::append_byte(SliceWriter* w, uint8_t* level, uint8_t value) {
    if (w->cur == w->slice_end) {
        // Current slice payload region is full: grow the chain with a larger slice and
        // record the link in the old slice's trailing pointer bytes.
        const uint8_t next_level = kNextLevel[*level];
        uint32_t new_end = 0;
        const uint32_t new_head = alloc_slice(next_level, &new_end);
        write_ptr(w->slice_end, new_head);
        *level = next_level;
        w->cur = new_head;
        w->slice_end = new_end;
    }
    *at(w->cur) = value;
    ++w->cur;
    ++payload_bytes_;
}

CompactPostingPool::Cursor::Cursor(const CompactPostingPool* pool, uint32_t head, uint64_t budget)
        : pool_(pool), cur_(head), level_(0), budget_(budget) {
    // The first slice is level 0; its payload region ends kSliceSizes[0] bytes in.
    slice_end_ = head + CompactPostingPool::kSliceSizes[0];
}

bool CompactPostingPool::Cursor::has_next() const {
    if (budget_ == 0) return false;
    // At a slice boundary, the chain continues only if the forward pointer is non-zero;
    // a zero pointer is the tail marker (offset 0 is never a valid next-slice head). Peek
    // it so has_next() never reports a phantom byte that next() would have to fabricate.
    if (cur_ == slice_end_) return pool_->read_ptr(slice_end_) != 0;
    return true;
}

uint8_t CompactPostingPool::Cursor::next() {
    // Budget guard: the caller's stated upper bound is spent -- yield nothing more.
    if (budget_ == 0) return 0;
    if (cur_ == slice_end_) {
        // Reached this slice's payload boundary. Follow the forward pointer to the next
        // slice -- UNLESS it is zero, which marks the CHAIN TAIL (offset 0 is always the
        // pool's very first slice, never a valid *next*-slice head, so a zero pointer is
        // unambiguously "no more slices"). Without this tail check, an over-reading caller
        // would follow the zero pointer to offset 0 and alias block 0's bytes (or read an
        // unallocated block) -- UB. Stopping here makes the cursor self-terminating and
        // safe regardless of how large a budget the caller passed.
        const uint32_t next_head = pool_->read_ptr(slice_end_);
        if (next_head == 0) {
            budget_ = 0; // chain exhausted: no further bytes exist
            return 0;
        }
        level_ = CompactPostingPool::kNextLevel[level_];
        cur_ = next_head;
        slice_end_ = next_head + CompactPostingPool::kSliceSizes[level_];
    }
    const uint8_t v = *pool_->at(cur_);
    ++cur_;
    --budget_;
    return v;
}

} // namespace snii::writer
