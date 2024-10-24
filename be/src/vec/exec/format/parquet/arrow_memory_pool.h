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

#pragma once

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "vec/common/allocator.h"
#include "vec/common/allocator_fwd.h"

namespace doris::vectorized {

constexpr int64_t kDefaultBufferAlignment = 64;
static constexpr int64_t kDebugXorSuffix = -0x181fe80e0b464188LL;
#ifndef NDEBUG
static constexpr uint8_t kAllocPoison = 0xBC;
static constexpr uint8_t kReallocPoison = 0xBD;
static constexpr uint8_t kDeallocPoison = 0xBE;
#endif

// A static piece of memory for 0-size allocations, so as to return
// an aligned non-null pointer.  Note the correct value for DebugAllocator
// checks is hardcoded.
extern int64_t zero_size_area[1];
static uint8_t* const kZeroSizeArea = reinterpret_cast<uint8_t*>(&zero_size_area);

using ARROW_MEMORY_ALLOCATOR = DefaultMemoryAllocator;

class ArrowAllocator {
public:
    arrow::Status allocate_aligned(int64_t size, int64_t alignment, uint8_t** out);
    arrow::Status reallocate_aligned(int64_t old_size, int64_t new_size, int64_t alignment,
                                     uint8_t** ptr);
    void deallocate_aligned(uint8_t* ptr, int64_t size, int64_t alignment);
    void release_unused();

private:
    Allocator<false, false, false, ARROW_MEMORY_ALLOCATOR> _allocator;
};

///////////////////////////////////////////////////////////////////////
// Helper tracking memory statistics

/// \brief Memory pool statistics
///
/// 64-byte aligned so that all atomic values are on the same cache line.
class alignas(64) ArrowMemoryPoolStats {
private:
    // All atomics are updated according to Acquire-Release ordering.
    // https://en.cppreference.com/w/cpp/atomic/memory_order#Release-Acquire_ordering
    //
    // max_memory_, total_allocated_bytes_, and num_allocs_ only go up (they are
    // monotonically increasing) which can allow some optimizations.
    std::atomic<int64_t> max_memory_ {0};
    std::atomic<int64_t> bytes_allocated_ {0};
    std::atomic<int64_t> total_allocated_bytes_ {0};
    std::atomic<int64_t> num_allocs_ {0};

public:
    int64_t max_memory() const { return max_memory_.load(std::memory_order_acquire); }

    int64_t bytes_allocated() const { return bytes_allocated_.load(std::memory_order_acquire); }

    int64_t total_bytes_allocated() const {
        return total_allocated_bytes_.load(std::memory_order_acquire);
    }

    int64_t num_allocations() const { return num_allocs_.load(std::memory_order_acquire); }

    inline void did_allocate_bytes(int64_t size) {
        // Issue the load before everything else. max_memory_ is monotonically increasing,
        // so we can use a relaxed load before the read-modify-write.
        auto max_memory = max_memory_.load(std::memory_order_relaxed);
        const auto old_bytes_allocated =
                bytes_allocated_.fetch_add(size, std::memory_order_acq_rel);
        // Issue store operations on values that we don't depend on to proceed
        // with execution. When done, max_memory and old_bytes_allocated have
        // a higher chance of being available on CPU registers. This also has the
        // nice side-effect of putting 3 atomic stores close to each other in the
        // instruction stream.
        total_allocated_bytes_.fetch_add(size, std::memory_order_acq_rel);
        num_allocs_.fetch_add(1, std::memory_order_acq_rel);

        // If other threads are updating max_memory_ concurrently we leave the loop without
        // updating knowing that it already reached a value even higher than ours.
        const auto allocated = old_bytes_allocated + size;
        while (max_memory < allocated &&
               !max_memory_.compare_exchange_weak(
                       /*expected=*/max_memory, /*desired=*/allocated, std::memory_order_acq_rel)) {
        }
    }

    inline void did_reallocate_bytes(int64_t old_size, int64_t new_size) {
        if (new_size > old_size) {
            did_allocate_bytes(new_size - old_size);
        } else {
            did_free_bytes(old_size - new_size);
        }
    }

    inline void did_free_bytes(int64_t size) {
        bytes_allocated_.fetch_sub(size, std::memory_order_acq_rel);
    }
};

template <typename Allocator = ArrowAllocator>
class ArrowMemoryPool : public arrow::MemoryPool {
public:
    ~ArrowMemoryPool() override = default;

    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
        if (size < 0) {
            return arrow::Status::Invalid("negative malloc size");
        }
        if (static_cast<uint64_t>(size) >= std::numeric_limits<size_t>::max()) {
            return arrow::Status::OutOfMemory("malloc size overflows size_t");
        }
        RETURN_NOT_OK(_allocator.allocate_aligned(size, alignment, out));
#ifndef NDEBUG
        // Poison data
        if (size > 0) {
            DCHECK_NE(*out, nullptr);
            (*out)[0] = kAllocPoison;
            (*out)[size - 1] = kAllocPoison;
        }
#endif

        _stats.did_allocate_bytes(size);
        return arrow::Status::OK();
    }

    arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                             uint8_t** ptr) override {
        if (new_size < 0) {
            return arrow::Status::Invalid("negative realloc size");
        }
        if (static_cast<uint64_t>(new_size) >= std::numeric_limits<size_t>::max()) {
            return arrow::Status::OutOfMemory("realloc overflows size_t");
        }
        RETURN_NOT_OK(_allocator.reallocate_aligned(old_size, new_size, alignment, ptr));
#ifndef NDEBUG
        // Poison data
        if (new_size > old_size) {
            DCHECK_NE(*ptr, nullptr);
            (*ptr)[old_size] = kReallocPoison;
            (*ptr)[new_size - 1] = kReallocPoison;
        }
#endif

        _stats.did_reallocate_bytes(old_size, new_size);
        return arrow::Status::OK();
    }

    void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
#ifndef NDEBUG
        // Poison data
        if (size > 0) {
            DCHECK_NE(buffer, nullptr);
            buffer[0] = kDeallocPoison;
            buffer[size - 1] = kDeallocPoison;
        }
#endif
        _allocator.deallocate_aligned(buffer, size, alignment);

        _stats.did_free_bytes(size);
    }

    void ReleaseUnused() override { _allocator.release_unused(); }

    int64_t bytes_allocated() const override { return _stats.bytes_allocated(); }

    int64_t max_memory() const override { return _stats.max_memory(); }

    int64_t total_bytes_allocated() const override { return _stats.total_bytes_allocated(); }

    int64_t num_allocations() const override { return _stats.num_allocations(); }

    std::string backend_name() const override { return "ArrowMemoryPool"; }

protected:
    ArrowMemoryPoolStats _stats;
    Allocator _allocator;
};

} // namespace doris::vectorized
