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

#include <sanitizer/asan_interface.h>

#include <list>
#include <mutex>

#include "runtime/memory/chunk.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/system_allocator.h"
#include "runtime/thread_context.h"
#include "util/bit_util.h"
#include "util/cpu_info.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"

namespace doris {

// <= MIN_CHUNK_SIZE, A large number of small chunks will waste extra storage and increase lock time.
static constexpr size_t MIN_CHUNK_SIZE = 4096; // 4K
// >= MAX_CHUNK_SIZE, Large chunks may not be used for a long time, wasting memory.
static constexpr size_t MAX_CHUNK_SIZE = 64 * (1ULL << 20); // 64M

ChunkAllocator* ChunkAllocator::_s_instance = nullptr;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_local_core_alloc_count, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_other_core_alloc_count, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_system_alloc_count, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_system_free_count, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_system_alloc_cost_ns, MetricUnit::NANOSECONDS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_system_free_cost_ns, MetricUnit::NANOSECONDS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(chunk_pool_reserved_bytes, MetricUnit::NOUNIT);

static IntCounter* chunk_pool_local_core_alloc_count;
static IntCounter* chunk_pool_other_core_alloc_count;
static IntCounter* chunk_pool_system_alloc_count;
static IntCounter* chunk_pool_system_free_count;
static IntCounter* chunk_pool_system_alloc_cost_ns;
static IntCounter* chunk_pool_system_free_cost_ns;
static IntGauge* chunk_pool_reserved_bytes;

#ifdef BE_TEST
static std::mutex s_mutex;
ChunkAllocator* ChunkAllocator::instance() {
    std::lock_guard<std::mutex> l(s_mutex);
    if (_s_instance == nullptr) {
        CpuInfo::init();
        ChunkAllocator::init_instance(4096);
    }
    return _s_instance;
}
#endif

// Keep free chunk's ptr in size separated free list.
// This class is thread-safe.
class ChunkArena {
    int TRY_LOCK_TIMES = 3;

public:
    ChunkArena() : _chunk_lists(64) {}

    ~ChunkArena() {
        for (int i = 0; i < 64; ++i) {
            if (_chunk_lists[i].empty()) continue;
            size_t size = (uint64_t)1 << i;
            for (auto ptr : _chunk_lists[i]) {
                SystemAllocator::free(ptr, size);
            }
        }
    }

    // Try to pop a free chunk from corresponding free list.
    // Return true if success
    bool pop_free_chunk(size_t size, uint8_t** ptr) {
        int idx = BitUtil::Log2Ceiling64(size);
        auto& free_list = _chunk_lists[idx];

        if (free_list.empty()) return false;

        for (int i = 0; i < TRY_LOCK_TIMES; ++i) {
            if (_lock.try_lock()) {
                if (free_list.empty()) {
                    _lock.unlock();
                    return false;
                } else {
                    *ptr = free_list.back();
                    free_list.pop_back();
                    ASAN_UNPOISON_MEMORY_REGION(*ptr, size);
                    _lock.unlock();
                    return true;
                }
            }
        }
        return false;
    }

    void push_free_chunk(uint8_t* ptr, size_t size) {
        int idx = BitUtil::Log2Ceiling64(size);
        // Poison this chunk to make asan can detect invalid access
        ASAN_POISON_MEMORY_REGION(ptr, size);
        std::lock_guard<SpinLock> l(_lock);
        _chunk_lists[idx].push_back(ptr);
    }

private:
    SpinLock _lock;
    std::vector<std::vector<uint8_t*>> _chunk_lists;
};

void ChunkAllocator::init_instance(size_t reserve_limit) {
    if (_s_instance != nullptr) return;
    _s_instance = new ChunkAllocator(reserve_limit);
}

ChunkAllocator::ChunkAllocator(size_t reserve_limit)
        : _reserve_bytes_limit(reserve_limit),
          _steal_arena_limit(reserve_limit * 0.1),
          _reserved_bytes(0),
          _arenas(CpuInfo::get_max_num_cores()) {
    _mem_tracker = std::make_unique<MemTrackerLimiter>(-1, "ChunkAllocator");
    for (int i = 0; i < _arenas.size(); ++i) {
        _arenas[i].reset(new ChunkArena());
    }

    _chunk_allocator_metric_entity =
            DorisMetrics::instance()->metric_registry()->register_entity("chunk_allocator");
    INT_COUNTER_METRIC_REGISTER(_chunk_allocator_metric_entity, chunk_pool_local_core_alloc_count);
    INT_COUNTER_METRIC_REGISTER(_chunk_allocator_metric_entity, chunk_pool_other_core_alloc_count);
    INT_COUNTER_METRIC_REGISTER(_chunk_allocator_metric_entity, chunk_pool_system_alloc_count);
    INT_COUNTER_METRIC_REGISTER(_chunk_allocator_metric_entity, chunk_pool_system_free_count);
    INT_COUNTER_METRIC_REGISTER(_chunk_allocator_metric_entity, chunk_pool_system_alloc_cost_ns);
    INT_COUNTER_METRIC_REGISTER(_chunk_allocator_metric_entity, chunk_pool_system_free_cost_ns);
    INT_GAUGE_METRIC_REGISTER(_chunk_allocator_metric_entity, chunk_pool_reserved_bytes);
}

Status ChunkAllocator::allocate(size_t size, Chunk* chunk) {
    CHECK((size > 0 && (size & (size - 1)) == 0));

    int core_id = CpuInfo::get_current_core();
    chunk->core_id = core_id;
    chunk->size = size;
    if (!config::disable_chunk_allocator) {
        // fast path: allocate from current core arena
        if (_arenas[core_id]->pop_free_chunk(size, &chunk->data)) {
            DCHECK_GE(_reserved_bytes, 0);
            _reserved_bytes.fetch_sub(size);
            chunk_pool_local_core_alloc_count->increment(1);
            // transfer the memory ownership of allocate from ChunkAllocator::tracker to the tls tracker.
            THREAD_MEM_TRACKER_TRANSFER_FROM(size, _mem_tracker.get());
            return Status::OK();
        }
        // Second path: try to allocate from other core's arena
        // When the reserved bytes is greater than the limit, the chunk is stolen from other arena.
        // Otherwise, it is allocated from the system first, which can reserve enough memory as soon as possible.
        // After that, allocate from current core arena as much as possible.
        if (_reserved_bytes > _steal_arena_limit) {
            ++core_id;
            for (int i = 1; i < _arenas.size(); ++i, ++core_id) {
                if (_arenas[core_id % _arenas.size()]->pop_free_chunk(size, &chunk->data)) {
                    DCHECK_GE(_reserved_bytes, 0);
                    _reserved_bytes.fetch_sub(size);
                    chunk_pool_other_core_alloc_count->increment(1);
                    // reset chunk's core_id to other
                    chunk->core_id = core_id % _arenas.size();
                    // transfer the memory ownership of allocate from ChunkAllocator::tracker to the tls tracker.
                    THREAD_MEM_TRACKER_TRANSFER_FROM(size, _mem_tracker.get());
                    return Status::OK();
                }
            }
        }
    }

    int64_t cost_ns = 0;
    {
        SCOPED_RAW_TIMER(&cost_ns);
        // allocate from system allocator
        chunk->data = SystemAllocator::allocate(size);
    }
    chunk_pool_system_alloc_count->increment(1);
    chunk_pool_system_alloc_cost_ns->increment(cost_ns);
    if (chunk->data == nullptr) {
        return Status::MemoryAllocFailed("ChunkAllocator failed to allocate chunk {} bytes", size);
    }
    return Status::OK();
}

void ChunkAllocator::free(const Chunk& chunk) {
    DCHECK(chunk.core_id != -1);
    CHECK((chunk.size & (chunk.size - 1)) == 0);
    if (config::disable_chunk_allocator) {
        SystemAllocator::free(chunk.data, chunk.size);
        return;
    }

    int64_t old_reserved_bytes = _reserved_bytes;
    int64_t new_reserved_bytes = 0;
    do {
        new_reserved_bytes = old_reserved_bytes + chunk.size;
        if (chunk.size <= MIN_CHUNK_SIZE || chunk.size >= MAX_CHUNK_SIZE ||
            new_reserved_bytes > _reserve_bytes_limit) {
            int64_t cost_ns = 0;
            {
                SCOPED_RAW_TIMER(&cost_ns);
                SystemAllocator::free(chunk.data, chunk.size);
            }
            chunk_pool_system_free_count->increment(1);
            chunk_pool_system_free_cost_ns->increment(cost_ns);

            return;
        }
    } while (!_reserved_bytes.compare_exchange_weak(old_reserved_bytes, new_reserved_bytes));

    // The memory size of allocate/free is a multiple of 2, so `_reserved_bytes% 100 == 32`
    // will definitely happen, and the latest `_reserved_bytes` value will be set every time.
    // The real-time and accurate `_reserved_bytes` value is not required. Usually,
    // the value of `_reserved_bytes` is equal to ChunkAllocator MemTracker.
    // The `_reserved_bytes` metric is only concerned when verifying the accuracy of MemTracker.
    // Therefore, reduce the number of sets and reduce the performance impact.
    if (_reserved_bytes % 100 == 32) {
        chunk_pool_reserved_bytes->set_value(_reserved_bytes);
    }
    // The chunk's memory ownership is transferred from tls tracker to ChunkAllocator.
    THREAD_MEM_TRACKER_TRANSFER_TO(chunk.size, _mem_tracker.get());
    _arenas[chunk.core_id]->push_free_chunk(chunk.data, chunk.size);
}

Status ChunkAllocator::allocate_align(size_t size, Chunk* chunk) {
    return allocate(BitUtil::RoundUpToPowerOfTwo(size), chunk);
}

void ChunkAllocator::free(uint8_t* data, size_t size) {
    Chunk chunk;
    chunk.data = data;
    chunk.size = size;
    chunk.core_id = CpuInfo::get_current_core();
    free(chunk);
}

} // namespace doris
