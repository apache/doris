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

#include <atomic>
#include <list>
#include <mutex>

#include "gutil/dynamic_annotations.h"
#include "runtime/memory/chunk.h"
#include "runtime/memory/system_allocator.h"
#include "util/bit_util.h"
#include "util/cpu_info.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"

namespace doris {

ChunkAllocator* ChunkAllocator::_s_instance = nullptr;

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_local_core_alloc_count, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_other_core_alloc_count, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_system_alloc_count, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_system_free_count, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_system_alloc_cost_ns, MetricUnit::NANOSECONDS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(chunk_pool_system_free_cost_ns, MetricUnit::NANOSECONDS);

static IntCounter* chunk_pool_local_core_alloc_count;
static IntCounter* chunk_pool_other_core_alloc_count;
static IntCounter* chunk_pool_system_alloc_count;
static IntCounter* chunk_pool_system_free_count;
static IntCounter* chunk_pool_system_alloc_cost_ns;
static IntCounter* chunk_pool_system_free_cost_ns;

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

        std::lock_guard<SpinLock> l(_lock);
        if (free_list.empty()) {
            return false;
        }
        *ptr = free_list.back();
        free_list.pop_back();
        ASAN_UNPOISON_MEMORY_REGION(*ptr, size);
        return true;
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
          _reserved_bytes(0),
          _arenas(CpuInfo::get_max_num_cores()) {
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
}

bool ChunkAllocator::allocate(size_t size, Chunk* chunk) {
    // fast path: allocate from current core arena
    int core_id = CpuInfo::get_current_core();
    chunk->size = size;
    chunk->core_id = core_id;

    if (_arenas[core_id]->pop_free_chunk(size, &chunk->data)) {
        DCHECK_GE(_reserved_bytes, 0);
        _reserved_bytes.fetch_sub(size);
        chunk_pool_local_core_alloc_count->increment(1);
        return true;
    }
    if (_reserved_bytes > size) {
        // try to allocate from other core's arena
        ++core_id;
        for (int i = 1; i < _arenas.size(); ++i, ++core_id) {
            if (_arenas[core_id % _arenas.size()]->pop_free_chunk(size, &chunk->data)) {
                DCHECK_GE(_reserved_bytes, 0);
                _reserved_bytes.fetch_sub(size);
                chunk_pool_other_core_alloc_count->increment(1);
                // reset chunk's core_id to other
                chunk->core_id = core_id % _arenas.size();
                return true;
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
        return false;
    }
    return true;
}

void ChunkAllocator::free(const Chunk& chunk) {
    if (chunk.core_id == -1) {
        return;
    }
    int64_t old_reserved_bytes = _reserved_bytes;
    int64_t new_reserved_bytes = 0;
    do {
        new_reserved_bytes = old_reserved_bytes + chunk.size;
        if (new_reserved_bytes > _reserve_bytes_limit) {
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

    _arenas[chunk.core_id]->push_free_chunk(chunk.data, chunk.size);
}

bool ChunkAllocator::allocate_align(size_t size, Chunk* chunk) {
    return allocate(BitUtil::RoundUpToPowerOfTwo(size), chunk);
}

} // namespace doris
