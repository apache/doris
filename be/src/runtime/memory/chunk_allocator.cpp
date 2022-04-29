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

#include "runtime/mem_tracker.h"
#include "runtime/memory/chunk.h"
#include "runtime/memory/system_allocator.h"
#include "runtime/thread_context.h"
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
        // TODO(zxy) The memory of vector resize is not recorded in chunk allocator mem tracker
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
    _mem_tracker =
            MemTracker::create_tracker(-1, "ChunkAllocator", nullptr, MemTrackerLevel::OVERVIEW);
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_END_CLEAR(_mem_tracker);
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

Status ChunkAllocator::allocate(size_t size, Chunk* chunk, MemTracker* tracker, bool check_limits) {
    MemTracker* reset_tracker =
            tracker ? tracker : tls_ctx()->_thread_mem_tracker_mgr->mem_tracker().get();
    // In advance, transfer the memory ownership of allocate from ChunkAllocator::tracker to the parameter tracker.
    // Next, if the allocate is successful, it will exit normally;
    // if the allocate fails, return this part of the memory to the parameter tracker.
    if (check_limits) {
        RETURN_IF_ERROR(_mem_tracker->try_transfer_to(reset_tracker, size));
    } else {
        _mem_tracker->transfer_to(reset_tracker, size);
    }

    // fast path: allocate from current core arena
    int core_id = CpuInfo::get_current_core();
    chunk->size = size;
    chunk->core_id = core_id;

    if (_arenas[core_id]->pop_free_chunk(size, &chunk->data)) {
        DCHECK_GE(_reserved_bytes, 0);
        _reserved_bytes.fetch_sub(size);
        chunk_pool_local_core_alloc_count->increment(1);
        return Status::OK();
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
                return Status::OK();
            }
        }
    }

    int64_t cost_ns = 0;
    {
        SCOPED_RAW_TIMER(&cost_ns);
        // allocate from system allocator
        chunk->data = SystemAllocator::allocate(size);
        // The allocated chunk is consumed in the tls mem tracker, we want to consume in the ChunkAllocator tracker,
        // transfer memory ownership. TODO(zxy) replace with switch tls tracker
        tls_ctx()->_thread_mem_tracker_mgr->mem_tracker()->transfer_to(_mem_tracker.get(), size);
    }
    chunk_pool_system_alloc_count->increment(1);
    chunk_pool_system_alloc_cost_ns->increment(cost_ns);
    if (chunk->data == nullptr) {
        // allocate fails, return this part of the memory to the parameter tracker.
        reset_tracker->transfer_to(_mem_tracker.get(), size);
        return Status::MemoryAllocFailed(
                fmt::format("ChunkAllocator failed to allocate chunk {} bytes", size));
    }
    return Status::OK();
}

void ChunkAllocator::free(const Chunk& chunk, MemTracker* tracker) {
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
                // The freed chunk is released in the tls mem tracker. When the chunk was allocated,
                // it was consumed in the parameter tracker, so if the tls mem tracker and the parameter
                // tracker are different, transfer memory ownership.
                if (tracker)
                    tracker->transfer_to(tls_ctx()->_thread_mem_tracker_mgr->mem_tracker().get(),
                                         chunk.size);
            }
            chunk_pool_system_free_count->increment(1);
            chunk_pool_system_free_cost_ns->increment(cost_ns);

            return;
        }
    } while (!_reserved_bytes.compare_exchange_weak(old_reserved_bytes, new_reserved_bytes));

    // The chunk's memory ownership is transferred from MemPool to ChunkAllocator.
    if (tracker) {
        tracker->transfer_to(_mem_tracker.get(), chunk.size);
    } else {
        tls_ctx()->_thread_mem_tracker_mgr->mem_tracker()->transfer_to(_mem_tracker.get(),
                                                                       chunk.size);
    }
    _arenas[chunk.core_id]->push_free_chunk(chunk.data, chunk.size);
}

Status ChunkAllocator::allocate_align(size_t size, Chunk* chunk, MemTracker* tracker,
                                      bool check_limits) {
    return allocate(BitUtil::RoundUpToPowerOfTwo(size), chunk, tracker, check_limits);
}

} // namespace doris
