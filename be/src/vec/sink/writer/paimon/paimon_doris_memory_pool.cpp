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

#include "vec/sink/writer/paimon/paimon_doris_memory_pool.h"

#ifdef WITH_PAIMON_CPP

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "util/defer_op.h"

namespace doris::vectorized {

namespace {

constexpr uint64_t kDefaultAlignment = 64;

alignas(kDefaultAlignment) int64_t g_zero_size_area[1] = {0};

inline uint64_t _normalize_alignment(uint64_t alignment) {
    return alignment == 0 ? kDefaultAlignment : alignment;
}

inline void _update_max(std::atomic<uint64_t>* max_value, uint64_t candidate) {
    uint64_t cur = max_value->load(std::memory_order_relaxed);
    while (candidate > cur &&
           !max_value->compare_exchange_weak(cur, candidate, std::memory_order_acq_rel)) {
    }
}

} // namespace

PaimonDorisMemoryPool::PaimonDorisMemoryPool(
        const std::shared_ptr<::doris::MemTrackerLimiter>& query_mem_tracker)
        : _query_mem_tracker(query_mem_tracker) {}

void* PaimonDorisMemoryPool::_zero_size_ptr() {
    return reinterpret_cast<void*>(&g_zero_size_area);
}

void PaimonDorisMemoryPool::_ensure_thread_context(
        std::unique_ptr<::doris::SwitchThreadMemTrackerLimiter>* switcher,
        std::unique_ptr<::doris::AttachTask>* attacher) {
    if (_query_mem_tracker == nullptr) {
        return;
    }

    ::doris::ThreadContext* tctx = nullptr;
    try {
        tctx = ::doris::thread_context();
    } catch (const ::doris::Exception&) {
        return;
    }
    if (tctx == nullptr || !tctx->is_attach_task()) {
        *attacher = std::make_unique<::doris::AttachTask>(_query_mem_tracker);
        return;
    }

    if (!(tctx->thread_mem_tracker_mgr->limiter_mem_tracker()->type() ==
          doris::MemTrackerLimiter::Type::QUERY)) {
        *attacher = std::make_unique<::doris::AttachTask>(_query_mem_tracker);
        return;
    }

    if (_query_mem_tracker.get() != tctx->thread_mem_tracker_mgr->limiter_mem_tracker()) {
        *switcher = std::make_unique<::doris::SwitchThreadMemTrackerLimiter>(_query_mem_tracker);
    }
}

void* PaimonDorisMemoryPool::Malloc(uint64_t size, uint64_t alignment) {
    if (size == 0) {
        return _zero_size_ptr();
    }
    ::doris::ThreadLocalHandle::create_thread_local_if_not_exits();
    Defer clear_thread_local {
            [&]() { ::doris::ThreadLocalHandle::del_thread_local_if_count_is_zero(); }};
    std::unique_ptr<::doris::SwitchThreadMemTrackerLimiter> switcher;
    std::unique_ptr<::doris::AttachTask> attacher;
    _ensure_thread_context(&switcher, &attacher);
    void* ptr = _allocator.alloc(size, _normalize_alignment(alignment));
    if (ptr == nullptr) {
        return nullptr;
    }
    uint64_t after = _bytes_allocated.fetch_add(size, std::memory_order_acq_rel) + size;
    _update_max(&_max_bytes_allocated, after);
    return ptr;
}

void* PaimonDorisMemoryPool::Realloc(void* p, size_t old_size, size_t new_size,
                                     uint64_t alignment) {
    if (p == _zero_size_ptr()) {
        old_size = 0;
    }
    if (new_size == 0) {
        if (p != _zero_size_ptr() && old_size > 0) {
            Free(p, old_size);
        }
        return _zero_size_ptr();
    }

    void* previous = p == _zero_size_ptr() ? nullptr : p;
    ::doris::ThreadLocalHandle::create_thread_local_if_not_exits();
    Defer clear_thread_local {
            [&]() { ::doris::ThreadLocalHandle::del_thread_local_if_count_is_zero(); }};
    std::unique_ptr<::doris::SwitchThreadMemTrackerLimiter> switcher;
    std::unique_ptr<::doris::AttachTask> attacher;
    _ensure_thread_context(&switcher, &attacher);
    void* new_ptr =
            _allocator.realloc(previous, old_size, new_size, _normalize_alignment(alignment));
    if (new_ptr == nullptr) {
        return nullptr;
    }

    if (new_size > old_size) {
        uint64_t delta = new_size - old_size;
        uint64_t after = _bytes_allocated.fetch_add(delta, std::memory_order_acq_rel) + delta;
        _update_max(&_max_bytes_allocated, after);
    } else if (old_size > new_size) {
        _bytes_allocated.fetch_sub(old_size - new_size, std::memory_order_acq_rel);
    }
    return new_ptr;
}

void PaimonDorisMemoryPool::Free(void* p, uint64_t size) {
    if (p == nullptr) {
        return;
    }
    if (p == _zero_size_ptr()) {
        return;
    }
    ::doris::ThreadLocalHandle::create_thread_local_if_not_exits();
    Defer clear_thread_local {
            [&]() { ::doris::ThreadLocalHandle::del_thread_local_if_count_is_zero(); }};
    std::unique_ptr<::doris::SwitchThreadMemTrackerLimiter> switcher;
    std::unique_ptr<::doris::AttachTask> attacher;
    _ensure_thread_context(&switcher, &attacher);
    _allocator.free(p, size);
    _bytes_allocated.fetch_sub(size, std::memory_order_acq_rel);
}

uint64_t PaimonDorisMemoryPool::CurrentUsage() const {
    return _bytes_allocated.load(std::memory_order_acquire);
}

uint64_t PaimonDorisMemoryPool::MaxMemoryUsage() const {
    return _max_bytes_allocated.load(std::memory_order_acquire);
}

} // namespace doris::vectorized

#endif
