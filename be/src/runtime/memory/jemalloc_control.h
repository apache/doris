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

#include <condition_variable>
#include <string>

#include "common/logging.h"

#ifdef USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif
#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
#include <gperftools/malloc_extension.h>
#endif

namespace doris {
#include "common/compile_check_begin.h"

class JemallocControl {
public:
    static inline int64_t get_tc_metrics(const std::string& name) {
#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
        size_t value = 0;
        MallocExtension::instance()->GetNumericProperty(name.c_str(), &value);
        return value;
#endif
        return 0;
    }

    template <typename T>
    static inline T get_jemallctl_value(const std::string& name) {
#ifdef USE_JEMALLOC
        T value;
        size_t value_size = sizeof(T);
        if (jemallctl(name.c_str(), &value, &value_size, nullptr, 0) != 0) {
            LOG(WARNING) << fmt::format("Failed, jemallctl get {}", name);
        }
        return value;
#endif
        return 0;
    }

    template <typename T>
    static inline void set_jemallctl_value(const std::string& name, T value) {
#ifdef USE_JEMALLOC
        T old_value;
        size_t old_value_size = sizeof(T);
        try {
            int err = jemallctl(name.c_str(), &old_value, &old_value_size,
                                reinterpret_cast<void*>(&value), sizeof(T));
            if (err) {
                LOG(WARNING) << fmt::format("Failed, jemallctl value for {} set to {} (old {})",
                                            name, value, old_value);
            }
        } catch (...) {
            LOG(WARNING) << fmt::format("Exception, jemallctl value for {} set to {} (old {})",
                                        name, value, old_value);
        }
#endif
    }

    static void action_jemallctl(const std::string& name);
    static int64_t get_je_all_arena_metrics(const std::string& name);
    static int64_t get_je_all_arena_extents_metrics(int64_t page_size_index,
                                                    const std::string& extent_type);
    static void je_purge_all_arena_dirty_pages();
    static void je_reset_all_arena_dirty_decay_ms(ssize_t dirty_decay_ms);
    static void je_decay_all_arena_dirty_pages();
    // the limit of `tcache` is the number of pages, not the total number of page bytes.
    // `tcache` has two cleaning opportunities: 1. the number of memory alloc and releases reaches a certain number,
    // recycle pages that has not been used for a long time; 2. recycle all `tcache` when the thread exits.
    // here add a total size limit.
    // only free the thread cache of the current thread, which will be fast.
    static void je_thread_tcache_flush();

    // Tcmalloc property `generic.total_physical_bytes` records the total length of the virtual memory
    // obtained by the process malloc, not the physical memory actually used by the process in the OS.
    static void refresh_allocator_mem();

    static inline size_t je_cache_bytes() {
        return je_cache_bytes_.load(std::memory_order_relaxed);
    }
    static inline size_t je_tcache_mem() { return je_tcache_mem_.load(std::memory_order_relaxed); }
    static inline size_t je_metadata_mem() {
        return je_metadata_mem_.load(std::memory_order_relaxed);
    }
    static inline int64_t je_dirty_pages_mem() {
        return je_dirty_pages_mem_.load(std::memory_order_relaxed);
    }
    static inline size_t je_virtual_memory_used() {
        return je_virtual_memory_used_.load(std::memory_order_relaxed);
    }

    static std::mutex je_purge_dirty_pages_lock;
    static std::atomic<bool> je_purge_dirty_pages_notify;
    static void notify_je_purge_dirty_pages() {
        je_purge_dirty_pages_notify.store(true, std::memory_order_relaxed);
    }

    static std::mutex je_reset_dirty_decay_lock;
    static std::atomic<bool> je_enable_dirty_page;
    static std::condition_variable je_reset_dirty_decay_cv;
    static std::atomic<bool> je_reset_dirty_decay_notify;
    static void notify_je_reset_dirty_decay() {
        je_reset_dirty_decay_notify.store(true, std::memory_order_relaxed);
        je_reset_dirty_decay_cv.notify_all();
    }

private:
    static std::atomic<int64_t> je_cache_bytes_;
    static std::atomic<int64_t> je_tcache_mem_;
    static std::atomic<int64_t> je_metadata_mem_;
    static std::atomic<int64_t> je_dirty_pages_mem_;
    static std::atomic<int64_t> je_virtual_memory_used_;
};

#include "common/compile_check_end.h"
} // namespace doris
