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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/mem-info.h
// and modified by Doris

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <string>

#if !defined(__APPLE__) || !defined(_POSIX_C_SOURCE)
#include <unistd.h>
#else
#include <mach/vm_page_size.h>
#endif

#include "common/logging.h"
#ifdef USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif
#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
#include <gperftools/malloc_extension.h>
#endif
#include "common/config.h"
#include "util/perf_counters.h"
#include "util/pretty_printer.h"

namespace doris {

class RuntimeProfile;

// Provides the amount of physical memory available.
// Populated from /proc/meminfo.
// TODO: Combine mem-info, cpu-info and disk-info into hardware-info/perf_counters ?
class MemInfo {
public:
    // Initialize MemInfo.
    static void init();

    static inline bool initialized() { return _s_initialized; }

    static int get_page_size() {
#if !defined(__APPLE__) || !defined(_POSIX_C_SOURCE)
        return getpagesize();
#else
        return vm_page_size;
#endif
    }

    // Get total physical memory in bytes (if has cgroups memory limits, return the limits).
    static inline int64_t physical_mem() {
        DCHECK(_s_initialized);
        return _s_physical_mem.load(std::memory_order_relaxed);
    }

    static void refresh_proc_meminfo();

    static void refresh_memory_bvar();

    static inline int64_t sys_mem_available_low_water_mark() {
        return _s_sys_mem_available_low_water_mark;
    }
    static inline int64_t sys_mem_available_warning_water_mark() {
        return _s_sys_mem_available_warning_water_mark;
    }
    static inline int64_t process_minor_gc_size() {
        return _s_process_minor_gc_size.load(std::memory_order_relaxed);
    }
    static inline int64_t process_full_gc_size() {
        return _s_process_full_gc_size.load(std::memory_order_relaxed);
    }

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
        if (jemallctl(name.c_str(), &value, &value_size, nullptr, 0) == 0) {
            return value;
        } else {
            LOG(WARNING) << fmt::format("Failed, jemallctl get {}", name);
        }
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

    static inline void action_jemallctl(const std::string& name) {
#ifdef USE_JEMALLOC
        try {
            int err = jemallctl(name.c_str(), nullptr, nullptr, nullptr, 0);
            if (err) {
                LOG(WARNING) << fmt::format("Failed, jemallctl action {}", name);
            }
        } catch (...) {
            LOG(WARNING) << fmt::format("Exception, jemallctl action {}", name);
        }
#endif
    }

    static inline int64_t get_je_all_arena_metrics(const std::string& name) {
#ifdef USE_JEMALLOC
        return get_jemallctl_value<int64_t>(
                fmt::format("stats.arenas.{}.{}", MALLCTL_ARENAS_ALL, name));
#endif
        return 0;
    }

    static inline int64_t get_je_all_arena_extents_metrics(int64_t page_size_index,
                                                           const std::string& extent_type) {
#ifdef USE_JEMALLOC
        return get_jemallctl_value<int64_t>(fmt::format(
                "stats.arenas.{}.extents.{}.{}", MALLCTL_ARENAS_ALL, page_size_index, extent_type));
#endif
        return 0;
    }

    static inline void je_purge_all_arena_dirty_pages() {
#ifdef USE_JEMALLOC
        // https://github.com/jemalloc/jemalloc/issues/2470
        // If there is a core dump here, it may cover up the real stack, if stack trace indicates heap corruption
        // (which led to invalid jemalloc metadata), like double free or use-after-free in the application.
        // Try sanitizers such as ASAN, or build jemalloc with --enable-debug to investigate further.
        action_jemallctl(fmt::format("arena.{}.purge", MALLCTL_ARENAS_ALL));
#endif
    }

    static inline void je_reset_all_arena_dirty_decay_ms(ssize_t dirty_decay_ms) {
#ifdef USE_JEMALLOC
        // Each time this interface is set, all currently unused dirty pages are considered
        // to have fully decayed, which causes immediate purging of all unused dirty pages unless
        // the decay time is set to -1
        //
        // NOTE: Using "arena.MALLCTL_ARENAS_ALL.dirty_decay_ms" to modify all arenas will fail or even crash,
        // which may be a bug.
        for (unsigned i = 0; i < get_jemallctl_value<unsigned>("arenas.narenas"); i++) {
            set_jemallctl_value<ssize_t>(fmt::format("arena.{}.dirty_decay_ms", i), dirty_decay_ms);
        }
#endif
    }

    static inline void je_decay_all_arena_dirty_pages() {
#ifdef USE_JEMALLOC
        // Trigger decay-based purging of unused dirty/muzzy pages for arena <i>, or for all arenas if <i> equals
        // MALLCTL_ARENAS_ALL. The proportion of unused dirty/muzzy pages to be purged depends on the
        // current time; see opt.dirty_decay_ms and opt.muzy_decay_ms for details.
        action_jemallctl(fmt::format("arena.{}.decay", MALLCTL_ARENAS_ALL));
#endif
    }

    // the limit of `tcache` is the number of pages, not the total number of page bytes.
    // `tcache` has two cleaning opportunities: 1. the number of memory alloc and releases reaches a certain number,
    // recycle pages that has not been used for a long time; 2. recycle all `tcache` when the thread exits.
    // here add a total size limit.
    // only free the thread cache of the current thread, which will be fast.
    static inline void je_thread_tcache_flush() {
#ifdef USE_JEMALLOC
        constexpr size_t TCACHE_LIMIT = (1ULL << 30); // 1G
        if (allocator_cache_mem() - je_dirty_pages_mem() > TCACHE_LIMIT) {
            action_jemallctl("thread.tcache.flush");
        }
#endif
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

    static inline size_t allocator_virtual_mem() {
        return _s_virtual_memory_used.load(std::memory_order_relaxed);
    }
    static inline size_t allocator_cache_mem() {
        return _s_allocator_cache_mem.load(std::memory_order_relaxed);
    }
    static inline size_t allocator_metadata_mem() {
        return _s_allocator_metadata_mem.load(std::memory_order_relaxed);
    }
    static inline int64_t je_dirty_pages_mem() {
        return _s_je_dirty_pages_mem.load(std::memory_order_relaxed);
    }

    // Tcmalloc property `generic.total_physical_bytes` records the total length of the virtual memory
    // obtained by the process malloc, not the physical memory actually used by the process in the OS.
    static void refresh_allocator_mem();

    static inline int64_t mem_limit() {
        DCHECK(_s_initialized);
        return _s_mem_limit.load(std::memory_order_relaxed);
    }
    static inline std::string mem_limit_str() {
        DCHECK(_s_initialized);
        return PrettyPrinter::print(_s_mem_limit.load(std::memory_order_relaxed), TUnit::BYTES);
    }
    static inline int64_t soft_mem_limit() {
        DCHECK(_s_initialized);
        return _s_soft_mem_limit.load(std::memory_order_relaxed);
    }
    static inline std::string soft_mem_limit_str() {
        DCHECK(_s_initialized);
        return PrettyPrinter::print(_s_soft_mem_limit.load(std::memory_order_relaxed),
                                    TUnit::BYTES);
    }
    static inline int64_t cgroup_mem_limit() {
        DCHECK(_s_initialized);
        return _s_cgroup_mem_limit.load(std::memory_order_relaxed);
    }
    static inline int64_t cgroup_mem_usage() {
        DCHECK(_s_initialized);
        return _s_cgroup_mem_usage.load(std::memory_order_relaxed);
    }
    static inline int64_t cgroup_mem_refresh_state() {
        DCHECK(_s_initialized);
        return _s_cgroup_mem_refresh_state.load(std::memory_order_relaxed);
    }

    static std::string debug_string();

private:
    friend class GlobalMemoryArbitrator;

    static bool _s_initialized;
    static std::atomic<int64_t> _s_physical_mem;
    static std::atomic<int64_t> _s_mem_limit;
    static std::atomic<int64_t> _s_soft_mem_limit;

    static std::atomic<int64_t> _s_allocator_cache_mem;
    static std::atomic<int64_t> _s_allocator_metadata_mem;
    static std::atomic<int64_t> _s_je_dirty_pages_mem;
    static std::atomic<int64_t> _s_virtual_memory_used;

    static std::atomic<int64_t> _s_cgroup_mem_limit;
    static std::atomic<int64_t> _s_cgroup_mem_usage;
    static std::atomic<bool> _s_cgroup_mem_refresh_state;
    static int64_t _s_cgroup_mem_refresh_wait_times;

    static std::atomic<int64_t> _s_sys_mem_available;
    static int64_t _s_sys_mem_available_low_water_mark;
    static int64_t _s_sys_mem_available_warning_water_mark;
    static std::atomic<int64_t> _s_process_minor_gc_size;
    static std::atomic<int64_t> _s_process_full_gc_size;
};

} // namespace doris
