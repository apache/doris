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
    static inline int64_t get_je_metrics(const std::string& name) {
#ifdef USE_JEMALLOC
        size_t value = 0;
        size_t sz = sizeof(value);
        if (jemallctl(name.c_str(), &value, &sz, nullptr, 0) == 0) {
            return value;
        }
#endif
        return 0;
    }

    static inline unsigned get_je_unsigned_metrics(const std::string& name) {
#ifdef USE_JEMALLOC
        unsigned value = 0;
        size_t sz = sizeof(value);
        if (jemallctl(name.c_str(), &value, &sz, nullptr, 0) == 0) {
            return value;
        }
#endif
        return 0;
    }

    static inline int64_t get_je_all_arena_metrics(const std::string& name) {
#ifdef USE_JEMALLOC
        return get_je_metrics(fmt::format("stats.arenas.{}.{}", MALLCTL_ARENAS_ALL, name));
#endif
        return 0;
    }

    static inline int64_t get_je_all_arena_extents_metrics(int64_t page_size_index,
                                                           const std::string& extent_type) {
#ifdef USE_JEMALLOC
        return get_je_metrics(fmt::format("stats.arenas.{}.extents.{}.{}", MALLCTL_ARENAS_ALL,
                                          page_size_index, extent_type));
#endif
        return 0;
    }

    static inline void je_purge_all_arena_dirty_pages() {
#ifdef USE_JEMALLOC
        // https://github.com/jemalloc/jemalloc/issues/2470
        // If there is a core dump here, it may cover up the real stack, if stack trace indicates heap corruption
        // (which led to invalid jemalloc metadata), like double free or use-after-free in the application.
        // Try sanitizers such as ASAN, or build jemalloc with --enable-debug to investigate further.
        if (config::enable_je_purge_dirty_pages) {
            try {
                // Purge all unused dirty pages for arena <i>, or for all arenas if <i> equals MALLCTL_ARENAS_ALL.
                int err = jemallctl(fmt::format("arena.{}.purge", MALLCTL_ARENAS_ALL).c_str(),
                                    nullptr, nullptr, nullptr, 0);
                if (err) {
                    LOG(WARNING) << "Jemalloc purge all unused dirty pages failed";
                }
            } catch (...) {
                LOG(WARNING) << "Purge all unused dirty pages for all arenas failed";
            }
        }
#endif
    }

    // the limit of `tcache` is the number of pages, not the total number of page bytes.
    // `tcache` has two cleaning opportunities: 1. the number of memory alloc and releases reaches a certain number,
    // recycle pages that has not been used for a long time; 2. recycle all `tcache` when the thread exits.
    // here add a total size limit.
    static inline void je_thread_tcache_flush() {
#ifdef USE_JEMALLOC
        constexpr size_t TCACHE_LIMIT = (1ULL << 30); // 1G
        if (allocator_cache_mem() - je_dirty_pages_mem() > TCACHE_LIMIT) {
            int err = jemallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
            if (err) {
                LOG(WARNING) << "Jemalloc thread.tcache.flush failed";
            }
        }
#endif
    }

    static std::mutex je_purge_dirty_pages_lock;
    static std::condition_variable je_purge_dirty_pages_cv;
    static std::atomic<bool> je_purge_dirty_pages_notify;
    static void notify_je_purge_dirty_pages() {
        je_purge_dirty_pages_notify.store(true, std::memory_order_relaxed);
        je_purge_dirty_pages_cv.notify_all();
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
    static inline int64_t je_dirty_pages_mem_limit() {
        return _s_je_dirty_pages_mem_limit.load(std::memory_order_relaxed);
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
    static std::atomic<int64_t> _s_je_dirty_pages_mem_limit;
    static std::atomic<int64_t> _s_virtual_memory_used;

    static int64_t _s_cgroup_mem_limit;
    static int64_t _s_cgroup_mem_usage;
    static bool _s_cgroup_mem_refresh_state;
    static int64_t _s_cgroup_mem_refresh_wait_times;

    static std::atomic<int64_t> _s_sys_mem_available;
    static int64_t _s_sys_mem_available_low_water_mark;
    static int64_t _s_sys_mem_available_warning_water_mark;
    static std::atomic<int64_t> _s_process_minor_gc_size;
    static std::atomic<int64_t> _s_process_full_gc_size;
};

} // namespace doris
