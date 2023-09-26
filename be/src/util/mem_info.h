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
#include <string>

#if !defined(__APPLE__) || !defined(_POSIX_C_SOURCE)
#include <unistd.h>
#else
#include <mach/vm_page_size.h>
#endif

#include "common/logging.h"
#ifdef USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#else
#include <gperftools/malloc_extension.h>
#endif
#include "common/config.h"
#include "util/perf_counters.h"

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
        return _s_physical_mem;
    }

    static void refresh_proc_meminfo();

    static inline int64_t sys_mem_available() {
        return _s_sys_mem_available.load(std::memory_order_relaxed) -
               refresh_interval_memory_growth;
    }
    static inline std::string sys_mem_available_str() { return _s_sys_mem_available_str; }
    static inline int64_t sys_mem_available_low_water_mark() {
        return _s_sys_mem_available_low_water_mark;
    }
    static inline int64_t sys_mem_available_warning_water_mark() {
        return _s_sys_mem_available_warning_water_mark;
    }

    static inline int64_t get_tc_metrics(const std::string& name) {
#ifndef USE_JEMALLOC
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

    static inline int64_t get_je_all_arena_metrics(const std::string& name) {
#ifdef USE_JEMALLOC
        return get_je_metrics(fmt::format("stats.arenas.{}.{}", MALLCTL_ARENAS_ALL, name));
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
                jemallctl(fmt::format("arena.{}.purge", MALLCTL_ARENAS_ALL).c_str(), nullptr,
                          nullptr, nullptr, 0);
            } catch (...) {
                LOG(WARNING) << "Purge all unused dirty pages for all arenas failed";
            }
        }
#endif
    }

    static inline size_t allocator_virtual_mem() {
        return _s_virtual_memory_used.load(std::memory_order_relaxed);
    }
    static inline size_t allocator_cache_mem() {
        return _s_allocator_cache_mem.load(std::memory_order_relaxed);
    }
    static inline std::string allocator_cache_mem_str() { return _s_allocator_cache_mem_str; }
    static inline int64_t proc_mem_no_allocator_cache() {
        return _s_proc_mem_no_allocator_cache.load(std::memory_order_relaxed) +
               refresh_interval_memory_growth;
    }

    // Tcmalloc property `generic.total_physical_bytes` records the total length of the virtual memory
    // obtained by the process malloc, not the physical memory actually used by the process in the OS.
    static void refresh_allocator_mem();

    /** jemalloc pdirty is number of pages within unused extents that are potentially
      * dirty, and for which madvise() or similar has not been called.
      *
      * So they will be subtracted from RSS to make accounting more
      * accurate, since those pages are not really RSS but a memory
      * that can be used at anytime via jemalloc.
      */
    static inline void refresh_proc_mem_no_allocator_cache() {
        _s_proc_mem_no_allocator_cache.store(
                PerfCounters::get_vm_rss() - static_cast<int64_t>(_s_allocator_cache_mem.load(
                                                     std::memory_order_relaxed)),
                std::memory_order_relaxed);
        refresh_interval_memory_growth = 0;
    }

    static inline int64_t mem_limit() {
        DCHECK(_s_initialized);
        return _s_mem_limit;
    }
    static inline std::string mem_limit_str() {
        DCHECK(_s_initialized);
        return _s_mem_limit_str;
    }
    static inline int64_t soft_mem_limit() {
        DCHECK(_s_initialized);
        return _s_soft_mem_limit;
    }
    static inline std::string soft_mem_limit_str() {
        DCHECK(_s_initialized);
        return _s_soft_mem_limit_str;
    }
    static bool is_exceed_soft_mem_limit(int64_t bytes = 0) {
        return proc_mem_no_allocator_cache() + bytes >= soft_mem_limit() ||
               sys_mem_available() < sys_mem_available_warning_water_mark();
    }

    static std::string debug_string();

    static bool process_minor_gc();
    static bool process_full_gc();

    static int64_t tg_hard_memory_limit_gc();
    static int64_t tg_soft_memory_limit_gc(int64_t request_free_memory, RuntimeProfile* profile);

    // It is only used after the memory limit is exceeded. When multiple threads are waiting for the available memory of the process,
    // avoid multiple threads starting at the same time and causing OOM.
    static std::atomic<int64_t> refresh_interval_memory_growth;

private:
    static bool _s_initialized;
    static int64_t _s_physical_mem;
    static int64_t _s_mem_limit;
    static std::string _s_mem_limit_str;
    static int64_t _s_soft_mem_limit;
    static std::string _s_soft_mem_limit_str;

    static std::atomic<int64_t> _s_allocator_cache_mem;
    static std::string _s_allocator_cache_mem_str;
    static std::atomic<int64_t> _s_virtual_memory_used;
    static std::atomic<int64_t> _s_proc_mem_no_allocator_cache;

    static std::atomic<int64_t> _s_sys_mem_available;
    static std::string _s_sys_mem_available_str;
    static int64_t _s_sys_mem_available_low_water_mark;
    static int64_t _s_sys_mem_available_warning_water_mark;
    static int64_t _s_process_minor_gc_size;
    static int64_t _s_process_full_gc_size;
};

} // namespace doris
