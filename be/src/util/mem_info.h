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

#include <gperftools/malloc_extension.h>

#include <string>

#include "common/logging.h"
#ifdef USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#else
#include <gperftools/malloc_extension.h>
#endif
#include "util/perf_counters.h"
#include "util/pretty_printer.h"

namespace doris {

// Provides the amount of physical memory available.
// Populated from /proc/meminfo.
// TODO: Combine mem-info, cpu-info and disk-info into hardware-info/perf_counters ?
class MemInfo {
public:
    // Initialize MemInfo.
    static void init();

    static inline bool initialized() { return _s_initialized; }

    // Get total physical memory in bytes (if has cgroups memory limits, return the limits).
    static inline int64_t physical_mem() {
        DCHECK(_s_initialized);
        return _s_physical_mem;
    }

    static void refresh_proc_meminfo();

    static inline int64_t sys_mem_available() { return _s_sys_mem_available; }
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
        if (je_mallctl(name.c_str(), &value, &sz, nullptr, 0) == 0) {
            return value;
        }
#endif
        return 0;
    }
    static inline size_t allocator_virtual_mem() { return _s_virtual_memory_used; }
    static inline size_t allocator_cache_mem() { return _s_allocator_cache_mem; }
    static inline std::string allocator_cache_mem_str() { return _s_allocator_cache_mem_str; }
    static inline int64_t proc_mem_no_allocator_cache() { return _s_proc_mem_no_allocator_cache; }

    // Tcmalloc property `generic.total_physical_bytes` records the total length of the virtual memory
    // obtained by the process malloc, not the physical memory actually used by the process in the OS.
    static void refresh_allocator_mem();

    static inline void refresh_proc_mem_no_allocator_cache() {
        _s_proc_mem_no_allocator_cache =
                PerfCounters::get_vm_rss() - static_cast<int64_t>(_s_allocator_cache_mem);
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

    static std::string debug_string();

private:
    static bool _s_initialized;
    static int64_t _s_physical_mem;
    static int64_t _s_mem_limit;
    static std::string _s_mem_limit_str;
    static int64_t _s_soft_mem_limit;

    static int64_t _s_allocator_cache_mem;
    static std::string _s_allocator_cache_mem_str;
    static int64_t _s_virtual_memory_used;
    static int64_t _s_proc_mem_no_allocator_cache;

    static int64_t _s_sys_mem_available;
    static std::string _s_sys_mem_available_str;
    static int64_t _s_sys_mem_available_low_water_mark;
    static int64_t _s_sys_mem_available_warning_water_mark;
};

} // namespace doris
