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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

#if !defined(__APPLE__) || !defined(_POSIX_C_SOURCE)
#include <unistd.h>
#else
#include <mach/vm_page_size.h>
#endif

#include "common/config.h"
#include "common/logging.h"
#include "util/perf_counters.h"
#include "util/pretty_printer.h"

namespace doris {

// Provides the amount of physical memory available and memory limit.
// Populated from /proc/meminfo and other system attributes.
// Note: The memory values ​​in MemInfo are original values,
// usually you should use the memory values ​​in GlobalMemoryArbitrator,
// which are corrected values ​​that better meet your needs.
class MemInfo {
public:
    // Initialize MemInfo.
    static void init();

    static inline bool initialized() { return _s_initialized; }

    static int get_page_size() {
#if !defined(__APPLE__) || !defined(_POSIX_C_SOURCE)
        return getpagesize();
#else
        return int(vm_page_size);
#endif
    }

    // Get total physical memory in bytes (if has cgroups memory limits, return the limits).
    static inline int64_t physical_mem() {
        DCHECK(_s_initialized);
        return _s_physical_mem.load(std::memory_order_relaxed);
    }

    static void refresh_proc_meminfo();

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

    static std::atomic<int64_t> _s_cgroup_mem_limit;
    static std::atomic<int64_t> _s_cgroup_mem_usage;
    static std::atomic<bool> _s_cgroup_mem_refresh_state;
    static int64_t _s_cgroup_mem_refresh_wait_times;

    // If you need use system available memory size, use GlobalMemoryArbitrator::sys_mem_available(),
    // this value in MemInfo is the original value.
    static std::atomic<int64_t> _s_sys_mem_available;
    static int64_t _s_sys_mem_available_low_water_mark;
    static int64_t _s_sys_mem_available_warning_water_mark;
    static std::atomic<int64_t> _s_process_minor_gc_size;
    static std::atomic<int64_t> _s_process_full_gc_size;
};

} // namespace doris
