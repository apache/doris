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

#include "util/mem_info.h"

namespace doris {

class GlobalMemoryArbitrator {
public:
    /** jemalloc pdirty is number of pages within unused extents that are potentially
      * dirty, and for which madvise() or similar has not been called.
      *
      * So they will be subtracted from RSS to make accounting more
      * accurate, since those pages are not really RSS but a memory
      * that can be used at anytime via jemalloc.
      */
    static inline int64_t vm_rss_sub_allocator_cache() {
        return PerfCounters::get_vm_rss() - static_cast<int64_t>(MemInfo::allocator_cache_mem());
    }

    static inline void reset_refresh_interval_memory_growth() {
        refresh_interval_memory_growth = 0;
    }

    // If need to use process memory in your execution logic, pls use it.
    // equal to real process memory(vm_rss), subtract jemalloc dirty page cache,
    // add reserved memory and growth memory since the last vm_rss update.
    static inline int64_t process_memory_usage() {
        return vm_rss_sub_allocator_cache() +
               refresh_interval_memory_growth.load(std::memory_order_relaxed) +
               process_reserved_memory();
    }

    static std::string process_memory_used_str() {
        auto msg = fmt::format("process memory used {}",
                               PrettyPrinter::print(process_memory_usage(), TUnit::BYTES));
#ifdef ADDRESS_SANITIZER
        msg = "[ASAN]" + msg;
#endif
        return msg;
    }

    static std::string process_memory_used_details_str() {
        auto msg = fmt::format(
                "process memory used {}(= {}[vm/rss] - {}[tc/jemalloc_cache] + {}[reserved] + "
                "{}B[waiting_refresh])",
                PrettyPrinter::print(process_memory_usage(), TUnit::BYTES),
                PerfCounters::get_vm_rss_str(),
                PrettyPrinter::print(static_cast<uint64_t>(MemInfo::allocator_cache_mem()),
                                     TUnit::BYTES),
                PrettyPrinter::print(process_reserved_memory(), TUnit::BYTES),
                refresh_interval_memory_growth);
#ifdef ADDRESS_SANITIZER
        msg = "[ASAN]" + msg;
#endif
        return msg;
    }

    static inline int64_t sys_mem_available() {
        return MemInfo::_s_sys_mem_available.load(std::memory_order_relaxed) -
               refresh_interval_memory_growth.load(std::memory_order_relaxed) -
               process_reserved_memory();
    }

    static inline std::string sys_mem_available_str() {
        auto msg = fmt::format("sys available memory {}",
                               PrettyPrinter::print(sys_mem_available(), TUnit::BYTES));
#ifdef ADDRESS_SANITIZER
        msg = "[ASAN]" + msg;
#endif
        return msg;
    }

    static inline std::string sys_mem_available_details_str() {
        auto msg = fmt::format(
                "sys available memory {}(= {}[proc/available] - {}[reserved] - "
                "{}B[waiting_refresh])",
                PrettyPrinter::print(sys_mem_available(), TUnit::BYTES),
                PrettyPrinter::print(MemInfo::_s_sys_mem_available.load(std::memory_order_relaxed),
                                     TUnit::BYTES),
                PrettyPrinter::print(process_reserved_memory(), TUnit::BYTES),
                refresh_interval_memory_growth);
#ifdef ADDRESS_SANITIZER
        msg = "[ASAN]" + msg;
#endif
        return msg;
    }

    static inline bool try_reserve_process_memory(int64_t bytes) {
        if (sys_mem_available() - bytes < MemInfo::sys_mem_available_low_water_mark()) {
            return false;
        }
        int64_t old_reserved_mem = _s_process_reserved_memory.load(std::memory_order_relaxed);
        int64_t new_reserved_mem = 0;
        do {
            new_reserved_mem = old_reserved_mem + bytes;
            if (UNLIKELY(vm_rss_sub_allocator_cache() +
                                 refresh_interval_memory_growth.load(std::memory_order_relaxed) +
                                 new_reserved_mem >=
                         MemInfo::mem_limit())) {
                return false;
            }
        } while (!_s_process_reserved_memory.compare_exchange_weak(
                old_reserved_mem, new_reserved_mem, std::memory_order_relaxed));
        return true;
    }

    static inline void release_process_reserved_memory(int64_t bytes) {
        _s_process_reserved_memory.fetch_sub(bytes, std::memory_order_relaxed);
    }

    static inline int64_t process_reserved_memory() {
        return _s_process_reserved_memory.load(std::memory_order_relaxed);
    }

    static bool is_exceed_soft_mem_limit(int64_t bytes = 0) {
        return process_memory_usage() + bytes >= MemInfo::soft_mem_limit() ||
               sys_mem_available() - bytes < MemInfo::sys_mem_available_warning_water_mark();
    }

    static bool is_exceed_hard_mem_limit(int64_t bytes = 0) {
        // Limit process memory usage using the actual physical memory of the process in `/proc/self/status`.
        // This is independent of the consumption value of the mem tracker, which counts the virtual memory
        // of the process malloc.
        // for fast, expect MemInfo::initialized() to be true.
        //
        // tcmalloc/jemalloc allocator cache does not participate in the mem check as part of the process physical memory.
        // because `new/malloc` will trigger mem hook when using tcmalloc/jemalloc allocator cache,
        // but it may not actually alloc physical memory, which is not expected in mem hook fail.
        return process_memory_usage() + bytes >= MemInfo::mem_limit() ||
               sys_mem_available() - bytes < MemInfo::sys_mem_available_low_water_mark();
    }

    static std::string process_mem_log_str() {
        return fmt::format(
                "os physical memory {}. {}, limit {}, soft limit {}. {}, low water mark {}, "
                "warning water mark {}.",
                PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES),
                process_memory_used_details_str(), MemInfo::mem_limit_str(),
                MemInfo::soft_mem_limit_str(), sys_mem_available_details_str(),
                PrettyPrinter::print(MemInfo::sys_mem_available_low_water_mark(), TUnit::BYTES),
                PrettyPrinter::print(MemInfo::sys_mem_available_warning_water_mark(),
                                     TUnit::BYTES));
    }

    static std::string process_limit_exceeded_errmsg_str() {
        return fmt::format(
                "{} exceed limit {} or {} less than low water mark {}", process_memory_used_str(),
                MemInfo::mem_limit_str(), sys_mem_available_str(),
                PrettyPrinter::print(MemInfo::sys_mem_available_low_water_mark(), TUnit::BYTES));
    }

    static std::string process_soft_limit_exceeded_errmsg_str() {
        return fmt::format("{} exceed soft limit {} or {} less than warning water mark {}.",
                           process_memory_used_str(), MemInfo::soft_mem_limit_str(),
                           sys_mem_available_str(),
                           PrettyPrinter::print(MemInfo::sys_mem_available_warning_water_mark(),
                                                TUnit::BYTES));
    }

    // It is only used after the memory limit is exceeded. When multiple threads are waiting for the available memory of the process,
    // avoid multiple threads starting at the same time and causing OOM.
    static std::atomic<int64_t> refresh_interval_memory_growth;

private:
    static std::atomic<int64_t> _s_process_reserved_memory;
};

} // namespace doris
