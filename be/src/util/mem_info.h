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

#ifndef DORIS_BE_SRC_COMMON_UTIL_MEM_INFO_H
#define DORIS_BE_SRC_COMMON_UTIL_MEM_INFO_H

#include <gperftools/malloc_extension.h>

#include <string>

#include "common/logging.h"

namespace doris {

// Provides the amount of physical memory available.
// Populated from /proc/meminfo.
// TODO: Combine mem-info, cpu-info and disk-info into hardware-info/perf_counters ?
class MemInfo {
public:
    // Initialize MemInfo.
    static void init();

    // Get total physical memory in bytes (if has cgroups memory limits, return the limits).
    static inline int64_t physical_mem() {
        DCHECK(_s_initialized);
        return _s_physical_mem;
    }

    static inline size_t current_mem() { return _s_allocator_physical_mem; }
    static inline size_t allocator_cache_mem() { return _s_allocator_cache_mem; }

    // Tcmalloc property `generic.total_physical_bytes` records the total length of the virtual memory
    // obtained by the process malloc, not the physical memory actually used by the process in the OS.
    static inline void refresh_allocator_mem() {
        MallocExtension::instance()->GetNumericProperty("generic.total_physical_bytes",
                                                        &_s_allocator_physical_mem);
        MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes",
                                                        &_s_tcmalloc_pageheap_free_bytes);
        MallocExtension::instance()->GetNumericProperty("tcmalloc.central_cache_free_bytes",
                                                        &_s_tcmalloc_central_bytes);
        MallocExtension::instance()->GetNumericProperty("tcmalloc.transfer_cache_free_bytes",
                                                        &_s_tcmalloc_transfer_bytes);
        MallocExtension::instance()->GetNumericProperty("tcmalloc.thread_cache_free_bytes",
                                                        &_s_tcmalloc_thread_bytes);
        _s_allocator_cache_mem = _s_tcmalloc_pageheap_free_bytes + _s_tcmalloc_central_bytes +
                                 _s_tcmalloc_transfer_bytes + _s_tcmalloc_thread_bytes;
    }

    static inline int64_t mem_limit() {
        DCHECK(_s_initialized);
        return _s_mem_limit;
    }
    static inline int64_t hard_mem_limit() { return _s_hard_mem_limit; }

    static std::string debug_string();

private:
    static bool _s_initialized;
    static int64_t _s_physical_mem;
    static int64_t _s_mem_limit;
    static int64_t _s_hard_mem_limit;
    static size_t _s_allocator_physical_mem;
    static size_t _s_tcmalloc_pageheap_free_bytes;
    static size_t _s_tcmalloc_central_bytes;
    static size_t _s_tcmalloc_transfer_bytes;
    static size_t _s_tcmalloc_thread_bytes;
    static size_t _s_allocator_cache_mem;
};

} // namespace doris
#endif
