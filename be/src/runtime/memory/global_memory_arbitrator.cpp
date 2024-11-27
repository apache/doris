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

#include "runtime/memory/global_memory_arbitrator.h"

#include <bvar/bvar.h>

#include "runtime/thread_context.h"

namespace doris {

std::mutex GlobalMemoryArbitrator::_reserved_trackers_lock;
std::unordered_map<std::string, MemTracker::MemCounter> GlobalMemoryArbitrator::_reserved_trackers;

bvar::PassiveStatus<int64_t> g_vm_rss_sub_allocator_cache(
        "meminfo_vm_rss_sub_allocator_cache",
        [](void*) { return GlobalMemoryArbitrator::vm_rss_sub_allocator_cache(); }, nullptr);
bvar::PassiveStatus<int64_t> g_process_memory_usage(
        "meminfo_process_memory_usage",
        [](void*) { return GlobalMemoryArbitrator::process_memory_usage(); }, nullptr);
bvar::PassiveStatus<int64_t> g_sys_mem_avail(
        "meminfo_sys_mem_avail", [](void*) { return GlobalMemoryArbitrator::sys_mem_available(); },
        nullptr);

std::atomic<int64_t> GlobalMemoryArbitrator::_s_process_reserved_memory = 0;
std::atomic<int64_t> GlobalMemoryArbitrator::refresh_interval_memory_growth = 0;

bool GlobalMemoryArbitrator::try_reserve_process_memory(int64_t bytes) {
    if (sys_mem_available() - bytes < MemInfo::sys_mem_available_warning_water_mark()) {
        return false;
    }
    int64_t old_reserved_mem = _s_process_reserved_memory.load(std::memory_order_relaxed);
    int64_t new_reserved_mem = 0;
    do {
        new_reserved_mem = old_reserved_mem + bytes;
        if (UNLIKELY(vm_rss_sub_allocator_cache() +
                             refresh_interval_memory_growth.load(std::memory_order_relaxed) +
                             new_reserved_mem >=
                     MemInfo::soft_mem_limit())) {
            return false;
        }
    } while (!_s_process_reserved_memory.compare_exchange_weak(old_reserved_mem, new_reserved_mem,
                                                               std::memory_order_relaxed));
    {
        std::lock_guard<std::mutex> l(_reserved_trackers_lock);
        _reserved_trackers[doris::thread_context()->thread_mem_tracker()->label()].add(bytes);
    }
    return true;
}

void GlobalMemoryArbitrator::release_process_reserved_memory(int64_t bytes) {
    _s_process_reserved_memory.fetch_sub(bytes, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> l(_reserved_trackers_lock);
        auto label = doris::thread_context()->thread_mem_tracker()->label();
        auto it = _reserved_trackers.find(label);
        if (it == _reserved_trackers.end()) {
            DCHECK(false) << "release unknown reserved memory " << label << ", bytes: " << bytes;
            return;
        }
        _reserved_trackers[label].sub(bytes);
        if (_reserved_trackers[label].current_value() == 0) {
            _reserved_trackers.erase(it);
        }
    }
}

} // namespace doris
