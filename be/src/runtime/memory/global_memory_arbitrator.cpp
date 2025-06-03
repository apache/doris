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

#include "runtime/memory/jemalloc_control.h"
#include "runtime/process_profile.h"
#include "runtime/thread_context.h"
#include "util/mem_info.h"

namespace doris {
#include "common/compile_check_begin.h"

static bvar::PassiveStatus<int64_t> memory_process_memory_usage(
        "meminfo_process_memory_usage",
        [](void*) { return GlobalMemoryArbitrator::process_memory_usage(); }, nullptr);
static bvar::PassiveStatus<int64_t> memory_sys_mem_avail(
        "meminfo_sys_mem_avail", [](void*) { return GlobalMemoryArbitrator::sys_mem_available(); },
        nullptr);
static bvar::Adder<int64_t> memory_jemalloc_cache_bytes("memory_jemalloc_cache_bytes");
static bvar::Adder<int64_t> memory_jemalloc_dirty_pages_bytes("memory_jemalloc_dirty_pages_bytes");
static bvar::Adder<int64_t> memory_jemalloc_metadata_bytes("memory_jemalloc_metadata_bytes");
static bvar::Adder<int64_t> memory_jemalloc_virtual_bytes("memory_jemalloc_virtual_bytes");
static bvar::Adder<int64_t> memory_cgroup_usage_bytes("memory_cgroup_usage_bytes");
static bvar::Adder<int64_t> memory_sys_available_bytes("memory_sys_available_bytes");
static bvar::Adder<int64_t> memory_arbitrator_sys_available_bytes(
        "memory_arbitrator_sys_available_bytes");
static bvar::Adder<int64_t> memory_arbitrator_process_usage_bytes(
        "memory_arbitrator_process_usage_bytes");
static bvar::Adder<int64_t> memory_arbitrator_reserve_memory_bytes(
        "memory_arbitrator_reserve_memory_bytes");
static bvar::Adder<int64_t> memory_arbitrator_refresh_interval_growth_bytes(
        "memory_arbitrator_refresh_interval_growth_bytes");

std::atomic<int64_t> GlobalMemoryArbitrator::_process_reserved_memory = 0;
std::atomic<int64_t> GlobalMemoryArbitrator::refresh_interval_memory_growth = 0;
std::mutex GlobalMemoryArbitrator::cache_adjust_capacity_lock;
std::condition_variable GlobalMemoryArbitrator::cache_adjust_capacity_cv;
std::atomic<bool> GlobalMemoryArbitrator::cache_adjust_capacity_notify {false};
// This capacity is set by `refresh_cache_capacity`, it is running periodicity.
// modified when process memory changes.
std::atomic<double> GlobalMemoryArbitrator::last_periodic_refreshed_cache_capacity_adjust_weighted {
        1};
// This capacity is set by workload group mgr `handle_paused_queries`,
// modified when a query enters paused state due to insufficient process memory.
std::atomic<double> GlobalMemoryArbitrator::last_memory_exceeded_cache_capacity_adjust_weighted {1};
// The value that take affect
std::atomic<double> GlobalMemoryArbitrator::last_affected_cache_capacity_adjust_weighted {1};
std::atomic<bool> GlobalMemoryArbitrator::any_workload_group_exceed_limit {false};
std::mutex GlobalMemoryArbitrator::memtable_memory_refresh_lock;
std::condition_variable GlobalMemoryArbitrator::memtable_memory_refresh_cv;
std::atomic<bool> GlobalMemoryArbitrator::memtable_memory_refresh_notify {false};

void GlobalMemoryArbitrator::refresh_memory_bvar() {
    memory_jemalloc_cache_bytes << JemallocControl::je_cache_bytes() -
                                           memory_jemalloc_cache_bytes.get_value();
    memory_jemalloc_dirty_pages_bytes << JemallocControl::je_dirty_pages_mem() -
                                                 memory_jemalloc_dirty_pages_bytes.get_value();
    memory_jemalloc_metadata_bytes
            << JemallocControl::je_metadata_mem() - memory_jemalloc_metadata_bytes.get_value();
    memory_jemalloc_virtual_bytes << JemallocControl::je_virtual_memory_used() -
                                             memory_jemalloc_virtual_bytes.get_value();

    memory_cgroup_usage_bytes << MemInfo::cgroup_mem_usage() -
                                         memory_cgroup_usage_bytes.get_value();
    memory_sys_available_bytes << MemInfo::_s_sys_mem_available.load(std::memory_order_relaxed) -
                                          memory_sys_available_bytes.get_value();

    memory_arbitrator_sys_available_bytes
            << GlobalMemoryArbitrator::sys_mem_available() -
                       memory_arbitrator_sys_available_bytes.get_value();
    memory_arbitrator_process_usage_bytes
            << GlobalMemoryArbitrator::process_memory_usage() -
                       memory_arbitrator_process_usage_bytes.get_value();
    memory_arbitrator_reserve_memory_bytes
            << GlobalMemoryArbitrator::process_reserved_memory() -
                       memory_arbitrator_reserve_memory_bytes.get_value();
    memory_arbitrator_refresh_interval_growth_bytes
            << GlobalMemoryArbitrator::refresh_interval_memory_growth -
                       memory_arbitrator_refresh_interval_growth_bytes.get_value();
}

bool GlobalMemoryArbitrator::reserve_process_memory(int64_t bytes) {
    int64_t old_reserved_mem = _process_reserved_memory.load(std::memory_order_relaxed);
    int64_t new_reserved_mem = 0;
    do {
        new_reserved_mem = old_reserved_mem + bytes;
    } while (!_process_reserved_memory.compare_exchange_weak(old_reserved_mem, new_reserved_mem,
                                                             std::memory_order_relaxed));
    return true;
}

bool GlobalMemoryArbitrator::try_reserve_process_memory(int64_t bytes) {
    if (sys_mem_available() - bytes < MemInfo::sys_mem_available_warning_water_mark()) {
        doris::ProcessProfile::instance()->memory_profile()->print_log_process_usage();
        return false;
    }
    int64_t old_reserved_mem = _process_reserved_memory.load(std::memory_order_relaxed);
    int64_t new_reserved_mem = 0;
    do {
        new_reserved_mem = old_reserved_mem + bytes;
        if (UNLIKELY(PerfCounters::get_vm_rss() +
                             refresh_interval_memory_growth.load(std::memory_order_relaxed) +
                             new_reserved_mem >=
                     MemInfo::soft_mem_limit())) {
            doris::ProcessProfile::instance()->memory_profile()->print_log_process_usage();
            return false;
        }
    } while (!_process_reserved_memory.compare_exchange_weak(old_reserved_mem, new_reserved_mem,
                                                             std::memory_order_relaxed));
    return true;
}

void GlobalMemoryArbitrator::shrink_process_reserved(int64_t bytes) {
    _process_reserved_memory.fetch_sub(bytes, std::memory_order_relaxed);
}

int64_t GlobalMemoryArbitrator::sub_thread_reserve_memory(int64_t bytes) {
    return bytes - doris::thread_context()->thread_mem_tracker_mgr->reserved_mem();
}

#include "common/compile_check_end.h"
} // namespace doris
