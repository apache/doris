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

#include "common/daemon.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <gflags/gflags.h>
#include <gperftools/malloc_extension.h> // IWYU pragma: keep
// IWYU pragma: no_include <bits/std_abs.h>
#include <butil/iobuf.h>
#include <math.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <map>
#include <ostream>
#include <set>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "runtime/block_spill_manager.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/task_group/task_group_manager.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/network_util.h"
#include "util/perf_counters.h"
#include "util/system_metrics.h"
#include "util/thrift_util.h"
#include "util/time.h"

namespace doris {

void Daemon::tcmalloc_gc_thread() {
    // TODO All cache GC wish to be supported
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER) && \
        !defined(USE_JEMALLOC)

    // Limit size of tcmalloc cache via release_rate and max_cache_percent.
    // We adjust release_rate according to memory_pressure, which is usage percent of memory.
    int64_t max_cache_percent = 60;
    double release_rates[10] = {1.0, 1.0, 1.0, 5.0, 5.0, 20.0, 50.0, 100.0, 500.0, 2000.0};
    int64_t pressure_limit = 90;
    bool is_performance_mode = false;
    int64_t physical_limit_bytes =
            std::min(MemInfo::physical_mem() - MemInfo::sys_mem_available_low_water_mark(),
                     MemInfo::mem_limit());

    if (config::memory_mode == std::string("performance")) {
        max_cache_percent = 100;
        pressure_limit = 90;
        is_performance_mode = true;
        physical_limit_bytes = std::min(MemInfo::mem_limit(), MemInfo::physical_mem());
    } else if (config::memory_mode == std::string("compact")) {
        max_cache_percent = 20;
        pressure_limit = 80;
    }

    int last_ms = 0;
    const int kMaxLastMs = 30000;
    const int kIntervalMs = 10;
    size_t init_aggressive_decommit = 0;
    size_t current_aggressive_decommit = 0;
    size_t expected_aggressive_decommit = 0;
    int64_t last_memory_pressure = 0;

    MallocExtension::instance()->GetNumericProperty("tcmalloc.aggressive_memory_decommit",
                                                    &init_aggressive_decommit);
    current_aggressive_decommit = init_aggressive_decommit;

    while (!_stop_background_threads_latch.wait_for(std::chrono::milliseconds(kIntervalMs))) {
        size_t tc_used_bytes = 0;
        size_t tc_alloc_bytes = 0;
        size_t rss = PerfCounters::get_vm_rss();

        MallocExtension::instance()->GetNumericProperty("generic.total_physical_bytes",
                                                        &tc_alloc_bytes);
        MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes",
                                                        &tc_used_bytes);
        int64_t tc_cached_bytes = (int64_t)tc_alloc_bytes - (int64_t)tc_used_bytes;
        int64_t to_free_bytes =
                (int64_t)tc_cached_bytes - ((int64_t)tc_used_bytes * max_cache_percent / 100);
        to_free_bytes = std::max(to_free_bytes, (int64_t)0);

        int64_t memory_pressure = 0;
        int64_t rss_pressure = 0;
        int64_t alloc_bytes = std::max(rss, tc_alloc_bytes);
        memory_pressure = alloc_bytes * 100 / physical_limit_bytes;
        rss_pressure = rss * 100 / physical_limit_bytes;

        expected_aggressive_decommit = init_aggressive_decommit;
        if (memory_pressure > pressure_limit) {
            // We are reaching oom, so release cache aggressively.
            // Ideally, we should reuse cache and not allocate from system any more,
            // however, it is hard to set limit on cache of tcmalloc and doris
            // use mmap in vectorized mode.
            // Limit cache capactiy is enough.
            if (rss_pressure > pressure_limit) {
                int64_t min_free_bytes = alloc_bytes - physical_limit_bytes * 9 / 10;
                to_free_bytes = std::max(to_free_bytes, min_free_bytes);
                to_free_bytes = std::max(to_free_bytes, tc_cached_bytes * 30 / 100);
                // We assure that we have at least 500M bytes in cache.
                to_free_bytes = std::min(to_free_bytes, tc_cached_bytes - 500 * 1024 * 1024);
                expected_aggressive_decommit = 1;
            }
            last_ms = kMaxLastMs;
        } else if (memory_pressure > (pressure_limit - 10)) {
            // In most cases, adjusting release rate is enough, if memory are consumed quickly
            // we should release manually.
            if (last_memory_pressure <= (pressure_limit - 10)) {
                to_free_bytes = std::max(to_free_bytes, tc_cached_bytes * 10 / 100);
            }
        }

        int release_rate_index = memory_pressure / 10;
        double release_rate = 1.0;
        if (release_rate_index >= sizeof(release_rates) / sizeof(release_rates[0])) {
            release_rate = 2000.0;
        } else {
            release_rate = release_rates[release_rate_index];
        }
        MallocExtension::instance()->SetMemoryReleaseRate(release_rate);

        if ((current_aggressive_decommit != expected_aggressive_decommit) && !is_performance_mode) {
            MallocExtension::instance()->SetNumericProperty("tcmalloc.aggressive_memory_decommit",
                                                            expected_aggressive_decommit);
            current_aggressive_decommit = expected_aggressive_decommit;
        }

        last_memory_pressure = memory_pressure;
        // We release at least 2% bytes once, frequent releasing hurts performance.
        if (to_free_bytes > (physical_limit_bytes * 2 / 100)) {
            last_ms += kIntervalMs;
            if (last_ms >= kMaxLastMs) {
                LOG(INFO) << "generic.current_allocated_bytes " << tc_used_bytes
                          << ", generic.total_physical_bytes " << tc_alloc_bytes << ", rss " << rss
                          << ", max_cache_percent " << max_cache_percent << ", release_rate "
                          << release_rate << ", memory_pressure " << memory_pressure
                          << ", physical_limit_bytes " << physical_limit_bytes << ", to_free_bytes "
                          << to_free_bytes << ", current_aggressive_decommit "
                          << current_aggressive_decommit;
                MallocExtension::instance()->ReleaseToSystem(to_free_bytes);
                last_ms = 0;
            }
        } else {
            last_ms = 0;
        }
    }
#endif
}

void Daemon::memory_maintenance_thread() {
    int32_t interval_milliseconds = config::memory_maintenance_sleep_time_ms;
    int64_t last_print_proc_mem = PerfCounters::get_vm_rss();
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(interval_milliseconds))) {
        // Refresh process memory metrics.
        doris::PerfCounters::refresh_proc_status();
        doris::MemInfo::refresh_proc_meminfo();
        doris::MemInfo::refresh_proc_mem_no_allocator_cache();

        // Update and print memory stat when the memory changes by 256M.
        if (abs(last_print_proc_mem - PerfCounters::get_vm_rss()) > 268435456) {
            last_print_proc_mem = PerfCounters::get_vm_rss();
            doris::MemTrackerLimiter::enable_print_log_process_usage();

            // Refresh mem tracker each type counter.
            doris::MemTrackerLimiter::refresh_global_counter();

            // Refresh allocator memory metrics.
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
            doris::MemInfo::refresh_allocator_mem();
            if (config::enable_system_metrics) {
                DorisMetrics::instance()->system_metrics()->update_allocator_metrics();
            }
#endif

            ExecEnv::GetInstance()->brpc_iobuf_block_memory_tracker()->set_consumption(
                    butil::IOBuf::block_memory());
            LOG(INFO) << MemTrackerLimiter::
                            process_mem_log_str(); // print mem log when memory state by 256M
        }
    }
}

void Daemon::memory_gc_thread() {
    int32_t interval_milliseconds = config::memory_maintenance_sleep_time_ms;
    int32_t memory_minor_gc_sleep_time_ms = 0;
    int32_t memory_full_gc_sleep_time_ms = 0;
    int32_t memory_gc_sleep_time_ms = config::memory_gc_sleep_time_ms;
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(interval_milliseconds))) {
        if (config::disable_memory_gc) {
            continue;
        }
        auto sys_mem_available = doris::MemInfo::sys_mem_available();
        auto proc_mem_no_allocator_cache = doris::MemInfo::proc_mem_no_allocator_cache();

        // GC excess memory for resource groups that not enable overcommit
        auto tg_free_mem = doris::MemInfo::tg_not_enable_overcommit_group_gc();
        sys_mem_available += tg_free_mem;
        proc_mem_no_allocator_cache -= tg_free_mem;

        if (memory_full_gc_sleep_time_ms <= 0 &&
            (sys_mem_available < doris::MemInfo::sys_mem_available_low_water_mark() ||
             proc_mem_no_allocator_cache >= doris::MemInfo::mem_limit())) {
            // No longer full gc and minor gc during sleep.
            memory_full_gc_sleep_time_ms = memory_gc_sleep_time_ms;
            memory_minor_gc_sleep_time_ms = memory_gc_sleep_time_ms;
            LOG(INFO) << fmt::format("[MemoryGC] start full GC, {}.",
                                     MemTrackerLimiter::process_limit_exceeded_errmsg_str());
            doris::MemTrackerLimiter::print_log_process_usage();
            if (doris::MemInfo::process_full_gc()) {
                // If there is not enough memory to be gc, the process memory usage will not be printed in the next continuous gc.
                doris::MemTrackerLimiter::enable_print_log_process_usage();
            }
        } else if (memory_minor_gc_sleep_time_ms <= 0 &&
                   (sys_mem_available < doris::MemInfo::sys_mem_available_warning_water_mark() ||
                    proc_mem_no_allocator_cache >= doris::MemInfo::soft_mem_limit())) {
            // No minor gc during sleep, but full gc is possible.
            memory_minor_gc_sleep_time_ms = memory_gc_sleep_time_ms;
            LOG(INFO) << fmt::format("[MemoryGC] start minor GC, {}.",
                                     MemTrackerLimiter::process_soft_limit_exceeded_errmsg_str());
            doris::MemTrackerLimiter::print_log_process_usage();
            if (doris::MemInfo::process_minor_gc()) {
                doris::MemTrackerLimiter::enable_print_log_process_usage();
            }
        } else {
            if (memory_full_gc_sleep_time_ms > 0) {
                memory_full_gc_sleep_time_ms -= interval_milliseconds;
            }
            if (memory_minor_gc_sleep_time_ms > 0) {
                memory_minor_gc_sleep_time_ms -= interval_milliseconds;
            }
        }
    }
}

void Daemon::memtable_memory_limiter_tracker_refresh_thread() {
    // Refresh the memory statistics of the load channel tracker more frequently,
    // which helps to accurately control the memory of LoadChannelMgr.
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::memtable_mem_tracker_refresh_interval_ms))) {
        doris::ExecEnv::GetInstance()->memtable_memory_limiter()->refresh_mem_tracker();
    }
}

/*
 * this thread will calculate some metrics at a fix interval(15 sec)
 * 1. push bytes per second
 * 2. scan bytes per second
 * 3. max io util of all disks
 * 4. max network send bytes rate
 * 5. max network receive bytes rate
 */
void Daemon::calculate_metrics_thread() {
    int64_t last_ts = -1L;
    int64_t lst_query_bytes = -1;

    std::map<std::string, int64_t> lst_disks_io_time;
    std::map<std::string, int64_t> lst_net_send_bytes;
    std::map<std::string, int64_t> lst_net_receive_bytes;

    do {
        DorisMetrics::instance()->metric_registry()->trigger_all_hooks(true);

        if (last_ts == -1L) {
            last_ts = GetMonoTimeMicros() / 1000;
            lst_query_bytes = DorisMetrics::instance()->query_scan_bytes->value();
            if (config::enable_system_metrics) {
                DorisMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);
                DorisMetrics::instance()->system_metrics()->get_network_traffic(
                        &lst_net_send_bytes, &lst_net_receive_bytes);
            }
        } else {
            int64_t current_ts = GetMonoTimeMicros() / 1000;
            long interval = (current_ts - last_ts) / 1000;
            last_ts = current_ts;

            // 1. query bytes per second
            int64_t current_query_bytes = DorisMetrics::instance()->query_scan_bytes->value();
            int64_t qps = (current_query_bytes - lst_query_bytes) / (interval + 1);
            DorisMetrics::instance()->query_scan_bytes_per_second->set_value(qps < 0 ? 0 : qps);
            lst_query_bytes = current_query_bytes;

            if (config::enable_system_metrics) {
                // 2. max disk io util
                DorisMetrics::instance()->system_metrics()->update_max_disk_io_util_percent(
                        lst_disks_io_time, 15);

                // update lst map
                DorisMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);

                // 3. max network traffic
                int64_t max_send = 0;
                int64_t max_receive = 0;
                DorisMetrics::instance()->system_metrics()->get_max_net_traffic(
                        lst_net_send_bytes, lst_net_receive_bytes, 15, &max_send, &max_receive);
                DorisMetrics::instance()->system_metrics()->update_max_network_send_bytes_rate(
                        max_send);
                DorisMetrics::instance()->system_metrics()->update_max_network_receive_bytes_rate(
                        max_receive);
                // update lst map
                DorisMetrics::instance()->system_metrics()->get_network_traffic(
                        &lst_net_send_bytes, &lst_net_receive_bytes);
            }

            DorisMetrics::instance()->all_rowsets_num->set_value(
                    StorageEngine::instance()->tablet_manager()->get_rowset_nums());
            DorisMetrics::instance()->all_segments_num->set_value(
                    StorageEngine::instance()->tablet_manager()->get_segment_nums());
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(15)));
}

// clean up stale spilled files
void Daemon::block_spill_gc_thread() {
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(60))) {
        ExecEnv::GetInstance()->block_spill_mgr()->gc(200);
    }
}

void Daemon::start() {
    Status st;
    st = Thread::create(
            "Daemon", "tcmalloc_gc_thread", [this]() { this->tcmalloc_gc_thread(); },
            &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "memory_maintenance_thread", [this]() { this->memory_maintenance_thread(); },
            &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "memory_gc_thread", [this]() { this->memory_gc_thread(); },
            &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "memtable_memory_limiter_tracker_refresh_thread",
            [this]() { this->memtable_memory_limiter_tracker_refresh_thread(); },
            &_threads.emplace_back());
    CHECK(st.ok()) << st;

    if (config::enable_metric_calculator) {
        st = Thread::create(
                "Daemon", "calculate_metrics_thread",
                [this]() { this->calculate_metrics_thread(); }, &_threads.emplace_back());
        CHECK(st.ok()) << st;
    }
    st = Thread::create(
            "Daemon", "block_spill_gc_thread", [this]() { this->block_spill_gc_thread(); },
            &_threads.emplace_back());
    CHECK(st.ok()) << st;
}

void Daemon::stop() {
    LOG(INFO) << "Doris daemon is stopping.";
    if (_stop_background_threads_latch.count() == 0) {
        LOG(INFO) << "Doris daemon stop returned since no bg threads latch.";
        return;
    }
    _stop_background_threads_latch.count_down();
    for (auto&& t : _threads) {
        if (t) {
            t->join();
        }
    }
    LOG(INFO) << "Doris daemon stopped after background threads are joined.";
}

} // namespace doris
