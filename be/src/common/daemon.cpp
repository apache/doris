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
#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
#include <gperftools/malloc_extension.h> // IWYU pragma: keep
#endif
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

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "runtime/be_proc_monitor.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/memory_reclamation.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/workload_group/workload_group_manager.h"
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
namespace {

int64_t last_print_proc_mem = 0;
int32_t refresh_cache_capacity_sleep_time_ms = 0;
#ifdef USE_JEMALLOC
int32_t je_purge_dirty_pages_sleep_time_ms = 0;
#endif

void update_rowsets_and_segments_num_metrics() {
    if (config::is_cloud_mode()) {
        // TODO(plat1ko): CloudStorageEngine
    } else {
        StorageEngine& engine = ExecEnv::GetInstance()->storage_engine().to_local();
        auto* metrics = DorisMetrics::instance();
        metrics->all_rowsets_num->set_value(engine.tablet_manager()->get_rowset_nums());
        metrics->all_segments_num->set_value(engine.tablet_manager()->get_segment_nums());
    }
}

} // namespace

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

void refresh_process_memory_metrics() {
    doris::PerfCounters::refresh_proc_status();
    doris::MemInfo::refresh_proc_meminfo();
    doris::GlobalMemoryArbitrator::reset_refresh_interval_memory_growth();
    ExecEnv::GetInstance()->brpc_iobuf_block_memory_tracker()->set_consumption(
            butil::IOBuf::block_memory());
}

void refresh_common_allocator_metrics() {
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
    doris::MemInfo::refresh_allocator_mem();
    if (config::enable_system_metrics) {
        DorisMetrics::instance()->system_metrics()->update_allocator_metrics();
    }
#endif
    MemInfo::refresh_memory_bvar();
}

void refresh_memory_state_after_memory_change() {
    if (abs(last_print_proc_mem - PerfCounters::get_vm_rss()) > 268435456) {
        last_print_proc_mem = PerfCounters::get_vm_rss();
        doris::MemTrackerLimiter::clean_tracker_limiter_group();
        doris::MemTrackerLimiter::enable_print_log_process_usage();
        // Refresh mem tracker each type counter.
        doris::MemTrackerLimiter::refresh_global_counter();
        LOG(INFO) << doris::GlobalMemoryArbitrator::
                        process_mem_log_str(); // print mem log when memory state by 256M
    }
}

void refresh_cache_capacity() {
    if (refresh_cache_capacity_sleep_time_ms <= 0) {
        auto cache_capacity_reduce_mem_limit = uint64_t(
                doris::MemInfo::soft_mem_limit() * config::cache_capacity_reduce_mem_limit_frac);
        int64_t process_memory_usage = doris::GlobalMemoryArbitrator::process_memory_usage();
        double new_cache_capacity_adjust_weighted =
                process_memory_usage <= cache_capacity_reduce_mem_limit
                        ? 1
                        : std::min<double>(
                                  1 - (process_memory_usage - cache_capacity_reduce_mem_limit) /
                                                  (doris::MemInfo::soft_mem_limit() -
                                                   cache_capacity_reduce_mem_limit),
                                  0);
        if (new_cache_capacity_adjust_weighted !=
            doris::GlobalMemoryArbitrator::last_cache_capacity_adjust_weighted) {
            doris::GlobalMemoryArbitrator::last_cache_capacity_adjust_weighted =
                    new_cache_capacity_adjust_weighted;
            doris::GlobalMemoryArbitrator::notify_cache_adjust_capacity();
            refresh_cache_capacity_sleep_time_ms = config::memory_gc_sleep_time_ms;
        }
    }
    refresh_cache_capacity_sleep_time_ms -= config::memory_maintenance_sleep_time_ms;
}

void je_purge_dirty_pages() {
#ifdef USE_JEMALLOC
    if (je_purge_dirty_pages_sleep_time_ms <= 0 &&
        doris::MemInfo::je_dirty_pages_mem() > doris::MemInfo::je_dirty_pages_mem_limit() &&
        GlobalMemoryArbitrator::is_exceed_soft_mem_limit()) {
        doris::MemInfo::notify_je_purge_dirty_pages();
        je_purge_dirty_pages_sleep_time_ms = config::memory_gc_sleep_time_ms;
    }
    je_purge_dirty_pages_sleep_time_ms -= config::memory_maintenance_sleep_time_ms;
#endif
}

void Daemon::memory_maintenance_thread() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::memory_maintenance_sleep_time_ms))) {
        // step 1. Refresh process memory metrics.
        refresh_process_memory_metrics();

        // step 2. Refresh jemalloc/tcmalloc metrics.
        refresh_common_allocator_metrics();

        // step 3. Update and print memory stat when the memory changes by 256M.
        refresh_memory_state_after_memory_change();

        // step 4. Asyn Refresh cache capacity
        // TODO adjust cache capacity based on smoothstep (smooth gradient).
        refresh_cache_capacity();

        // step 5. Cancel top memory task when process memory exceed hard limit.
        // TODO replace memory_gc_thread.

        // step 6. Refresh weighted memory ratio of workload groups.
        doris::ExecEnv::GetInstance()->workload_group_mgr()->refresh_wg_weighted_memory_limit();

        // step 7. Analyze blocking queries.
        // TODO sort the operators that can spill, wake up the pipeline task spill
        // or continue execution according to certain rules or cancel query.

        // step 8. Flush memtable
        doris::GlobalMemoryArbitrator::notify_memtable_memory_refresh();
        // TODO notify flush memtable

        // step 9. Jemalloc purge all arena dirty pages
        je_purge_dirty_pages();
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
        auto sys_mem_available = doris::GlobalMemoryArbitrator::sys_mem_available();
        auto process_memory_usage = doris::GlobalMemoryArbitrator::process_memory_usage();

        // GC excess memory for resource groups that not enable overcommit
        auto tg_free_mem = doris::MemoryReclamation::tg_disable_overcommit_group_gc();
        sys_mem_available += tg_free_mem;
        process_memory_usage -= tg_free_mem;

        if (memory_full_gc_sleep_time_ms <= 0 &&
            (sys_mem_available < doris::MemInfo::sys_mem_available_low_water_mark() ||
             process_memory_usage >= doris::MemInfo::mem_limit())) {
            // No longer full gc and minor gc during sleep.
            std::string mem_info =
                    doris::GlobalMemoryArbitrator::process_limit_exceeded_errmsg_str();
            memory_full_gc_sleep_time_ms = memory_gc_sleep_time_ms;
            memory_minor_gc_sleep_time_ms = memory_gc_sleep_time_ms;
            LOG(INFO) << fmt::format("[MemoryGC] start full GC, {}.", mem_info);
            doris::MemTrackerLimiter::print_log_process_usage();
            if (doris::MemoryReclamation::process_full_gc(std::move(mem_info))) {
                // If there is not enough memory to be gc, the process memory usage will not be printed in the next continuous gc.
                doris::MemTrackerLimiter::enable_print_log_process_usage();
            }
        } else if (memory_minor_gc_sleep_time_ms <= 0 &&
                   (sys_mem_available < doris::MemInfo::sys_mem_available_warning_water_mark() ||
                    process_memory_usage >= doris::MemInfo::soft_mem_limit())) {
            // No minor gc during sleep, but full gc is possible.
            std::string mem_info =
                    doris::GlobalMemoryArbitrator::process_soft_limit_exceeded_errmsg_str();
            memory_minor_gc_sleep_time_ms = memory_gc_sleep_time_ms;
            LOG(INFO) << fmt::format("[MemoryGC] start minor GC, {}.", mem_info);
            doris::MemTrackerLimiter::print_log_process_usage();
            if (doris::MemoryReclamation::process_minor_gc(std::move(mem_info))) {
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

void Daemon::memtable_memory_refresh_thread() {
    // Refresh the memory statistics of the load channel tracker more frequently,
    // which helps to accurately control the memory of LoadChannelMgr.
    do {
        std::unique_lock<std::mutex> l(doris::GlobalMemoryArbitrator::memtable_memory_refresh_lock);
        while (_stop_background_threads_latch.count() != 0 &&
               !doris::GlobalMemoryArbitrator::memtable_memory_refresh_notify.load(
                       std::memory_order_relaxed)) {
            doris::GlobalMemoryArbitrator::memtable_memory_refresh_cv.wait_for(
                    l, std::chrono::seconds(1));
        }
        if (_stop_background_threads_latch.count() == 0) {
            break;
        }
        doris::ExecEnv::GetInstance()->memtable_memory_limiter()->refresh_mem_tracker();
        doris::GlobalMemoryArbitrator::memtable_memory_refresh_notify.store(
                false, std::memory_order_relaxed);
    } while (true);
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
            update_rowsets_and_segments_num_metrics();
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(15)));
}

void Daemon::report_runtime_query_statistics_thread() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::report_query_statistics_interval_ms))) {
        ExecEnv::GetInstance()->runtime_query_statistics_mgr()->report_runtime_query_statistics();
    }
}

void Daemon::je_purge_dirty_pages_thread() const {
    do {
        std::unique_lock<std::mutex> l(doris::MemInfo::je_purge_dirty_pages_lock);
        while (_stop_background_threads_latch.count() != 0 &&
               !doris::MemInfo::je_purge_dirty_pages_notify.load(std::memory_order_relaxed)) {
            doris::MemInfo::je_purge_dirty_pages_cv.wait_for(l, std::chrono::seconds(1));
        }
        if (_stop_background_threads_latch.count() == 0) {
            break;
        }
        if (config::disable_memory_gc) {
            continue;
        }
        doris::MemInfo::je_purge_all_arena_dirty_pages();
        doris::MemInfo::je_purge_dirty_pages_notify.store(false, std::memory_order_relaxed);
    } while (true);
}

void Daemon::cache_adjust_capacity_thread() {
    do {
        std::unique_lock<std::mutex> l(doris::GlobalMemoryArbitrator::cache_adjust_capacity_lock);
        while (_stop_background_threads_latch.count() != 0 &&
               !doris::GlobalMemoryArbitrator::cache_adjust_capacity_notify.load(
                       std::memory_order_relaxed)) {
            doris::GlobalMemoryArbitrator::cache_adjust_capacity_cv.wait_for(
                    l, std::chrono::seconds(1));
        }
        double adjust_weighted = GlobalMemoryArbitrator::last_cache_capacity_adjust_weighted;
        if (_stop_background_threads_latch.count() == 0) {
            break;
        }
        if (config::disable_memory_gc) {
            continue;
        }
        std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("");
        auto freed_mem = CacheManager::instance()->for_each_cache_refresh_capacity(adjust_weighted,
                                                                                   profile.get());
        std::stringstream ss;
        profile->pretty_print(&ss);
        LOG(INFO) << fmt::format(
                "[MemoryGC] refresh cache capacity end, free memory {}, details: {}",
                PrettyPrinter::print(freed_mem, TUnit::BYTES), ss.str());
        doris::GlobalMemoryArbitrator::cache_adjust_capacity_notify.store(
                false, std::memory_order_relaxed);
    } while (true);
}

void Daemon::cache_prune_stale_thread() {
    int32_t interval = config::cache_periodic_prune_stale_sweep_sec;
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(interval))) {
        if (interval <= 0) {
            LOG(WARNING) << "config of cache clean interval is illegal: [" << interval
                         << "], force set to 3600 ";
            interval = 3600;
        }
        if (config::disable_memory_gc) {
            continue;
        }
        CacheManager::instance()->for_each_cache_prune_stale();
    }
}

void Daemon::be_proc_monitor_thread() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::be_proc_monitor_interval_ms))) {
        LOG(INFO) << "log be thread num, " << BeProcMonitor::get_be_thread_info();
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
            "Daemon", "memtable_memory_refresh_thread",
            [this]() { this->memtable_memory_refresh_thread(); }, &_threads.emplace_back());
    CHECK(st.ok()) << st;

    if (config::enable_metric_calculator) {
        st = Thread::create(
                "Daemon", "calculate_metrics_thread",
                [this]() { this->calculate_metrics_thread(); }, &_threads.emplace_back());
        CHECK(st.ok()) << st;
    }
    st = Thread::create(
            "Daemon", "je_purge_dirty_pages_thread",
            [this]() { this->je_purge_dirty_pages_thread(); }, &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "cache_adjust_capacity_thread",
            [this]() { this->cache_adjust_capacity_thread(); }, &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "cache_prune_stale_thread", [this]() { this->cache_prune_stale_thread(); },
            &_threads.emplace_back());
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "query_runtime_statistics_thread",
            [this]() { this->report_runtime_query_statistics_thread(); }, &_threads.emplace_back());
    CHECK(st.ok()) << st;

    if (config::enable_be_proc_monitor) {
        st = Thread::create(
                "Daemon", "be_proc_monitor_thread", [this]() { this->be_proc_monitor_thread(); },
                &_threads.emplace_back());
    }
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
