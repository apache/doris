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

#include <gflags/gflags.h>
#include <gperftools/malloc_extension.h>
#include <signal.h>

#include "common/config.h"
#include "common/logging.h"
#include "exprs/array_functions.h"
#include "exprs/bitmap_function.h"
#include "exprs/cast_functions.h"
#include "exprs/compound_predicate.h"
#include "exprs/decimalv2_operators.h"
#include "exprs/encryption_functions.h"
#include "exprs/es_functions.h"
#include "exprs/grouping_sets_functions.h"
#include "exprs/hash_functions.h"
#include "exprs/hll_function.h"
#include "exprs/hll_hash_function.h"
#include "exprs/is_null_predicate.h"
#include "exprs/json_functions.h"
#include "exprs/like_predicate.h"
#include "exprs/math_functions.h"
#include "exprs/new_in_predicate.h"
#include "exprs/operators.h"
#include "exprs/quantile_function.h"
#include "exprs/string_functions.h"
#include "exprs/table_function/dummy_table_functions.h"
#include "exprs/time_operators.h"
#include "exprs/timestamp_functions.h"
#include "exprs/topn_function.h"
#include "exprs/utility_functions.h"
#include "geo/geo_functions.h"
#include "olap/options.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/memory/chunk_allocator.h"
#include "runtime/user_function_cache.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/network_util.h"
#include "util/system_metrics.h"
#include "util/thrift_util.h"
#include "util/time.h"

namespace doris {

bool k_doris_exit = false;

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
    size_t physical_limit_bytes =
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
        int64_t tc_cached_bytes = tc_alloc_bytes - tc_used_bytes;
        int64_t to_free_bytes =
                (int64_t)tc_cached_bytes - (tc_used_bytes * max_cache_percent / 100);

        int64_t memory_pressure = 0;
        int64_t alloc_bytes = std::max(rss, tc_alloc_bytes);
        memory_pressure = alloc_bytes * 100 / physical_limit_bytes;

        expected_aggressive_decommit = init_aggressive_decommit;
        if (memory_pressure > pressure_limit) {
            // We are reaching oom, so release cache aggressively.
            // Ideally, we should reuse cache and not allocate from system any more,
            // however, it is hard to set limit on cache of tcmalloc and doris
            // use mmap in vectorized mode.
            if (last_memory_pressure <= pressure_limit) {
                int64_t min_free_bytes = alloc_bytes - physical_limit_bytes * 9 / 10;
                to_free_bytes = std::max(to_free_bytes, min_free_bytes);
                to_free_bytes = std::max(to_free_bytes, tc_cached_bytes * 30 / 100);
                to_free_bytes = std::min(to_free_bytes, tc_cached_bytes);
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
        if (release_rate_index >= sizeof(release_rates)) {
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
        if (to_free_bytes > 0) {
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
            DCHECK(tc_cached_bytes <= tc_used_bytes * max_cache_percent / 100);
            last_ms = 0;
        }
    }
#endif
}

void Daemon::buffer_pool_gc_thread() {
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(10))) {
        ExecEnv* env = ExecEnv::GetInstance();
        // ExecEnv may not have been created yet or this may be the catalogd or statestored,
        // which don't have ExecEnvs.
        if (env != nullptr) {
            BufferPool* buffer_pool = env->buffer_pool();
            if (buffer_pool != nullptr) {
                buffer_pool->Maintenance();
            }
        }
    }
}

void Daemon::memory_maintenance_thread() {
    int64_t interval_milliseconds = config::memory_maintenance_sleep_time_ms;
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(interval_milliseconds))) {
        // Refresh process memory metrics.
        doris::PerfCounters::refresh_proc_status();
        doris::MemInfo::refresh_proc_meminfo();

        // Refresh allocator memory metrics.
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
        doris::MemInfo::refresh_allocator_mem();
#endif
        doris::MemInfo::refresh_proc_mem_no_allocator_cache();
        LOG_EVERY_N(INFO, 10) << MemTrackerLimiter::process_mem_log_str();

        // Refresh mem tracker each type metrics.
        doris::MemTrackerLimiter::refresh_global_counter();
        if (doris::config::memory_debug) {
            doris::MemTrackerLimiter::print_log_process_usage("memory_debug", false);
        }
        doris::MemTrackerLimiter::enable_print_log_process_usage();

        // If system available memory is not enough, or the process memory exceeds the limit, reduce refresh interval.
        if (doris::MemInfo::sys_mem_available() <
                    doris::MemInfo::sys_mem_available_low_water_mark() ||
            doris::MemInfo::proc_mem_no_allocator_cache() >= doris::MemInfo::mem_limit()) {
            interval_milliseconds = 100;
        } else if (doris::MemInfo::sys_mem_available() <
                           doris::MemInfo::sys_mem_available_warning_water_mark() ||
                   doris::MemInfo::proc_mem_no_allocator_cache() >=
                           doris::MemInfo::soft_mem_limit()) {
            interval_milliseconds = 200;
        } else {
            interval_milliseconds = config::memory_maintenance_sleep_time_ms;
        }
    }
}

void Daemon::load_channel_tracker_refresh_thread() {
    // Refresh the memory statistics of the load channel tracker more frequently,
    // which helps to accurately control the memory of LoadChannelMgr.
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::load_channel_memory_refresh_sleep_time_ms))) {
        doris::ExecEnv::GetInstance()->load_channel_mgr()->refresh_mem_tracker();
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
            last_ts = GetCurrentTimeMicros() / 1000;
            lst_query_bytes = DorisMetrics::instance()->query_scan_bytes->value();
            DorisMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);
            DorisMetrics::instance()->system_metrics()->get_network_traffic(&lst_net_send_bytes,
                                                                            &lst_net_receive_bytes);
        } else {
            int64_t current_ts = GetCurrentTimeMicros() / 1000;
            long interval = (current_ts - last_ts) / 1000;
            last_ts = current_ts;

            // 1. query bytes per second
            int64_t current_query_bytes = DorisMetrics::instance()->query_scan_bytes->value();
            int64_t qps = (current_query_bytes - lst_query_bytes) / (interval + 1);
            DorisMetrics::instance()->query_scan_bytes_per_second->set_value(qps < 0 ? 0 : qps);
            lst_query_bytes = current_query_bytes;

            // 2. max disk io util
            DorisMetrics::instance()->max_disk_io_util_percent->set_value(
                    DorisMetrics::instance()->system_metrics()->get_max_io_util(lst_disks_io_time,
                                                                                15));
            // update lst map
            DorisMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);

            // 3. max network traffic
            int64_t max_send = 0;
            int64_t max_receive = 0;
            DorisMetrics::instance()->system_metrics()->get_max_net_traffic(
                    lst_net_send_bytes, lst_net_receive_bytes, 15, &max_send, &max_receive);
            DorisMetrics::instance()->max_network_send_bytes_rate->set_value(max_send);
            DorisMetrics::instance()->max_network_receive_bytes_rate->set_value(max_receive);
            // update lst map
            DorisMetrics::instance()->system_metrics()->get_network_traffic(&lst_net_send_bytes,
                                                                            &lst_net_receive_bytes);
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(15)));
}

static void init_doris_metrics(const std::vector<StorePath>& store_paths) {
    bool init_system_metrics = config::enable_system_metrics;
    std::set<std::string> disk_devices;
    std::vector<std::string> network_interfaces;
    std::vector<std::string> paths;
    for (auto& store_path : store_paths) {
        paths.emplace_back(store_path.path);
    }
    if (init_system_metrics) {
        auto st = DiskInfo::get_disk_devices(paths, &disk_devices);
        if (!st.ok()) {
            LOG(WARNING) << "get disk devices failed, status=" << st.get_error_msg();
            return;
        }
        st = get_inet_interfaces(&network_interfaces);
        if (!st.ok()) {
            LOG(WARNING) << "get inet interfaces failed, status=" << st.get_error_msg();
            return;
        }
    }
    DorisMetrics::instance()->initialize(init_system_metrics, disk_devices, network_interfaces);
}

void signal_handler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        k_doris_exit = true;
    }
}

int install_signal(int signo, void (*handler)(int)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    auto ret = sigaction(signo, &sa, nullptr);
    if (ret != 0) {
        char buf[64];
        LOG(ERROR) << "install signal failed, signo=" << signo << ", errno=" << errno
                   << ", errmsg=" << strerror_r(errno, buf, sizeof(buf));
    }
    return ret;
}

void init_signals() {
    auto ret = install_signal(SIGINT, signal_handler);
    if (ret < 0) {
        exit(-1);
    }
    ret = install_signal(SIGTERM, signal_handler);
    if (ret < 0) {
        exit(-1);
    }
}

void Daemon::init(int argc, char** argv, const std::vector<StorePath>& paths) {
    // google::SetVersionString(get_build_version(false));
    // google::ParseCommandLineFlags(&argc, &argv, true);
    google::ParseCommandLineFlags(&argc, &argv, true);
    init_glog("be");

    LOG(INFO) << get_version_string(false);

    init_thrift_logging();
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    UserFunctionCache::instance()->init(config::user_function_dir);
    Operators::init();
    IsNullPredicate::init();
    LikePredicate::init();
    StringFunctions::init();
    ArrayFunctions::init();
    CastFunctions::init();
    InPredicate::init();
    MathFunctions::init();
    EncryptionFunctions::init();
    TimestampFunctions::init();
    DecimalV2Operators::init();
    TimeOperators::init();
    UtilityFunctions::init();
    CompoundPredicate::init();
    JsonFunctions::init();
    HllHashFunctions::init();
    ESFunctions::init();
    GeoFunctions::init();
    GroupingSetsFunctions::init();
    BitmapFunctions::init();
    HllFunctions::init();
    QuantileStateFunctions::init();
    HashFunctions::init();
    TopNFunctions::init();
    DummyTableFunctions::init();

    LOG(INFO) << CpuInfo::debug_string();
    LOG(INFO) << DiskInfo::debug_string();
    LOG(INFO) << MemInfo::debug_string();

    init_doris_metrics(paths);
    init_signals();
}

void Daemon::start() {
    Status st;
    st = Thread::create(
            "Daemon", "tcmalloc_gc_thread", [this]() { this->tcmalloc_gc_thread(); },
            &_tcmalloc_gc_thread);
    CHECK(st.ok()) << st.to_string();
    st = Thread::create(
            "Daemon", "buffer_pool_gc_thread", [this]() { this->buffer_pool_gc_thread(); },
            &_buffer_pool_gc_thread);
    CHECK(st.ok()) << st.to_string();
    st = Thread::create(
            "Daemon", "memory_maintenance_thread", [this]() { this->memory_maintenance_thread(); },
            &_memory_maintenance_thread);
    CHECK(st.ok()) << st.to_string();
    st = Thread::create(
            "Daemon", "load_channel_tracker_refresh_thread",
            [this]() { this->load_channel_tracker_refresh_thread(); },
            &_load_channel_tracker_refresh_thread);
    CHECK(st.ok()) << st.to_string();

    if (config::enable_metric_calculator) {
        CHECK(DorisMetrics::instance()->is_inited())
                << "enable metric calculator failed, maybe you set enable_system_metrics to false "
                << " or there may be some hardware error which causes metric init failed, please "
                   "check log first;"
                << " you can set enable_metric_calculator = false to quickly recover ";

        st = Thread::create(
                "Daemon", "calculate_metrics_thread",
                [this]() { this->calculate_metrics_thread(); }, &_calculate_metrics_thread);
        CHECK(st.ok()) << st.to_string();
    }
}

void Daemon::stop() {
    _stop_background_threads_latch.count_down();

    if (_tcmalloc_gc_thread) {
        _tcmalloc_gc_thread->join();
    }
    if (_buffer_pool_gc_thread) {
        _buffer_pool_gc_thread->join();
    }
    if (_memory_maintenance_thread) {
        _memory_maintenance_thread->join();
    }
    if (_load_channel_tracker_refresh_thread) {
        _load_channel_tracker_refresh_thread->join();
    }
    if (_calculate_metrics_thread) {
        _calculate_metrics_thread->join();
    }
}

} // namespace doris
