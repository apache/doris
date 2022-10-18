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
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(10))) {
        size_t used_size = 0;
        size_t free_size = 0;

        MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes",
                                                        &used_size);
        MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes", &free_size);
        size_t alloc_size = used_size + free_size;
        LOG(INFO) << "tcmalloc.pageheap_free_bytes " << free_size
                  << ", generic.current_allocated_bytes " << used_size;

        if (alloc_size > config::tc_use_memory_min) {
            size_t max_free_size = alloc_size * config::tc_free_memory_rate / 100;
            if (free_size > max_free_size) {
                MallocExtension::instance()->ReleaseToSystem(free_size - max_free_size);
            }
        }
    }
#endif
}

void Daemon::memory_maintenance_thread() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::memory_maintenance_sleep_time_s))) {
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
            "Daemon", "memory_maintenance_thread", [this]() { this->memory_maintenance_thread(); },
            &_memory_maintenance_thread);
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
    if (_memory_maintenance_thread) {
        _memory_maintenance_thread->join();
    }
    if (_calculate_metrics_thread) {
        _calculate_metrics_thread->join();
    }
}

} // namespace doris
