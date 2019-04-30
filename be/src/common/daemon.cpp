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

#include "common/config.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/network_util.h"
#include "util/thrift_util.h"
#include "util/doris_metrics.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/user_function_cache.h"
#include "exprs/operators.h"
#include "exprs/is_null_predicate.h"
#include "exprs/like_predicate.h"
#include "exprs/compound_predicate.h"
#include "exprs/new_in_predicate.h"
#include "exprs/string_functions.h"
#include "exprs/cast_functions.h"
#include "exprs/math_functions.h"
#include "exprs/encryption_functions.h"
#include "exprs/es_functions.h"
#include "exprs/timestamp_functions.h"
#include "exprs/decimal_operators.h"
#include "exprs/decimalv2_operators.h"
#include "exprs/utility_functions.h"
#include "exprs/json_functions.h"
#include "exprs/hll_hash_function.h"
#include "olap/options.h"
#include "util/time.h"
#include "util/system_metrics.h"

namespace doris {

bool k_doris_exit = false;

void* tcmalloc_gc_thread(void* dummy) {
    while (1) {
        sleep(10);
        size_t used_size = 0;
        size_t free_size = 0;

#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
        MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &used_size);
        MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes", &free_size);
#endif
        size_t alloc_size = used_size + free_size;

        if (alloc_size > config::tc_use_memory_min) {
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
            size_t max_free_size = alloc_size * config::tc_free_memory_rate / 100;
            if (free_size > max_free_size) {
                MallocExtension::instance()->ReleaseToSystem(free_size - max_free_size);
            }
#endif
        }
    }

    return NULL;
}
    
void* memory_maintenance_thread(void* dummy) {
    while (true) {
        sleep(config::memory_maintenance_sleep_time_s);
        ExecEnv* env = ExecEnv::GetInstance();
        // ExecEnv may not have been created yet or this may be the catalogd or statestored,
        // which don't have ExecEnvs.
        if (env != nullptr) {
            BufferPool* buffer_pool = env->buffer_pool();
            if (buffer_pool != nullptr) buffer_pool->Maintenance();
            
            // The process limit as measured by our trackers may get out of sync with the
            // process usage if memory is allocated or freed without updating a MemTracker.
            // The metric is refreshed whenever memory is consumed or released via a MemTracker,
            // so on a system with queries executing it will be refreshed frequently. However
            // if the system is idle, we need to refresh the tracker occasionally since
            // untracked memory may be allocated or freed, e.g. by background threads.
            if (env->process_mem_tracker() != nullptr && 
                     !env->process_mem_tracker()->is_consumption_metric_null()) {
                env->process_mem_tracker()->RefreshConsumptionFromMetric();
            }   
        }   
    }   
    
    return NULL;
}

/*
 * this thread will calculate some metrics at a fix interval(15 sec)
 * 1. push bytes per second
 * 2. scan bytes per second
 * 3. max io util of all disks
 * 4. max network send bytes rate
 * 5. max network receive bytes rate
 */
void* calculate_metrics(void* dummy) {
    int64_t last_ts = -1L;
    int64_t lst_push_bytes = -1;
    int64_t lst_query_bytes = -1;

    std::map<std::string, int64_t> lst_disks_io_time;
    std::map<std::string, int64_t> lst_net_send_bytes;
    std::map<std::string, int64_t> lst_net_receive_bytes;

    while (true) {
        DorisMetrics::metrics()->trigger_hook();

        if (last_ts == -1L) {
            last_ts = GetCurrentTimeMicros() / 1000;
            lst_push_bytes = DorisMetrics::push_request_write_bytes.value();
            lst_query_bytes = DorisMetrics::query_scan_bytes.value();
            DorisMetrics::system_metrics()->get_disks_io_time(&lst_disks_io_time);
            DorisMetrics::system_metrics()->get_network_traffic(&lst_net_send_bytes, &lst_net_receive_bytes);
        } else {
            int64_t current_ts = GetCurrentTimeMicros() / 1000;
            long interval = (current_ts - last_ts) / 1000;
            last_ts = current_ts;

            // 1. push bytes per second
            int64_t current_push_bytes = DorisMetrics::push_request_write_bytes.value();
            int64_t pps = (current_push_bytes - lst_push_bytes) / (interval + 1);
            DorisMetrics::push_request_write_bytes_per_second.set_value(
                pps < 0 ? 0 : pps);
            lst_push_bytes = current_push_bytes;

            // 2. query bytes per second
            int64_t current_query_bytes = DorisMetrics::query_scan_bytes.value();
            int64_t qps = (current_query_bytes - lst_query_bytes) / (interval + 1);
            DorisMetrics::query_scan_bytes_per_second.set_value(
                qps < 0 ? 0 : qps);
            lst_query_bytes = current_query_bytes;

            // 3. max disk io util
            DorisMetrics::max_disk_io_util_percent.set_value(
                DorisMetrics::system_metrics()->get_max_io_util(lst_disks_io_time, 15));
            // update lst map
            DorisMetrics::system_metrics()->get_disks_io_time(&lst_disks_io_time);

            // 4. max network traffic
            int64_t max_send = 0;
            int64_t max_receive = 0;
            DorisMetrics::system_metrics()->get_max_net_traffic(
                lst_net_send_bytes, lst_net_receive_bytes, 15, &max_send, &max_receive);
            DorisMetrics::max_network_send_bytes_rate.set_value(max_send);
            DorisMetrics::max_network_receive_bytes_rate.set_value(max_receive);
            // update lst map
            DorisMetrics::system_metrics()->get_network_traffic(&lst_net_send_bytes, &lst_net_receive_bytes);
        }

        sleep(15); // 15 seconds
    }   
    
    return NULL;
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
            LOG(WARNING) << "get disk devices failed, stauts=" << st.get_error_msg();
            return;
        }
        st = get_inet_interfaces(&network_interfaces);
        if (!st.ok()) {
            LOG(WARNING) << "get inet interfaces failed, stauts=" << st.get_error_msg();
            return;
        }
    }
    DorisMetrics::instance()->initialize(
        "doris_be", paths, init_system_metrics, disk_devices, network_interfaces);

    if (config::enable_metric_calculator) {
        pthread_t calculator_pid;
        pthread_create(&calculator_pid, NULL, calculate_metrics, NULL);
    }
}

void sigterm_handler(int signo) {
    k_doris_exit = true;
}

int install_signal(int signo, void(*handler)(int)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    auto ret = sigaction(signo, &sa, nullptr);
    if (ret != 0) {
        char buf[64];
        LOG(ERROR) << "install signal failed, signo=" << signo
            << ", errno=" << errno
            << ", errmsg=" << strerror_r(errno, buf, sizeof(buf));
    }
    return ret;
}

void init_signals() {
    auto ret = install_signal(SIGINT, sigterm_handler);
    if (ret < 0) {
        exit(-1);
    }
    ret = install_signal(SIGTERM, sigterm_handler);
    if (ret < 0) {
        exit(-1);
    }
}

void init_daemon(int argc, char** argv, const std::vector<StorePath>& paths) {
    // google::SetVersionString(get_build_version(false));
    // google::ParseCommandLineFlags(&argc, &argv, true);
    google::ParseCommandLineFlags(&argc, &argv, true);
    init_glog("be", true);

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
    CastFunctions::init();
    InPredicate::init();
    MathFunctions::init();
    EncryptionFunctions::init();
    TimestampFunctions::init();
    DecimalOperators::init();
    DecimalV2Operators::init();
    UtilityFunctions::init();
    CompoundPredicate::init();
    JsonFunctions::init();
    HllHashFunctions::init();
    ESFunctions::init();

    pthread_t tc_malloc_pid;
    pthread_create(&tc_malloc_pid, NULL, tcmalloc_gc_thread, NULL);

    pthread_t buffer_pool_pid;
    pthread_create(&buffer_pool_pid, NULL, memory_maintenance_thread, NULL);

    LOG(INFO) << CpuInfo::debug_string();
    LOG(INFO) << DiskInfo::debug_string();
    LOG(INFO) << MemInfo::debug_string();
    init_doris_metrics(paths);
    init_signals();
}

}
