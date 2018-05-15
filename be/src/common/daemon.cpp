// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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
#include "util/palo_metrics.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/lib_cache.h"
#include "exprs/operators.h"
#include "exprs/is_null_predicate.h"
#include "exprs/like_predicate.h"
#include "exprs/compound_predicate.h"
#include "exprs/new_in_predicate.h"
#include "exprs/string_functions.h"
#include "exprs/cast_functions.h"
#include "exprs/math_functions.h"
#include "exprs/encryption_functions.h"
#include "exprs/timestamp_functions.h"
#include "exprs/decimal_operators.h"
#include "exprs/utility_functions.h"
#include "exprs/json_functions.h"
#include "exprs/hll_hash_function.h"
#include "olap/olap_rootpath.h"

namespace palo {

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
        sleep(config::FLAGS_memory_maintenance_sleep_time_s);
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

static void init_palo_metrics() {
    bool init_system_metrics = config::enable_system_metrics;
    std::set<std::string> disk_devices;
    std::vector<std::string> network_interfaces;
    if (init_system_metrics) {
        std::vector<std::string> paths;
        std::vector<int64_t> capacities;
        auto res = OLAPRootPath::parse_root_paths_from_string(
            config::storage_root_path.c_str(), &paths, &capacities);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "parse storage_root_path failed, res=" << res
                << ", path=" << config::storage_root_path;
            return;
        }
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
    PaloMetrics::instance()->initialize(
        "palo_be", init_system_metrics, disk_devices, network_interfaces);
}

void init_daemon(int argc, char** argv) {
    // google::SetVersionString(get_build_version(false));
    // google::ParseCommandLineFlags(&argc, &argv, true);
    google::ParseCommandLineFlags(&argc, &argv, true);
    init_glog("be", true);

    LOG(INFO) << get_version_string(false);

    init_thrift_logging();
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    LibCache::init();
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
    UtilityFunctions::init();
    CompoundPredicate::init();
    JsonFunctions::init();
    HllHashFunctions::init();

    pthread_t tc_malloc_pid;
    pthread_create(&tc_malloc_pid, NULL, tcmalloc_gc_thread, NULL);

    pthread_t buffer_pool_pid;
    pthread_create(&buffer_pool_pid, NULL, memory_maintenance_thread, NULL);
    
    LOG(INFO) << CpuInfo::debug_string();
    LOG(INFO) << DiskInfo::debug_string();
    LOG(INFO) << MemInfo::debug_string();
    init_palo_metrics();
}

}
