// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/olap_server.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cmath>
#include <ctime>
#include <string>

#include <gperftools/profiler.h>

#include "olap/command_executor.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_engine.h"
#include "olap/olap_snapshot.h"
#include "agent/cgroups_mgr.h"


using std::string;

namespace palo {

// number of running SCHEMA-CHANGE threads
volatile uint32_t g_schema_change_active_threads = 0;

MutexLock OLAPServer::_s_garbage_sweeper_mutex = MutexLock();
Condition OLAPServer::_s_garbage_sweeper_cond = Condition(OLAPServer::_s_garbage_sweeper_mutex);

MutexLock OLAPServer::_s_disk_stat_monitor_mutex = MutexLock();
Condition OLAPServer::_s_disk_stat_monitor_cond = Condition(OLAPServer::_s_disk_stat_monitor_mutex);

MutexLock OLAPServer::_s_unused_index_mutex = MutexLock();
Condition OLAPServer::_s_unused_index_cond = Condition(OLAPServer::_s_unused_index_mutex);

MutexLock OLAPServer::_s_check_disks_mutex = MutexLock();
Condition OLAPServer::_s_check_disks_cond = Condition(OLAPServer::_s_check_disks_mutex);

MutexLock OLAPServer::_s_session_timeout_mutex = MutexLock();
Condition OLAPServer::_s_session_timeout_cond = Condition(OLAPServer::_s_session_timeout_mutex);

OLAPServer::OLAPServer() { }

OLAPStatus OLAPServer::init(const char* config_path, const char* config_file) {
    // start thread for monitoring the snapshot and trash folder
    if (pthread_create(&_garbage_sweeper_thread,
                       NULL,
                       _garbage_sweeper_thread_callback,
                       NULL) != 0) {
        OLAP_LOG_FATAL("failed to start garbage sweeper thread.");
        return OLAP_ERR_INIT_FAILED;
    }

    // start thread for monitoring the table with io error
    if (pthread_create(&_disk_stat_monitor_thread,
                       NULL,
                       _disk_stat_monitor_thread_callback,
                       NULL) != 0) {
        OLAP_LOG_FATAL("failed to start disk stat monitor thread.");
        return OLAP_ERR_INIT_FAILED;
    }

    // start thread for monitoring the unused index
    if (pthread_create(&_unused_index_thread,
                       NULL,
                       _unused_index_thread_callback,
                       NULL) != 0) {
        OLAP_LOG_FATAL("failed to start unused index thread.");
        return OLAP_ERR_INIT_FAILED;
    }

    // start be and ce threads for merge data
    int32_t base_compaction_num_threads = config::base_compaction_num_threads;
    _base_compaction_threads.resize(base_compaction_num_threads, -1);
    for (uint32_t i = 0; i < base_compaction_num_threads; ++i) {
        if (0 != pthread_create(&_base_compaction_threads[i],
                                NULL,
                                _base_compaction_thread_callback,
                                NULL)) {
            OLAP_LOG_FATAL("failed to start base compaction thread. [id=%u]", i); 
            return OLAP_ERR_INIT_FAILED;
        }
    }

    int32_t cumulative_compaction_num_threads = config::cumulative_compaction_num_threads;
    _cumulative_compaction_threads.resize(cumulative_compaction_num_threads, -1);
    for (uint32_t i = 0; i < cumulative_compaction_num_threads; ++i) {
        if (0 != pthread_create(&(_cumulative_compaction_threads[i]), 
                                NULL, 
                                _cumulative_compaction_thread_callback, 
                                NULL)) {
            OLAP_LOG_FATAL("failed to start cumulative thread. [id=%u]", i); 
            return OLAP_ERR_INIT_FAILED;
        }
    }

    if (0 != pthread_create(&_fd_cache_clean_thread, NULL, _fd_cache_clean_callback, NULL)) {
        OLAP_LOG_FATAL("failed to start fd_cache_clean thread"); 
        return OLAP_ERR_INIT_FAILED;
    }

    OLAP_LOG_TRACE("init finished.");
    return OLAP_SUCCESS;
}

void* OLAPServer::_fd_cache_clean_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    uint32_t interval = config::file_descriptor_cache_clean_interval;
    if (interval <= 0) {
        OLAP_LOG_WARNING("config of file descriptor clean interval is illegal: [%d], "
                         "force set to 3600", interval);
        interval = 3600;
    }
    while (true) {
        sleep(interval);
        OLAPEngine::get_instance()->start_clean_fd_cache();
    }

    return NULL;
}

void* OLAPServer::_base_compaction_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    uint32_t interval = config::base_compaction_check_interval_seconds;
    if (interval <= 0) {
        OLAP_LOG_WARNING("base compaction check interval config is illegal: [%d], "
                         "force set to 1", interval);
        interval = 1;
    }

    string last_base_compaction_fs;
    TTabletId last_base_compaction_tablet_id = -1;
    while (true) {
        // must be here, because this thread is start on start and
        // cgroup is not initialized at this time
        // add tid to cgroup
        CgroupsMgr::apply_system_cgroup();
        OLAPEngine::get_instance()->start_base_compaction(&last_base_compaction_fs, &last_base_compaction_tablet_id);

        usleep(interval * 1000000);
    }

    return NULL;
}

void* OLAPServer::_garbage_sweeper_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    uint32_t max_interval = config::max_garbage_sweep_interval;
    uint32_t min_interval = config::min_garbage_sweep_interval;
    AutoMutexLock l(&_s_garbage_sweeper_mutex);

    if (!(max_interval >= min_interval && min_interval > 0)) {
        OLAP_LOG_WARNING("garbage sweep interval config is illegal: [max=%d min=%d].",
                         max_interval, min_interval);
        min_interval = 1;
        max_interval = max_interval >= min_interval ? max_interval : min_interval;
        OLAP_LOG_INFO("force reset garbage sweep interval.  [max=%d min=%d].",
                      max_interval, min_interval);
    }

    const double pi = 4 * std::atan(1);
    double usage = 1.0;
    // 程序启动后经过min_interval后触发第一轮扫描
    while (true) {
        usage *= 100.0;
        // 该函数特性：当磁盘使用率<60%的时候，ratio接近于1；
        // 当使用率介于[60%, 75%]之间时，ratio急速从0.87降到0.27；
        // 当使用率大于75%时，ratio值开始缓慢下降
        // 当usage=90%时，ratio约为0.0057
        double ratio = (1.1 * (pi / 2 - std::atan(usage / 5 - 14)) - 0.28) / pi;
        ratio = ratio > 0 ? ratio : 0;
        uint32_t curr_interval = max_interval * ratio;
        // 此时的特性，当usage<60%时，curr_interval的时间接近max_interval，
        // 当usage > 80%时，curr_interval接近min_interval
        curr_interval = curr_interval > min_interval ? curr_interval : min_interval;
        _s_garbage_sweeper_cond.wait_for_seconds(curr_interval);

        // 开始清理，并得到清理后的磁盘使用率
        OLAPStatus res = OLAPEngine::get_instance()->start_trash_sweep(&usage);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("one or more errors occur when sweep trash."
                    "see previous message for detail. [err code=%d]", res);
            // do nothing. continue next loop.
        }
    }

    return NULL;
}

void* OLAPServer::_disk_stat_monitor_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    uint32_t interval = config::disk_stat_monitor_interval;
    AutoMutexLock l(&_s_disk_stat_monitor_mutex);

    if (interval <= 0) {
        OLAP_LOG_WARNING("disk_stat_monitor_interval config is illegal: [%d], "
                         "force set to 1", interval);
        interval = 1;
    }

    while (true) {
        OLAPRootPath::get_instance()->start_disk_stat_monitor();
        _s_disk_stat_monitor_cond.wait_for_seconds(interval);
    }

    return NULL;
}

void* OLAPServer::_unused_index_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    uint32_t interval = config::unused_index_monitor_interval;
    AutoMutexLock l(&_s_unused_index_mutex);

    if (interval <= 0) {
        OLAP_LOG_WARNING("unused_index_monitor_interval config is illegal: [%d], "
                         "force set to 1", interval);
        interval = 1;
    }

    while (true) {
        OLAPUnusedIndex::get_instance()->start_delete_unused_index();
        _s_unused_index_cond.wait_for_seconds(interval);
    }

    return NULL;
}

void* OLAPServer::_cumulative_compaction_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    OLAP_LOG_INFO("try to start cumulative compaction process!");
    uint32_t interval = config::cumulative_compaction_check_interval_seconds;
    if (interval <= 0) {
        OLAP_LOG_WARNING("cumulative compaction check interval config is illegal: [%d], "
                         "force set to 1", interval);
        interval = 1;
    }

    while (true) {
        // must be here, because this thread is start on start and
        // cgroup is not initialized at this time
        // add tid to cgroup
        CgroupsMgr::apply_system_cgroup();
        OLAPEngine::get_instance()->start_cumulative_priority();
        usleep(interval * 1000000);
    }

    return NULL;
}

}  // namespace palo
