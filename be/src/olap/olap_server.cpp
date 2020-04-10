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

#include "olap/storage_engine.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cmath>
#include <ctime>
#include <string>

#include <gperftools/profiler.h>

#include "olap/cumulative_compaction.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "agent/cgroups_mgr.h"
#include "util/time.h"

using std::string;

namespace doris {

// TODO(yingchun): should be more graceful in the future refactor.
#define SLEEP_IN_BG_WORKER(seconds)               \
  int64_t left_seconds = (seconds);               \
  while (!_stop_bg_worker && left_seconds > 0) {  \
      sleep(1);                                   \
      --left_seconds;                             \
  }                                               \
  if (_stop_bg_worker) {                          \
      break;                                      \
  }

// number of running SCHEMA-CHANGE threads
volatile uint32_t g_schema_change_active_threads = 0;

OLAPStatus StorageEngine::_start_bg_worker() {
    _unused_rowset_monitor_thread =  std::thread(
        [this] {
            _unused_rowset_monitor_thread_callback(nullptr);
        });
    _unused_rowset_monitor_thread.detach();

    // start thread for monitoring the snapshot and trash folder
    _garbage_sweeper_thread = std::thread(
        [this] {
            _garbage_sweeper_thread_callback(nullptr);
        });
    _garbage_sweeper_thread.detach();
    // start thread for monitoring the tablet with io error
    _disk_stat_monitor_thread = std::thread(
        [this] {
            _disk_stat_monitor_thread_callback(nullptr);
        });
    _disk_stat_monitor_thread.detach();

    // convert store map to vector
    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
    }
    int32_t data_dir_num = data_dirs.size();

    // base and cumulative compaction threads
    int32_t base_compaction_num_threads_per_disk = std::max<int32_t>(1, config::base_compaction_num_threads_per_disk);
    int32_t cumulative_compaction_num_threads_per_disk = std::max<int32_t>(1, config::cumulative_compaction_num_threads_per_disk);
    int32_t base_compaction_num_threads = base_compaction_num_threads_per_disk * data_dir_num;
    int32_t cumulative_compaction_num_threads = cumulative_compaction_num_threads_per_disk * data_dir_num;
    // calc the max concurrency of compaction tasks
    int32_t max_compaction_concurrency = config::max_compaction_concurrency;
    if (max_compaction_concurrency < 0
        || max_compaction_concurrency > base_compaction_num_threads + cumulative_compaction_num_threads) {
        max_compaction_concurrency = base_compaction_num_threads + cumulative_compaction_num_threads;
    }
    Compaction::init(max_compaction_concurrency);

    _base_compaction_threads.reserve(base_compaction_num_threads);
    for (uint32_t i = 0; i < base_compaction_num_threads; ++i) {
        _base_compaction_threads.emplace_back(
            [this, data_dir_num, data_dirs, i] {
                _base_compaction_thread_callback(nullptr, data_dirs[i % data_dir_num]);
            });
    }
    for (auto& thread : _base_compaction_threads) {
        thread.detach();
    }

    _cumulative_compaction_threads.reserve(cumulative_compaction_num_threads);
    for (uint32_t i = 0; i < cumulative_compaction_num_threads; ++i) {
        _cumulative_compaction_threads.emplace_back(
            [this, data_dir_num, data_dirs, i] {
                _cumulative_compaction_thread_callback(nullptr, data_dirs[i % data_dir_num]);
            });
    }
    for (auto& thread : _cumulative_compaction_threads) {
        thread.detach();
    }

    // tablet checkpoint thread
    for (auto data_dir : data_dirs) {
        _tablet_checkpoint_threads.emplace_back(
        [this, data_dir] {
            _tablet_checkpoint_callback((void*)data_dir);
        });
    }
    for (auto& thread : _tablet_checkpoint_threads) {
        thread.detach();
    }

    // fd cache clean thread
    _fd_cache_clean_thread = std::thread(
        [this] {
            _fd_cache_clean_callback(nullptr);
        });
    _fd_cache_clean_thread.detach();

    // path scan and gc thread
    if (config::path_gc_check) {
        for (auto data_dir : get_stores()) {
            _path_scan_threads.emplace_back(
            [this, data_dir] {
                _path_scan_thread_callback((void*)data_dir);
            });

            _path_gc_threads.emplace_back(
            [this, data_dir] {
                _path_gc_thread_callback((void*)data_dir);
            });
        }
        for (auto& thread : _path_scan_threads) {
            thread.detach();
        }
        for (auto& thread : _path_gc_threads) {
            thread.detach();
        }
    }

    VLOG(10) << "all bg worker started.";
    return OLAP_SUCCESS;
}

void* StorageEngine::_fd_cache_clean_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_stop_bg_worker) {
        int32_t interval = config::file_descriptor_cache_clean_interval;
        if (interval <= 0) {
            OLAP_LOG_WARNING("config of file descriptor clean interval is illegal: [%d], "
                             "force set to 3600", interval);
            interval = 3600;
        }
        SLEEP_IN_BG_WORKER(interval);

        _start_clean_fd_cache();
    }

    return nullptr;
}

void* StorageEngine::_base_compaction_thread_callback(void* arg, DataDir* data_dir) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    //string last_base_compaction_fs;
    //TTabletId last_base_compaction_tablet_id = -1;
    while (!_stop_bg_worker) {
        // must be here, because this thread is start on start and
        // cgroup is not initialized at this time
        // add tid to cgroup
        CgroupsMgr::apply_system_cgroup();
        if (!data_dir->reach_capacity_limit(0)) {
            _perform_base_compaction(data_dir);
        }

        int32_t interval = config::base_compaction_check_interval_seconds;
        if (interval <= 0) {
            OLAP_LOG_WARNING("base compaction check interval config is illegal: [%d], "
                             "force set to 1", interval);
            interval = 1;
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_garbage_sweeper_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    uint32_t max_interval = config::max_garbage_sweep_interval;
    uint32_t min_interval = config::min_garbage_sweep_interval;

    if (!(max_interval >= min_interval && min_interval > 0)) {
        OLAP_LOG_WARNING("garbage sweep interval config is illegal: [max=%d min=%d].",
                         max_interval, min_interval);
        min_interval = 1;
        max_interval = max_interval >= min_interval ? max_interval : min_interval;
        LOG(INFO) << "force reset garbage sweep interval. "
                  << "max_interval=" << max_interval
                  << ", min_interval=" << min_interval;
    }

    const double pi = 4 * std::atan(1);
    double usage = 1.0;
    // 程序启动后经过min_interval后触发第一轮扫描
    while (!_stop_bg_worker) {
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
        SLEEP_IN_BG_WORKER(curr_interval);

        // 开始清理，并得到清理后的磁盘使用率
        OLAPStatus res = _start_trash_sweep(&usage);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("one or more errors occur when sweep trash."
                    "see previous message for detail. [err code=%d]", res);
            // do nothing. continue next loop.
        }
    }

    return nullptr;
}

void* StorageEngine::_disk_stat_monitor_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_stop_bg_worker) {
        _start_disk_stat_monitor();

        int32_t interval = config::disk_stat_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "disk_stat_monitor_interval config is illegal: " << interval
                         << ", force set to 1";
            interval = 1;
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_cumulative_compaction_thread_callback(void* arg, DataDir* data_dir) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start cumulative compaction process!";

    while (!_stop_bg_worker) {
        // must be here, because this thread is start on start and
        // cgroup is not initialized at this time
        // add tid to cgroup
        CgroupsMgr::apply_system_cgroup();
        if (!data_dir->reach_capacity_limit(0)) {
            _perform_cumulative_compaction(data_dir);
        }

        int32_t interval = config::cumulative_compaction_check_interval_seconds;
        if (interval <= 0) {
            LOG(WARNING) << "cumulative compaction check interval config is illegal:" << interval
                         << "will be forced set to one";
            interval = 1;
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_unused_rowset_monitor_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    while (!_stop_bg_worker) {
        start_delete_unused_rowset();

        int32_t interval = config::unused_rowset_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "unused_rowset_monitor_interval config is illegal: " << interval
                         << ", force set to 1";
            interval = 1;
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}



void* StorageEngine::_path_gc_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    LOG(INFO) << "try to start path gc thread!";

    while (!_stop_bg_worker) {
        LOG(INFO) << "try to perform path gc!";
        // perform path gc by rowset id
        ((DataDir*)arg)->perform_path_gc_by_rowsetid();

        int32_t interval = config::path_gc_check_interval_second;
        if (interval <= 0) {
            LOG(WARNING) << "path gc thread check interval config is illegal:" << interval
                         << "will be forced set to half hour";
            interval = 1800; // 0.5 hour
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_path_scan_thread_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    LOG(INFO) << "try to start path scan thread!";

    while (!_stop_bg_worker) {
        LOG(INFO) << "try to perform path scan!";
        ((DataDir*)arg)->perform_path_scan();

        int32_t interval = config::path_scan_interval_second;
        if (interval <= 0) {
            LOG(WARNING) << "path gc thread check interval config is illegal:" << interval
                         << "will be forced set to one day";
            interval = 24 * 3600; // one day
        }
        SLEEP_IN_BG_WORKER(interval);
    }

    return nullptr;
}

void* StorageEngine::_tablet_checkpoint_callback(void* arg) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start tablet meta checkpoint thread!";
    while (!_stop_bg_worker) {
        LOG(INFO) << "begin to do tablet meta checkpoint:" << ((DataDir*)arg)->path();
        int64_t start_time = UnixMillis();
        _tablet_manager->do_tablet_meta_checkpoint((DataDir*)arg);
        int64_t used_time = (UnixMillis() - start_time) / 1000;
        if (used_time < config::tablet_meta_checkpoint_min_interval_secs) {
            int64_t interval = config::tablet_meta_checkpoint_min_interval_secs - used_time;
            SLEEP_IN_BG_WORKER(interval);
        } else {
            sleep(1);
        }
    }

    return nullptr;
}

}  // namespace doris
