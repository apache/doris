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
#include <boost/algorithm/string.hpp>

#include "common/status.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "util/doris_metrics.h"
#include "util/time.h"

using std::string;

namespace doris {

// number of running SCHEMA-CHANGE threads
volatile uint32_t g_schema_change_active_threads = 0;

Status StorageEngine::start_bg_threads() {
    RETURN_IF_ERROR(
        Thread::create("StorageEngine", "unused_rowset_monitor_thread",
                       [this]() { this->_unused_rowset_monitor_thread_callback(); },
                       &_unused_rowset_monitor_thread));
    LOG(INFO) << "unused rowset monitor thread started";

    // start thread for monitoring the snapshot and trash folder
    RETURN_IF_ERROR(
        Thread::create("StorageEngine", "garbage_sweeper_thread",
                       [this]() { this->_garbage_sweeper_thread_callback(); },
                       &_garbage_sweeper_thread));
    LOG(INFO) << "garbage sweeper thread started";

    // start thread for monitoring the tablet with io error
    RETURN_IF_ERROR(
        Thread::create("StorageEngine", "disk_stat_monitor_thread",
                       [this]() { this->_disk_stat_monitor_thread_callback(); },
                       &_disk_stat_monitor_thread));
    LOG(INFO) << "disk stat monitor thread started";

    
    // convert store map to vector
    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
//        tmp_store.second->update_disks_compaction_score(0);
//        tmp_store.second->update_disks_compaction_num(0);
    }
//    DorisMetrics::instance()->total_compaction_score->set_value(0);
//    DorisMetrics::instance()->total_compaction_num->set_value(0);

    // check cumulative compaction config
    _check_cumulative_compaction_config();

    int32_t max_thread_num = config::max_compaction_threads;
    int32_t min_thread_num = config::min_compaction_threads;
    ThreadPoolBuilder("CompactionTaskThreadPool").set_min_threads(min_thread_num).set_max_threads(max_thread_num).build(&_thread_pool_compaction);

    // compaction tasks producer thread
    uint32_t total_permits = config::total_permits_for_compaction_score;
    _permit_limiter.init(total_permits, true);
    RETURN_IF_ERROR(
            Thread::create("StorageEngine", "compaction_tasks_producer_thread",
                           [this]() { this->_compaction_tasks_producer_callback(); },
                           &_compaction_tasks_producer_thread));
    LOG(INFO) << "compaction tasks producer thread started";

    // tablet checkpoint thread
    for (auto data_dir : data_dirs) {
        scoped_refptr <Thread> tablet_checkpoint_thread;
        RETURN_IF_ERROR(
                Thread::create("StorageEngine", "tablet_checkpoint_thread",
                               [this, data_dir]() { this->_tablet_checkpoint_callback(data_dir); },
                               &tablet_checkpoint_thread));
        _tablet_checkpoint_threads.emplace_back(tablet_checkpoint_thread);
    }
    LOG(INFO) << "tablet checkpint thread started";

    // fd cache clean thread
    RETURN_IF_ERROR(
            Thread::create("StorageEngine", "fd_cache_clean_thread",
                           [this]() { this->_fd_cache_clean_callback(); },
                           &_fd_cache_clean_thread));
    LOG(INFO) << "fd cache clean thread started";

    // path scan and gc thread
    if (config::path_gc_check) {
        for (auto data_dir : get_stores()) {
            scoped_refptr <Thread> path_scan_thread;
            RETURN_IF_ERROR(
                    Thread::create("StorageEngine", "path_scan_thread",
                                   [this, data_dir]() { this->_path_scan_thread_callback(data_dir); },
                                   &path_scan_thread));
            _path_scan_threads.emplace_back(path_scan_thread);

            scoped_refptr <Thread> path_gc_thread;
            RETURN_IF_ERROR(
                    Thread::create("StorageEngine", "path_gc_thread",
                                   [this, data_dir]() { this->_path_gc_thread_callback(data_dir); },
                                   &path_gc_thread));
            _path_gc_threads.emplace_back(path_gc_thread);
        }
        LOG(INFO) << "path scan/gc threads started. number:" << get_stores().size();
    }

    LOG(INFO) << "all storage engine's backgroud threads are started.";
    return Status::OK();
}

void StorageEngine::_fd_cache_clean_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    int32_t interval = 600;
    while (!_stop_background_threads_latch.wait_for(MonoDelta::FromSeconds(interval))) {
        interval = config::file_descriptor_cache_clean_interval;
        if (interval <= 0) {
            OLAP_LOG_WARNING("config of file descriptor clean interval is illegal: [%d], "
                             "force set to 3600", interval);
            interval = 3600;
        }

        _start_clean_fd_cache();
    }
}

void StorageEngine::_base_compaction_task(TabletSharedPtr tablet, uint32_t permits) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start base compaction process!";
    {
        std::unique_lock<std::mutex> lock(_mutex_compaction_metrics);
        tablet->data_dir()->update_disks_compaction_score(tablet->data_dir()->get_disks_compaction_score() + permits);
        tablet->data_dir()->update_disks_compaction_num(tablet->data_dir()->get_disks_compaction_num() + 1);
        //DorisMetrics::instance()->total_compaction_score->increment(permits);
        DorisMetrics::instance()->total_compaction_score->set_value(DorisMetrics::instance()->total_compaction_score->value() + permits);
        DorisMetrics::instance()->total_compaction_num->increment(1);
    }
    _perform_base_compaction(tablet);
    sleep(25);
    _permit_limiter.release(permits);
    {
        std::unique_lock<std::mutex> lock(_mutex_compaction_metrics);
        tablet->data_dir()->update_disks_compaction_score(tablet->data_dir()->get_disks_compaction_score() - permits);
        tablet->data_dir()->update_disks_compaction_num(tablet->data_dir()->get_disks_compaction_num() - 1);
        //DorisMetrics::instance()->total_compaction_score->increment(-permits);
        DorisMetrics::instance()->total_compaction_score->set_value(DorisMetrics::instance()->total_compaction_score->value() + permits);
        DorisMetrics::instance()->total_compaction_num->increment(-1);
    }
}

void StorageEngine::_garbage_sweeper_thread_callback() {
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
    uint32_t curr_interval = min_interval;
    while (!_stop_background_threads_latch.wait_for(MonoDelta::FromSeconds(curr_interval))) {
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

        // 开始清理，并得到清理后的磁盘使用率
        OLAPStatus res = _start_trash_sweep(&usage);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("one or more errors occur when sweep trash."
                    "see previous message for detail. [err code=%d]", res);
            // do nothing. continue next loop.
        }
    }
}

void StorageEngine::_disk_stat_monitor_thread_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    int32_t interval = config::disk_stat_monitor_interval;
    do {
        _start_disk_stat_monitor();

        interval = config::disk_stat_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "disk_stat_monitor_interval config is illegal: " << interval
                         << ", force set to 1";
            interval = 1;
        }
    } while (!_stop_background_threads_latch.wait_for(MonoDelta::FromSeconds(interval)));
}

void StorageEngine::_check_cumulative_compaction_config() {

    std::string cumulative_compaction_type = config::cumulative_compaction_policy;
    boost::to_upper(cumulative_compaction_type);

    // if size_based policy is used, check size_based policy configs
    if (cumulative_compaction_type == CUMULATIVE_SIZE_BASED_POLICY) {
        int64_t size_based_promotion_size =
                config::cumulative_size_based_promotion_size_mbytes;
        int64_t size_based_promotion_min_size =
                config::cumulative_size_based_promotion_min_size_mbytes;
        int64_t size_based_compaction_lower_bound_size =
                config::cumulative_size_based_compaction_lower_size_mbytes;

        // check size_based_promotion_size must be greater than size_based_promotion_min_size and 2 * size_based_compaction_lower_bound_size
        int64_t should_min_size_based_promotion_size =
                std::max(size_based_promotion_min_size, 2 * size_based_compaction_lower_bound_size);

        if (size_based_promotion_size < should_min_size_based_promotion_size) {
            size_based_promotion_size = should_min_size_based_promotion_size;
            LOG(WARNING) << "the config size_based_promotion_size is adjusted to "
                            "size_based_promotion_min_size or  2 * size_based_compaction_lower_bound_size "
                         << should_min_size_based_promotion_size
                         << ", because size_based_promotion_size is small";
        }
    }
}

void StorageEngine::_cumulative_compaction_task(TabletSharedPtr tablet, uint32_t permits) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start cumulative compaction process!";
    {
        std::unique_lock<std::mutex> lock(_mutex_compaction_metrics);
        tablet->data_dir()->update_disks_compaction_score(tablet->data_dir()->get_disks_compaction_score() + permits);
        tablet->data_dir()->update_disks_compaction_num(tablet->data_dir()->get_disks_compaction_num() + 1);
        //DorisMetrics::instance()->total_compaction_score->increment(permits);
        DorisMetrics::instance()->total_compaction_score->set_value(DorisMetrics::instance()->total_compaction_score->value() + permits);
        DorisMetrics::instance()->total_compaction_num->increment(1);
    }
    _perform_cumulative_compaction(tablet);
    sleep(25);
    _permit_limiter.release(permits);
    {
        std::unique_lock<std::mutex> lock(_mutex_compaction_metrics);
        tablet->data_dir()->update_disks_compaction_score(tablet->data_dir()->get_disks_compaction_score() - permits);
        tablet->data_dir()->update_disks_compaction_num(tablet->data_dir()->get_disks_compaction_num() - 1);
        //DorisMetrics::instance()->total_compaction_score->increment(-permits);
        DorisMetrics::instance()->total_compaction_score->set_value(DorisMetrics::instance()->total_compaction_score->value() - permits);
        DorisMetrics::instance()->total_compaction_num->increment(-1);
    }
}

void StorageEngine::_unused_rowset_monitor_thread_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    int32_t interval = config::unused_rowset_monitor_interval;
    do {
        start_delete_unused_rowset();

        interval = config::unused_rowset_monitor_interval;
        if (interval <= 0) {
            LOG(WARNING) << "unused_rowset_monitor_interval config is illegal: " << interval
                         << ", force set to 1";
            interval = 1;
        }
    } while (!_stop_background_threads_latch.wait_for(MonoDelta::FromSeconds(interval)));
}

void StorageEngine::_path_gc_thread_callback(DataDir* data_dir) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    LOG(INFO) << "try to start path gc thread!";
    int32_t interval = config::path_gc_check_interval_second;
    do {
        LOG(INFO) << "try to perform path gc by tablet!";
        data_dir->perform_path_gc_by_tablet();
        
        LOG(INFO) << "try to perform path gc by rowsetid!";
        data_dir->perform_path_gc_by_rowsetid();

        interval = config::path_gc_check_interval_second;
        if (interval <= 0) {
            LOG(WARNING) << "path gc thread check interval config is illegal:" << interval
                         << "will be forced set to half hour";
            interval = 1800; // 0.5 hour
        }
    } while (!_stop_background_threads_latch.wait_for(MonoDelta::FromSeconds(interval)));
}

void StorageEngine::_path_scan_thread_callback(DataDir* data_dir) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    int32_t interval = config::path_scan_interval_second;
    do {
        LOG(INFO) << "try to perform path scan!";
        data_dir->perform_path_scan();

        interval = config::path_scan_interval_second;
        if (interval <= 0) {
            LOG(WARNING) << "path gc thread check interval config is illegal:" << interval
                         << "will be forced set to one day";
            interval = 24 * 3600; // one day
        }
    } while (!_stop_background_threads_latch.wait_for(MonoDelta::FromSeconds(interval)));
}

void StorageEngine::_tablet_checkpoint_callback(DataDir* data_dir) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    int64_t interval = config::tablet_meta_checkpoint_min_interval_secs;
    do {
        LOG(INFO) << "begin to do tablet meta checkpoint:" << data_dir->path();
        int64_t start_time = UnixMillis();
        _tablet_manager->do_tablet_meta_checkpoint(data_dir);
        int64_t used_time = (UnixMillis() - start_time) / 1000;
        if (used_time < config::tablet_meta_checkpoint_min_interval_secs) {
            interval = config::tablet_meta_checkpoint_min_interval_secs - used_time;
        } else {
            interval = 1;
        }
    } while (!_stop_background_threads_latch.wait_for(MonoDelta::FromSeconds(interval)));
}

Status StorageEngine::_compaction_tasks_producer_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start compaction producer process!";

    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
    }

    int round = 0;
    CompactionType compaction_type;
    do {
        if(round < 9) {
            compaction_type = CompactionType::CUMULATIVE_COMPACTION;
            round++;
        } else {
            compaction_type = CompactionType::BASE_COMPACTION;
            round = 0;
        }
        LOG(INFO) << "try to generate a batch of compaction tasks!";
        vector<TabletSharedPtr> tablets_compaction = _compaction_tasks_generator(compaction_type, data_dirs);
        for (int i = 0; i < tablets_compaction.size(); i++) {
            if(tablets_compaction[i]->data_dir()->get_disks_compaction_num() < 5) {
                uint32_t permits;
                if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                    permits = tablets_compaction[i]->calc_cumulative_compaction_score();
                } else {
                    permits = tablets_compaction[i]->calc_base_compaction_score();
                }
                if (_permit_limiter.request(permits)) {
                    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                        _thread_pool_compaction->submit_func([this, i, tablets_compaction, permits]() {this->_cumulative_compaction_task(tablets_compaction[i], permits);});
                    } else {
                        _thread_pool_compaction->submit_func([this, i, tablets_compaction, permits]() {this->_base_compaction_task(tablets_compaction[i], permits);});
                    }
                }
            }
        }
        sleep(5);
    } while (true);
}

vector<TabletSharedPtr> StorageEngine::_compaction_tasks_generator(CompactionType compaction_type, std::vector<DataDir*> data_dirs) {
    vector<TabletSharedPtr> tablets_compaction;
    std::random_shuffle(data_dirs.begin(), data_dirs.end());
    for (auto data_dir : data_dirs) {
        if (!data_dir->reach_capacity_limit(0)) {
            TabletSharedPtr tablet = _tablet_manager->find_best_tablet_to_compaction(compaction_type, data_dir);
            tablets_compaction.emplace_back(tablet);
        }
    }
    return tablets_compaction;
}
}  // namespace doris
