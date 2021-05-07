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

#include <gperftools/profiler.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <cmath>
#include <ctime>
#include <string>

#include "agent/cgroups_mgr.h"
#include "common/status.h"
#include "olap/cumulative_compaction.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "util/time.h"

using std::string;

namespace doris {

// number of running SCHEMA-CHANGE threads
volatile uint32_t g_schema_change_active_threads = 0;

Status StorageEngine::start_bg_threads() {
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "unused_rowset_monitor_thread",
            [this]() { this->_unused_rowset_monitor_thread_callback(); },
            &_unused_rowset_monitor_thread));
    LOG(INFO) << "unused rowset monitor thread started";

    // start thread for monitoring the snapshot and trash folder
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "garbage_sweeper_thread",
            [this]() { this->_garbage_sweeper_thread_callback(); }, &_garbage_sweeper_thread));
    LOG(INFO) << "garbage sweeper thread started";

    // start thread for monitoring the tablet with io error
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "disk_stat_monitor_thread",
            [this]() { this->_disk_stat_monitor_thread_callback(); }, &_disk_stat_monitor_thread));
    LOG(INFO) << "disk stat monitor thread started";

    // convert store map to vector
    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
    }

    int32_t max_thread_num = config::max_compaction_threads;
    ThreadPoolBuilder("CompactionTaskThreadPool")
            .set_min_threads(max_thread_num)
            .set_max_threads(max_thread_num)
            .build(&_compaction_thread_pool);

    // compaction tasks producer thread
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "compaction_tasks_producer_thread",
            [this]() { this->_compaction_tasks_producer_callback(); },
            &_compaction_tasks_producer_thread));
    LOG(INFO) << "compaction tasks producer thread started";

    int32_t max_checkpoint_thread_num = config::max_meta_checkpoint_threads;
    if (max_checkpoint_thread_num < 0) {
        max_checkpoint_thread_num = data_dirs.size();
    }
    ThreadPoolBuilder("TabletMetaCheckpointTaskThreadPool")
            .set_max_threads(max_checkpoint_thread_num)
            .build(&_tablet_meta_checkpoint_thread_pool);

    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "tablet_checkpoint_tasks_producer_thread",
            [this, data_dirs]() { this->_tablet_checkpoint_callback(data_dirs); },
            &_tablet_checkpoint_tasks_producer_thread));
    LOG(INFO) << "tablet checkpoint tasks producer thread started";

    // fd cache clean thread
    RETURN_IF_ERROR(Thread::create(
            "StorageEngine", "fd_cache_clean_thread",
            [this]() { this->_fd_cache_clean_callback(); }, &_fd_cache_clean_thread));
    LOG(INFO) << "fd cache clean thread started";

    // path scan and gc thread
    if (config::path_gc_check) {
        for (auto data_dir : get_stores()) {
            scoped_refptr<Thread> path_scan_thread;
            RETURN_IF_ERROR(Thread::create(
                    "StorageEngine", "path_scan_thread",
                    [this, data_dir]() { this->_path_scan_thread_callback(data_dir); },
                    &path_scan_thread));
            _path_scan_threads.emplace_back(path_scan_thread);

            scoped_refptr<Thread> path_gc_thread;
            RETURN_IF_ERROR(Thread::create(
                    "StorageEngine", "path_gc_thread",
                    [this, data_dir]() { this->_path_gc_thread_callback(data_dir); },
                    &path_gc_thread));
            _path_gc_threads.emplace_back(path_gc_thread);
        }
        LOG(INFO) << "path scan/gc threads started. number:" << get_stores().size();
    }

    LOG(INFO) << "all storage engine's background threads are started.";
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
            OLAP_LOG_WARNING(
                    "config of file descriptor clean interval is illegal: [%d], "
                    "force set to 3600",
                    interval);
            interval = 3600;
        }

        _start_clean_fd_cache();
    }
}

void StorageEngine::_garbage_sweeper_thread_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    uint32_t max_interval = config::max_garbage_sweep_interval;
    uint32_t min_interval = config::min_garbage_sweep_interval;

    if (!(max_interval >= min_interval && min_interval > 0)) {
        OLAP_LOG_WARNING("garbage sweep interval config is illegal: [max=%d min=%d].", max_interval,
                         min_interval);
        min_interval = 1;
        max_interval = max_interval >= min_interval ? max_interval : min_interval;
        LOG(INFO) << "force reset garbage sweep interval. "
                  << "max_interval=" << max_interval << ", min_interval=" << min_interval;
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
            OLAP_LOG_WARNING(
                    "one or more errors occur when sweep trash."
                    "see previous message for detail. [err code=%d]",
                    res);
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
    int64_t size_based_promotion_size = config::cumulative_size_based_promotion_size_mbytes;
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
                        "size_based_promotion_min_size or  2 * "
                        "size_based_compaction_lower_bound_size "
                     << should_min_size_based_promotion_size
                     << ", because size_based_promotion_size is small";
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

void StorageEngine::_tablet_checkpoint_callback(const std::vector<DataDir*>& data_dirs) {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif

    int64_t interval = config::generate_tablet_meta_checkpoint_tasks_interval_secs;
    do {
        LOG(INFO) << "begin to produce tablet meta checkpoint tasks.";
        for (auto data_dir : data_dirs) {
            auto st =_tablet_meta_checkpoint_thread_pool->submit_func([=]() {
                CgroupsMgr::apply_system_cgroup();
                _tablet_manager->do_tablet_meta_checkpoint(data_dir);
            });
            if (!st.ok()) {
                LOG(WARNING) << "submit tablet checkpoint tasks failed.";
            }
        }
        interval = config::generate_tablet_meta_checkpoint_tasks_interval_secs;
    } while (!_stop_background_threads_latch.wait_for(MonoDelta::FromSeconds(interval)));
}

void StorageEngine::_compaction_tasks_producer_callback() {
#ifdef GOOGLE_PROFILER
    ProfilerRegisterThread();
#endif
    LOG(INFO) << "try to start compaction producer process!";

    std::unordered_set<TTabletId> tablet_submitted_cumu;
    std::unordered_set<TTabletId> tablet_submitted_base;
    std::vector<DataDir*> data_dirs;
    for (auto& tmp_store : _store_map) {
        data_dirs.push_back(tmp_store.second);
        _tablet_submitted_cumu_compaction[tmp_store.second] = tablet_submitted_cumu;
        _tablet_submitted_base_compaction[tmp_store.second] = tablet_submitted_base;
    }

    int round = 0;
    CompactionType compaction_type;

    // Used to record the time when the score metric was last updated.
    // The update of the score metric is accompanied by the logic of selecting the tablet.
    // If there is no slot available, the logic of selecting the tablet will be terminated,
    // which causes the score metric update to be terminated.
    // In order to avoid this situation, we need to update the score regularly.
    int64_t last_cumulative_score_update_time = 0;
    int64_t last_base_score_update_time = 0;
    static const int64_t check_score_interval_ms = 5000; // 5 secs

    int64_t interval = config::generate_compaction_tasks_min_interval_ms;
    do {
        if (!config::disable_auto_compaction) {
            bool check_score = false;
            int64_t cur_time = UnixMillis();
            if (round < config::cumulative_compaction_rounds_for_each_base_compaction_round) {
                compaction_type = CompactionType::CUMULATIVE_COMPACTION;
                round++;
                if (cur_time - last_cumulative_score_update_time >= check_score_interval_ms) {
                    check_score = true;
                    last_cumulative_score_update_time = cur_time;
                }
            } else {
                compaction_type = CompactionType::BASE_COMPACTION;
                round = 0;
                if (cur_time - last_base_score_update_time >= check_score_interval_ms) {
                    check_score = true;
                    last_base_score_update_time = cur_time;
                }
            }
            std::vector<TabletSharedPtr> tablets_compaction =
                    _generate_compaction_tasks(compaction_type, data_dirs, check_score);
            if (tablets_compaction.size() == 0) {
                std::unique_lock<std::mutex> lock(_compaction_producer_sleep_mutex);
                _wakeup_producer_flag = 0;
                // It is necessary to wake up the thread on timeout to prevent deadlock
                // in case of no running compaction task.
                _compaction_producer_sleep_cv.wait_for(lock, std::chrono::milliseconds(2000),
                                                       [=] { return _wakeup_producer_flag == 1; });
                continue;
            }

            /// Regardless of whether the tablet is submitted for compaction or not,
            /// we need to call 'reset_compaction' to clean up the base_compaction or cumulative_compaction objects
            /// in the tablet, because these two objects store the tablet's own shared_ptr.
            /// If it is not cleaned up, the reference count of the tablet will always be greater than 1,
            /// thus cannot be collected by the garbage collector. (TabletManager::start_trash_sweep)
            for (const auto& tablet : tablets_compaction) {
                int64_t permits = tablet->prepare_compaction_and_calculate_permits(compaction_type, tablet);
                if (permits > 0 && _permit_limiter.request(permits)) {
                    // Push to _tablet_submitted_compaction before submitting task
                    _push_tablet_into_submitted_compaction(tablet, compaction_type);
                    auto st =_compaction_thread_pool->submit_func([=]() {
                      CgroupsMgr::apply_system_cgroup();
                      tablet->execute_compaction(compaction_type);
                      _permit_limiter.release(permits);
                      _pop_tablet_from_submitted_compaction(tablet, compaction_type);
                      // reset compaction
                      tablet->reset_compaction(compaction_type); 
                    });
                    if (!st.ok()) {
                        _permit_limiter.release(permits);
                        _pop_tablet_from_submitted_compaction(tablet, compaction_type);
                        // reset compaction
                        tablet->reset_compaction(compaction_type); 
                    }
                } else {
                    // reset compaction
                    tablet->reset_compaction(compaction_type);
                }
            }
            interval = config::generate_compaction_tasks_min_interval_ms;
        } else {
            interval = config::check_auto_compaction_interval_seconds * 1000;
        }
    } while (!_stop_background_threads_latch.wait_for(MonoDelta::FromMilliseconds(interval)));
}

std::vector<TabletSharedPtr> StorageEngine::_generate_compaction_tasks(
        CompactionType compaction_type, std::vector<DataDir*>& data_dirs, bool check_score) {
    std::string current_policy = "";
    {
        std::lock_guard<std::mutex> lock(*config::get_mutable_string_config_lock());
        current_policy = config::cumulative_compaction_policy;
    }
    boost::to_upper(current_policy);
    if (_cumulative_compaction_policy == nullptr ||
        _cumulative_compaction_policy->name() != current_policy) {
        if (current_policy == CUMULATIVE_SIZE_BASED_POLICY) {
            // check size_based cumulative compaction config
            _check_cumulative_compaction_config();
        }
        _cumulative_compaction_policy =
                CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(current_policy);
    }

    std::vector<TabletSharedPtr> tablets_compaction;
    uint32_t max_compaction_score = 0;
    std::random_shuffle(data_dirs.begin(), data_dirs.end());

    // Copy _tablet_submitted_xxx_compaction map so that we don't need to hold _tablet_submitted_compaction_mutex
    // when travesing the data dir
    std::map<DataDir*, std::unordered_set<TTabletId>> copied_cumu_map; 
    std::map<DataDir*, std::unordered_set<TTabletId>> copied_base_map; 
    {
        std::unique_lock<std::mutex> lock(_tablet_submitted_compaction_mutex);
        copied_cumu_map = _tablet_submitted_cumu_compaction;
        copied_base_map = _tablet_submitted_base_compaction;
    }
    for (auto data_dir : data_dirs) {
        bool need_pick_tablet = true;
        // We need to reserve at least one Slot for cumulative compaction.
        // So when there is only one Slot, we have to judge whether there is a cumulative compaction
        // in the current submitted tasks.
        // If so, the last Slot can be assigned to Base compaction,
        // otherwise, this Slot needs to be reserved for cumulative compaction.
        int count = copied_cumu_map[data_dir].size() + copied_base_map[data_dir].size();
        if (count >= config::compaction_task_num_per_disk) {
            // Return if no available slot
            need_pick_tablet = false;
            if (!check_score) {
                continue;
            }
        } else if (count >= config::compaction_task_num_per_disk - 1) {
            // Only one slot left, check if it can be assigned to base compaction task.
            if (compaction_type == CompactionType::BASE_COMPACTION) {
                if (copied_cumu_map[data_dir].empty()) {
                    need_pick_tablet = false;
                    if (!check_score) {
                        continue;
                    }
                }
            }
        }

        // Even if need_pick_tablet is false, we still need to call find_best_tablet_to_compaction(),
        // So that we can update the max_compaction_score metric.
        if (!data_dir->reach_capacity_limit(0)) {
            uint32_t disk_max_score = 0;
            TabletSharedPtr tablet = _tablet_manager->find_best_tablet_to_compaction(
                    compaction_type, data_dir,
                    compaction_type == CompactionType::CUMULATIVE_COMPACTION ?
                        copied_cumu_map[data_dir] : copied_base_map[data_dir],
                    &disk_max_score, _cumulative_compaction_policy);
            if (tablet != nullptr) {
                if (need_pick_tablet) {
                    tablets_compaction.emplace_back(tablet);
                }
                max_compaction_score = std::max(max_compaction_score, disk_max_score);
            }
        }
    }

    if (max_compaction_score > 0) {
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            DorisMetrics::instance()->tablet_base_max_compaction_score->set_value(max_compaction_score);
        } else {
            DorisMetrics::instance()->tablet_cumulative_max_compaction_score->set_value(max_compaction_score);
        }
    }
    return tablets_compaction;
}

void StorageEngine::_push_tablet_into_submitted_compaction(TabletSharedPtr tablet, CompactionType compaction_type) {
    std::unique_lock<std::mutex> lock(_tablet_submitted_compaction_mutex);
    switch (compaction_type) {
        case CompactionType::CUMULATIVE_COMPACTION:
            _tablet_submitted_cumu_compaction[tablet->data_dir()].insert(tablet->tablet_id());
            break;
        default:
            _tablet_submitted_base_compaction[tablet->data_dir()].insert(tablet->tablet_id());
            break;
    }
}

void StorageEngine::_pop_tablet_from_submitted_compaction(TabletSharedPtr tablet, CompactionType compaction_type) {
    std::unique_lock<std::mutex> lock(_tablet_submitted_compaction_mutex);
    int removed = 0;
    switch (compaction_type) {
        case CompactionType::CUMULATIVE_COMPACTION:
            removed = _tablet_submitted_cumu_compaction[tablet->data_dir()].erase(tablet->tablet_id());
            break;
        default:
            removed = _tablet_submitted_base_compaction[tablet->data_dir()].erase(tablet->tablet_id());
            break;
    }

    if (removed == 1) {
        std::unique_lock<std::mutex> lock(_compaction_producer_sleep_mutex);
        _wakeup_producer_flag = 1;
        _compaction_producer_sleep_cv.notify_one();
    }
}

} // namespace doris
