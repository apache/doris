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

#pragma once

#include <gen_cpp/BackendService_types.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>
#include <unordered_set>

#include "common/status.h"
#include "service/backend_options.h"
#include "util/hash_util.hpp"

namespace doris {

class MemTrackerLimiter;
class RuntimeProfile;
class ThreadPool;
class ExecEnv;
class CgroupCpuCtl;
class QueryContext;
class IOThrottle;

namespace vectorized {
class SimplifiedScanScheduler;
}

namespace pipeline {
class PipelineTask;
class TaskScheduler;
} // namespace pipeline

class WorkloadGroup;
struct WorkloadGroupInfo;
struct TrackerLimiterGroup;
class WorkloadGroupMetrics;

class WorkloadGroup : public std::enable_shared_from_this<WorkloadGroup> {
public:
    explicit WorkloadGroup(const WorkloadGroupInfo& tg_info);

    explicit WorkloadGroup(const WorkloadGroupInfo& tg_info, bool need_create_query_thread_pool);

    int64_t version() const { return _version; }

    uint64_t cpu_share() const { return _cpu_share.load(); }

    int cpu_hard_limit() const { return _cpu_hard_limit.load(); }

    uint64_t id() const { return _id; }

    std::string name() const { return _name; };

    bool enable_memory_overcommit() const {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _enable_memory_overcommit;
    };

    int64_t memory_limit() const {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _memory_limit;
    };

    int64_t weighted_memory_limit() const { return _weighted_memory_limit; };

    void set_weighted_memory_limit(int64_t weighted_memory_limit) {
        _weighted_memory_limit = weighted_memory_limit;
    }

    // make memory snapshots and refresh total memory used at the same time.
    int64_t make_memory_tracker_snapshots(
            std::list<std::shared_ptr<MemTrackerLimiter>>* tracker_snapshots);
    // call make_memory_tracker_snapshots, so also refresh total memory used.
    int64_t memory_used();
    void refresh_memory(int64_t used_memory);

    int spill_threshold_low_water_mark() const {
        return _spill_low_watermark.load(std::memory_order_relaxed);
    }
    int spill_threashold_high_water_mark() const {
        return _spill_high_watermark.load(std::memory_order_relaxed);
    }

    void set_weighted_memory_ratio(double ratio);
    bool add_wg_refresh_interval_memory_growth(int64_t size) {
        auto realtime_total_mem_used =
                _total_mem_used + _wg_refresh_interval_memory_growth.load() + size;
        if ((realtime_total_mem_used >
             ((double)_weighted_memory_limit *
              _spill_high_watermark.load(std::memory_order_relaxed) / 100))) {
            return false;
        } else {
            _wg_refresh_interval_memory_growth.fetch_add(size);
            return true;
        }
    }
    void sub_wg_refresh_interval_memory_growth(int64_t size) {
        _wg_refresh_interval_memory_growth.fetch_sub(size);
    }

    void check_mem_used(bool* is_low_wartermark, bool* is_high_wartermark) const {
        auto realtime_total_mem_used = _total_mem_used + _wg_refresh_interval_memory_growth.load();
        *is_low_wartermark = (realtime_total_mem_used >
                              ((double)_weighted_memory_limit *
                               _spill_low_watermark.load(std::memory_order_relaxed) / 100));
        *is_high_wartermark = (realtime_total_mem_used >
                               ((double)_weighted_memory_limit *
                                _spill_high_watermark.load(std::memory_order_relaxed) / 100));
    }

    std::string debug_string() const;
    std::string memory_debug_string() const;

    void check_and_update(const WorkloadGroupInfo& tg_info);

    void add_mem_tracker_limiter(std::shared_ptr<MemTrackerLimiter> mem_tracker_ptr);

    void remove_mem_tracker_limiter(std::shared_ptr<MemTrackerLimiter> mem_tracker_ptr);

    // when mem_limit <=0 , it's an invalid value, then current group not participating in memory GC
    // because mem_limit is not a required property
    bool is_mem_limit_valid() {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _memory_limit > 0;
    }

    Status add_query(TUniqueId query_id, std::shared_ptr<QueryContext> query_ctx) {
        std::unique_lock<std::shared_mutex> wlock(_mutex);
        if (_is_shutdown) {
            // If the workload group is set shutdown, then should not run any more,
            // because the scheduler pool and other pointer may be released.
            return Status::InternalError(
                    "Failed add query to wg {}, the workload group is shutdown. host: {}", _id,
                    BackendOptions::get_localhost());
        }
        _query_ctxs.insert({query_id, query_ctx});
        return Status::OK();
    }

    void remove_query(TUniqueId query_id) {
        std::unique_lock<std::shared_mutex> wlock(_mutex);
        _query_ctxs.erase(query_id);
    }

    void shutdown() {
        std::unique_lock<std::shared_mutex> wlock(_mutex);
        _is_shutdown = true;
    }

    bool can_be_dropped() {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _is_shutdown && _query_ctxs.empty();
    }

    int query_num() {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _query_ctxs.size();
    }

    int64_t gc_memory(int64_t need_free_mem, RuntimeProfile* profile, bool is_minor_gc);

    void upsert_task_scheduler(WorkloadGroupInfo* tg_info, ExecEnv* exec_env);

    void get_query_scheduler(doris::pipeline::TaskScheduler** exec_sched,
                             vectorized::SimplifiedScanScheduler** scan_sched,
                             ThreadPool** non_pipe_thread_pool,
                             vectorized::SimplifiedScanScheduler** remote_scan_sched);

    void try_stop_schedulers();

    std::unordered_map<TUniqueId, std::weak_ptr<QueryContext>> queries() {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _query_ctxs;
    }

    std::string thread_debug_info();

    std::shared_ptr<IOThrottle> get_local_scan_io_throttle(const std::string& disk_dir);

    std::shared_ptr<IOThrottle> get_remote_scan_io_throttle();

    void upsert_scan_io_throttle(WorkloadGroupInfo* tg_info);

    void update_cpu_time(int64_t delta_cpu_time);

    void update_local_scan_io(std::string path, size_t scan_bytes);

    void update_remote_scan_io(size_t scan_bytes);

    int64_t get_mem_used();

    void create_cgroup_cpu_ctl();

    std::weak_ptr<CgroupCpuCtl> get_cgroup_cpu_ctl_wptr();

    ThreadPool* get_memtable_flush_pool_ptr() {
        // no lock here because this is called by memtable flush,
        // to avoid lock competition with the workload thread pool's update
        return _memtable_flush_pool.get();
    }

    std::shared_ptr<WorkloadGroupMetrics> get_metrics() { return _wg_metrics; }

    friend class WorkloadGroupMetrics;

private:
    void create_cgroup_cpu_ctl_no_lock();
    void upsert_cgroup_cpu_ctl_no_lock(WorkloadGroupInfo* wg_info);
    void upsert_thread_pool_no_lock(WorkloadGroupInfo* wg_info,
                                    std::shared_ptr<CgroupCpuCtl> cg_cpu_ctl_ptr,
                                    ExecEnv* exec_env);

    mutable std::shared_mutex _mutex; // lock _name, _version, _cpu_share, _memory_limit
    const uint64_t _id;
    std::string _name;
    int64_t _version;
    int64_t _memory_limit; // bytes
    // `weighted_memory_limit` less than or equal to _memory_limit, calculate after exclude public memory.
    // more detailed description in `refresh_wg_weighted_memory_limit`.
    std::atomic<int64_t> _weighted_memory_limit {0}; //
    // last value of make_memory_tracker_snapshots, refresh every time make_memory_tracker_snapshots is called.
    std::atomic_int64_t _total_mem_used = 0; // bytes
    std::atomic_int64_t _wg_refresh_interval_memory_growth;
    bool _enable_memory_overcommit;
    std::atomic<uint64_t> _cpu_share;
    std::vector<TrackerLimiterGroup> _mem_tracker_limiter_pool;
    std::atomic<int> _cpu_hard_limit;
    std::atomic<int> _scan_thread_num;
    std::atomic<int> _max_remote_scan_thread_num;
    std::atomic<int> _min_remote_scan_thread_num;
    std::atomic<int> _spill_low_watermark;
    std::atomic<int> _spill_high_watermark;
    std::atomic<int64_t> _scan_bytes_per_second {-1};
    std::atomic<int64_t> _remote_scan_bytes_per_second {-1};

    // means workload group is mark dropped
    // new query can not submit
    // waiting running query to be cancelled or finish
    bool _is_shutdown = false;
    std::unordered_map<TUniqueId, std::weak_ptr<QueryContext>> _query_ctxs;

    std::shared_mutex _task_sched_lock;
    // _cgroup_cpu_ctl not only used by threadpool which managed by WorkloadGroup,
    // but also some global background threadpool which not owned by WorkloadGroup,
    // so it should be shared ptr;
    std::shared_ptr<CgroupCpuCtl> _cgroup_cpu_ctl {nullptr};
    std::unique_ptr<doris::pipeline::TaskScheduler> _task_sched {nullptr};
    std::unique_ptr<vectorized::SimplifiedScanScheduler> _scan_task_sched {nullptr};
    std::unique_ptr<vectorized::SimplifiedScanScheduler> _remote_scan_task_sched {nullptr};
    std::unique_ptr<ThreadPool> _memtable_flush_pool {nullptr};

    std::map<std::string, std::shared_ptr<IOThrottle>> _scan_io_throttle_map;
    std::shared_ptr<IOThrottle> _remote_scan_io_throttle {nullptr};

    // for some background workload, it doesn't need to create query thread pool
    const bool _need_create_query_thread_pool;

    std::shared_ptr<WorkloadGroupMetrics> _wg_metrics {nullptr};
};

using WorkloadGroupPtr = std::shared_ptr<WorkloadGroup>;

struct WorkloadGroupInfo {
    uint64_t id;
    std::string name;
    uint64_t cpu_share;
    int64_t memory_limit;
    bool enable_memory_overcommit;
    int64_t version;
    int cpu_hard_limit;
    bool enable_cpu_hard_limit;
    int scan_thread_num;
    int max_remote_scan_thread_num;
    int min_remote_scan_thread_num;
    int spill_low_watermark;
    int spill_high_watermark;
    int read_bytes_per_second;
    int remote_read_bytes_per_second;
    // log cgroup cpu info
    uint64_t cgroup_cpu_shares = 0;
    int cgroup_cpu_hard_limit = 0;

    static Status parse_topic_info(const TWorkloadGroupInfo& tworkload_group_info,
                                   WorkloadGroupInfo* workload_group_info);
};

} // namespace doris
