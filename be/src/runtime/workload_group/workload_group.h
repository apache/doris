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

#include "common/factory_creator.h"
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
class ResourceContext;

namespace vectorized {
class SimplifiedScanScheduler;
}

namespace pipeline {
class TaskScheduler;
} // namespace pipeline

class WorkloadGroup;
struct WorkloadGroupInfo;
struct TrackerLimiterGroup;
class WorkloadGroupMetrics;

class WorkloadGroup : public std::enable_shared_from_this<WorkloadGroup> {
    ENABLE_FACTORY_CREATOR(WorkloadGroup);

public:
    explicit WorkloadGroup(const WorkloadGroupInfo& tg_info);

    explicit WorkloadGroup(const WorkloadGroupInfo& tg_info, bool need_create_query_thread_pool);

    virtual ~WorkloadGroup();

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
    }

    int64_t total_mem_used() const { return _total_mem_used; }

    int64_t write_buffer_size() const { return _write_buffer_size; }

    void enable_write_buffer_limit(bool enable_limit) { _enable_write_buffer_limit = enable_limit; }

    bool enable_write_buffer_limit() const { return _enable_write_buffer_limit; }

    bool exceed_write_buffer_limit() const { return _write_buffer_size > write_buffer_limit(); }

    // make memory snapshots and refresh total memory used at the same time.
    int64_t refresh_memory_usage();
    int64_t memory_used();

    void do_sweep();

    int memory_low_watermark() const {
        return _memory_low_watermark.load(std::memory_order_relaxed);
    }

    int memory_high_watermark() const {
        return _memory_high_watermark.load(std::memory_order_relaxed);
    }

    void set_weighted_memory_ratio(double ratio);

    int total_query_slot_count() const {
        return _total_query_slot_count.load(std::memory_order_relaxed);
    }

    void add_wg_refresh_interval_memory_growth(int64_t size) {
        _wg_refresh_interval_memory_growth.fetch_add(size);
    }

    bool try_add_wg_refresh_interval_memory_growth(int64_t size);

    void sub_wg_refresh_interval_memory_growth(int64_t size) {
        _wg_refresh_interval_memory_growth.fetch_sub(size);
    }

    int64_t wg_refresh_interval_memory_growth() {
        return _wg_refresh_interval_memory_growth.load();
    }

    void check_mem_used(bool* is_low_watermark, bool* is_high_watermark) const {
        auto realtime_total_mem_used = _total_mem_used + _wg_refresh_interval_memory_growth.load();
        *is_low_watermark = (realtime_total_mem_used >
                             ((double)_memory_limit *
                              _memory_low_watermark.load(std::memory_order_relaxed) / 100));
        *is_high_watermark = (realtime_total_mem_used >
                              ((double)_memory_limit *
                               _memory_high_watermark.load(std::memory_order_relaxed) / 100));
    }

    std::string debug_string() const;
    std::string memory_debug_string() const;

    void check_and_update(const WorkloadGroupInfo& tg_info);

    // when mem_limit <=0 , it's an invalid value, then current group not participating in memory GC
    // because mem_limit is not a required property
    bool is_mem_limit_valid() {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _memory_limit > 0;
    }

    TWgSlotMemoryPolicy::type slot_memory_policy() const { return _slot_mem_policy; }

    bool exceed_limit() {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _memory_limit > 0 ? _total_mem_used > _memory_limit : false;
    }

    Status add_resource_ctx(TUniqueId query_id, std::shared_ptr<ResourceContext> resource_ctx) {
        std::unique_lock<std::shared_mutex> wlock(_mutex);
        if (_is_shutdown) {
            // If the workload group is set shutdown, then should not run any more,
            // because the scheduler pool and other pointer may be released.
            return Status::InternalError(
                    "Failed add task to wg {}, the workload group is shutdown. host: {}", _id,
                    BackendOptions::get_localhost());
        }
        _resource_ctxs.insert({query_id, resource_ctx});
        return Status::OK();
    }

    void shutdown() {
        std::unique_lock<std::shared_mutex> wlock(_mutex);
        _is_shutdown = true;
    }

    bool can_be_dropped() {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _is_shutdown && _resource_ctxs.empty();
    }

    std::unordered_map<TUniqueId, std::weak_ptr<ResourceContext>> resource_ctxs() {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _resource_ctxs;
    }

    void upsert_task_scheduler(WorkloadGroupInfo* tg_info);

    virtual void get_query_scheduler(doris::pipeline::TaskScheduler** exec_sched,
                                     vectorized::SimplifiedScanScheduler** scan_sched,
                                     vectorized::SimplifiedScanScheduler** remote_scan_sched);

    void try_stop_schedulers();

    std::string thread_debug_info();

    std::shared_ptr<IOThrottle> get_local_scan_io_throttle(const std::string& disk_dir);

    std::shared_ptr<IOThrottle> get_remote_scan_io_throttle();

    void upsert_scan_io_throttle(WorkloadGroupInfo* tg_info);

    void update_cpu_time(int64_t delta_cpu_time);

    void update_local_scan_io(std::string path, size_t scan_bytes);

    void update_remote_scan_io(size_t scan_bytes);

    int64_t get_mem_used();

    virtual ThreadPool* get_memtable_flush_pool() {
        // no lock here because this is called by memtable flush,
        // to avoid lock competition with the workload thread pool's update
        return _memtable_flush_pool.get();
    }
    void create_cgroup_cpu_ctl();

    std::weak_ptr<CgroupCpuCtl> get_cgroup_cpu_ctl_wptr();

    std::shared_ptr<WorkloadGroupMetrics> get_metrics() { return _wg_metrics; }

    friend class WorkloadGroupMetrics;

    int64_t write_buffer_limit() const { return _memory_limit * _load_buffer_ratio / 100; }

    int64_t revoke_memory(int64_t need_free_mem, const std::string& revoke_reason,
                          RuntimeProfile* profile);

    friend class DummyWorkloadGroupTest;

    vectorized::SimplifiedScanScheduler* get_remote_scan_task_scheduler() {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _remote_scan_task_sched.get();
    }

private:
    void create_cgroup_cpu_ctl_no_lock();
    void upsert_cgroup_cpu_ctl_no_lock(WorkloadGroupInfo* wg_info);
    void upsert_thread_pool_no_lock(WorkloadGroupInfo* wg_info,
                                    std::shared_ptr<CgroupCpuCtl> cg_cpu_ctl_ptr);

    std::string _memory_debug_string() const;

    mutable std::shared_mutex _mutex; // lock _name, _version, _cpu_share, _memory_limit
    const uint64_t _id;
    std::string _name;
    int64_t _version;
    int64_t _memory_limit; // bytes
    // For example, load memtable, write to parquet.
    // If the wg's memory reached high water mark, then the load buffer
    // will be restricted to this limit.
    int64_t _load_buffer_ratio = 0;
    std::atomic<bool> _enable_write_buffer_limit = false;

    std::atomic_int64_t _total_mem_used = 0; // bytes
    std::atomic_int64_t _write_buffer_size = 0;
    std::atomic_int64_t _wg_refresh_interval_memory_growth;
    bool _enable_memory_overcommit;
    std::atomic<uint64_t> _cpu_share;
    std::atomic<int> _cpu_hard_limit;
    std::atomic<int> _scan_thread_num;
    std::atomic<int> _max_remote_scan_thread_num;
    std::atomic<int> _min_remote_scan_thread_num;
    std::atomic<int> _memory_low_watermark;
    std::atomic<int> _memory_high_watermark;
    std::atomic<int64_t> _scan_bytes_per_second {-1};
    std::atomic<int64_t> _remote_scan_bytes_per_second {-1};
    std::atomic<int> _total_query_slot_count = 0;
    std::atomic<TWgSlotMemoryPolicy::type> _slot_mem_policy {TWgSlotMemoryPolicy::NONE};

    // means workload group is mark dropped
    // new query can not submit
    // waiting running query to be cancelled or finish
    bool _is_shutdown = false;
    std::unordered_map<TUniqueId, std::weak_ptr<ResourceContext>> _resource_ctxs;

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
    const uint64_t id = 0;
    const std::string name = "";
    const uint64_t cpu_share = 0;
    const int64_t memory_limit = 0;
    const bool enable_memory_overcommit = false;
    const int64_t version = 0;
    const int cpu_hard_limit = 0;
    const bool enable_cpu_hard_limit = false;
    const int scan_thread_num = 0;
    const int max_remote_scan_thread_num = 0;
    const int min_remote_scan_thread_num = 0;
    const int memory_low_watermark = 0;
    const int memory_high_watermark = 0;
    const int read_bytes_per_second = -1;
    const int remote_read_bytes_per_second = -1;
    const int total_query_slot_count = 0;
    const TWgSlotMemoryPolicy::type slot_mem_policy = TWgSlotMemoryPolicy::NONE;
    const int write_buffer_ratio = 0;
    // log cgroup cpu info
    uint64_t cgroup_cpu_shares = 0;
    int cgroup_cpu_hard_limit = 0;
    const bool valid = true;

    static WorkloadGroupInfo parse_topic_info(const TWorkloadGroupInfo& tworkload_group_info);
};

} // namespace doris
