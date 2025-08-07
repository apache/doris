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

#include "workload_group.h"

#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>

#include <charconv>
#include <map>
#include <mutex>
#include <ostream>
#include <utility>

#include "common/logging.h"
#include "exec/schema_scanner/schema_scanner_helper.h"
#include "io/fs/local_file_reader.h"
#include "olap/storage_engine.h"
#include "pipeline/task_queue.h"
#include "pipeline/task_scheduler.h"
#include "runtime/exec_env.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/memory_reclamation.h"
#include "runtime/workload_group/workload_group_metrics.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/mem_info.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "util/threadpool.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {

#include "common/compile_check_begin.h"

const static int MAX_MEMORY_PERCENT_DEFAULT_VALUE = 100;
const static int MAX_CPU_PERCENT_DEFAULT_VALUE = 100;

const static int MEMORY_LOW_WATERMARK_DEFAULT_VALUE = 80;
const static int MEMORY_HIGH_WATERMARK_DEFAULT_VALUE = 95;
// This is a invalid value, and should ignore this value during usage
const static int TOTAL_QUERY_SLOT_COUNT_DEFAULT_VALUE = 0;
const static int LOAD_BUFFER_RATIO_DEFAULT_VALUE = 20;

WorkloadGroup::WorkloadGroup(const WorkloadGroupInfo& wg_info)
        : _id(wg_info.id),
          _name(wg_info.name),
          _version(wg_info.version),
          _min_cpu_percent(wg_info.min_cpu_percent),
          _max_cpu_percent(wg_info.max_cpu_percent),
          _memory_limit(wg_info.memory_limit),
          _min_memory_percent(wg_info.min_memory_percent),
          _max_memory_percent(wg_info.max_memory_percent),
          _memory_low_watermark(wg_info.memory_low_watermark),
          _memory_high_watermark(wg_info.memory_high_watermark),
          _load_buffer_ratio(wg_info.write_buffer_ratio),
          _scan_thread_num(wg_info.scan_thread_num),
          _max_remote_scan_thread_num(wg_info.max_remote_scan_thread_num),
          _min_remote_scan_thread_num(wg_info.min_remote_scan_thread_num),
          _scan_bytes_per_second(wg_info.read_bytes_per_second),
          _remote_scan_bytes_per_second(wg_info.remote_read_bytes_per_second),
          _total_query_slot_count(wg_info.total_query_slot_count),
          _slot_mem_policy(wg_info.slot_mem_policy) {
    std::vector<DataDirInfo>& data_dir_list = io::BeConfDataDirReader::be_config_data_dir_list;
    for (const auto& data_dir : data_dir_list) {
        _scan_io_throttle_map[data_dir.path] = std::make_shared<IOThrottle>(data_dir.metric_name);
    }
    _remote_scan_io_throttle = std::make_shared<IOThrottle>();

    _wg_metrics = std::make_shared<WorkloadGroupMetrics>(this);
}

WorkloadGroup::~WorkloadGroup() = default;

std::string WorkloadGroup::debug_string() const {
    std::shared_lock<std::shared_mutex> rl {_mutex};
    return fmt::format(
            "WorkloadGroup[id = {}, name = {}, version = {}, "
            "min_cpu_percent = {}, max_cpu_percent = {}, "
            "{}"
            "scan_thread_num = {}, max_remote_scan_thread_num = {}, "
            "min_remote_scan_thread_num = {}, "
            "is_shutdown={}, query_num={}, "
            "read_bytes_per_second={}, remote_read_bytes_per_second={}]",
            _id, _name, _version, _min_cpu_percent, _max_cpu_percent, _memory_debug_string(),
            _scan_thread_num, _max_remote_scan_thread_num, _min_remote_scan_thread_num,
            _is_shutdown, _resource_ctxs.size(), _scan_bytes_per_second,
            _remote_scan_bytes_per_second);
}

bool WorkloadGroup::try_add_wg_refresh_interval_memory_growth(int64_t size) {
    auto realtime_total_mem_used =
            _total_mem_used + _wg_refresh_interval_memory_growth.load() + size;
    if (((double)realtime_total_mem_used >
         ((double)_memory_limit * _memory_high_watermark.load(std::memory_order_relaxed) / 100))) {
        // If a group is enable memory overcommit, then not need check the limit
        // It is always true, and it will only fail when process memory is not
        // enough.
        return false;
    } else {
        _wg_refresh_interval_memory_growth.fetch_add(size);
        return true;
    }
}

std::string WorkloadGroup::_memory_debug_string() const {
    auto realtime_total_mem_used = _total_mem_used + _wg_refresh_interval_memory_growth.load();
    auto mem_used_ratio = (double)realtime_total_mem_used / ((double)_memory_limit + 1);
    auto mem_used_ratio_int = (int64_t)(mem_used_ratio * 100 + 0.5);
    mem_used_ratio = (double)mem_used_ratio_int / 100;
    return fmt::format(
            "min_memory_percent = {}% , max_memory_percent = {}% , memory_limit = {}B, "
            "slot_memory_policy = {}, total_query_slot_count = {}, "
            "memory_low_watermark = {}, memory_high_watermark = {}, "
            "enable_write_buffer_limit = {}, write_buffer_ratio = {}% , " // add a blackspace after % to avoid log4j format bugs
            "write_buffer_limit = {}, "
            "mem_used_ratio = {}, total_mem_used = {}(write_buffer_size = {}, "
            "wg_refresh_interval_memory_growth = {}",
            _min_memory_percent, _max_memory_percent,
            PrettyPrinter::print(_memory_limit, TUnit::BYTES), to_string(_slot_mem_policy),
            _total_query_slot_count, _memory_low_watermark, _memory_high_watermark,
            _enable_write_buffer_limit, _load_buffer_ratio,
            PrettyPrinter::print(write_buffer_limit(), TUnit::BYTES), mem_used_ratio,
            PrettyPrinter::print(_total_mem_used.load(), TUnit::BYTES),
            PrettyPrinter::print(_write_buffer_size.load(), TUnit::BYTES),
            PrettyPrinter::print(_wg_refresh_interval_memory_growth.load(), TUnit::BYTES));
}

std::string WorkloadGroup::memory_debug_string() const {
    return fmt::format(
            "WorkloadGroup[id = {}, name = {}, version = {}, {}, "
            "is_shutdown={}, query_num={}]",
            _id, _name, _version, _memory_debug_string(), _is_shutdown, _resource_ctxs.size());
}

void WorkloadGroup::check_and_update(const WorkloadGroupInfo& wg_info) {
    if (UNLIKELY(wg_info.id != _id)) {
        return;
    }
    {
        std::shared_lock<std::shared_mutex> rl {_mutex};
        if (LIKELY(wg_info.version <= _version)) {
            return;
        }
    }
    {
        std::lock_guard<std::shared_mutex> wl {_mutex};
        if (wg_info.version > _version) {
            _name = wg_info.name;
            _version = wg_info.version;
            _min_cpu_percent = wg_info.min_cpu_percent;
            _max_cpu_percent = wg_info.max_cpu_percent;
            _memory_limit = wg_info.memory_limit;
            _min_memory_percent = wg_info.min_memory_percent;
            _max_memory_percent = wg_info.max_memory_percent;
            _scan_thread_num = wg_info.scan_thread_num;
            _max_remote_scan_thread_num = wg_info.max_remote_scan_thread_num;
            _min_remote_scan_thread_num = wg_info.min_remote_scan_thread_num;
            _memory_low_watermark = wg_info.memory_low_watermark;
            _memory_high_watermark = wg_info.memory_high_watermark;
            _scan_bytes_per_second = wg_info.read_bytes_per_second;
            _remote_scan_bytes_per_second = wg_info.remote_read_bytes_per_second;
            _total_query_slot_count = wg_info.total_query_slot_count;
            _load_buffer_ratio = wg_info.write_buffer_ratio;
            _slot_mem_policy = wg_info.slot_mem_policy;
        } else {
            return;
        }
    }
}

// MemtrackerLimiter is not removed during query context release, so that should remove it here.
int64_t WorkloadGroup::refresh_memory_usage() {
    int64_t fragment_used_memory = 0;
    int64_t write_buffer_size = 0;
    {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        for (const auto& pair : _resource_ctxs) {
            auto resource_ctx = pair.second.lock();
            if (!resource_ctx) {
                continue;
            }
            DCHECK(resource_ctx->memory_context()->mem_tracker() != nullptr);
            fragment_used_memory += resource_ctx->memory_context()->current_memory_bytes();
            write_buffer_size += resource_ctx->memory_context()->mem_tracker()->write_buffer_size();
        }
    }

    _total_mem_used = fragment_used_memory + write_buffer_size;
    _wg_metrics->update_memory_used_bytes(_total_mem_used);
    _write_buffer_size = write_buffer_size;
    // reserve memory is recorded in the query mem tracker
    // and _total_mem_used already contains all the current reserve memory.
    // so after refreshing _total_mem_used, reset _wg_refresh_interval_memory_growth.
    _wg_refresh_interval_memory_growth.store(0.0);
    return _total_mem_used;
}

int64_t WorkloadGroup::memory_used() {
    return refresh_memory_usage();
}

void WorkloadGroup::do_sweep() {
    // Clear resource context that is registered during add_resource_ctx
    std::unique_lock<std::shared_mutex> wlock(_mutex);
    for (auto iter = _resource_ctxs.begin(); iter != _resource_ctxs.end();) {
        if (iter->second.lock() == nullptr) {
            iter = _resource_ctxs.erase(iter);
        } else {
            iter++;
        }
    }
}

int64_t WorkloadGroup::revoke_memory(int64_t need_free_mem, const std::string& revoke_reason,
                                     RuntimeProfile* profile) {
    if (need_free_mem <= 0) {
        return 0;
    }
    int64_t used_memory = memory_used();
    int64_t freed_mem = 0;
    MonotonicStopWatch watch;
    watch.start();
    RuntimeProfile* group_revoke_profile =
            profile->create_child(fmt::format("RevokeGroupMemory:id {}", _id), true, true);

    std::vector<std::shared_ptr<ResourceContext>> resource_ctxs;
    {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        for (const auto& pair : _resource_ctxs) {
            auto resource_ctx = pair.second.lock();
            if (resource_ctx) {
                resource_ctxs.push_back(resource_ctx);
            }
        }
    }

    auto group_revoke_reason = fmt::format(
            "{}, revoke group id:{}, name:{}, used:{}, limit:{}", revoke_reason, _id, _name,
            PrettyPrinter::print_bytes(used_memory), PrettyPrinter::print_bytes(_memory_limit));
    LOG(INFO) << fmt::format(
            "[MemoryGC] start WorkloadGroup::revoke_memory, {}, need free size: {}.",
            group_revoke_reason, PrettyPrinter::print_bytes(need_free_mem));
    Defer defer {[&]() {
        std::stringstream ss;
        group_revoke_profile->pretty_print(&ss);
        LOG(INFO) << fmt::format(
                "[MemoryGC] end WorkloadGroup::revoke_memory, {}, need free size: {}, free Memory "
                "{}. cost(us): {}, details: {}",
                group_revoke_reason, PrettyPrinter::print_bytes(need_free_mem),
                PrettyPrinter::print_bytes(freed_mem), watch.elapsed_time() / 1000, ss.str());
    }};

    // step 1. free top overcommit query
    RuntimeProfile* free_top_profile = group_revoke_profile->create_child(
            fmt::format("FreeGroupTopOvercommitQuery:Name {}", _name), true, true);
    freed_mem += MemoryReclamation::revoke_tasks_memory(
            need_free_mem - freed_mem, resource_ctxs, group_revoke_reason, free_top_profile,
            MemoryReclamation::PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY,
            {MemoryReclamation::FilterFunc::EXCLUDE_IS_SMALL,
             MemoryReclamation::FilterFunc::IS_QUERY},
            MemoryReclamation::ActionFunc::CANCEL);
    if (freed_mem >= need_free_mem) {
        return freed_mem;
    }

    // step 2. free top usage query
    free_top_profile = group_revoke_profile->create_child(
            fmt::format("FreeGroupTopUsageQuery:Name {}", _name), true, true);
    freed_mem += MemoryReclamation::revoke_tasks_memory(
            need_free_mem - freed_mem, resource_ctxs, group_revoke_reason, free_top_profile,
            MemoryReclamation::PriorityCmpFunc::TOP_MEMORY,
            {MemoryReclamation::FilterFunc::EXCLUDE_IS_SMALL,
             MemoryReclamation::FilterFunc::EXCLUDE_IS_OVERCOMMITED,
             MemoryReclamation::FilterFunc::IS_QUERY},
            MemoryReclamation::ActionFunc::CANCEL); // skip overcommited query, cancelled in step 1.
    if (freed_mem >= need_free_mem) {
        return freed_mem;
    }

    // step 3. free top overcommit load
    free_top_profile = group_revoke_profile->create_child(
            fmt::format("FreeGroupTopOvercommitLoad:Name {}", _name), true, true);
    freed_mem += MemoryReclamation::revoke_tasks_memory(
            need_free_mem - freed_mem, resource_ctxs, group_revoke_reason, free_top_profile,
            MemoryReclamation::PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY,
            {MemoryReclamation::FilterFunc::EXCLUDE_IS_SMALL,
             MemoryReclamation::FilterFunc::IS_LOAD},
            MemoryReclamation::ActionFunc::CANCEL);
    if (freed_mem >= need_free_mem) {
        return freed_mem;
    }

    // step 4. free top usage load
    free_top_profile = group_revoke_profile->create_child(
            fmt::format("FreeGroupTopUsageLoad:Name {}", _name), true, true);
    freed_mem += MemoryReclamation::revoke_tasks_memory(
            need_free_mem - freed_mem, resource_ctxs, group_revoke_reason, free_top_profile,
            MemoryReclamation::PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY,
            {MemoryReclamation::FilterFunc::EXCLUDE_IS_SMALL,
             MemoryReclamation::FilterFunc::EXCLUDE_IS_OVERCOMMITED,
             MemoryReclamation::FilterFunc::IS_LOAD},
            MemoryReclamation::ActionFunc::CANCEL);
    return freed_mem;
}

WorkloadGroupInfo WorkloadGroupInfo::parse_topic_info(
        const TWorkloadGroupInfo& tworkload_group_info) {
    // 1 id
    uint64_t wg_id = 0;
    if (tworkload_group_info.__isset.id) {
        wg_id = tworkload_group_info.id;
    } else {
        return {.name = "", .valid = false};
    }

    // 2 name
    std::string name = "INVALID_NAME";
    if (tworkload_group_info.__isset.name) {
        name = tworkload_group_info.name;
    }

    // 3 version
    int64_t version = 0;
    if (tworkload_group_info.__isset.version) {
        version = tworkload_group_info.version;
    } else {
        return {.name {}, .valid = false};
    }

    // 4 min cpu percent
    int min_cpu_percent = 0;
    if (tworkload_group_info.__isset.min_cpu_percent && tworkload_group_info.min_cpu_percent >= 0) {
        min_cpu_percent = tworkload_group_info.min_cpu_percent;
    }

    // 5 max cpu percent
    int max_cpu_percent = MAX_CPU_PERCENT_DEFAULT_VALUE;
    if (tworkload_group_info.__isset.max_cpu_percent && tworkload_group_info.max_cpu_percent >= 0) {
        max_cpu_percent = tworkload_group_info.max_cpu_percent;
    }

    // 6 max memory percent is the mem_limit of the workload group
    int max_memory_percent = MAX_MEMORY_PERCENT_DEFAULT_VALUE;
    if (tworkload_group_info.__isset.max_memory_percent &&
        tworkload_group_info.max_memory_percent >= 0) {
        max_memory_percent = tworkload_group_info.max_memory_percent;
    }
    std::string mem_limit_str = fmt::format("{}%", max_memory_percent);
    bool is_percent = true;
    int64_t memory_limit =
            ParseUtil::parse_mem_spec(mem_limit_str, -1, MemInfo::mem_limit(), &is_percent);
    DCHECK(is_percent) << "mem_limit_str: " << mem_limit_str;

    // 7. min memory percent
    int min_memory_percent = 0;
    if (tworkload_group_info.__isset.min_memory_percent &&
        tworkload_group_info.min_memory_percent >= 0) {
        min_memory_percent = tworkload_group_info.min_memory_percent;
    }

    // 9 scan thread num
    int scan_thread_num = config::doris_scanner_thread_pool_thread_num;
    if (tworkload_group_info.__isset.scan_thread_num && tworkload_group_info.scan_thread_num > 0) {
        scan_thread_num = tworkload_group_info.scan_thread_num;
    }

    // 10 max remote scan thread num
    int max_remote_scan_thread_num = vectorized::ScannerScheduler::get_remote_scan_thread_num();
    if (tworkload_group_info.__isset.max_remote_scan_thread_num &&
        tworkload_group_info.max_remote_scan_thread_num > 0) {
        max_remote_scan_thread_num = tworkload_group_info.max_remote_scan_thread_num;
    }

    // 11 min remote scan thread num
    int min_remote_scan_thread_num = config::doris_scanner_min_thread_pool_thread_num;
    if (tworkload_group_info.__isset.min_remote_scan_thread_num &&
        tworkload_group_info.min_remote_scan_thread_num > 0) {
        min_remote_scan_thread_num = tworkload_group_info.min_remote_scan_thread_num;
    }

    // NOTE: currently not support set exec thread num in FE.
    // Hard code here for unified code style
    int exec_thread_num = config::pipeline_executor_size;
    if (exec_thread_num <= 0) {
        exec_thread_num = CpuInfo::num_cores();
    }

    int num_disk = 1;
    int num_cpus = 1;
#ifndef BE_TEST
    num_disk = ExecEnv::GetInstance()->storage_engine().get_disk_num();
    num_cpus = std::thread::hardware_concurrency();
#endif
    num_disk = std::max(1, num_disk);
    int min_flush_thread_num = std::max(1, config::flush_thread_num_per_store);
    int max_flush_thread_num = num_cpus == 0
                                       ? num_disk * min_flush_thread_num
                                       : std::min(num_disk * min_flush_thread_num,
                                                  num_cpus * config::max_flush_thread_num_per_cpu);

    // 12 memory low watermark
    int memory_low_watermark = MEMORY_LOW_WATERMARK_DEFAULT_VALUE;
    if (tworkload_group_info.__isset.memory_low_watermark) {
        memory_low_watermark = tworkload_group_info.memory_low_watermark;
    }

    // 13 memory high watermark
    int memory_high_watermark = MEMORY_HIGH_WATERMARK_DEFAULT_VALUE;
    if (tworkload_group_info.__isset.memory_high_watermark) {
        memory_high_watermark = tworkload_group_info.memory_high_watermark;
    }

    // 14 scan io
    int64_t read_bytes_per_second = -1;
    if (tworkload_group_info.__isset.read_bytes_per_second &&
        tworkload_group_info.read_bytes_per_second > 0) {
        read_bytes_per_second = tworkload_group_info.read_bytes_per_second;
    }

    // 15 remote scan io
    int64_t remote_read_bytes_per_second = -1;
    if (tworkload_group_info.__isset.remote_read_bytes_per_second &&
        tworkload_group_info.remote_read_bytes_per_second > 0) {
        remote_read_bytes_per_second = tworkload_group_info.remote_read_bytes_per_second;
    }

    // 16 total slots
    int total_query_slot_count = TOTAL_QUERY_SLOT_COUNT_DEFAULT_VALUE;
    if (tworkload_group_info.__isset.total_query_slot_count) {
        total_query_slot_count = tworkload_group_info.total_query_slot_count;
    }

    // 17 load buffer memory limit
    int write_buffer_ratio = LOAD_BUFFER_RATIO_DEFAULT_VALUE;
    if (tworkload_group_info.__isset.write_buffer_ratio) {
        write_buffer_ratio = tworkload_group_info.write_buffer_ratio;
    }

    // 18 slot memory policy
    TWgSlotMemoryPolicy::type slot_mem_policy = TWgSlotMemoryPolicy::NONE;
    if (tworkload_group_info.__isset.slot_memory_policy) {
        slot_mem_policy = tworkload_group_info.slot_memory_policy;
    }

    return {.id = wg_id,
            .name = name,
            .version = version,
            .min_cpu_percent = min_cpu_percent,
            .max_cpu_percent = max_cpu_percent,
            .memory_limit = memory_limit,
            .min_memory_percent = min_memory_percent,
            .max_memory_percent = max_memory_percent,
            .memory_low_watermark = memory_low_watermark,
            .memory_high_watermark = memory_high_watermark,
            .scan_thread_num = scan_thread_num,
            .max_remote_scan_thread_num = max_remote_scan_thread_num,
            .min_remote_scan_thread_num = min_remote_scan_thread_num,
            .read_bytes_per_second = read_bytes_per_second,
            .remote_read_bytes_per_second = remote_read_bytes_per_second,
            .total_query_slot_count = total_query_slot_count,
            .slot_mem_policy = slot_mem_policy,
            .write_buffer_ratio = write_buffer_ratio,
            .pipeline_exec_thread_num = exec_thread_num,
            .max_flush_thread_num = max_flush_thread_num,
            .min_flush_thread_num = min_flush_thread_num};
}

std::weak_ptr<CgroupCpuCtl> WorkloadGroup::get_cgroup_cpu_ctl_wptr() {
    std::shared_lock<std::shared_mutex> rlock(_task_sched_lock);
    return _cgroup_cpu_ctl;
}

void WorkloadGroup::create_cgroup_cpu_ctl() {
    std::lock_guard<std::shared_mutex> wlock(_task_sched_lock);
    create_cgroup_cpu_ctl_no_lock();
}

void WorkloadGroup::create_cgroup_cpu_ctl_no_lock() {
    if (config::doris_cgroup_cpu_path != "" && _cgroup_cpu_ctl == nullptr) {
        std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl = CgroupCpuCtl::create_cgroup_cpu_ctl(_id);
        if (cgroup_cpu_ctl) {
            Status ret = cgroup_cpu_ctl->init();
            if (ret.ok()) {
                _cgroup_cpu_ctl = std::move(cgroup_cpu_ctl);
                LOG(INFO) << "[upsert wg thread pool] cgroup init success, wg_id=" << _id;
            } else {
                LOG(INFO) << "[upsert wg thread pool] cgroup init failed, wg_id=" << _id
                          << ", reason=" << ret.to_string();
            }
        } else {
            LOG(INFO) << "[upsert wg thread pool] create cgroup cpu ctl wg_id=" << _id << " failed";
        }
    }
}

Status WorkloadGroup::upsert_thread_pool_no_lock(WorkloadGroupInfo* wg_info,
                                                 std::shared_ptr<CgroupCpuCtl> cg_cpu_ctl_ptr) {
    Status upsert_ret = Status::OK();
    uint64_t wg_id = wg_info->id;
    std::string wg_name = wg_info->name;
    int pipeline_exec_thread_num = wg_info->pipeline_exec_thread_num;
    int scan_thread_num = wg_info->scan_thread_num;
    int max_remote_scan_thread_num = wg_info->max_remote_scan_thread_num;
    int min_remote_scan_thread_num = wg_info->min_remote_scan_thread_num;
    int max_flush_thread_num = wg_info->max_flush_thread_num;
    int min_flush_thread_num = wg_info->min_flush_thread_num;

    // 1 create thread pool
    if (_task_sched == nullptr) {
        std::unique_ptr<pipeline::TaskScheduler> pipeline_task_scheduler =
                std::make_unique<pipeline::TaskScheduler>(pipeline_exec_thread_num, "p_" + wg_name,
                                                          cg_cpu_ctl_ptr);
        Status ret = pipeline_task_scheduler->start();
        if (ret.ok()) {
            _task_sched = std::move(pipeline_task_scheduler);
        } else {
            upsert_ret = ret;
            LOG(INFO) << "[upsert wg thread pool] task scheduler start failed, gid= " << wg_id;
        }
    }

    if (_scan_task_sched == nullptr) {
        std::unique_ptr<vectorized::SimplifiedScanScheduler> scan_scheduler;
        if (config::enable_task_executor_in_internal_table) {
            scan_scheduler = std::make_unique<vectorized::TaskExecutorSimplifiedScanScheduler>(
                    "ls_" + wg_name, cg_cpu_ctl_ptr, wg_name);
        } else {
            scan_scheduler = std::make_unique<vectorized::ThreadPoolSimplifiedScanScheduler>(
                    "ls_" + wg_name, cg_cpu_ctl_ptr, wg_name);
        }

        Status ret = scan_scheduler->start(scan_thread_num, scan_thread_num,
                                           config::doris_scanner_thread_pool_queue_size);
        if (ret.ok()) {
            _scan_task_sched = std::move(scan_scheduler);
        } else {
            upsert_ret = ret;
            LOG(INFO) << "[upsert wg thread pool] scan scheduler start failed, gid=" << wg_id;
        }
    }

    if (_remote_scan_task_sched == nullptr) {
        int remote_scan_thread_queue_size =
                vectorized::ScannerScheduler::get_remote_scan_thread_queue_size();
        std::unique_ptr<vectorized::SimplifiedScanScheduler> remote_scan_scheduler;
        if (config::enable_task_executor_in_external_table) {
            remote_scan_scheduler =
                    std::make_unique<vectorized::TaskExecutorSimplifiedScanScheduler>(
                            "rs_" + wg_name, cg_cpu_ctl_ptr, wg_name);
        } else {
            remote_scan_scheduler = std::make_unique<vectorized::ThreadPoolSimplifiedScanScheduler>(
                    "rs_" + wg_name, cg_cpu_ctl_ptr, wg_name);
        }
        Status ret =
                remote_scan_scheduler->start(max_remote_scan_thread_num, min_remote_scan_thread_num,
                                             remote_scan_thread_queue_size);
        if (ret.ok()) {
            _remote_scan_task_sched = std::move(remote_scan_scheduler);
        } else {
            upsert_ret = ret;
            LOG(INFO) << "[upsert wg thread pool] remote scan scheduler start failed, gid="
                      << wg_id;
        }
    }

    if (_memtable_flush_pool == nullptr) {
        std::unique_ptr<ThreadPool> thread_pool = nullptr;
        std::string pool_name = "mf_" + wg_name;
        auto ret = ThreadPoolBuilder(pool_name)
                           .set_min_threads(min_flush_thread_num)
                           .set_max_threads(max_flush_thread_num)
                           .set_cgroup_cpu_ctl(cg_cpu_ctl_ptr)
                           .build(&thread_pool);
        if (ret.ok()) {
            _memtable_flush_pool = std::move(thread_pool);
            LOG(INFO) << "[upsert wg thread pool] create " + pool_name + " succ, gid=" << wg_id
                      << ", max thread num=" << max_flush_thread_num
                      << ", min thread num=" << min_flush_thread_num;
        } else {
            upsert_ret = ret;
            LOG(INFO) << "[upsert wg thread pool] create " + pool_name + " failed, gid=" << wg_id;
        }
    }

    // 2 update thread pool
    if (scan_thread_num > 0 && _scan_task_sched) {
        _scan_task_sched->reset_thread_num(scan_thread_num, scan_thread_num);
    }

    if (max_remote_scan_thread_num >= min_remote_scan_thread_num && _remote_scan_task_sched) {
        _remote_scan_task_sched->reset_thread_num(max_remote_scan_thread_num,
                                                  min_remote_scan_thread_num);
    }

    return upsert_ret;
}

void WorkloadGroup::upsert_cgroup_cpu_ctl_no_lock(WorkloadGroupInfo* wg_info) {
    int max_cpu_percent = wg_info->max_cpu_percent;
    int min_cpu_percent = wg_info->min_cpu_percent;
    create_cgroup_cpu_ctl_no_lock();

    if (_cgroup_cpu_ctl) {
        _cgroup_cpu_ctl->update_cpu_hard_limit(max_cpu_percent);
        _cgroup_cpu_ctl->update_cpu_soft_limit(min_cpu_percent);
        _cgroup_cpu_ctl->get_cgroup_cpu_info(&(wg_info->cgroup_cpu_shares),
                                             &(wg_info->cgroup_cpu_hard_limit));
    }
}

Status WorkloadGroup::upsert_task_scheduler(WorkloadGroupInfo* wg_info) {
    std::lock_guard<std::shared_mutex> wlock(_task_sched_lock);
    upsert_cgroup_cpu_ctl_no_lock(wg_info);

    return upsert_thread_pool_no_lock(wg_info, _cgroup_cpu_ctl);
}

void WorkloadGroup::get_query_scheduler(doris::pipeline::TaskScheduler** exec_sched,
                                        vectorized::SimplifiedScanScheduler** scan_sched,
                                        vectorized::SimplifiedScanScheduler** remote_scan_sched) {
    std::shared_lock<std::shared_mutex> rlock(_task_sched_lock);
    *exec_sched = _task_sched.get();
    *scan_sched = _scan_task_sched.get();
    *remote_scan_sched = _remote_scan_task_sched.get();
}

std::string WorkloadGroup::thread_debug_info() {
    std::string str = "";
    if (_task_sched != nullptr) {
        std::vector<int> exec_t_info = _task_sched->thread_debug_info();
        str = fmt::format("[exec num:{}, real_num:{}, min_num:{}, max_num:{}],", exec_t_info[0],
                          exec_t_info[1], exec_t_info[2], exec_t_info[3]);
    }

    if (_scan_task_sched != nullptr) {
        std::vector<int> exec_t_info = _scan_task_sched->thread_debug_info();
        str += fmt::format("[l_scan num:{}, real_num:{}, min_num:{}, max_num:{}],", exec_t_info[0],
                           exec_t_info[1], exec_t_info[2], exec_t_info[3]);
    }

    if (_remote_scan_task_sched != nullptr) {
        std::vector<int> exec_t_info = _remote_scan_task_sched->thread_debug_info();
        str += fmt::format("[r_scan num:{}, real_num:{}, min_num:{}, max_num:{}],", exec_t_info[0],
                           exec_t_info[1], exec_t_info[2], exec_t_info[3]);
    }

    if (_memtable_flush_pool != nullptr) {
        std::vector<int> exec_t_info = _memtable_flush_pool->debug_info();
        str += fmt::format("[mem_tab_flush num:{}, real_num:{}, min_num:{}, max_num:{}]",
                           exec_t_info[0], exec_t_info[1], exec_t_info[2], exec_t_info[3]);
    }
    return str;
}

void WorkloadGroup::upsert_scan_io_throttle(WorkloadGroupInfo* wg_info) {
    for (const auto& [key, io_throttle] : _scan_io_throttle_map) {
        io_throttle->set_io_bytes_per_second(wg_info->read_bytes_per_second);
    }

    _remote_scan_io_throttle->set_io_bytes_per_second(wg_info->remote_read_bytes_per_second);
}

std::shared_ptr<IOThrottle> WorkloadGroup::get_local_scan_io_throttle(const std::string& disk_dir) {
    auto find_ret = _scan_io_throttle_map.find(disk_dir);
    if (find_ret != _scan_io_throttle_map.end()) {
        return find_ret->second;
    }
    return nullptr;
}

std::shared_ptr<IOThrottle> WorkloadGroup::get_remote_scan_io_throttle() {
    return _remote_scan_io_throttle;
}

void WorkloadGroup::update_cpu_time(int64_t delta_cpu_time) {
    _wg_metrics->update_cpu_time_nanos(delta_cpu_time);
}

void WorkloadGroup::update_local_scan_io(std::string path, size_t scan_bytes) {
    _wg_metrics->update_local_scan_io_bytes(path, (uint64_t)scan_bytes);
}

void WorkloadGroup::update_remote_scan_io(size_t scan_bytes) {
    _wg_metrics->update_remote_scan_io_bytes((uint64_t)scan_bytes);
}

int64_t WorkloadGroup::get_mem_used() {
    return _total_mem_used;
}

void WorkloadGroup::try_stop_schedulers() {
    std::lock_guard<std::shared_mutex> wlock(_task_sched_lock);
    if (_task_sched) {
        _task_sched->stop();
    }
    if (_scan_task_sched) {
        _scan_task_sched->stop();
    }
    if (_remote_scan_task_sched) {
        _remote_scan_task_sched->stop();
    }
    if (_memtable_flush_pool) {
        _memtable_flush_pool->shutdown();
        _memtable_flush_pool->wait();
    }
}

#include "common/compile_check_end.h"

} // namespace doris
