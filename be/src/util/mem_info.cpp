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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/mem-info.cc
// and modified by Doris

#include "mem_info.h"

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/segment_v2.pb.h>
#include <jemalloc/jemalloc.h>

#include <algorithm>
#include <boost/algorithm/string/trim.hpp>
#include <fstream>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/task_group/task_group.h"
#include "runtime/task_group/task_group_manager.h"
#include "util/cgroup_util.h"
#include "util/defer_op.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "util/string_parser.hpp"

namespace doris {

bool MemInfo::_s_initialized = false;
int64_t MemInfo::_s_physical_mem = -1;
int64_t MemInfo::_s_mem_limit = -1;
std::string MemInfo::_s_mem_limit_str = "";
int64_t MemInfo::_s_soft_mem_limit = -1;
std::string MemInfo::_s_soft_mem_limit_str = "";

std::atomic<int64_t> MemInfo::_s_allocator_cache_mem = 0;
std::string MemInfo::_s_allocator_cache_mem_str = "";
std::atomic<int64_t> MemInfo::_s_virtual_memory_used = 0;
std::atomic<int64_t> MemInfo::_s_proc_mem_no_allocator_cache = -1;
std::atomic<int64_t> MemInfo::refresh_interval_memory_growth = 0;

static std::unordered_map<std::string, int64_t> _mem_info_bytes;
std::atomic<int64_t> MemInfo::_s_sys_mem_available = -1;
std::string MemInfo::_s_sys_mem_available_str = "";
int64_t MemInfo::_s_sys_mem_available_low_water_mark = -1;
int64_t MemInfo::_s_sys_mem_available_warning_water_mark = -1;
int64_t MemInfo::_s_process_minor_gc_size = -1;
int64_t MemInfo::_s_process_full_gc_size = -1;

void MemInfo::refresh_allocator_mem() {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
#elif defined(USE_JEMALLOC)
    // 'epoch' is a special mallctl -- it updates the statistics. Without it, all
    // the following calls will return stale values. It increments and returns
    // the current epoch number, which might be useful to log as a sanity check.
    uint64_t epoch = 0;
    size_t sz = sizeof(epoch);
    jemallctl("epoch", &epoch, &sz, &epoch, sz);

    // https://jemalloc.net/jemalloc.3.html
    // https://www.bookstack.cn/read/aliyun-rds-core/4a0cdf677f62feb3.md
    _s_allocator_cache_mem.store(get_je_all_arena_metrics("tcache_bytes") +
                                         get_je_metrics("stats.metadata") +
                                         get_je_all_arena_metrics("pdirty") * get_page_size(),
                                 std::memory_order_relaxed);
    _s_allocator_cache_mem_str = PrettyPrinter::print(
            static_cast<uint64_t>(_s_allocator_cache_mem.load(std::memory_order_relaxed)),
            TUnit::BYTES);
    _s_virtual_memory_used.store(get_je_metrics("stats.mapped"), std::memory_order_relaxed);
#else
    _s_allocator_cache_mem.store(get_tc_metrics("tcmalloc.pageheap_free_bytes") +
                                         get_tc_metrics("tcmalloc.central_cache_free_bytes") +
                                         get_tc_metrics("tcmalloc.transfer_cache_free_bytes") +
                                         get_tc_metrics("tcmalloc.thread_cache_free_bytes"),
                                 std::memory_order_relaxed);
    _s_allocator_cache_mem_str = PrettyPrinter::print(
            static_cast<uint64_t>(_s_allocator_cache_mem.load(std::memory_order_relaxed)),
            TUnit::BYTES);
    _s_virtual_memory_used.store(get_tc_metrics("generic.total_physical_bytes") +
                                         get_tc_metrics("tcmalloc.pageheap_unmapped_bytes"),
                                 std::memory_order_relaxed);
#endif
}

// step1: free all cache
// step2: free resource groups memory that enable overcommit
// step3: free global top overcommit query, if enable query memory overcommit
// TODO Now, the meaning is different from java minor gc + full gc, more like small gc + large gc.
bool MemInfo::process_minor_gc() {
    MonotonicStopWatch watch;
    watch.start();
    int64_t freed_mem = 0;
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("");
    std::string pre_vm_rss = PerfCounters::get_vm_rss_str();
    std::string pre_sys_mem_available = MemInfo::sys_mem_available_str();

    Defer defer {[&]() {
        je_purge_all_arena_dirty_pages();
        std::stringstream ss;
        profile->pretty_print(&ss);
        LOG(INFO) << fmt::format("End Minor GC, Free Memory {}. cost(us): {}, details: {}",
                                 PrettyPrinter::print(freed_mem, TUnit::BYTES),
                                 watch.elapsed_time() / 1000, ss.str());
    }};

    freed_mem += CacheManager::instance()->for_each_cache_prune_stale(profile.get());
    je_purge_all_arena_dirty_pages();
    if (freed_mem > _s_process_minor_gc_size) {
        return true;
    }

    RuntimeProfile* tg_profile = profile->create_child("WorkloadGroup", true, true);
    freed_mem += tg_soft_memory_limit_gc(_s_process_minor_gc_size - freed_mem, tg_profile);
    if (freed_mem > _s_process_minor_gc_size) {
        return true;
    }

    if (config::enable_query_memory_overcommit) {
        VLOG_NOTICE << MemTrackerLimiter::type_detail_usage(
                "Before free top memory overcommit query in Minor GC",
                MemTrackerLimiter::Type::QUERY);
        RuntimeProfile* toq_profile =
                profile->create_child("FreeTopOvercommitMemoryQuery", true, true);
        freed_mem += MemTrackerLimiter::free_top_overcommit_query(
                _s_process_minor_gc_size - freed_mem, pre_vm_rss, pre_sys_mem_available,
                toq_profile);
        if (freed_mem > _s_process_minor_gc_size) {
            return true;
        }
    }
    return false;
}

// step1: free all cache
// step2: free resource groups memory that enable overcommit
// step3: free global top memory query
// step4: free top overcommit load, load retries are more expensive, So cancel at the end.
// step5: free top memory load
bool MemInfo::process_full_gc() {
    MonotonicStopWatch watch;
    watch.start();
    int64_t freed_mem = 0;
    std::unique_ptr<RuntimeProfile> profile = std::make_unique<RuntimeProfile>("");
    std::string pre_vm_rss = PerfCounters::get_vm_rss_str();
    std::string pre_sys_mem_available = MemInfo::sys_mem_available_str();

    Defer defer {[&]() {
        je_purge_all_arena_dirty_pages();
        std::stringstream ss;
        profile->pretty_print(&ss);
        LOG(INFO) << fmt::format("End Full GC, Free Memory {}. cost(us): {}, details: {}",
                                 PrettyPrinter::print(freed_mem, TUnit::BYTES),
                                 watch.elapsed_time() / 1000, ss.str());
    }};

    freed_mem += CacheManager::instance()->for_each_cache_prune_all(profile.get());
    je_purge_all_arena_dirty_pages();
    if (freed_mem > _s_process_full_gc_size) {
        return true;
    }

    RuntimeProfile* tg_profile = profile->create_child("WorkloadGroup", true, true);
    freed_mem += tg_soft_memory_limit_gc(_s_process_full_gc_size - freed_mem, tg_profile);
    if (freed_mem > _s_process_full_gc_size) {
        return true;
    }

    VLOG_NOTICE << MemTrackerLimiter::type_detail_usage("Before free top memory query in Full GC",
                                                        MemTrackerLimiter::Type::QUERY);
    RuntimeProfile* tmq_profile = profile->create_child("FreeTopMemoryQuery", true, true);
    freed_mem += MemTrackerLimiter::free_top_memory_query(
            _s_process_full_gc_size - freed_mem, pre_vm_rss, pre_sys_mem_available, tmq_profile);
    if (freed_mem > _s_process_full_gc_size) {
        return true;
    }

    if (config::enable_query_memory_overcommit) {
        VLOG_NOTICE << MemTrackerLimiter::type_detail_usage(
                "Before free top memory overcommit load in Full GC", MemTrackerLimiter::Type::LOAD);
        RuntimeProfile* tol_profile =
                profile->create_child("FreeTopMemoryOvercommitLoad", true, true);
        freed_mem += MemTrackerLimiter::free_top_overcommit_load(
                _s_process_full_gc_size - freed_mem, pre_vm_rss, pre_sys_mem_available,
                tol_profile);
        if (freed_mem > _s_process_full_gc_size) {
            return true;
        }
    }

    VLOG_NOTICE << MemTrackerLimiter::type_detail_usage("Before free top memory load in Full GC",
                                                        MemTrackerLimiter::Type::LOAD);
    RuntimeProfile* tml_profile = profile->create_child("FreeTopMemoryLoad", true, true);
    freed_mem += MemTrackerLimiter::free_top_memory_load(
            _s_process_full_gc_size - freed_mem, pre_vm_rss, pre_sys_mem_available, tml_profile);
    if (freed_mem > _s_process_full_gc_size) {
        return true;
    }
    return false;
}

int64_t MemInfo::tg_hard_memory_limit_gc() {
    MonotonicStopWatch watch;
    watch.start();
    std::vector<taskgroup::TaskGroupPtr> task_groups;
    std::unique_ptr<RuntimeProfile> tg_profile = std::make_unique<RuntimeProfile>("WorkloadGroup");
    int64_t total_free_memory = 0;

    Defer defer {[&]() {
        if (total_free_memory > 0) {
            std::stringstream ss;
            tg_profile->pretty_print(&ss);
            LOG(INFO) << fmt::format(
                    "End Task Group Overcommit Memory GC, Free Memory {}. cost(us): {}, "
                    "details: {}",
                    PrettyPrinter::print(total_free_memory, TUnit::BYTES),
                    watch.elapsed_time() / 1000, ss.str());
        }
    }};

    taskgroup::TaskGroupManager::instance()->get_resource_groups(
            [](const taskgroup::TaskGroupPtr& task_group) {
                return !task_group->enable_memory_overcommit();
            },
            &task_groups);

    for (const auto& task_group : task_groups) {
        taskgroup::TaskGroupInfo tg_info;
        task_group->task_group_info(&tg_info);
        auto used = task_group->memory_used();
        total_free_memory += MemTrackerLimiter::tg_memory_limit_gc(
                used - tg_info.memory_limit, used, tg_info.id, tg_info.name, tg_info.memory_limit,
                task_group->mem_tracker_limiter_pool(), tg_profile.get());
    }
    return total_free_memory;
}

int64_t MemInfo::tg_soft_memory_limit_gc(int64_t request_free_memory, RuntimeProfile* profile) {
    std::vector<taskgroup::TaskGroupPtr> task_groups;
    taskgroup::TaskGroupManager::instance()->get_resource_groups(
            [](const taskgroup::TaskGroupPtr& task_group) {
                return task_group->enable_memory_overcommit();
            },
            &task_groups);

    int64_t total_exceeded_memory = 0;
    std::vector<int64_t> used_memorys;
    std::vector<int64_t> exceeded_memorys;
    for (const auto& task_group : task_groups) {
        int64_t used_memory = task_group->memory_used();
        int64_t exceeded = used_memory - task_group->memory_limit();
        int64_t exceeded_memory = exceeded > 0 ? exceeded : 0;
        total_exceeded_memory += exceeded_memory;
        used_memorys.emplace_back(used_memory);
        exceeded_memorys.emplace_back(exceeded_memory);
    }

    int64_t total_free_memory = 0;
    bool gc_all_exceeded = request_free_memory >= total_exceeded_memory;
    for (int i = 0; i < task_groups.size(); ++i) {
        if (exceeded_memorys[i] == 0) {
            continue;
        }

        // todo: GC according to resource group priority
        int64_t tg_need_free_memory =
                gc_all_exceeded ? exceeded_memorys[i]
                                : static_cast<double>(exceeded_memorys[i]) / total_exceeded_memory *
                                          request_free_memory /* exceeded memory as a weight */;
        auto task_group = task_groups[i];
        taskgroup::TaskGroupInfo tg_info;
        task_group->task_group_info(&tg_info);
        total_free_memory += MemTrackerLimiter::tg_memory_limit_gc(
                tg_need_free_memory, used_memorys[i], tg_info.id, tg_info.name,
                tg_info.memory_limit, task_group->mem_tracker_limiter_pool(), profile);
    }
    return total_free_memory;
}

#ifndef __APPLE__
void MemInfo::refresh_proc_meminfo() {
    std::ifstream meminfo("/proc/meminfo", std::ios::in);
    std::string line;

    while (meminfo.good() && !meminfo.eof()) {
        getline(meminfo, line);
        std::vector<std::string> fields = strings::Split(line, " ", strings::SkipWhitespace());
        if (fields.size() < 2) continue;
        std::string key = fields[0].substr(0, fields[0].size() - 1);

        StringParser::ParseResult result;
        int64_t mem_value =
                StringParser::string_to_int<int64_t>(fields[1].data(), fields[1].size(), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            if (fields.size() == 2) {
                _mem_info_bytes[key] = mem_value;
            } else if (fields[2].compare("kB") == 0) {
                _mem_info_bytes[key] = mem_value * 1024L;
            }
        }
    }
    if (meminfo.is_open()) meminfo.close();

    if (_mem_info_bytes.find("MemAvailable") != _mem_info_bytes.end()) {
        _s_sys_mem_available.store(_mem_info_bytes["MemAvailable"], std::memory_order_relaxed);
        _s_sys_mem_available_str = PrettyPrinter::print(
                _s_sys_mem_available.load(std::memory_order_relaxed), TUnit::BYTES);
    }
}

void MemInfo::init() {
    refresh_proc_meminfo();
    _s_physical_mem = _mem_info_bytes["MemTotal"];

    int64_t cgroup_mem_limit = 0;
    Status status = CGroupUtil::find_cgroup_mem_limit(&cgroup_mem_limit);
    if (status.ok() && cgroup_mem_limit > 0) {
        _s_physical_mem = std::min(_s_physical_mem, cgroup_mem_limit);
    }

    if (_s_physical_mem == -1) {
        LOG(WARNING) << "Could not determine amount of physical memory on this machine.";
    }

    bool is_percent = true;
    _s_mem_limit = ParseUtil::parse_mem_spec(config::mem_limit, -1, _s_physical_mem, &is_percent);
    if (_s_mem_limit <= 0) {
        LOG(WARNING) << "Failed to parse mem limit from '" + config::mem_limit + "'.";
    }
    if (_s_mem_limit > _s_physical_mem) {
        LOG(WARNING) << "Memory limit " << PrettyPrinter::print(_s_mem_limit, TUnit::BYTES)
                     << " exceeds physical memory of "
                     << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES)
                     << ". Using physical memory instead";
        _s_mem_limit = _s_physical_mem;
    }
    _s_mem_limit_str = PrettyPrinter::print(_s_mem_limit, TUnit::BYTES);
    _s_soft_mem_limit = _s_mem_limit * config::soft_mem_limit_frac;
    _s_soft_mem_limit_str = PrettyPrinter::print(_s_soft_mem_limit, TUnit::BYTES);

    _s_process_minor_gc_size =
            ParseUtil::parse_mem_spec(config::process_minor_gc_size, -1, _s_mem_limit, &is_percent);
    _s_process_full_gc_size =
            ParseUtil::parse_mem_spec(config::process_full_gc_size, -1, _s_mem_limit, &is_percent);

    std::string line;
    int64_t _s_vm_min_free_kbytes = 0;
    std::ifstream vminfo("/proc/sys/vm/min_free_kbytes", std::ios::in);
    if (vminfo.good() && !vminfo.eof()) {
        getline(vminfo, line);
        boost::algorithm::trim(line);
        StringParser::ParseResult result;
        int64_t mem_value = StringParser::string_to_int<int64_t>(line.data(), line.size(), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            _s_vm_min_free_kbytes = mem_value * 1024L;
        }
    }
    if (vminfo.is_open()) vminfo.close();

    // Redhat 4.x OS, `/proc/meminfo` has no `MemAvailable`.
    if (_mem_info_bytes.find("MemAvailable") != _mem_info_bytes.end()) {
        // MemAvailable = MemFree - LowWaterMark + (PageCache - min(PageCache / 2, LowWaterMark))
        // LowWaterMark = /proc/sys/vm/min_free_kbytes
        // Ref:
        // https://serverfault.com/questions/940196/why-is-memavailable-a-lot-less-than-memfreebufferscached
        // https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=34e431b0ae398fc54ea69ff85ec700722c9da773
        //
        // upper sys_mem_available_low_water_mark, avoid wasting too much memory.
        _s_sys_mem_available_low_water_mark = std::max<int64_t>(
                std::min<int64_t>(
                        std::min<int64_t>(_s_physical_mem - _s_mem_limit, _s_physical_mem * 0.1),
                        config::max_sys_mem_available_low_water_mark_bytes),
                0);
        _s_sys_mem_available_warning_water_mark = _s_sys_mem_available_low_water_mark * 2;
    }

    // Expect vm overcommit memory value to be 1, system will no longer throw bad_alloc, memory alloc are always accepted,
    // memory limit check is handed over to Doris Allocator, make sure throw exception position is controllable,
    // otherwise bad_alloc can be thrown anywhere and it will be difficult to achieve exception safety.
    std::ifstream sys_vm("/proc/sys/vm/overcommit_memory", std::ios::in);
    std::string vm_overcommit;
    getline(sys_vm, vm_overcommit);
    if (sys_vm.is_open()) sys_vm.close();
    if (std::stoi(vm_overcommit) == 2) {
        std::cout << "/proc/sys/vm/overcommit_memory: " << vm_overcommit
                  << ", expect is 1, memory limit check is handed over to Doris Allocator, "
                     "otherwise BE may crash even with remaining memory"
                  << std::endl;
    }

    LOG(INFO) << "Physical Memory: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES)
              << ", Mem Limit: " << _s_mem_limit_str
              << ", origin config value: " << config::mem_limit
              << ", System Mem Available Min Reserve: "
              << PrettyPrinter::print(_s_sys_mem_available_low_water_mark, TUnit::BYTES)
              << ", Vm Min Free KBytes: "
              << PrettyPrinter::print(_s_vm_min_free_kbytes, TUnit::BYTES)
              << ", Vm Overcommit Memory: " << vm_overcommit;
    _s_initialized = true;
}
#else
void MemInfo::refresh_proc_meminfo() {}

void MemInfo::init() {
    size_t size = sizeof(_s_physical_mem);
    if (sysctlbyname("hw.memsize", &_s_physical_mem, &size, nullptr, 0) != 0) {
        LOG(WARNING) << "Could not determine amount of physical memory on this machine.";
        _s_physical_mem = -1;
    }

    bool is_percent = true;
    _s_mem_limit = ParseUtil::parse_mem_spec(config::mem_limit, -1, _s_physical_mem, &is_percent);
    _s_mem_limit_str = PrettyPrinter::print(_s_mem_limit, TUnit::BYTES);
    _s_soft_mem_limit = _s_mem_limit * config::soft_mem_limit_frac;
    _s_soft_mem_limit_str = PrettyPrinter::print(_s_soft_mem_limit, TUnit::BYTES);

    LOG(INFO) << "Physical Memory: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES);
    _s_initialized = true;
}
#endif

std::string MemInfo::debug_string() {
    DCHECK(_s_initialized);
    CGroupUtil util;
    std::stringstream stream;
    stream << "Physical Memory: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES)
           << std::endl;
    stream << "Memory Limt: " << PrettyPrinter::print(_s_mem_limit, TUnit::BYTES) << std::endl;
    stream << "CGroup Info: " << util.debug_string() << std::endl;
    return stream.str();
}

} // namespace doris
