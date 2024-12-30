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

#include "runtime/memory/mem_tracker_limiter.h"

#include <fmt/format.h>
#include <gen_cpp/types.pb.h>
#include <stdlib.h>

#include <functional>
#include <mutex>
#include <queue>
#include <utility>

#include "bvar/bvar.h"
#include "common/config.h"
#include "olap/memtable_memory_limiter.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "service/backend_options.h"
#include "util/mem_info.h"
#include "util/perf_counters.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "util/stack_util.h"

namespace doris {

static bvar::Adder<int64_t> memory_memtrackerlimiter_cnt("memory_memtrackerlimiter_cnt");
static bvar::Adder<int64_t> memory_all_trackers_sum_bytes("memory_all_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_global_trackers_sum_bytes("memory_global_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_query_trackers_sum_bytes("memory_query_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_load_trackers_sum_bytes("memory_load_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_compaction_trackers_sum_bytes(
        "memory_compaction_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_schema_change_trackers_sum_bytes(
        "memory_schema_change_trackers_sum_bytes");
static bvar::Adder<int64_t> memory_other_trackers_sum_bytes("memory_other_trackers_sum_bytes");

constexpr auto GC_MAX_SEEK_TRACKER = 1000;

std::atomic<bool> MemTrackerLimiter::_enable_print_log_process_usage {true};

// Reset before each free
static std::unique_ptr<RuntimeProfile> free_top_memory_task_profile {
        std::make_unique<RuntimeProfile>("-")};
static RuntimeProfile::Counter* find_cost_time =
        ADD_TIMER(free_top_memory_task_profile, "FindCostTime");
static RuntimeProfile::Counter* cancel_cost_time =
        ADD_TIMER(free_top_memory_task_profile, "CancelCostTime");
static RuntimeProfile::Counter* freed_memory_counter =
        ADD_COUNTER(free_top_memory_task_profile, "FreedMemory", TUnit::BYTES);
static RuntimeProfile::Counter* cancel_tasks_counter =
        ADD_COUNTER(free_top_memory_task_profile, "CancelTasksNum", TUnit::UNIT);
static RuntimeProfile::Counter* seek_tasks_counter =
        ADD_COUNTER(free_top_memory_task_profile, "SeekTasksNum", TUnit::UNIT);
static RuntimeProfile::Counter* previously_canceling_tasks_counter =
        ADD_COUNTER(free_top_memory_task_profile, "PreviouslyCancelingTasksNum", TUnit::UNIT);

MemTrackerLimiter::MemTrackerLimiter(Type type, const std::string& label, int64_t byte_limit) {
    DCHECK_GE(byte_limit, -1);
    _consumption = std::make_shared<MemCounter>();
    _type = type;
    _label = label;
    _limit = byte_limit;
    if (_type == Type::GLOBAL) {
        _group_num = 0;
    } else {
        _group_num = random() % 999 + 1;
    }

    // currently only select/load need runtime query statistics
    if (_type == Type::LOAD || _type == Type::QUERY) {
        _query_statistics = std::make_shared<QueryStatistics>();
    }
    memory_memtrackerlimiter_cnt << 1;
}

std::shared_ptr<MemTrackerLimiter> MemTrackerLimiter::create_shared(MemTrackerLimiter::Type type,
                                                                    const std::string& label,
                                                                    int64_t byte_limit) {
    auto tracker = std::make_shared<MemTrackerLimiter>(type, label, byte_limit);
#ifndef BE_TEST
    DCHECK(ExecEnv::tracking_memory());
    std::lock_guard<std::mutex> l(
            ExecEnv::GetInstance()->mem_tracker_limiter_pool[tracker->group_num()].group_lock);
    ExecEnv::GetInstance()->mem_tracker_limiter_pool[tracker->group_num()].trackers.insert(
            ExecEnv::GetInstance()->mem_tracker_limiter_pool[tracker->group_num()].trackers.end(),
            tracker);
#endif
    return tracker;
}

bool MemTrackerLimiter::open_memory_tracker_inaccurate_detect() {
    return doris::config::crash_in_memory_tracker_inaccurate &&
           (_type == Type::COMPACTION || _type == Type::SCHEMA_CHANGE || _type == Type::QUERY ||
            (_type == Type::LOAD && !is_group_commit_load));
}

MemTrackerLimiter::~MemTrackerLimiter() {
    consume(_untracked_mem);
    static std::string mem_tracker_inaccurate_msg =
            "mem tracker not equal to 0 when mem tracker destruct, this usually means that "
            "memory tracking is inaccurate and SCOPED_ATTACH_TASK and "
            "SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER are not used correctly. "
            "If the log is truncated, search for `Address Sanitizer` in the be.INFO log to see "
            "more information."
            "1. For query and load, memory leaks may have occurred, it is expected that the query "
            "mem tracker will be bound to the thread context using SCOPED_ATTACH_TASK and "
            "SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER before all memory alloc and free. "
            "2. If a memory alloc is recorded by this tracker, it is expected that be "
            "recorded in this tracker when memory is freed. "
            "3. Merge the remaining memory tracking value by "
            "this tracker into Orphan, if you observe that Orphan is not equal to 0 in the mem "
            "tracker web or log, this indicates that there may be a memory leak. "
            "4. If you need to "
            "transfer memory tracking value between two trackers, can use transfer_to.";
    if (_consumption->current_value() != 0) {
        if (open_memory_tracker_inaccurate_detect()) {
            std::string err_msg =
                    fmt::format("mem tracker label: {}, consumption: {}, peak consumption: {}, {}.",
                                label(), _consumption->current_value(), _consumption->peak_value(),
                                mem_tracker_inaccurate_msg);
            LOG(FATAL) << err_msg << print_address_sanitizers();
        }
        if (ExecEnv::tracking_memory()) {
            ExecEnv::GetInstance()->orphan_mem_tracker()->consume(_consumption->current_value());
        }
        _consumption->set(0);
    } else if (doris::config::crash_in_memory_tracker_inaccurate && !_address_sanitizers.empty() &&
               !is_group_commit_load) {
        LOG(FATAL) << "[Address Sanitizer] consumption is 0, but address sanitizers not empty. "
                   << ", mem tracker label: " << _label
                   << ", peak consumption: " << _consumption->peak_value()
                   << print_address_sanitizers();
    }
    memory_memtrackerlimiter_cnt << -1;
}

void MemTrackerLimiter::add_address_sanitizers(void* buf, size_t size) {
    if (open_memory_tracker_inaccurate_detect()) {
        std::lock_guard<std::mutex> l(_address_sanitizers_mtx);
        auto it = _address_sanitizers.find(buf);
        if (it != _address_sanitizers.end()) {
            _error_address_sanitizers.emplace_back(
                    fmt::format("[Address Sanitizer] memory buf repeat add, mem tracker label: {}, "
                                "consumption: {}, peak consumption: {}, buf: {}, size: {}, old "
                                "buf: {}, old size: {}, new stack_trace: {}, old stack_trace: {}.",
                                _label, _consumption->current_value(), _consumption->peak_value(),
                                buf, size, it->first, it->second.size,
                                get_stack_trace(1, "FULL_WITH_INLINE"), it->second.stack_trace));
        }

        // if alignment not equal to 0, maybe usable_size > size.
        AddressSanitizer as = {size, doris::config::enable_address_sanitizers_with_stack_trace
                                             ? get_stack_trace(1, "DISABLED")
                                             : ""};
        _address_sanitizers.emplace(buf, as);
    }
}

void MemTrackerLimiter::remove_address_sanitizers(void* buf, size_t size) {
    if (open_memory_tracker_inaccurate_detect()) {
        std::lock_guard<std::mutex> l(_address_sanitizers_mtx);
        auto it = _address_sanitizers.find(buf);
        if (it != _address_sanitizers.end()) {
            if (it->second.size != size) {
                _error_address_sanitizers.emplace_back(fmt::format(
                        "[Address Sanitizer] free memory buf size inaccurate, mem tracker label: "
                        "{}, consumption: {}, peak consumption: {}, buf: {}, size: {}, old buf: "
                        "{}, old size: {}, new stack_trace: {}, old stack_trace: {}.",
                        _label, _consumption->current_value(), _consumption->peak_value(), buf,
                        size, it->first, it->second.size, get_stack_trace(1, "FULL_WITH_INLINE"),
                        it->second.stack_trace));
            }
            _address_sanitizers.erase(buf);
        } else {
            _error_address_sanitizers.emplace_back(fmt::format(
                    "[Address Sanitizer] memory buf not exist, mem tracker label: {}, consumption: "
                    "{}, peak consumption: {}, buf: {}, size: {}, stack_trace: {}.",
                    _label, _consumption->current_value(), _consumption->peak_value(), buf, size,
                    get_stack_trace(1, "FULL_WITH_INLINE")));
        }
    }
}

std::string MemTrackerLimiter::print_address_sanitizers() {
    std::lock_guard<std::mutex> l(_address_sanitizers_mtx);
    std::string detail = "[Address Sanitizer]:";
    detail += "\n memory not be freed:";
    for (const auto& it : _address_sanitizers) {
        auto msg = fmt::format(
                "\n    [Address Sanitizer] buf not be freed, mem tracker label: {}, consumption: "
                "{}, peak consumption: {}, buf: {}, size {}, strack trace: {}",
                _label, _consumption->current_value(), _consumption->peak_value(), it.first,
                it.second.size, it.second.stack_trace);
        LOG(INFO) << msg;
        detail += msg;
    }
    detail += "\n incorrect memory alloc and free:";
    for (const auto& err_msg : _error_address_sanitizers) {
        LOG(INFO) << err_msg;
        detail += fmt::format("\n    {}", err_msg);
    }
    return detail;
}

MemTracker::Snapshot MemTrackerLimiter::make_snapshot() const {
    Snapshot snapshot;
    snapshot.type = type_string(_type);
    snapshot.label = _label;
    snapshot.limit = _limit;
    snapshot.cur_consumption = _consumption->current_value();
    snapshot.peak_consumption = _consumption->peak_value();
    return snapshot;
}

void MemTrackerLimiter::refresh_global_counter() {
    std::unordered_map<Type, int64_t> type_mem_sum = {
            {Type::GLOBAL, 0},     {Type::QUERY, 0},         {Type::LOAD, 0},
            {Type::COMPACTION, 0}, {Type::SCHEMA_CHANGE, 0}, {Type::OTHER, 0}};
    // always ExecEnv::ready(), because Daemon::_stop_background_threads_latch
    for (auto& group : ExecEnv::GetInstance()->mem_tracker_limiter_pool) {
        std::lock_guard<std::mutex> l(group.group_lock);
        for (auto trackerWptr : group.trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr) {
                type_mem_sum[tracker->type()] += tracker->consumption();
            }
        }
    }
    int64_t all_trackers_mem_sum = 0;
    for (auto it : type_mem_sum) {
        MemTrackerLimiter::TypeMemSum[it.first]->set(it.second);
        all_trackers_mem_sum += it.second;
        switch (it.first) {
        case Type::GLOBAL:
            memory_global_trackers_sum_bytes
                    << it.second - memory_global_trackers_sum_bytes.get_value();
            break;
        case Type::QUERY:
            memory_query_trackers_sum_bytes
                    << it.second - memory_query_trackers_sum_bytes.get_value();
            break;
        case Type::LOAD:
            memory_load_trackers_sum_bytes
                    << it.second - memory_load_trackers_sum_bytes.get_value();
            break;
        case Type::COMPACTION:
            memory_compaction_trackers_sum_bytes
                    << it.second - memory_compaction_trackers_sum_bytes.get_value();
            break;
        case Type::SCHEMA_CHANGE:
            memory_schema_change_trackers_sum_bytes
                    << it.second - memory_schema_change_trackers_sum_bytes.get_value();
            break;
        case Type::OTHER:
            memory_other_trackers_sum_bytes
                    << it.second - memory_other_trackers_sum_bytes.get_value();
        }
    }
    all_trackers_mem_sum += MemInfo::allocator_cache_mem();
    all_trackers_mem_sum += MemInfo::allocator_metadata_mem();
    memory_all_trackers_sum_bytes << all_trackers_mem_sum -
                                             memory_all_trackers_sum_bytes.get_value();
}

void MemTrackerLimiter::clean_tracker_limiter_group() {
#ifndef BE_TEST
    if (ExecEnv::tracking_memory()) {
        for (auto& group : ExecEnv::GetInstance()->mem_tracker_limiter_pool) {
            std::lock_guard<std::mutex> l(group.group_lock);
            auto it = group.trackers.begin();
            while (it != group.trackers.end()) {
                if ((*it).expired()) {
                    it = group.trackers.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
#endif
}

void MemTrackerLimiter::make_process_snapshots(std::vector<MemTracker::Snapshot>* snapshots) {
    MemTrackerLimiter::refresh_global_counter();
    int64_t all_trackers_mem_sum = 0;
    Snapshot snapshot;
    for (auto it : MemTrackerLimiter::TypeMemSum) {
        snapshot.type = "overview";
        snapshot.label = type_string(it.first);
        snapshot.limit = -1;
        snapshot.cur_consumption = it.second->current_value();
        snapshot.peak_consumption = it.second->peak_value();
        (*snapshots).emplace_back(snapshot);
        all_trackers_mem_sum += it.second->current_value();
    }

    snapshot.type = "overview";
    snapshot.label = "tc/jemalloc_cache";
    snapshot.limit = -1;
    snapshot.cur_consumption = MemInfo::allocator_cache_mem();
    snapshot.peak_consumption = -1;
    (*snapshots).emplace_back(snapshot);
    all_trackers_mem_sum += MemInfo::allocator_cache_mem();

    snapshot.type = "overview";
    snapshot.label = "tc/jemalloc_metadata";
    snapshot.limit = -1;
    snapshot.cur_consumption = MemInfo::allocator_metadata_mem();
    snapshot.peak_consumption = -1;
    (*snapshots).emplace_back(snapshot);
    all_trackers_mem_sum += MemInfo::allocator_metadata_mem();

    snapshot.type = "overview";
    snapshot.label = "reserved_memory";
    snapshot.limit = -1;
    snapshot.cur_consumption = GlobalMemoryArbitrator::process_reserved_memory();
    snapshot.peak_consumption = -1;
    (*snapshots).emplace_back(snapshot);

    snapshot.type = "overview";
    snapshot.label = "sum_of_all_trackers"; // is virtual memory
    snapshot.limit = -1;
    snapshot.cur_consumption = all_trackers_mem_sum;
    snapshot.peak_consumption = -1;
    (*snapshots).emplace_back(snapshot);

    snapshot.type = "overview";
#ifdef ADDRESS_SANITIZER
    snapshot.label = "[ASAN]VmRSS(process resident memory)"; // from /proc VmRSS VmHWM
#else
    snapshot.label = "VmRSS(process resident memory)"; // from /proc VmRSS VmHWM
#endif
    snapshot.limit = -1;
    snapshot.cur_consumption = PerfCounters::get_vm_rss();
    snapshot.peak_consumption = PerfCounters::get_vm_hwm();
    (*snapshots).emplace_back(snapshot);

    snapshot.type = "overview";
    snapshot.label = "VmSize(process virtual memory)"; // from /proc VmSize VmPeak
    snapshot.limit = -1;
    snapshot.cur_consumption = PerfCounters::get_vm_size();
    snapshot.peak_consumption = PerfCounters::get_vm_peak();
    (*snapshots).emplace_back(snapshot);
}

void MemTrackerLimiter::make_type_snapshots(std::vector<MemTracker::Snapshot>* snapshots,
                                            MemTrackerLimiter::Type type) {
    if (type == Type::GLOBAL) {
        std::lock_guard<std::mutex> l(
                ExecEnv::GetInstance()->mem_tracker_limiter_pool[0].group_lock);
        for (auto trackerWptr : ExecEnv::GetInstance()->mem_tracker_limiter_pool[0].trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr) {
                (*snapshots).emplace_back(tracker->make_snapshot());
                MemTracker::make_group_snapshot(snapshots, tracker->group_num(), tracker->label());
            }
        }
    } else {
        for (unsigned i = 1; i < ExecEnv::GetInstance()->mem_tracker_limiter_pool.size(); ++i) {
            std::lock_guard<std::mutex> l(
                    ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].group_lock);
            for (auto trackerWptr : ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].trackers) {
                auto tracker = trackerWptr.lock();
                if (tracker != nullptr && tracker->type() == type) {
                    (*snapshots).emplace_back(tracker->make_snapshot());
                    MemTracker::make_group_snapshot(snapshots, tracker->group_num(),
                                                    tracker->label());
                }
            }
        }
    }
}

void MemTrackerLimiter::make_top_consumption_snapshots(std::vector<MemTracker::Snapshot>* snapshots,
                                                       int top_num) {
    std::priority_queue<MemTracker::Snapshot> max_pq;
    // not include global type.
    for (unsigned i = 1; i < ExecEnv::GetInstance()->mem_tracker_limiter_pool.size(); ++i) {
        std::lock_guard<std::mutex> l(
                ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].group_lock);
        for (auto trackerWptr : ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr) {
                max_pq.emplace(tracker->make_snapshot());
            }
        }
    }

    while (!max_pq.empty() && top_num > 0) {
        (*snapshots).emplace_back(max_pq.top());
        top_num--;
        max_pq.pop();
    }
}

void MemTrackerLimiter::make_all_trackers_snapshots(std::vector<MemTracker::Snapshot>* snapshots) {
    for (auto& i : ExecEnv::GetInstance()->mem_tracker_limiter_pool) {
        std::lock_guard<std::mutex> l(i.group_lock);
        for (auto trackerWptr : i.trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr) {
                (*snapshots).emplace_back(tracker->make_snapshot());
            }
        }
    }
}

void MemTrackerLimiter::make_all_memory_state_snapshots(
        std::vector<MemTracker::Snapshot>* snapshots) {
    make_process_snapshots(snapshots);
    make_all_trackers_snapshots(snapshots);
    MemTracker::make_all_trackers_snapshots(snapshots);
}

std::string MemTrackerLimiter::log_usage(MemTracker::Snapshot snapshot) {
    return fmt::format(
            "MemTrackerLimiter Label={}, Type={}, Limit={}({} B), Used={}({} B), Peak={}({} B)",
            snapshot.label, snapshot.type, print_bytes(snapshot.limit), snapshot.limit,
            print_bytes(snapshot.cur_consumption), snapshot.cur_consumption,
            print_bytes(snapshot.peak_consumption), snapshot.peak_consumption);
}

std::string MemTrackerLimiter::type_log_usage(MemTracker::Snapshot snapshot) {
    return fmt::format("Type={}, Used={}({} B), Peak={}({} B)", snapshot.type,
                       print_bytes(snapshot.cur_consumption), snapshot.cur_consumption,
                       print_bytes(snapshot.peak_consumption), snapshot.peak_consumption);
}

std::string MemTrackerLimiter::type_detail_usage(const std::string& msg, Type type) {
    std::string detail = fmt::format("{}, Type:{}, Memory Tracker Summary", msg, type_string(type));
    for (unsigned i = 1; i < ExecEnv::GetInstance()->mem_tracker_limiter_pool.size(); ++i) {
        std::lock_guard<std::mutex> l(
                ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].group_lock);
        for (auto trackerWptr : ExecEnv::GetInstance()->mem_tracker_limiter_pool[i].trackers) {
            auto tracker = trackerWptr.lock();
            if (tracker != nullptr && tracker->type() == type) {
                detail += "\n    " + MemTrackerLimiter::log_usage(tracker->make_snapshot());
            }
        }
    }
    return detail;
}

void MemTrackerLimiter::print_log_usage(const std::string& msg) {
    if (_enable_print_log_usage) {
        _enable_print_log_usage = false;
        std::string detail = msg;
        detail += "\nProcess Memory Summary:\n    " + GlobalMemoryArbitrator::process_mem_log_str();
        detail += "\nMemory Tracker Summary:    " + log_usage();
        std::string child_trackers_usage;
        std::vector<MemTracker::Snapshot> snapshots;
        MemTracker::make_group_snapshot(&snapshots, _group_num, _label);
        for (const auto& snapshot : snapshots) {
            child_trackers_usage += "\n    " + MemTracker::log_usage(snapshot);
        }
        if (!child_trackers_usage.empty()) {
            detail += child_trackers_usage;
        }

        LOG(WARNING) << detail;
    }
}

std::string MemTrackerLimiter::log_process_usage_str() {
    std::string detail;
    detail += "\nProcess Memory Summary:\n    " + GlobalMemoryArbitrator::process_mem_log_str();
    std::vector<MemTracker::Snapshot> snapshots;
    MemTrackerLimiter::make_process_snapshots(&snapshots);
    MemTrackerLimiter::make_type_snapshots(&snapshots, MemTrackerLimiter::Type::GLOBAL);
    MemTrackerLimiter::make_top_consumption_snapshots(&snapshots, 15);

    // Add additional tracker printed when memory exceeds limit.
    snapshots.emplace_back(
            ExecEnv::GetInstance()->memtable_memory_limiter()->mem_tracker()->make_snapshot());

    detail += "\nMemory Tracker Summary:";
    for (const auto& snapshot : snapshots) {
        if (snapshot.label.empty() && snapshot.parent_label.empty()) {
            detail += "\n    " + MemTrackerLimiter::type_log_usage(snapshot);
        } else if (snapshot.parent_label.empty()) {
            detail += "\n    " + MemTrackerLimiter::log_usage(snapshot);
        } else {
            detail += "\n    " + MemTracker::log_usage(snapshot);
        }
    }
    return detail;
}

void MemTrackerLimiter::print_log_process_usage() {
    // The default interval between two prints is 100ms (config::memory_maintenance_sleep_time_ms).
    if (MemTrackerLimiter::_enable_print_log_process_usage) {
        MemTrackerLimiter::_enable_print_log_process_usage = false;
        LOG(WARNING) << log_process_usage_str();
    }
}

std::string MemTrackerLimiter::tracker_limit_exceeded_str() {
    std::string err_msg = fmt::format(
            "memory tracker limit exceeded, tracker label:{}, type:{}, limit "
            "{}, peak used {}, current used {}. backend {}, {}.",
            label(), type_string(_type), print_bytes(limit()),
            print_bytes(_consumption->peak_value()), print_bytes(_consumption->current_value()),
            BackendOptions::get_localhost(), GlobalMemoryArbitrator::process_memory_used_str());
    if (_type == Type::QUERY || _type == Type::LOAD) {
        err_msg += fmt::format(
                " exec node:<{}>, can `set exec_mem_limit=8G` to change limit, details see "
                "be.INFO.",
                doris::thread_context()->thread_mem_tracker_mgr->last_consumer_tracker());
    } else if (_type == Type::SCHEMA_CHANGE) {
        err_msg += fmt::format(
                " can modify `memory_limitation_per_thread_for_schema_change_bytes` in be.conf to "
                "change limit, details see be.INFO.");
    }
    return err_msg;
}

int64_t MemTrackerLimiter::free_top_memory_query(int64_t min_free_mem,
                                                 const std::string& cancel_reason,
                                                 RuntimeProfile* profile, Type type) {
    return free_top_memory_query(
            min_free_mem, type, ExecEnv::GetInstance()->mem_tracker_limiter_pool,
            [&cancel_reason, &type](int64_t mem_consumption, const std::string& label) {
                return fmt::format(
                        "Process memory not enough, cancel top memory used {}: "
                        "<{}> consumption {}, backend {}, {}. Execute again "
                        "after enough memory, details see be.INFO.",
                        type_string(type), label, print_bytes(mem_consumption),
                        BackendOptions::get_localhost(), cancel_reason);
            },
            profile, GCType::PROCESS);
}

int64_t MemTrackerLimiter::free_top_memory_query(
        int64_t min_free_mem, Type type, std::vector<TrackerLimiterGroup>& tracker_groups,
        const std::function<std::string(int64_t, const std::string&)>& cancel_msg,
        RuntimeProfile* profile, GCType GCtype) {
    using MemTrackerMinQueue = std::priority_queue<std::pair<int64_t, std::string>,
                                                   std::vector<std::pair<int64_t, std::string>>,
                                                   std::greater<std::pair<int64_t, std::string>>>;
    MemTrackerMinQueue min_pq;
    // After greater than min_free_mem, will not be modified.
    int64_t prepare_free_mem = 0;
    std::vector<std::string> canceling_task;
    int seek_num = 0;
    COUNTER_SET(cancel_cost_time, (int64_t)0);
    COUNTER_SET(find_cost_time, (int64_t)0);
    COUNTER_SET(freed_memory_counter, (int64_t)0);
    COUNTER_SET(cancel_tasks_counter, (int64_t)0);
    COUNTER_SET(seek_tasks_counter, (int64_t)0);
    COUNTER_SET(previously_canceling_tasks_counter, (int64_t)0);

    std::string log_prefix = fmt::format("[MemoryGC] GC free {} top memory used {}, ",
                                         gc_type_string(GCtype), type_string(type));
    LOG(INFO) << fmt::format("{}, start seek all {}, running query and load num: {}", log_prefix,
                             type_string(type),
                             ExecEnv::GetInstance()->fragment_mgr()->running_query_num());

    {
        SCOPED_TIMER(find_cost_time);
        for (unsigned i = 1; i < tracker_groups.size(); ++i) {
            if (seek_num > GC_MAX_SEEK_TRACKER) {
                break;
            }
            std::lock_guard<std::mutex> l(tracker_groups[i].group_lock);
            for (auto trackerWptr : tracker_groups[i].trackers) {
                auto tracker = trackerWptr.lock();
                if (tracker != nullptr && tracker->type() == type) {
                    seek_num++;
                    if (tracker->is_query_cancelled()) {
                        canceling_task.push_back(fmt::format("{}:{} Bytes", tracker->label(),
                                                             tracker->consumption()));
                        continue;
                    }
                    if (tracker->consumption() > min_free_mem) {
                        min_pq = MemTrackerMinQueue();
                        min_pq.emplace(tracker->consumption(), tracker->label());
                        prepare_free_mem = tracker->consumption();
                        break;
                    } else if (tracker->consumption() + prepare_free_mem < min_free_mem) {
                        min_pq.emplace(tracker->consumption(), tracker->label());
                        prepare_free_mem += tracker->consumption();
                    } else if (!min_pq.empty() && tracker->consumption() > min_pq.top().first) {
                        min_pq.emplace(tracker->consumption(), tracker->label());
                        prepare_free_mem += tracker->consumption();
                        while (prepare_free_mem - min_pq.top().first > min_free_mem) {
                            prepare_free_mem -= min_pq.top().first;
                            min_pq.pop();
                        }
                    }
                }
            }
            if (prepare_free_mem > min_free_mem && min_pq.size() == 1) {
                // Found a big task, short circuit seek.
                break;
            }
        }
    }

    COUNTER_UPDATE(seek_tasks_counter, seek_num);
    COUNTER_UPDATE(previously_canceling_tasks_counter, canceling_task.size());

    LOG(INFO) << log_prefix << "seek finished, seek " << seek_num << " tasks. among them, "
              << min_pq.size() << " tasks will be canceled, " << prepare_free_mem
              << " memory size prepare free; " << canceling_task.size()
              << " tasks is being canceled and has not been completed yet;"
              << (!canceling_task.empty() ? " consist of: " + join(canceling_task, ",") : "");

    std::vector<std::string> usage_strings;
    {
        SCOPED_TIMER(cancel_cost_time);
        while (!min_pq.empty()) {
            TUniqueId cancelled_queryid = label_to_queryid(min_pq.top().second);
            if (cancelled_queryid == TUniqueId()) {
                min_pq.pop();
                continue;
            }
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                    cancelled_queryid, PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED,
                    cancel_msg(min_pq.top().first, min_pq.top().second));

            COUNTER_UPDATE(freed_memory_counter, min_pq.top().first);
            COUNTER_UPDATE(cancel_tasks_counter, 1);
            usage_strings.push_back(fmt::format("{} memory used {} Bytes", min_pq.top().second,
                                                min_pq.top().first));
            min_pq.pop();
        }
    }

    profile->merge(free_top_memory_task_profile.get());
    LOG(INFO) << log_prefix << "cancel finished, " << cancel_tasks_counter->value()
              << " tasks canceled, memory size being freed: " << freed_memory_counter->value()
              << ", consist of: " << join(usage_strings, ",");
    return freed_memory_counter->value();
}

int64_t MemTrackerLimiter::free_top_overcommit_query(int64_t min_free_mem,
                                                     const std::string& cancel_reason,
                                                     RuntimeProfile* profile, Type type) {
    return free_top_overcommit_query(
            min_free_mem, type, ExecEnv::GetInstance()->mem_tracker_limiter_pool,
            [&cancel_reason, &type](int64_t mem_consumption, const std::string& label) {
                return fmt::format(
                        "Process memory not enough, cancel top memory overcommit {}: "
                        "<{}> consumption {}, backend {}, {}. Execute again "
                        "after enough memory, details see be.INFO.",
                        type_string(type), label, print_bytes(mem_consumption),
                        BackendOptions::get_localhost(), cancel_reason);
            },
            profile, GCType::PROCESS);
}

int64_t MemTrackerLimiter::free_top_overcommit_query(
        int64_t min_free_mem, Type type, std::vector<TrackerLimiterGroup>& tracker_groups,
        const std::function<std::string(int64_t, const std::string&)>& cancel_msg,
        RuntimeProfile* profile, GCType GCtype) {
    std::priority_queue<std::pair<int64_t, std::string>> max_pq;
    std::unordered_map<std::string, int64_t> query_consumption;
    std::vector<std::string> canceling_task;
    int seek_num = 0;
    int small_num = 0;
    COUNTER_SET(cancel_cost_time, (int64_t)0);
    COUNTER_SET(find_cost_time, (int64_t)0);
    COUNTER_SET(freed_memory_counter, (int64_t)0);
    COUNTER_SET(cancel_tasks_counter, (int64_t)0);
    COUNTER_SET(seek_tasks_counter, (int64_t)0);
    COUNTER_SET(previously_canceling_tasks_counter, (int64_t)0);

    std::string log_prefix = fmt::format("[MemoryGC] GC free {} top memory overcommit {}, ",
                                         gc_type_string(GCtype), type_string(type));
    LOG(INFO) << fmt::format("{}, start seek all {}, running query and load num: {}", log_prefix,
                             type_string(type),
                             ExecEnv::GetInstance()->fragment_mgr()->running_query_num());

    {
        SCOPED_TIMER(find_cost_time);
        for (unsigned i = 1; i < tracker_groups.size(); ++i) {
            if (seek_num > GC_MAX_SEEK_TRACKER) {
                break;
            }
            std::lock_guard<std::mutex> l(tracker_groups[i].group_lock);
            for (auto trackerWptr : tracker_groups[i].trackers) {
                auto tracker = trackerWptr.lock();
                if (tracker != nullptr && tracker->type() == type) {
                    seek_num++;
                    // 32M small query does not cancel
                    if (tracker->consumption() <= 33554432 ||
                        tracker->consumption() < tracker->limit()) {
                        small_num++;
                        continue;
                    }
                    if (tracker->is_query_cancelled()) {
                        canceling_task.push_back(fmt::format("{}:{} Bytes", tracker->label(),
                                                             tracker->consumption()));
                        continue;
                    }
                    auto overcommit_ratio = int64_t(
                            (static_cast<double>(tracker->consumption()) / tracker->limit()) *
                            10000);
                    max_pq.emplace(overcommit_ratio, tracker->label());
                    query_consumption[tracker->label()] = tracker->consumption();
                }
            }
        }
    }

    COUNTER_UPDATE(seek_tasks_counter, seek_num);
    COUNTER_UPDATE(previously_canceling_tasks_counter, canceling_task.size());

    LOG(INFO) << log_prefix << "seek finished, seek " << seek_num << " tasks. among them, "
              << query_consumption.size() << " tasks can be canceled; " << small_num
              << " small tasks that were skipped; " << canceling_task.size()
              << " tasks is being canceled and has not been completed yet;"
              << (!canceling_task.empty() ? " consist of: " + join(canceling_task, ",") : "");

    // Minor gc does not cancel when there is only one query.
    if (query_consumption.empty()) {
        LOG(INFO) << log_prefix << "finished, no task need be canceled.";
        return 0;
    }
    if (query_consumption.size() == 1) {
        auto iter = query_consumption.begin();
        LOG(INFO) << log_prefix << "finished, only one task: " << iter->first
                  << ", memory consumption: " << iter->second << ", no cancel.";
        return 0;
    }

    std::vector<std::string> usage_strings;
    {
        SCOPED_TIMER(cancel_cost_time);
        while (!max_pq.empty()) {
            TUniqueId cancelled_queryid = label_to_queryid(max_pq.top().second);
            if (cancelled_queryid == TUniqueId()) {
                max_pq.pop();
                continue;
            }
            int64_t query_mem = query_consumption[max_pq.top().second];
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                    cancelled_queryid, PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED,
                    cancel_msg(query_mem, max_pq.top().second));

            usage_strings.push_back(fmt::format("{} memory used {} Bytes, overcommit ratio: {}",
                                                max_pq.top().second, query_mem,
                                                max_pq.top().first));
            COUNTER_UPDATE(freed_memory_counter, query_mem);
            COUNTER_UPDATE(cancel_tasks_counter, 1);
            if (freed_memory_counter->value() > min_free_mem) {
                break;
            }
            max_pq.pop();
        }
    }

    profile->merge(free_top_memory_task_profile.get());
    LOG(INFO) << log_prefix << "cancel finished, " << cancel_tasks_counter->value()
              << " tasks canceled, memory size being freed: " << freed_memory_counter->value()
              << ", consist of: " << join(usage_strings, ",");
    return freed_memory_counter->value();
}

} // namespace doris
