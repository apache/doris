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
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "service/backend_options.h"
#include "util/mem_info.h"
#include "util/perf_counters.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"

namespace doris {

bvar::Adder<int64_t> g_memtrackerlimiter_cnt("memtrackerlimiter_cnt");
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
    g_memtrackerlimiter_cnt << 1;
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

MemTrackerLimiter::~MemTrackerLimiter() {
    consume(_untracked_mem);
    static std::string mem_tracker_inaccurate_msg =
            ", mem tracker not equal to 0 when mem tracker destruct, this usually means that "
            "memory tracking is inaccurate and SCOPED_ATTACH_TASK and "
            "SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER are not used correctly. "
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
        // TODO, expect mem tracker equal to 0 at the task end.
        if (doris::config::enable_memory_orphan_check && _type == Type::QUERY) {
            LOG(INFO) << "mem tracker label: " << _label
                      << ", consumption: " << _consumption->current_value()
                      << ", peak consumption: " << _consumption->peak_value()
                      << mem_tracker_inaccurate_msg;
        }
        if (ExecEnv::tracking_memory()) {
            ExecEnv::GetInstance()->orphan_mem_tracker()->consume(_consumption->current_value());
        }
        _consumption->set(0);
    }
    g_memtrackerlimiter_cnt << -1;
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
    for (auto it : type_mem_sum) {
        MemTrackerLimiter::TypeMemSum[it.first]->set(it.second);
    }
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
    int64_t all_tracker_mem_sum = 0;
    Snapshot snapshot;
    for (auto it : MemTrackerLimiter::TypeMemSum) {
        snapshot.type = type_string(it.first);
        snapshot.label = "";
        snapshot.limit = -1;
        snapshot.cur_consumption = it.second->current_value();
        snapshot.peak_consumption = it.second->peak_value();
        (*snapshots).emplace_back(snapshot);
        all_tracker_mem_sum += it.second->current_value();
    }

    snapshot.type = "tc/jemalloc cache";
    snapshot.label = "";
    snapshot.limit = -1;
    snapshot.cur_consumption = MemInfo::allocator_cache_mem();
    snapshot.peak_consumption = -1;
    (*snapshots).emplace_back(snapshot);
    all_tracker_mem_sum += MemInfo::allocator_cache_mem();

    snapshot.type = "sum of all trackers"; // is virtual memory
    snapshot.label = "";
    snapshot.limit = -1;
    snapshot.cur_consumption = all_tracker_mem_sum;
    snapshot.peak_consumption = -1;
    (*snapshots).emplace_back(snapshot);

#ifdef ADDRESS_SANITIZER
    snapshot.type = "[ASAN]process resident memory"; // from /proc VmRSS VmHWM
#else
    snapshot.type = "process resident memory"; // from /proc VmRSS VmHWM
#endif
    snapshot.label = "";
    snapshot.limit = -1;
    snapshot.cur_consumption = PerfCounters::get_vm_rss();
    snapshot.peak_consumption = PerfCounters::get_vm_hwm();
    (*snapshots).emplace_back(snapshot);

    snapshot.type = "process virtual memory"; // from /proc VmSize VmPeak
    snapshot.label = "";
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
        detail += "\nProcess Memory Summary:\n    " + MemTrackerLimiter::process_mem_log_str();
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
    detail += "\nProcess Memory Summary:\n    " + MemTrackerLimiter::process_mem_log_str();
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

bool MemTrackerLimiter::sys_mem_exceed_limit_check(int64_t bytes) {
    // Limit process memory usage using the actual physical memory of the process in `/proc/self/status`.
    // This is independent of the consumption value of the mem tracker, which counts the virtual memory
    // of the process malloc.
    // for fast, expect MemInfo::initialized() to be true.
    //
    // tcmalloc/jemalloc allocator cache does not participate in the mem check as part of the process physical memory.
    // because `new/malloc` will trigger mem hook when using tcmalloc/jemalloc allocator cache,
    // but it may not actually alloc physical memory, which is not expected in mem hook fail.
    return MemInfo::proc_mem_no_allocator_cache() + bytes >= MemInfo::mem_limit() ||
           MemInfo::sys_mem_available() < MemInfo::sys_mem_available_low_water_mark();
}

std::string MemTrackerLimiter::process_mem_log_str() {
    return fmt::format(
            "os physical memory {}. process memory used {}, limit {}, soft limit {}. sys "
            "available memory {}, low water mark {}, warning water mark {}. Refresh interval "
            "memory growth {} B",
            PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES),
            PerfCounters::get_vm_rss_str(), MemInfo::mem_limit_str(), MemInfo::soft_mem_limit_str(),
            MemInfo::sys_mem_available_str(),
            PrettyPrinter::print(MemInfo::sys_mem_available_low_water_mark(), TUnit::BYTES),
            PrettyPrinter::print(MemInfo::sys_mem_available_warning_water_mark(), TUnit::BYTES),
            MemInfo::refresh_interval_memory_growth);
}

std::string MemTrackerLimiter::process_limit_exceeded_errmsg_str() {
    return fmt::format(
            "process memory used {} exceed limit {} or sys available memory {} less than low "
            "water mark {}",
            PerfCounters::get_vm_rss_str(), MemInfo::mem_limit_str(),
            MemInfo::sys_mem_available_str(),
            PrettyPrinter::print(MemInfo::sys_mem_available_low_water_mark(), TUnit::BYTES));
}

std::string MemTrackerLimiter::process_soft_limit_exceeded_errmsg_str() {
    return fmt::format(
            "process memory used {} exceed soft limit {} or sys available memory {} less than "
            "warning water mark {}.",
            PerfCounters::get_vm_rss_str(), MemInfo::soft_mem_limit_str(),
            MemInfo::sys_mem_available_str(),
            PrettyPrinter::print(MemInfo::sys_mem_available_warning_water_mark(), TUnit::BYTES));
}

std::string MemTrackerLimiter::tracker_limit_exceeded_str() {
    std::string err_msg = fmt::format(
            "memory tracker limit exceeded, tracker label:{}, type:{}, limit "
            "{}, peak used {}, current used {}. backend {} process memory used {}.",
            label(), type_string(_type), print_bytes(limit()),
            print_bytes(_consumption->peak_value()), print_bytes(_consumption->current_value()),
            BackendOptions::get_localhost(), PerfCounters::get_vm_rss_str());
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
                                                 const std::string& vm_rss_str,
                                                 const std::string& mem_available_str,
                                                 RuntimeProfile* profile, Type type) {
    return free_top_memory_query(
            min_free_mem, type, ExecEnv::GetInstance()->mem_tracker_limiter_pool,
            [&vm_rss_str, &mem_available_str, &type](int64_t mem_consumption,
                                                     const std::string& label) {
                return fmt::format(
                        "Process has no memory available, cancel top memory used {}: "
                        "{} memory tracker <{}> consumption {}, backend {} "
                        "process memory used {} exceed limit {} or sys available memory {} "
                        "less than low water mark {}. Execute again after enough memory, "
                        "details see be.INFO.",
                        type_string(type), type_string(type), label, print_bytes(mem_consumption),
                        BackendOptions::get_localhost(), vm_rss_str, MemInfo::mem_limit_str(),
                        mem_available_str,
                        print_bytes(MemInfo::sys_mem_available_low_water_mark()));
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
                    } else if (tracker->consumption() > min_pq.top().first) {
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
                LOG(WARNING) << log_prefix
                             << "Task ID parsing failed, label: " << min_pq.top().second;
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
                                                     const std::string& vm_rss_str,
                                                     const std::string& mem_available_str,
                                                     RuntimeProfile* profile, Type type) {
    return free_top_overcommit_query(
            min_free_mem, type, ExecEnv::GetInstance()->mem_tracker_limiter_pool,
            [&vm_rss_str, &mem_available_str, &type](int64_t mem_consumption,
                                                     const std::string& label) {
                return fmt::format(
                        "Process has less memory, cancel top memory overcommit {}: "
                        "{} memory tracker <{}> consumption {}, backend {} "
                        "process memory used {} exceed soft limit {} or sys available memory {} "
                        "less than warning water mark {}. Execute again after enough memory, "
                        "details see be.INFO.",
                        type_string(type), type_string(type), label, print_bytes(mem_consumption),
                        BackendOptions::get_localhost(), vm_rss_str, MemInfo::soft_mem_limit_str(),
                        mem_available_str,
                        print_bytes(MemInfo::sys_mem_available_warning_water_mark()));
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
                LOG(WARNING) << log_prefix
                             << "Task ID parsing failed, label: " << max_pq.top().second;
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
