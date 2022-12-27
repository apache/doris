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

#include <boost/stacktrace.hpp>
#include <queue>

#include "runtime/fragment_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/pretty_printer.h"
#include "util/stack_util.h"

namespace doris {

struct TrackerLimiterGroup {
    std::list<MemTrackerLimiter*> trackers;
    std::mutex group_lock;
};

// Save all MemTrackerLimiters in use.
// Each group corresponds to several MemTrackerLimiters and has a lock.
// Multiple groups are used to reduce the impact of locks.
static std::vector<TrackerLimiterGroup> mem_tracker_limiter_pool(1000);

std::atomic<bool> MemTrackerLimiter::_enable_print_log_process_usage {true};
bool MemTrackerLimiter::_oom_avoidance {true};

MemTrackerLimiter::MemTrackerLimiter(Type type, const std::string& label, int64_t byte_limit,
                                     RuntimeProfile* profile,
                                     const std::string& profile_counter_name) {
    DCHECK_GE(byte_limit, -1);
    if (profile == nullptr) {
        _consumption = std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES);
    } else {
        _consumption = profile->AddSharedHighWaterMarkCounter(profile_counter_name, TUnit::BYTES);
    }
    _type = type;
    _label = label;
    _limit = byte_limit;
    if (_type == Type::GLOBAL) {
        _group_num = 0;
    } else {
        _group_num = random() % 999 + 1;
    }
    {
        std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[_group_num].group_lock);
        _tracker_limiter_group_it = mem_tracker_limiter_pool[_group_num].trackers.insert(
                mem_tracker_limiter_pool[_group_num].trackers.end(), this);
    }
}

MemTrackerLimiter::~MemTrackerLimiter() {
    if (_type == Type::GLOBAL) return;
    consume(_untracked_mem);
    // mem hook record tracker cannot guarantee that the final consumption is 0,
    // nor can it guarantee that the memory alloc and free are recorded in a one-to-one correspondence.
    // In order to ensure `consumption of all limiter trackers` + `orphan tracker consumption` = `process tracker consumption`
    // in real time. Merge its consumption into orphan when parent is process, to avoid repetition.
    ExecEnv::GetInstance()->orphan_mem_tracker()->consume(_consumption->current_value());
    _consumption->set(0);
    {
        std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[_group_num].group_lock);
        if (_tracker_limiter_group_it != mem_tracker_limiter_pool[_group_num].trackers.end()) {
            mem_tracker_limiter_pool[_group_num].trackers.erase(_tracker_limiter_group_it);
            _tracker_limiter_group_it = mem_tracker_limiter_pool[_group_num].trackers.end();
        }
    }
}

MemTracker::Snapshot MemTrackerLimiter::make_snapshot() const {
    Snapshot snapshot;
    snapshot.type = TypeString[_type];
    snapshot.label = _label;
    snapshot.limit = _limit;
    snapshot.cur_consumption = _consumption->current_value();
    snapshot.peak_consumption = _consumption->value();
    return snapshot;
}

void MemTrackerLimiter::refresh_global_counter() {
    std::unordered_map<Type, int64_t> type_mem_sum = {
            {Type::GLOBAL, 0},     {Type::QUERY, 0},         {Type::LOAD, 0},
            {Type::COMPACTION, 0}, {Type::SCHEMA_CHANGE, 0}, {Type::CLONE, 0},
            {Type::BATCHLOAD, 0},  {Type::CONSISTENCY, 0}};
    for (unsigned i = 0; i < mem_tracker_limiter_pool.size(); ++i) {
        std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[i].group_lock);
        for (auto tracker : mem_tracker_limiter_pool[i].trackers) {
            type_mem_sum[tracker->type()] += tracker->consumption();
        }
    }
    for (auto it : type_mem_sum) {
        MemTrackerLimiter::TypeMemSum[it.first]->set(it.second);
    }
}

void MemTrackerLimiter::make_process_snapshots(std::vector<MemTracker::Snapshot>* snapshots) {
    MemTrackerLimiter::refresh_global_counter();
    int64_t process_mem_sum = 0;
    Snapshot snapshot;
    for (auto it : MemTrackerLimiter::TypeMemSum) {
        snapshot.type = TypeString[it.first];
        snapshot.label = "";
        snapshot.limit = -1;
        snapshot.cur_consumption = it.second->current_value();
        snapshot.peak_consumption = it.second->value();
        (*snapshots).emplace_back(snapshot);
        process_mem_sum += it.second->current_value();
    }

    snapshot.type = "tc/jemalloc_cache";
    snapshot.label = "";
    snapshot.limit = -1;
    snapshot.cur_consumption = MemInfo::allocator_cache_mem();
    snapshot.peak_consumption = -1;
    (*snapshots).emplace_back(snapshot);
    process_mem_sum += MemInfo::allocator_cache_mem();

    snapshot.type = "process";
    snapshot.label = "";
    snapshot.limit = -1;
    snapshot.cur_consumption = process_mem_sum;
    snapshot.peak_consumption = -1;
    (*snapshots).emplace_back(snapshot);
}

void MemTrackerLimiter::make_type_snapshots(std::vector<MemTracker::Snapshot>* snapshots,
                                            MemTrackerLimiter::Type type) {
    if (type == Type::GLOBAL) {
        std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[0].group_lock);
        for (auto tracker : mem_tracker_limiter_pool[0].trackers) {
            (*snapshots).emplace_back(tracker->make_snapshot());
            MemTracker::make_group_snapshot(snapshots, tracker->group_num(), tracker->label());
        }
    } else {
        for (unsigned i = 1; i < mem_tracker_limiter_pool.size(); ++i) {
            std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[i].group_lock);
            for (auto tracker : mem_tracker_limiter_pool[i].trackers) {
                if (tracker->type() == type) {
                    (*snapshots).emplace_back(tracker->make_snapshot());
                    MemTracker::make_group_snapshot(snapshots, tracker->group_num(),
                                                    tracker->label());
                }
            }
        }
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

void MemTrackerLimiter::print_log_usage(const std::string& msg) {
    if (_enable_print_log_usage) {
        _enable_print_log_usage = false;
        std::string detail = msg;
        detail += "\nProcess Memory Summary:\n    " + MemTrackerLimiter::process_mem_log_str();
        detail += "\nAlloc Stacktrace:\n" + get_stack_trace();
        detail += "\nMemory Tracker Summary:    " + log_usage();
        std::string child_trackers_usage;
        std::vector<MemTracker::Snapshot> snapshots;
        MemTracker::make_group_snapshot(&snapshots, _group_num, _label);
        for (const auto& snapshot : snapshots) {
            child_trackers_usage += "\n    " + MemTracker::log_usage(snapshot);
        }
        if (!child_trackers_usage.empty()) detail += child_trackers_usage;

        LOG(WARNING) << detail;
    }
}

void MemTrackerLimiter::print_log_process_usage(const std::string& msg, bool with_stacktrace) {
    if (MemTrackerLimiter::_enable_print_log_process_usage) {
        MemTrackerLimiter::_enable_print_log_process_usage = false;
        std::string detail = msg;
        detail += "\nProcess Memory Summary:\n    " + MemTrackerLimiter::process_mem_log_str();
        if (with_stacktrace) detail += "\nAlloc Stacktrace:\n" + get_stack_trace();
        std::vector<MemTracker::Snapshot> snapshots;
        MemTrackerLimiter::make_process_snapshots(&snapshots);
        MemTrackerLimiter::make_type_snapshots(&snapshots, MemTrackerLimiter::Type::GLOBAL);
        detail += "\nMemory Tracker Summary:";
        for (const auto& snapshot : snapshots) {
            if (snapshot.label == "" && snapshot.parent_label == "") {
                detail += "\n    " + MemTrackerLimiter::type_log_usage(snapshot);
            } else if (snapshot.parent_label == "") {
                detail += "\n    " + MemTrackerLimiter::log_usage(snapshot);
            } else {
                detail += "\n    " + MemTracker::log_usage(snapshot);
            }
        }
        LOG(WARNING) << detail;
    }
}

std::string MemTrackerLimiter::mem_limit_exceeded(const std::string& msg,
                                                  const std::string& limit_exceeded_errmsg) {
    STOP_CHECK_THREAD_MEM_TRACKER_LIMIT();
    std::string detail = fmt::format(
            "Memory limit exceeded:<consuming tracker:<{}>, {}>, executing msg:<{}>. backend {} "
            "process memory used {}, limit {}. If query tracker exceed, `set "
            "exec_mem_limit=8G` to change limit, details see be.INFO.",
            _label, limit_exceeded_errmsg, msg, BackendOptions::get_localhost(),
            PerfCounters::get_vm_rss_str(), MemInfo::mem_limit_str());
    return detail;
}

Status MemTrackerLimiter::fragment_mem_limit_exceeded(RuntimeState* state, const std::string& msg,
                                                      int64_t failed_alloc_size) {
    auto failed_msg =
            mem_limit_exceeded(msg, tracker_limit_exceeded_errmsg_str(failed_alloc_size, this));
    print_log_usage(failed_msg);
    state->log_error(failed_msg);
    return Status::MemoryLimitExceeded(failed_msg);
}

int64_t MemTrackerLimiter::free_top_memory_query(int64_t min_free_mem) {
    std::priority_queue<std::pair<int64_t, std::string>,
                        std::vector<std::pair<int64_t, std::string>>,
                        std::greater<std::pair<int64_t, std::string>>>
            min_pq;
    // After greater than min_free_mem, will not be modified.
    int64_t prepare_free_mem = 0;

    auto cancel_top_query = [&](auto min_pq) -> int64_t {
        std::vector<std::string> usage_strings;
        int64_t freed_mem = 0;
        while (!min_pq.empty()) {
            TUniqueId cancelled_queryid = label_to_queryid(min_pq.top().second);
            ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                    cancelled_queryid, PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED,
                    fmt::format("Process has no memory available, cancel top memory usage query: "
                                "query memory tracker <{}> consumption {}, backend {} "
                                "process memory used {} exceed limit {} or sys mem available {} "
                                "less than low water mark {}. Execute again after enough memory, "
                                "details see be.INFO.",
                                min_pq.top().second, print_bytes(min_pq.top().first),
                                BackendOptions::get_localhost(), PerfCounters::get_vm_rss_str(),
                                MemInfo::mem_limit_str(), MemInfo::sys_mem_available_str(),
                                print_bytes(MemInfo::sys_mem_available_low_water_mark())));

            freed_mem += min_pq.top().first;
            usage_strings.push_back(fmt::format("{} memory usage {} Bytes", min_pq.top().second,
                                                min_pq.top().first));
            min_pq.pop();
        }
        if (!usage_strings.empty()) {
            LOG(INFO) << "Process GC Free Top Memory Usage Query: " << join(usage_strings, ",");
        }
        return freed_mem;
    };

    for (unsigned i = 1; i < mem_tracker_limiter_pool.size(); ++i) {
        std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[i].group_lock);
        for (auto tracker : mem_tracker_limiter_pool[i].trackers) {
            if (tracker->type() == Type::QUERY) {
                if (tracker->consumption() > min_free_mem) {
                    std::priority_queue<std::pair<int64_t, std::string>,
                                        std::vector<std::pair<int64_t, std::string>>,
                                        std::greater<std::pair<int64_t, std::string>>>
                            min_pq_null;
                    std::swap(min_pq, min_pq_null);
                    min_pq.push(std::pair<int64_t, std::string>(tracker->consumption(),
                                                                tracker->label()));
                    return cancel_top_query(min_pq);
                } else if (tracker->consumption() + prepare_free_mem < min_free_mem) {
                    min_pq.push(std::pair<int64_t, std::string>(tracker->consumption(),
                                                                tracker->label()));
                    prepare_free_mem += tracker->consumption();
                } else if (tracker->consumption() > min_pq.top().first) {
                    // No need to modify prepare_free_mem, prepare_free_mem will always be greater than min_free_mem.
                    min_pq.push(std::pair<int64_t, std::string>(tracker->consumption(),
                                                                tracker->label()));
                    min_pq.pop();
                }
            }
        }
    }
    return cancel_top_query(min_pq);
}

int64_t MemTrackerLimiter::free_top_overcommit_query(int64_t min_free_mem) {
    std::priority_queue<std::pair<int64_t, std::string>,
                        std::vector<std::pair<int64_t, std::string>>,
                        std::greater<std::pair<int64_t, std::string>>>
            min_pq;
    std::unordered_map<std::string, int64_t> query_consumption;

    for (unsigned i = 1; i < mem_tracker_limiter_pool.size(); ++i) {
        std::lock_guard<std::mutex> l(mem_tracker_limiter_pool[i].group_lock);
        for (auto tracker : mem_tracker_limiter_pool[i].trackers) {
            if (tracker->type() == Type::QUERY) {
                int64_t overcommit_ratio =
                        (static_cast<double>(tracker->consumption()) / tracker->limit()) * 10000;
                if (overcommit_ratio == 0) { // Small query does not cancel
                    continue;
                }
                min_pq.push(std::pair<int64_t, std::string>(overcommit_ratio, tracker->label()));
                query_consumption[tracker->label()] = tracker->consumption();
            }
        }
    }

    std::priority_queue<std::pair<int64_t, std::string>> max_pq;
    // Min-heap to Max-heap.
    while (!min_pq.empty()) {
        max_pq.push(min_pq.top());
        min_pq.pop();
    }

    std::vector<std::string> usage_strings;
    int64_t freed_mem = 0;
    while (!max_pq.empty()) {
        TUniqueId cancelled_queryid = label_to_queryid(max_pq.top().second);
        int64_t query_mem = query_consumption[max_pq.top().second];
        ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
                cancelled_queryid, PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED,
                fmt::format("Process has no memory available, cancel top memory usage query: "
                            "query memory tracker <{}> consumption {}, backend {} "
                            "process memory used {} exceed limit {} or sys mem available {} "
                            "less than low water mark {}. Execute again after enough memory, "
                            "details see be.INFO.",
                            max_pq.top().second, print_bytes(query_mem),
                            BackendOptions::get_localhost(), PerfCounters::get_vm_rss_str(),
                            MemInfo::mem_limit_str(), MemInfo::sys_mem_available_str(),
                            print_bytes(MemInfo::sys_mem_available_low_water_mark())));

        usage_strings.push_back(fmt::format("{} memory usage {} Bytes, overcommit ratio: {}",
                                            max_pq.top().second, query_mem, max_pq.top().first));
        freed_mem += query_mem;
        if (freed_mem > min_free_mem) {
            break;
        }
        max_pq.pop();
    }
    if (!usage_strings.empty()) {
        LOG(INFO) << "Process GC Free Top Memory Overcommit Query: " << join(usage_strings, ",");
    }
    return freed_mem;
}

} // namespace doris
