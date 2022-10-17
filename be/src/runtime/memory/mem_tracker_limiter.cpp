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

#include "gutil/once.h"
#include "gutil/walltime.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/pretty_printer.h"
#include "util/string_util.h"

namespace doris {

MemTrackerLimiter::MemTrackerLimiter(int64_t byte_limit, const std::string& label,
                                     const std::shared_ptr<MemTrackerLimiter>& parent,
                                     RuntimeProfile* profile) {
    DCHECK_GE(byte_limit, -1);
    if (profile == nullptr) {
        _consumption = std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES);
    } else {
        _consumption = profile->AddSharedHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES);
    }
    _label = label;
    _limit = byte_limit;
    _group_num = GetCurrentTimeMicros() % 1000;
    if (parent || label == "Process") {
        _parent = parent;
    } else if (thread_context()->_thread_mem_tracker_mgr->limiter_mem_tracker_raw()->label() ==
               "Orphan") {
        _parent = ExecEnv::GetInstance()->process_mem_tracker();
    } else {
        _parent = thread_context()->_thread_mem_tracker_mgr->limiter_mem_tracker();
    }
    DCHECK(_parent || label == "Process");

    // Walks the MemTrackerLimiter hierarchy and populates _all_ancestors and _limited_ancestors
    MemTrackerLimiter* tracker = this;
    while (tracker != nullptr) {
        _all_ancestors.push_back(tracker);
        // Process tracker does not participate in the process memory limit, process tracker consumption is virtual memory,
        // and there is a diff between the real physical memory value of the process. It is replaced by check_sys_mem_info.
        if (tracker->has_limit() && tracker->label() != "Process")
            _limited_ancestors.push_back(tracker);
        tracker = tracker->_parent.get();
    }
    DCHECK_GT(_all_ancestors.size(), 0);
    DCHECK_EQ(_all_ancestors[0], this);
    if (_parent) {
        std::lock_guard<std::mutex> l(_parent->_child_tracker_limiter_lock);
        _child_tracker_it = _parent->_child_tracker_limiters.insert(
                _parent->_child_tracker_limiters.end(), this);
        _had_child_count++;
    }
}

MemTrackerLimiter::~MemTrackerLimiter() {
    // TCMalloc hook will be triggered during destructor memtracker, may cause crash.
    if (_label == "Process") doris::thread_context_ptr.init = false;
    DCHECK(remain_child_count() == 0 || _label == "Process");
    // In order to ensure `consumption of all limiter trackers` + `orphan tracker consumption` = `process tracker consumption`
    // in real time. Merge its consumption into orphan when parent is process, to avoid repetition.
    if (_parent && _parent->label() == "Process") {
        ExecEnv::GetInstance()->orphan_mem_tracker_raw()->cache_consume_local(
                _consumption->current_value());
    }
    if (_reset_zero) {
        reset_zero();
        _all_ancestors.clear();
        _all_ancestors.push_back(ExecEnv::GetInstance()->orphan_mem_tracker_raw());
    }
    consume_local(_untracked_mem);
    if (_parent) {
        std::lock_guard<std::mutex> l(_parent->_child_tracker_limiter_lock);
        if (_child_tracker_it != _parent->_child_tracker_limiters.end()) {
            _parent->_child_tracker_limiters.erase(_child_tracker_it);
            _child_tracker_it = _parent->_child_tracker_limiters.end();
        }
    }
}

MemTracker::Snapshot MemTrackerLimiter::make_snapshot(size_t level) const {
    Snapshot snapshot;
    snapshot.label = _label;
    snapshot.parent = _parent != nullptr ? _parent->label() : "Root";
    snapshot.level = level;
    snapshot.limit = _limit;
    snapshot.cur_consumption = _consumption->current_value();
    snapshot.peak_consumption = _consumption->value();
    snapshot.child_count = remain_child_count();
    return snapshot;
}

void MemTrackerLimiter::make_snapshot(std::vector<MemTracker::Snapshot>* snapshots,
                                      size_t cur_level, size_t upper_level) const {
    Snapshot snapshot = MemTrackerLimiter::make_snapshot(cur_level);
    (*snapshots).emplace_back(snapshot);
    if (cur_level < upper_level) {
        {
            std::lock_guard<std::mutex> l(_child_tracker_limiter_lock);
            for (const auto& child : _child_tracker_limiters) {
                child->make_snapshot(snapshots, cur_level + 1, upper_level);
            }
        }
        MemTracker::make_group_snapshot(snapshots, cur_level + 1, _group_num, _label);
    }
}

int64_t MemTrackerLimiter::spare_capacity() const {
    int64_t result = std::numeric_limits<int64_t>::max();
    for (const auto& tracker : _limited_ancestors) {
        int64_t mem_left = tracker->limit() - tracker->consumption();
        result = std::min(result, mem_left);
    }
    return result;
}

int64_t MemTrackerLimiter::get_lowest_limit() const {
    if (_limited_ancestors.empty()) return -1;
    int64_t min_limit = std::numeric_limits<int64_t>::max();
    for (const auto& tracker : _limited_ancestors) {
        DCHECK(tracker->has_limit());
        min_limit = std::min(min_limit, tracker->limit());
    }
    return min_limit;
}

// Calling this on the query tracker results in output like:
//
//  Query(4a4c81fedaed337d:4acadfda00000000) Limit=10.00 GB Total=508.28 MB Peak=508.45 MB
//    Fragment 4a4c81fedaed337d:4acadfda00000000: Total=8.00 KB Peak=8.00 KB
//      EXCHANGE_NODE (id=4): Total=0 Peak=0
//      DataStreamRecvr: Total=0 Peak=0
//    Block Manager: Limit=6.68 GB Total=394.00 MB Peak=394.00 MB
//    Fragment 4a4c81fedaed337d:4acadfda00000006: Total=233.72 MB Peak=242.24 MB
//      AGGREGATION_NODE (id=1): Total=139.21 MB Peak=139.84 MB
//      HDFS_SCAN_NODE (id=0): Total=93.94 MB Peak=102.24 MB
//      DataStreamSender (dst_id=2): Total=45.99 KB Peak=85.99 KB
//    Fragment 4a4c81fedaed337d:4acadfda00000003: Total=274.55 MB Peak=274.62 MB
//      AGGREGATION_NODE (id=3): Total=274.50 MB Peak=274.50 MB
//      EXCHANGE_NODE (id=2): Total=0 Peak=0
//      DataStreamRecvr: Total=45.91 KB Peak=684.07 KB
//      DataStreamSender (dst_id=4): Total=680.00 B Peak=680.00 B
//
// If 'reservation_metrics_' are set, we ge a more granular breakdown:
//   TrackerName: Limit=5.00 MB Reservation=5.00 MB OtherMemory=1.04 MB
//                Total=6.04 MB Peak=6.45 MB
//
std::string MemTrackerLimiter::log_usage(int max_recursive_depth, int64_t* logged_consumption) {
    int64_t curr_consumption = consumption();
    int64_t peak_consumption = _consumption->value();
    if (logged_consumption != nullptr) *logged_consumption = curr_consumption;

    std::string detail =
            "MemTrackerLimiter Label={}, Limit={}({} B), Used={}({} B), Peak={}({} B), Exceeded={}";
    detail = fmt::format(detail, _label, print_bytes(_limit), _limit, print_bytes(curr_consumption),
                         curr_consumption, print_bytes(peak_consumption), peak_consumption,
                         limit_exceeded() ? "true" : "false");

    // This call does not need the children, so return early.
    if (max_recursive_depth == 0) return detail;

    // Recurse and get information about the children
    int64_t child_consumption;
    std::string child_trackers_usage;
    {
        std::lock_guard<std::mutex> l(_child_tracker_limiter_lock);
        child_trackers_usage =
                log_usage(max_recursive_depth - 1, _child_tracker_limiters, &child_consumption);
    }
    std::vector<MemTracker::Snapshot> snapshots;
    MemTracker::make_group_snapshot(&snapshots, 0, _group_num, _label);
    for (const auto& snapshot : snapshots) {
        child_trackers_usage += "\n    " + MemTracker::log_usage(snapshot);
    }
    if (!child_trackers_usage.empty()) detail += child_trackers_usage;
    return detail;
}

std::string MemTrackerLimiter::log_usage(int max_recursive_depth,
                                         const std::list<MemTrackerLimiter*>& trackers,
                                         int64_t* logged_consumption) {
    *logged_consumption = 0;
    std::vector<std::string> usage_strings;
    for (const auto& tracker : trackers) {
        int64_t tracker_consumption;
        std::string usage_string = tracker->log_usage(max_recursive_depth, &tracker_consumption);
        if (!usage_string.empty()) usage_strings.push_back(usage_string);
        *logged_consumption += tracker_consumption;
    }
    return usage_strings.size() == 0 ? "" : "\n    " + join(usage_strings, "\n    ");
}

void MemTrackerLimiter::print_log_usage(const std::string& msg) {
    // only print the tracker log_usage in be log.
    std::string detail = msg;
    detail += "\n    " + fmt::format(
                                 "process memory used {}, limit {}, hard limit {}, tc/jemalloc "
                                 "allocator cache {}",
                                 PerfCounters::get_vm_rss_str(), MemInfo::mem_limit_str(),
                                 print_bytes(MemInfo::hard_mem_limit()),
                                 MemInfo::allocator_cache_mem_str());
    if (_print_log_usage) {
        if (_label == "Process") {
            // Dumping the process MemTracker is expensive. Limiting the recursive depth to two
            // levels limits the level of detail to a one-line summary for each query MemTracker.
            detail += "\n    " + log_usage(2);
        } else {
            detail += "\n    " + log_usage();
        }
        // TODO: memory leak by calling `boost::stacktrace` in tcmalloc hook,
        // test whether overwriting malloc/free is the same problem in jemalloc/tcmalloc.
        // detail += "\n" + boost::stacktrace::to_string(boost::stacktrace::stacktrace());
        LOG(WARNING) << detail;
        _print_log_usage = false;
    }
}

std::string MemTrackerLimiter::mem_limit_exceeded(const std::string& msg,
                                                  int64_t failed_allocation_size) {
    STOP_CHECK_THREAD_MEM_TRACKER_LIMIT();
    std::string detail = fmt::format("Memory limit exceeded:<consuming tracker:<{}>, ", _label);
    MemTrackerLimiter* exceeded_tracker = nullptr;
    MemTrackerLimiter* max_consumption_tracker = nullptr;
    int64_t free_size = INT64_MAX;
    // Find the tracker that exceed limit and has the least free.
    for (const auto& tracker : _limited_ancestors) {
        int64_t max_consumption = tracker->peak_consumption() > tracker->consumption()
                                          ? tracker->peak_consumption()
                                          : tracker->consumption();
        if (tracker->limit() < max_consumption + failed_allocation_size) {
            exceeded_tracker = tracker;
            break;
        }
        if (tracker->limit() - max_consumption < free_size) {
            free_size = tracker->limit() - max_consumption;
            max_consumption_tracker = tracker;
        }
    }

    MemTrackerLimiter* print_log_usage_tracker = nullptr;
    if (exceeded_tracker != nullptr) {
        detail += limit_exceeded_errmsg_prefix_str(failed_allocation_size, exceeded_tracker);
        print_log_usage_tracker = exceeded_tracker;
    } else if (sys_mem_exceed_limit_check(failed_allocation_size)) {
        detail += fmt::format("{}>, executing msg:<{}>",
                              limit_exceeded_errmsg_sys_str(failed_allocation_size), msg);
    } else if (max_consumption_tracker != nullptr) {
        // must after check_sys_mem_info false
        detail += fmt::format(
                "failed alloc size {}, max consumption tracker:<{}>, limit {}, peak used {}, "
                "current used {}>, executing msg:<{}>",
                print_bytes(failed_allocation_size), max_consumption_tracker->label(),
                print_bytes(max_consumption_tracker->limit()),
                print_bytes(max_consumption_tracker->peak_consumption()),
                print_bytes(max_consumption_tracker->consumption()), msg);
        print_log_usage_tracker = max_consumption_tracker;
    } else {
        // The limit of the current tracker and parents is less than 0, the consume will not fail,
        // and the current process memory has no excess limit.
        detail += fmt::format("unknown exceed reason, executing msg:<{}>", msg);
        print_log_usage_tracker = ExecEnv::GetInstance()->process_mem_tracker().get();
    }
    auto failed_msg = MemTrackerLimiter::limit_exceeded_errmsg_suffix_str(detail);
    if (print_log_usage_tracker != nullptr) print_log_usage_tracker->print_log_usage(failed_msg);
    return failed_msg;
}

std::string MemTrackerLimiter::mem_limit_exceeded(const std::string& msg,
                                                  MemTrackerLimiter* failed_tracker,
                                                  const std::string& limit_exceeded_errmsg_prefix) {
    STOP_CHECK_THREAD_MEM_TRACKER_LIMIT();
    std::string detail =
            fmt::format("Memory limit exceeded:<consuming tracker:<{}>, {}>, executing msg:<{}>",
                        _label, limit_exceeded_errmsg_prefix, msg);
    auto failed_msg = MemTrackerLimiter::limit_exceeded_errmsg_suffix_str(detail);
    failed_tracker->print_log_usage(failed_msg);
    return failed_msg;
}

Status MemTrackerLimiter::mem_limit_exceeded(RuntimeState* state, const std::string& msg,
                                             int64_t failed_alloc_size) {
    auto failed_msg = mem_limit_exceeded(msg, failed_alloc_size);
    state->log_error(failed_msg);
    return Status::MemoryLimitExceeded(failed_msg);
}

} // namespace doris
