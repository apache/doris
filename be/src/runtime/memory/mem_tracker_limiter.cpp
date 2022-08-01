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
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/pretty_printer.h"
#include "util/string_util.h"

namespace doris {

MemTrackerLimiter::MemTrackerLimiter(int64_t byte_limit, const std::string& label,
                                     const std::shared_ptr<MemTrackerLimiter>& parent,
                                     RuntimeProfile* profile)
        : MemTracker(label, profile, true) {
    DCHECK_GE(byte_limit, -1);
    _limit = byte_limit;
    _group_num = GetCurrentTimeMicros() % 1000;
    _parent = parent ? parent : thread_context()->_thread_mem_tracker_mgr->limiter_mem_tracker();
    DCHECK(_parent || label == "Process");

    // Walks the MemTrackerLimiter hierarchy and populates _all_ancestors and _limited_ancestors
    MemTrackerLimiter* tracker = this;
    while (tracker != nullptr) {
        _all_ancestors.push_back(tracker);
        if (tracker->has_limit()) _limited_ancestors.push_back(tracker);
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
    if (_label == "Process") doris::thread_context_ptr._init = false;
    DCHECK(remain_child_count() == 0 || _label == "Process");
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

bool MemTrackerLimiter::gc_memory(int64_t max_consumption) {
    if (max_consumption < 0) return true;
    std::lock_guard<std::mutex> l(_gc_lock);
    int64_t pre_gc_consumption = consumption();
    // Check if someone gc'd before us
    if (pre_gc_consumption < max_consumption) return false;

    int64_t curr_consumption = pre_gc_consumption;
    // Free some extra memory to avoid frequent GC, 4M is an empirical value, maybe it will be tested later.
    const int64_t EXTRA_BYTES_TO_FREE = 4L * 1024L * 1024L * 1024L;
    // Try to free up some memory
    for (int i = 0; i < _gc_functions.size(); ++i) {
        // Try to free up the amount we are over plus some extra so that we don't have to
        // immediately GC again. Don't free all the memory since that can be unnecessarily
        // expensive.
        int64_t bytes_to_free = curr_consumption - max_consumption + EXTRA_BYTES_TO_FREE;
        _gc_functions[i](bytes_to_free);
        curr_consumption = consumption();
        if (max_consumption - curr_consumption <= EXTRA_BYTES_TO_FREE) break;
    }

    return curr_consumption > max_consumption;
}

Status MemTrackerLimiter::try_gc_memory(int64_t bytes) {
    if (UNLIKELY(gc_memory(_limit - bytes))) {
        return Status::MemoryLimitExceeded(
                fmt::format("label={} TryConsume failed size={}, used={}, limit={}", label(), bytes,
                            _consumption->current_value(), _limit));
    }
    VLOG_NOTICE << "GC succeeded, TryConsume bytes=" << bytes
                << " consumption=" << _consumption->current_value() << " limit=" << _limit;
    return Status::OK();
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

    std::string detail = "MemTrackerLimiter Label={}, Limit={}, Used={}, Peak={}, Exceeded={}";
    detail = fmt::format(detail, _label, PrettyPrinter::print(_limit, TUnit::BYTES),
                         PrettyPrinter::print(curr_consumption, TUnit::BYTES),
                         PrettyPrinter::print(peak_consumption, TUnit::BYTES),
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
        child_trackers_usage += MemTracker::log_usage(snapshot);
    }
    if (!child_trackers_usage.empty()) detail += "\n" + child_trackers_usage;
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
    return join(usage_strings, "\n");
}

Status MemTrackerLimiter::mem_limit_exceeded(RuntimeState* state, const std::string& details,
                                             int64_t failed_allocation_size, Status failed_alloc) {
    STOP_CHECK_THREAD_MEM_TRACKER_LIMIT();
    std::string detail =
            "Memory exceed limit. fragment={}, details={}, on backend={}. Memory left in process "
            "limit={}.";
    detail = fmt::format(
            detail, state != nullptr ? print_id(state->fragment_instance_id()) : "", details,
            BackendOptions::get_localhost(),
            PrettyPrinter::print(ExecEnv::GetInstance()->process_mem_tracker()->spare_capacity(),
                                 TUnit::BYTES));
    if (!failed_alloc) {
        detail += " failed alloc=<{}>. current tracker={}.";
        detail = fmt::format(detail, failed_alloc.to_string(), _label);
    } else {
        detail += " current tracker <label={}, used={}, limit={}, failed alloc size={}>.";
        detail = fmt::format(detail, _label, _consumption->current_value(), _limit,
                             PrettyPrinter::print(failed_allocation_size, TUnit::BYTES));
    }
    detail += " If this is a query, can change the limit by session variable exec_mem_limit.";
    Status status = Status::MemoryLimitExceeded(detail);
    if (state != nullptr) state->log_error(detail);

    detail += "\n" + boost::stacktrace::to_string(boost::stacktrace::stacktrace());
    // only print the tracker log_usage in be log.
    if (ExecEnv::GetInstance()->process_mem_tracker()->spare_capacity() < failed_allocation_size) {
        // Dumping the process MemTracker is expensive. Limiting the recursive depth to two
        // levels limits the level of detail to a one-line summary for each query MemTracker.
        detail += "\n" + ExecEnv::GetInstance()->process_mem_tracker()->log_usage(2);
    } else {
        detail += "\n" + log_usage();
    }

    LOG(WARNING) << detail;
    return status;
}

} // namespace doris
