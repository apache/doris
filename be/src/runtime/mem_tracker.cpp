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

#include "runtime/mem_tracker.h"

#include <fmt/format.h>

#include <memory>

#include "exec/exec_node.h"
#include "gutil/once.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/pretty_printer.h"
#include "util/string_util.h"
#include "util/uid_util.h"

namespace doris {

const std::string MemTracker::COUNTER_NAME = "PeakMemoryUsage";

// The ancestor for all trackers. Every tracker is visible from the process down.
// All manually created trackers should specify the process tracker as the parent.
static std::shared_ptr<MemTracker> process_tracker;
static MemTracker* raw_process_tracker;
static GoogleOnceType process_tracker_once = GOOGLE_ONCE_INIT;

void MemTracker::create_process_tracker() {
    process_tracker.reset(
            new MemTracker(-1, "Process", nullptr, MemTrackerLevel::OVERVIEW, nullptr));
    process_tracker->init();
    raw_process_tracker = process_tracker.get();
}

std::shared_ptr<MemTracker> MemTracker::get_process_tracker() {
    GoogleOnceInit(&process_tracker_once, &MemTracker::create_process_tracker);
    return process_tracker;
}

MemTracker* MemTracker::get_raw_process_tracker() {
    GoogleOnceInit(&process_tracker_once, &MemTracker::create_process_tracker);
    return raw_process_tracker;
}

void MemTracker::list_process_trackers(std::vector<std::shared_ptr<MemTracker>>* trackers) {
    trackers->clear();
    std::deque<std::shared_ptr<MemTracker>> to_process;
    to_process.push_front(get_process_tracker());
    while (!to_process.empty()) {
        std::shared_ptr<MemTracker> t = to_process.back();
        to_process.pop_back();

        trackers->push_back(t);
        std::list<std::weak_ptr<MemTracker>> children;
        {
            lock_guard<SpinLock> l(t->_child_trackers_lock);
            children = t->_child_trackers;
        }
        for (const auto& child_weak : children) {
            std::shared_ptr<MemTracker> child = child_weak.lock();
            if (child && static_cast<decltype(config::mem_tracker_level)>(child->_level) <=
                                 config::mem_tracker_level) {
                to_process.emplace_back(std::move(child));
            }
        }
    }
}

std::shared_ptr<MemTracker> MemTracker::create_tracker(int64_t byte_limit, const std::string& label,
                                                       const std::shared_ptr<MemTracker>& parent,
                                                       MemTrackerLevel level,
                                                       RuntimeProfile* profile) {
    std::shared_ptr<MemTracker> reset_parent = parent ? parent : thread_local_ctx.get()->_thread_mem_tracker_mgr->mem_tracker();
    DCHECK(reset_parent);

    std::shared_ptr<MemTracker> tracker(
            new MemTracker(byte_limit, label, reset_parent,
                           level > reset_parent->_level ? level : reset_parent->_level, profile));
    reset_parent->add_child_tracker(tracker);
    tracker->init();
    return tracker;
}

std::shared_ptr<MemTracker> MemTracker::create_virtual_tracker(
        int64_t byte_limit, const std::string& label, const std::shared_ptr<MemTracker>& parent,
        MemTrackerLevel level) {
   std::shared_ptr<MemTracker> reset_parent = parent ? parent : thread_local_ctx.get()->_thread_mem_tracker_mgr->mem_tracker();
    DCHECK(reset_parent);

    std::shared_ptr<MemTracker> tracker(
            new MemTracker(byte_limit, "[Virtual]-" + label, reset_parent, level, nullptr));
    reset_parent->add_child_tracker(tracker);
    tracker->init_virtual();
    return tracker;
}

MemTracker::MemTracker(int64_t byte_limit, const std::string& label)
        : MemTracker(byte_limit, label, std::shared_ptr<MemTracker>(), MemTrackerLevel::VERBOSE,
                     nullptr) {}

MemTracker::MemTracker(int64_t byte_limit, const std::string& label,
                       const std::shared_ptr<MemTracker>& parent, MemTrackerLevel level,
                       RuntimeProfile* profile)
        : _limit(byte_limit),
          _label(label),
          _id(_label + std::to_string(GetCurrentTimeMicros()) + std::to_string(rand())),
          _parent(parent),
          _level(level) {
    if (profile == nullptr) {
        _consumption = std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES);
    } else {
        _consumption = profile->AddSharedHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES);
    }
}

void MemTracker::init() {
    DCHECK_GE(_limit, -1);
    MemTracker* tracker = this;
    while (tracker != nullptr && tracker->_virtual == false) {
        _all_trackers.push_back(tracker);
        if (tracker->has_limit()) _limit_trackers.push_back(tracker);
        tracker = tracker->_parent.get();
    }
    DCHECK_GT(_all_trackers.size(), 0);
    DCHECK_EQ(_all_trackers[0], this);
}

void MemTracker::init_virtual() {
    DCHECK_GE(_limit, -1);
    _all_trackers.push_back(this);
    if (this->has_limit()) _limit_trackers.push_back(this);
    _virtual = true;
}

MemTracker::~MemTracker() {
    consume(_untracked_mem.exchange(0)); // before memory_leak_check
    // TCMalloc hook will be triggered during destructor memtracker, may cause crash.
    if (_label == "Process") GLOBAL_STOP_THREAD_LOCAL_MEM_TRACKER();
    if (!_virtual && config::memory_leak_detection) MemTracker::memory_leak_check(this);
    if (!_virtual && parent()) {
        // Do not call release on the parent tracker to avoid repeated releases.
        // Ensure that all consume/release are triggered by TCMalloc new/delete hook.
        lock_guard<SpinLock> l(_parent->_child_trackers_lock);
        if (_child_tracker_it != _parent->_child_trackers.end()) {
            _parent->_child_trackers.erase(_child_tracker_it);
            _child_tracker_it = _parent->_child_trackers.end();
        }
    }
    DCHECK_EQ(_untracked_mem, 0);
}

void MemTracker::transfer_to_relative(MemTracker* dst, int64_t bytes) {
    if (id() == dst->id()) return;
    DCHECK_EQ(_all_trackers.back(), dst->_all_trackers.back()) << "Must have same ancestor";
    DCHECK(!dst->has_limit());
    // Find the common ancestor and update trackers between 'this'/'dst' and
    // the common ancestor. This logic handles all cases, including the
    // two trackers being the same or being ancestors of each other because
    // 'all_trackers_' includes the current tracker.
    int ancestor_idx = _all_trackers.size() - 1;
    int dst_ancestor_idx = dst->_all_trackers.size() - 1;
    while (ancestor_idx > 0 && dst_ancestor_idx > 0 &&
           _all_trackers[ancestor_idx - 1] == dst->_all_trackers[dst_ancestor_idx - 1]) {
        DCHECK(!dst->_all_trackers[dst_ancestor_idx - 1]->has_limit());
        --ancestor_idx;
        --dst_ancestor_idx;
    }
    MemTracker* common_ancestor = _all_trackers[ancestor_idx];
    release_local(bytes, common_ancestor);
    dst->consume_local(bytes, common_ancestor);
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
std::string MemTracker::log_usage(int max_recursive_depth, int64_t* logged_consumption) {
    // Make sure the consumption is up to date.
    int64_t curr_consumption = consumption();
    int64_t peak_consumption = _consumption->value();
    if (logged_consumption != nullptr) *logged_consumption = curr_consumption;

    if (_level > MemTrackerLevel::INSTANCE && curr_consumption == 0) return "";

    std::string detail =
            "MemTracker log_usage Label: {}, Limit: {}, Total: {}, Peak: {}, Exceeded: {}";
    detail = fmt::format(detail, _label, PrettyPrinter::print(_limit, TUnit::BYTES),
                         PrettyPrinter::print(curr_consumption, TUnit::BYTES),
                         PrettyPrinter::print(peak_consumption, TUnit::BYTES),
                         limit_exceeded() ? "true" : "false");

    // This call does not need the children, so return early.
    if (max_recursive_depth == 0) return detail;

    // Recurse and get information about the children
    int64_t child_consumption;
    std::string child_trackers_usage;
    std::list<std::weak_ptr<MemTracker>> children;
    {
        lock_guard<SpinLock> l(_child_trackers_lock);
        children = _child_trackers;
    }
    child_trackers_usage = log_usage(max_recursive_depth - 1, children, &child_consumption);
    if (!child_trackers_usage.empty()) detail += "\n" + child_trackers_usage;
    return detail;
}

std::string MemTracker::log_usage(int max_recursive_depth,
                                  const std::list<std::weak_ptr<MemTracker>>& trackers,
                                  int64_t* logged_consumption) {
    *logged_consumption = 0;
    std::vector<std::string> usage_strings;
    for (const auto& tracker_weak : trackers) {
        std::shared_ptr<MemTracker> tracker = tracker_weak.lock();
        if (tracker) {
            int64_t tracker_consumption;
            std::string usage_string =
                    tracker->log_usage(max_recursive_depth, &tracker_consumption);
            if (!usage_string.empty()) usage_strings.push_back(usage_string);
            *logged_consumption += tracker_consumption;
        }
    }
    return join(usage_strings, "\n");
}

Status MemTracker::mem_limit_exceeded(RuntimeState* state, const std::string& details,
                                      int64_t failed_allocation_size, Status failed_alloc) {
    MemTracker* process_tracker = MemTracker::get_raw_process_tracker();
    std::string detail =
            "Memory exceed limit. fragment={}, details={}, on backend={}. Memory left in process "
            "limit={}.";
    detail = fmt::format(detail, state != nullptr ? print_id(state->fragment_instance_id()) : "",
                         details, BackendOptions::get_localhost(),
                         PrettyPrinter::print(process_tracker->spare_capacity(), TUnit::BYTES));
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

    // only print the tracker log_usage in be log.
    if (process_tracker->spare_capacity() < failed_allocation_size) {
        // Dumping the process MemTracker is expensive. Limiting the recursive depth to two
        // levels limits the level of detail to a one-line summary for each query MemTracker.
        detail += "\n" + process_tracker->log_usage(2);
    }
    if (parent_task_mem_tracker() != nullptr) {
        detail += "\n" + parent_task_mem_tracker()->log_usage();
    }
    LOG(WARNING) << detail;

    return status;
}

bool MemTracker::gc_memory(int64_t max_consumption) {
    if (max_consumption < 0) return true;
    lock_guard<std::mutex> l(_gc_lock);
    int64_t pre_gc_consumption = consumption();
    // Check if someone gc'd before us
    if (pre_gc_consumption < max_consumption) return false;

    int64_t curr_consumption = pre_gc_consumption;
    const int64_t EXTRA_BYTES_TO_FREE = 4L * 1024L * 1024L * 1024L; // TODO(zxy) Consider as config
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

} // namespace doris
