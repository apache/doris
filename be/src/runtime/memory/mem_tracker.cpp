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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/mem-tracker.cpp
// and modified by Doris

#include "runtime/memory/mem_tracker.h"

#include <fmt/format.h>

#include "runtime/thread_context.h"
#include "util/string_util.h"
#include "util/time.h"

namespace doris {

struct TrackerGroup {
    std::list<MemTracker*> trackers;
    std::mutex group_lock;
};

// Save all MemTrackers in use to maintain the weak relationship between MemTracker and MemTrackerLimiter.
// When MemTrackerLimiter prints statistics, all MemTracker statistics with weak relationship will be printed together.
// Each group corresponds to several MemTrackerLimiters and has a lock.
// Multiple groups are used to reduce the impact of locks.
static std::vector<TrackerGroup> mem_tracker_pool(1000);

MemTracker::MemTracker(const std::string& label, RuntimeProfile* profile, MemTrackerLimiter* parent,
                       const std::string& profile_counter_name, bool only_track_alloc)
        : _label(label), _only_track_alloc(only_track_alloc) {
    if (profile == nullptr) {
        _consumption = std::make_shared<RuntimeProfile::HighWaterMarkCounter>(TUnit::BYTES);
    } else {
        // By default, memory consumption is tracked via calls to consume()/release(), either to
        // the tracker itself or to one of its descendents. Alternatively, a consumption metric
        // can be specified, and then the metric's value is used as the consumption rather than
        // the tally maintained by consume() and release(). A tcmalloc metric is used to track
        // process memory consumption, since the process memory usage may be higher than the
        // computed total memory (tcmalloc does not release deallocated memory immediately).
        // Other consumption metrics are used in trackers below the process level to account
        // for memory (such as free buffer pool buffers) that is not tracked by consume() and
        // release().
        _consumption = profile->AddSharedHighWaterMarkCounter(profile_counter_name, TUnit::BYTES);
    }

    if (parent) {
        _parent_label = parent->label();
        _parent_group_num = parent->group_num();
    } else if (thread_context_ptr.init) {
        _parent_label = thread_context()->thread_mem_tracker()->label();
        _parent_group_num = thread_context()->thread_mem_tracker()->group_num();
    }
    {
        std::lock_guard<std::mutex> l(mem_tracker_pool[_parent_group_num].group_lock);
        _tracker_group_it = mem_tracker_pool[_parent_group_num].trackers.insert(
                mem_tracker_pool[_parent_group_num].trackers.end(), this);
    }
}

MemTracker::~MemTracker() {
    if (_parent_group_num != -1) {
        std::lock_guard<std::mutex> l(mem_tracker_pool[_parent_group_num].group_lock);
        if (_tracker_group_it != mem_tracker_pool[_parent_group_num].trackers.end()) {
            mem_tracker_pool[_parent_group_num].trackers.erase(_tracker_group_it);
            _tracker_group_it = mem_tracker_pool[_parent_group_num].trackers.end();
        }
    }
}

MemTracker::Snapshot MemTracker::make_snapshot() const {
    Snapshot snapshot;
    snapshot.label = _label;
    snapshot.parent_label = _parent_label;
    snapshot.limit = -1;
    snapshot.cur_consumption = _consumption->current_value();
    snapshot.peak_consumption = _consumption->value();
    return snapshot;
}

void MemTracker::make_group_snapshot(std::vector<MemTracker::Snapshot>* snapshots,
                                     int64_t group_num, std::string parent_label) {
    std::lock_guard<std::mutex> l(mem_tracker_pool[group_num].group_lock);
    for (auto tracker : mem_tracker_pool[group_num].trackers) {
        if (tracker->parent_label() == parent_label) {
            snapshots->push_back(tracker->make_snapshot());
        }
    }
}

std::string MemTracker::log_usage(MemTracker::Snapshot snapshot) {
    return fmt::format("MemTracker Label={}, Parent Label={}, Used={}({} B), Peak={}({} B)",
                       snapshot.label, snapshot.parent_label, print_bytes(snapshot.cur_consumption),
                       snapshot.cur_consumption, print_bytes(snapshot.peak_consumption),
                       snapshot.peak_consumption);
}

} // namespace doris