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

#include <mutex>

#include "bvar/bvar.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"

namespace doris {

bvar::Adder<int64_t> g_memtracker_cnt("memtracker_cnt");

// Save all MemTrackers in use to maintain the weak relationship between MemTracker and MemTrackerLimiter.
// When MemTrackerLimiter prints statistics, all MemTracker statistics with weak relationship will be printed together.
// Each group corresponds to several MemTrackerLimiters and has a lock.
// Multiple groups are used to reduce the impact of locks.
std::vector<MemTracker::TrackerGroup> MemTracker::mem_tracker_pool(1000);

MemTracker::MemTracker(const std::string& label, MemTrackerLimiter* parent) : _label(label) {
    _consumption = std::make_shared<MemCounter>();
    bind_parent(parent);
}

void MemTracker::bind_parent(MemTrackerLimiter* parent) {
    if (parent) {
        _parent_label = parent->label();
        _parent_group_num = parent->group_num();
    } else if (doris::is_thread_context_init()) {
        _parent_label = thread_context()->thread_mem_tracker()->label();
        _parent_group_num = thread_context()->thread_mem_tracker()->group_num();
    }
    {
        std::lock_guard<std::mutex> l(mem_tracker_pool[_parent_group_num].group_lock);
        _tracker_group_it = mem_tracker_pool[_parent_group_num].trackers.insert(
                mem_tracker_pool[_parent_group_num].trackers.end(), this);
    }
    g_memtracker_cnt << 1;
}

MemTracker::~MemTracker() {
    if (_parent_group_num != -1) {
        std::lock_guard<std::mutex> l(mem_tracker_pool[_parent_group_num].group_lock);
        if (_tracker_group_it != mem_tracker_pool[_parent_group_num].trackers.end()) {
            mem_tracker_pool[_parent_group_num].trackers.erase(_tracker_group_it);
            _tracker_group_it = mem_tracker_pool[_parent_group_num].trackers.end();
        }
        g_memtracker_cnt << -1;
    }
}

MemTracker::Snapshot MemTracker::make_snapshot() const {
    Snapshot snapshot;
    snapshot.label = _label;
    snapshot.parent_label = _parent_label;
    snapshot.limit = -1;
    snapshot.cur_consumption = _consumption->current_value();
    snapshot.peak_consumption = _consumption->peak_value();
    return snapshot;
}

void MemTracker::make_group_snapshot(std::vector<MemTracker::Snapshot>* snapshots,
                                     int64_t group_num, std::string parent_label) {
    std::lock_guard<std::mutex> l(mem_tracker_pool[group_num].group_lock);
    for (auto tracker : mem_tracker_pool[group_num].trackers) {
        if (tracker->parent_label() == parent_label && tracker->peak_consumption() != 0) {
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