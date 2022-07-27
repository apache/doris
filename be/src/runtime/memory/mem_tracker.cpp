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
#include <parallel_hashmap/phmap.h>

#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/pretty_printer.h"
#include "util/time.h"

namespace doris {

const std::string MemTracker::COUNTER_NAME = "PeakMemoryUsage";

using StaticTrackersMap = phmap::parallel_flat_hash_map<
        std::string, MemTracker*, phmap::priv::hash_default_hash<std::string>,
        phmap::priv::hash_default_eq<std::string>,
        std::allocator<std::pair<const std::string, MemTracker*>>, 12, std::mutex>;

static StaticTrackersMap _static_mem_trackers;

MemTracker::MemTracker(const std::string& label, MemTrackerLimiter* parent, RuntimeProfile* profile,
                       bool is_limiter) {
    // Do not check limit exceed when add_child_tracker, otherwise it will cause deadlock when log_usage is called.
    STOP_CHECK_THREAD_MEM_TRACKER_LIMIT();
    _parent = parent ? parent : thread_context()->_thread_mem_tracker_mgr->limiter_mem_tracker();
    DCHECK(_parent || label == "Process");
    if (_parent && _parent->label().find("queryId=") != _parent->label().npos) {
        // Add the queryId suffix to the tracker below the query.
        _label = fmt::format("{}#{}", label,
                             _parent->label().substr(_parent->label().find("queryId="), -1));
    } else {
        _label = label;
    }
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
        _consumption = profile->AddSharedHighWaterMarkCounter(COUNTER_NAME, TUnit::BYTES);
    }
    _is_limiter = is_limiter;
    if (_parent && !_is_limiter) _parent->add_child(this);
}

MemTracker::~MemTracker() {
    if (_parent && !_is_limiter) _parent->remove_child(this);
}

// Count the memory in the scope to a temporary tracker with the specified label name.
// This is very useful when debugging. You can find the position where the tracker statistics are
// inaccurate through the temporary tracker layer by layer. As well as finding memory hotspots.
MemTracker* MemTracker::get_static_mem_tracker(const std::string& label) {
    // First time this label registered, make a new object, otherwise do nothing.
    // Avoid using locks to resolve erase conflicts.
    MemTracker* tracker;
    _static_mem_trackers.lazy_emplace_l(
            label, [&](MemTracker* v) { tracker = v; },
            [&](const auto& ctor) {
                tracker = new MemTracker(fmt::format("[Static]-{}", label));
                ctor(label, tracker);
            });
    return tracker;
}

MemTracker::Snapshot MemTracker::make_snapshot(size_t level) const {
    Snapshot snapshot;
    snapshot.label = _label;
    if (_parent != nullptr) {
        snapshot.parent = _parent->label();
    }
    snapshot.level = level;
    snapshot.limit = -1;
    snapshot.cur_consumption = _consumption->current_value();
    snapshot.peak_consumption = _consumption->value();
    snapshot.child_count = 0;
    return snapshot;
}

std::string MemTracker::log_usage() {
    // Make sure the consumption is up to date.
    int64_t curr_consumption = consumption();
    int64_t peak_consumption = _consumption->value();
    if (curr_consumption == 0) return "";
    std::string detail = "MemTracker Label={}, Total={}, Peak={}";
    detail = fmt::format(detail, _label, PrettyPrinter::print(curr_consumption, TUnit::BYTES),
                         PrettyPrinter::print(peak_consumption, TUnit::BYTES));
    return detail;
}

} // namespace doris