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

#include "runtime/memory/mem_tracker_observe.h"

#include <fmt/format.h>
#include <parallel_hashmap/phmap.h>

#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/pretty_printer.h"

namespace doris {

using TemporaryTrackersMap = phmap::parallel_flat_hash_map<
        std::string, MemTrackerObserve*, phmap::priv::hash_default_hash<std::string>,
        phmap::priv::hash_default_eq<std::string>,
        std::allocator<std::pair<const std::string, MemTrackerObserve*>>, 12, std::mutex>;

static TemporaryTrackersMap _temporary_mem_trackers;

MemTrackerObserve* MemTrackerObserve::create_tracker(const std::string& label,
                                                     RuntimeProfile* profile) {
    STOP_CHECK_THREAD_MEM_TRACKER_LIMIT();
    MemTrackerLimiter* parent = tls_ctx()->_thread_mem_tracker_mgr->limiter_mem_tracker();
    DCHECK(parent);
    std::string parent_label = parent->label();
    std::string reset_label;
    if (parent_label.find_first_of("#") != parent_label.npos) {
        reset_label = fmt::format("[Observe]-{}#{}", label,
                                  parent_label.substr(parent_label.find_first_of("#"), -1));
    } else {
        reset_label = fmt::format("[Observe]-{}", label);
    }
    MemTrackerObserve* tracker(new MemTrackerObserve(reset_label, parent, profile));
    parent->add_child_tracker(tracker);
    tracker->set_is_observed();
    return tracker;
}

MemTrackerObserve::~MemTrackerObserve() {
    if (parent()) {
        parent()->remove_child_tracker(this);
    }
}

// Count the memory in the scope to a temporary tracker with the specified label name.
// This is very useful when debugging. You can find the position where the tracker statistics are
// inaccurate through the temporary tracker layer by layer. As well as finding memory hotspots.
// TODO(zxy) track specifies the memory for each line in the code segment, instead of manually adding
// a switch temporary tracker to each line. Maybe there are open source tools to do this?
MemTrackerObserve* MemTrackerObserve::get_temporary_mem_tracker(const std::string& label) {
    // First time this label registered, make a new object, otherwise do nothing.
    // Avoid using locks to resolve erase conflicts.
    _temporary_mem_trackers.try_emplace_l(
            label, [](MemTrackerObserve*) {},
            MemTrackerObserve::create_tracker(fmt::format("[Temporary]-{}", label)));
    return _temporary_mem_trackers[label];
}

std::string MemTrackerObserve::log_usage(int64_t* logged_consumption) {
    // Make sure the consumption is up to date.
    int64_t curr_consumption = consumption();
    int64_t peak_consumption = _consumption->value();
    if (logged_consumption != nullptr) *logged_consumption = curr_consumption;
    if (curr_consumption == 0) return "";
    std::string detail = "MemTracker log_usage Label: {}, Total: {}, Peak: {}";
    detail = fmt::format(detail, _label, PrettyPrinter::print(curr_consumption, TUnit::BYTES),
                         PrettyPrinter::print(peak_consumption, TUnit::BYTES));
    return detail;
}

} // namespace doris
