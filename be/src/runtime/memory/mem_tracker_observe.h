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

#pragma once

#include "runtime/memory/mem_tracker_base.h"

namespace doris {

class MemTrackerLimiter;

// Used to manually track memory usage at specified locations, including all exec node trackers.
//
// There is no parent-child relationship between MemTrackerObserves. Both fathers are fragment instance trakcers,
// but their consumption will not consume fragment instance trakcers synchronously. Therefore, errors in statistics
// will not affect the memory tracking and restrictions of processes and Query.
class MemTrackerObserve final : public MemTrackerBase {
public:
    // Creates and adds the tracker to the tree
    static MemTrackerObserve* create_tracker(const std::string& label,
                                             RuntimeProfile* profile = nullptr);

    ~MemTrackerObserve();

    // Get a temporary tracker with a specified label, and the tracker will be created when the label is first get.
    // Temporary trackers are not automatically destructed, which is usually used for debugging.
    static MemTrackerObserve* get_temporary_mem_tracker(const std::string& label);

public:
    void consume(int64_t bytes);

    void release(int64_t bytes) { consume(-bytes); }

    static void batch_consume(int64_t bytes, const std::vector<MemTrackerObserve*>& trackers) {
        for (auto& tracker : trackers) {
            tracker->consume(bytes);
        }
    }

    // Transfer 'bytes' of consumption from this tracker to 'dst'.
    void transfer_to(MemTrackerObserve* dst, int64_t bytes);

    bool limit_exceeded(int64_t limit) const { return limit >= 0 && limit < consumption(); }

    std::string log_usage(int64_t* logged_consumption = nullptr);

    std::string debug_string() {
        std::stringstream msg;
        msg << "label: " << _label << "; "
            << "consumption: " << _consumption->current_value() << "; "
            << "parent is null: " << ((_parent == nullptr) ? "true" : "false") << "; ";
        return msg.str();
    }

    // Iterator into parent_->_child_observe_trackers for this object. Stored to have O(1) remove.
    std::list<MemTrackerObserve*>::iterator _child_tracker_it;

private:
    MemTrackerObserve(const std::string& label, MemTrackerLimiter* parent, RuntimeProfile* profile)
            : MemTrackerBase(label, parent, profile) {}
};

inline void MemTrackerObserve::consume(int64_t bytes) {
    if (bytes == 0) {
        return;
    } else {
        _consumption->add(bytes);
    }
}

inline void MemTrackerObserve::transfer_to(MemTrackerObserve* dst, int64_t bytes) {
    if (id() == dst->id()) return;
    release(bytes);
    dst->consume(bytes);
}

} // namespace doris
