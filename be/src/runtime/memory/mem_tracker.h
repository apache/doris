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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/mem-tracker.h
// and modified by Doris
#pragma once

#include "util/runtime_profile.h"

namespace doris {

class MemTrackerLimiter;

// Used to track memory usage.
//
// MemTracker can be consumed manually by consume()/release(), or put into SCOPED_CONSUME_MEM_TRACKER,
// which will automatically track all memory usage of the code segment where it is located.
//
// MemTracker's father can only be MemTrackerLimiter, which is only used to print tree-like statistics.
// Consuming MemTracker will not consume its father synchronously.
// Usually, it is not necessary to specify the father. by default, the MemTrackerLimiter in the thread context
// is used as the parent, which is specified when the thread starts.
//
// This class is thread-safe.
class MemTracker {
public:
    struct Snapshot {
        std::string label;
        std::string parent = "";
        size_t level = 0;
        int64_t limit = 0;
        int64_t cur_consumption = 0;
        int64_t peak_consumption = 0;
        size_t child_count = 0;
    };

    // Creates and adds the tracker to the tree.
    MemTracker(const std::string& label = std::string(), MemTrackerLimiter* parent = nullptr,
               RuntimeProfile* profile = nullptr, bool is_limiter = false);

    ~MemTracker();

    // Get a temporary tracker with a specified label, and the tracker will be created when the label is first get.
    // Temporary trackers are not automatically destructed, which is usually used for debugging.
    static MemTracker* get_static_mem_tracker(const std::string& label);

public:
    const std::string& label() const { return _label; }
    MemTrackerLimiter* parent() const { return _parent; }
    // Returns the memory consumed in bytes.
    int64_t consumption() const { return _consumption->current_value(); }
    int64_t peak_consumption() const { return _consumption->value(); }

public:
    void consume(int64_t bytes);
    void release(int64_t bytes) { consume(-bytes); }
    // Transfer 'bytes' of consumption from this tracker to 'dst'.
    void transfer_to(MemTracker* dst, int64_t bytes);

public:
    bool limit_exceeded(int64_t limit) const { return limit >= 0 && limit < consumption(); }
    // Return true, no exceeded limit
    bool check_limit(int64_t limit, int64_t bytes) const {
        return limit >= 0 && limit > consumption() + bytes;
    }

    // Usually, a negative values means that the statistics are not accurate,
    // 1. The released memory is not consumed.
    // 2. The same block of memory, tracker A calls consume, and tracker B calls release.
    // 3. Repeated releases of MemTacker. When the consume is called on the child MemTracker,
    //    after the release is called on the parent MemTracker,
    //    the child ~MemTracker will cause repeated releases.
    void memory_leak_check() { DCHECK_EQ(consumption(), 0) << std::endl << log_usage(); }

    Snapshot make_snapshot(size_t level) const;

    std::string log_usage();

    std::string debug_string() {
        std::stringstream msg;
        msg << "label: " << _label << "; "
            << "consumption: " << consumption() << "; "
            << "peak_consumption: " << peak_consumption() << "; ";
        return msg.str();
    }

    // Iterator into parent_->_child_trackers for this object. Stored to have O(1) remove.
    std::list<MemTracker*>::iterator _child_tracker_it;

    static const std::string COUNTER_NAME;

protected:
    // label used in the usage string (log_usage())
    std::string _label;

    std::shared_ptr<RuntimeProfile::HighWaterMarkCounter> _consumption; // in bytes

    MemTrackerLimiter* _parent; // The parent of this tracker.

    bool _is_limiter;
};

inline void MemTracker::consume(int64_t bytes) {
    if (bytes == 0) {
        return;
    } else {
        _consumption->add(bytes);
    }
}

inline void MemTracker::transfer_to(MemTracker* dst, int64_t bytes) {
    release(bytes);
    dst->consume(bytes);
}

} // namespace doris