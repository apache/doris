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

// Used to track memory usage.
//
// NewMemTracker can be consumed manually by consume()/release(), or put into SCOPED_CONSUME_MEM_TRACKER,
// which will automatically track all memory usage of the code segment where it is located.
//
// This class is thread-safe.
class NewMemTracker {
public:
    struct Snapshot {
        std::string label;
        // For NewMemTracker, it is only weakly related to parent through label, ensuring NewMemTracker Independence.
        // For MemTrackerLimiter, it is strongly related to parent and saves pointer objects to each other.
        std::string parent = "";
        size_t level = 0;
        int64_t limit = 0;
        int64_t cur_consumption = 0;
        int64_t peak_consumption = 0;
        size_t child_count = 0;
    };

    // Creates and adds the tracker to the mem_tracker_pool.
    NewMemTracker(const std::string& label, RuntimeProfile* profile = nullptr);
    // For MemTrackerLimiter
    NewMemTracker() { _bind_group_num = -1; }

    ~NewMemTracker();

public:
    const std::string& label() const { return _label; }
    // Returns the memory consumed in bytes.
    int64_t consumption() const { return _consumption->current_value(); }
    int64_t peak_consumption() const { return _consumption->value(); }

    void consume(int64_t bytes);
    void release(int64_t bytes) { consume(-bytes); }
    // Transfer 'bytes' of consumption from this tracker to 'dst'.
    void transfer_to(NewMemTracker* dst, int64_t bytes);

public:
    bool limit_exceeded(int64_t limit) const { return limit >= 0 && limit < consumption(); }
    // Return true, no exceeded limit
    bool check_limit(int64_t limit, int64_t bytes) const {
        return limit >= 0 && limit > consumption() + bytes;
    }

    Snapshot make_snapshot(size_t level) const;
    // Specify group_num from mem_tracker_pool to generate snapshot, requiring tracker.label to be related
    // with parameter related_label
    static void make_group_snapshot(std::vector<Snapshot>* snapshots, size_t level,
                                    int64_t group_num, std::string related_label);
    static std::string log_usage(NewMemTracker::Snapshot snapshot);

    std::string debug_string() {
        std::stringstream msg;
        msg << "label: " << _label << "; "
            << "consumption: " << consumption() << "; "
            << "peak_consumption: " << peak_consumption() << "; ";
        return msg.str();
    }

    static const std::string COUNTER_NAME;

protected:
    // label used in the make snapshot, not guaranteed unique.
    std::string _label;

    std::shared_ptr<RuntimeProfile::HighWaterMarkCounter> _consumption; // in bytes

    // Tracker is located in group num in mem_tracker_pool
    int64_t _bind_group_num;

    // Iterator into mem_tracker_pool for this object. Stored to have O(1) remove.
    std::list<NewMemTracker*>::iterator _tracker_group_it;
};

inline void NewMemTracker::consume(int64_t bytes) {
    if (bytes == 0) {
        return;
    } else {
        _consumption->add(bytes);
    }
}

inline void NewMemTracker::transfer_to(NewMemTracker* dst, int64_t bytes) {
    release(bytes);
    dst->consume(bytes);
}

} // namespace doris