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

#include "util/pretty_printer.h"
#include "util/runtime_profile.h"

namespace doris {

class MemTrackerLimiter;

// Used to track memory usage.
//
// MemTracker can be consumed manually by consume()/release(), or put into SCOPED_CONSUME_MEM_TRACKER,
// which will automatically track all memory usage of the code segment where it is located.
//
// This class is thread-safe.
class MemTracker {
public:
    struct Snapshot {
        std::string type = "";
        std::string label;
        std::string parent_label = "";
        int64_t limit = 0;
        int64_t cur_consumption = 0;
        int64_t peak_consumption = 0;
    };

    // Creates and adds the tracker to the mem_tracker_pool.
    MemTracker(const std::string& label, RuntimeProfile* profile = nullptr,
               MemTrackerLimiter* parent = nullptr);
    // For MemTrackerLimiter
    MemTracker() { _parent_group_num = -1; }

    ~MemTracker();

    static std::string print_bytes(int64_t bytes) {
        return bytes >= 0 ? PrettyPrinter::print(bytes, TUnit::BYTES)
                          : "-" + PrettyPrinter::print(std::abs(bytes), TUnit::BYTES);
    }

public:
    const std::string& label() const { return _label; }
    const std::string& parent_label() const { return _parent_label; }
    // Returns the memory consumed in bytes.
    int64_t consumption() const { return _consumption->current_value(); }
    int64_t peak_consumption() const { return _consumption->value(); }

    void consume(int64_t bytes) {
        if (bytes == 0) return;
        _consumption->add(bytes);
    }
    void release(int64_t bytes) { consume(-bytes); }
    void set_consumption(int64_t bytes) { _consumption->set(bytes); }

public:
    Snapshot make_snapshot() const;
    // Specify group_num from mem_tracker_pool to generate snapshot.
    static void make_group_snapshot(std::vector<Snapshot>* snapshots, int64_t group_num,
                                    std::string parent_label);
    static std::string log_usage(MemTracker::Snapshot snapshot);

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
    int64_t _parent_group_num;
    std::string _parent_label;

    // Iterator into mem_tracker_pool for this object. Stored to have O(1) remove.
    std::list<MemTracker*>::iterator _tracker_group_it;
};

} // namespace doris