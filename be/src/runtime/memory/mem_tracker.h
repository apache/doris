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

#include <gen_cpp/Metrics_types.h>
#include <stdint.h>

#include <atomic>
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath> // IWYU pragma: keep
#include <list>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "runtime/query_statistics.h"
#include "util/pretty_printer.h"

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
        std::string type;
        std::string label;
        std::string parent_label;
        int64_t limit = 0;
        int64_t cur_consumption = 0;
        int64_t peak_consumption = 0;

        bool operator<(const Snapshot& rhs) const { return cur_consumption < rhs.cur_consumption; }
    };

    struct TrackerGroup {
        std::list<MemTracker*> trackers;
        std::mutex group_lock;
    };

    enum class Type {
        GLOBAL = 0,        // Life cycle is the same as the process, e.g. Cache and default Orphan
        QUERY = 1,         // Count the memory consumption of all Query tasks.
        LOAD = 2,          // Count the memory consumption of all Load tasks.
        COMPACTION = 3,    // Count the memory consumption of all Base and Cumulative tasks.
        SCHEMA_CHANGE = 4, // Count the memory consumption of all SchemaChange tasks.
        OTHER = 5
    };

    static std::string type_string(Type type) {
        switch (type) {
        case Type::GLOBAL:
            return "global";
        case Type::QUERY:
            return "query";
        case Type::LOAD:
            return "load";
        case Type::COMPACTION:
            return "compaction";
        case Type::SCHEMA_CHANGE:
            return "schema_change";
        case Type::OTHER:
            return "other";
        default:
            LOG(FATAL) << "not match type of mem tracker limiter :" << static_cast<int>(type);
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }

    // A counter that keeps track of the current and peak value seen.
    // Relaxed ordering, not accurate in real time.
    class MemCounter {
    public:
        MemCounter() : _current_value(0), _peak_value(0) {}

        void add(int64_t delta) {
            int64_t value = _current_value.fetch_add(delta, std::memory_order_relaxed) + delta;
            update_peak(value);
        }

        void add_no_update_peak(int64_t delta) {
            _current_value.fetch_add(delta, std::memory_order_relaxed);
        }

        bool try_add(int64_t delta, int64_t max) {
            int64_t cur_val = _current_value.load(std::memory_order_relaxed);
            int64_t new_val = 0;
            do {
                new_val = cur_val + delta;
                if (UNLIKELY(new_val > max)) {
                    return false;
                }
            } while (UNLIKELY(!_current_value.compare_exchange_weak(cur_val, new_val,
                                                                    std::memory_order_relaxed)));
            update_peak(new_val);
            return true;
        }

        void sub(int64_t delta) { _current_value.fetch_sub(delta, std::memory_order_relaxed); }

        void set(int64_t v) {
            _current_value.store(v, std::memory_order_relaxed);
            update_peak(v);
        }

        void update_peak(int64_t value) {
            int64_t pre_value = _peak_value.load(std::memory_order_relaxed);
            while (value > pre_value && !_peak_value.compare_exchange_weak(
                                                pre_value, value, std::memory_order_relaxed)) {
            }
        }

        int64_t current_value() const { return _current_value.load(std::memory_order_relaxed); }
        int64_t peak_value() const { return _peak_value.load(std::memory_order_relaxed); }

    private:
        std::atomic<int64_t> _current_value;
        std::atomic<int64_t> _peak_value;
    };

    // Creates and adds the tracker to the mem_tracker_pool.
    MemTracker(const std::string& label, MemTrackerLimiter* parent = nullptr);
    // For MemTrackerLimiter
    MemTracker() { _parent_group_num = -1; }

    virtual ~MemTracker();

    static std::string print_bytes(int64_t bytes) {
        return bytes >= 0 ? PrettyPrinter::print(bytes, TUnit::BYTES)
                          : "-" + PrettyPrinter::print(std::abs(bytes), TUnit::BYTES);
    }

public:
    Type type() const { return _type; }
    const std::string& label() const { return _label; }
    const std::string& parent_label() const { return _parent_label; }
    const std::string& set_parent_label() const { return _parent_label; }
    // Returns the memory consumed in bytes.
    int64_t consumption() const { return _consumption->current_value(); }
    int64_t peak_consumption() const { return _consumption->peak_value(); }

    void consume(int64_t bytes) {
        if (UNLIKELY(bytes == 0)) {
            return;
        }
        _consumption->add(bytes);
        if (_query_statistics) {
            _query_statistics->set_max_peak_memory_bytes(_consumption->peak_value());
            _query_statistics->set_current_used_memory_bytes(_consumption->current_value());
        }
    }

    void consume_no_update_peak(int64_t bytes) { // need extreme fast
        _consumption->add_no_update_peak(bytes);
    }

    void release(int64_t bytes) { _consumption->sub(bytes); }

    void set_consumption(int64_t bytes) { _consumption->set(bytes); }

    std::shared_ptr<QueryStatistics> get_query_statistics() { return _query_statistics; }

public:
    virtual Snapshot make_snapshot() const;
    // Specify group_num from mem_tracker_pool to generate snapshot.
    static void make_group_snapshot(std::vector<Snapshot>* snapshots, int64_t group_num,
                                    std::string parent_label);
    static void make_all_trackers_snapshots(std::vector<Snapshot>* snapshots);
    static std::string log_usage(MemTracker::Snapshot snapshot);

    virtual std::string debug_string() {
        std::stringstream msg;
        msg << "label: " << _label << "; "
            << "consumption: " << consumption() << "; "
            << "peak_consumption: " << peak_consumption() << "; ";
        return msg.str();
    }

protected:
    void bind_parent(MemTrackerLimiter* parent);

    Type _type;

    // label used in the make snapshot, not guaranteed unique.
    std::string _label;

    std::shared_ptr<MemCounter> _consumption = nullptr;

    // Tracker is located in group num in mem_tracker_pool
    int64_t _parent_group_num = 0;

    // Use _parent_label to correlate with parent limiter tracker.
    std::string _parent_label = "-";

    static std::vector<TrackerGroup> mem_tracker_pool;

    // Iterator into mem_tracker_pool for this object. Stored to have O(1) remove.
    std::list<MemTracker*>::iterator _tracker_group_it;

    std::shared_ptr<QueryStatistics> _query_statistics = nullptr;
};

} // namespace doris
