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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/perf-counters.h
// and modified by Doris

#pragma once

#include <gen_cpp/Metrics_types.h>
#include <stdint.h>

#include <iostream>
#include <string>
#include <vector>

// This is a utility class that aggregates counters from the kernel.  These counters
// come from different sources.
//   - perf counter syscall (/usr/include/linux/perf_event.h")
//   - /proc/self/io: io stats
//   - /proc/self/status: memory stats
// The complexity here is that all these sources have data in a different and not
// easy to get at format.
//
// A typical usage pattern would be:
//  PerfCounters counters;
//  counters.add_default_counters();
//  counters.snapshot("After Init");
//  <do your work>
//  counters.snapshot("After Work");
//  counters.PrettyPrint(cout);
//
// TODO: Expect PerfCounters to be refactored to ProcessState.

namespace doris {

class PerfCounters {
public:
    enum Counter {
        PERF_COUNTER_SW_CPU_CLOCK,
        PERF_COUNTER_SW_PAGE_FAULTS,
        PERF_COUNTER_SW_CONTEXT_SWITCHES,
        PERF_COUNTER_SW_CPU_MIGRATIONS,

        PERF_COUNTER_HW_CPU_CYCLES,
        PERF_COUNTER_HW_INSTRUCTIONS,
        PERF_COUNTER_HW_CACHE_HIT,
        PERF_COUNTER_HW_CACHE_MISSES,
        PERF_COUNTER_HW_BRANCHES,
        PERF_COUNTER_HW_BRANCH_MISSES,
        PERF_COUNTER_HW_BUS_CYCLES,

        PERF_COUNTER_VM_USAGE,
        PERF_COUNTER_VM_PEAK_USAGE,
        PERF_COUNTER_RESIDENT_SET_SIZE,

        PERF_COUNTER_BYTES_READ,
        PERF_COUNTER_BYTES_WRITE,
        PERF_COUNTER_DISK_READ,
        PERF_COUNTER_DISK_WRITE,
    };

    // Add the 'default' counters as ones to collect.  Returns false if any of those
    // counters are not available.
    // Counters cannot be added after a snapshot has been taken.
    bool add_default_counters();

    // Add a specific counter to watch.  Return false if that counter is not available.
    // Counters cannot be added after a snapshot has been taken.
    bool add_counter(Counter);

    // Take a snapshot of all the counters and store it.  The caller can specify a name
    // for the snapshot.
    void snapshot(const std::string& name);

    // Returns the results of that snapshot
    const std::vector<int64_t>* counters(int snapshot) const;

    // Returns readable names for the added counters
    const std::vector<std::string>* counter_names() const { return &_counter_names; }

    // Prints out the names and results for all snapshots to 'out'
    void pretty_print(std::ostream* out) const;

    PerfCounters();
    ~PerfCounters();

    // Refactor

    struct ProcStatus {
        int64_t vm_size = 0;
        int64_t vm_peak = 0;
        int64_t vm_rss = 0;
        int64_t vm_hwm = 0;
    };

    static int parse_int(const std::string& state_key);
    static int64_t parse_int64(const std::string& state_key);
    static std::string parse_string(const std::string& state_key);
    // Original data's unit is B or KB.
    static int64_t parse_bytes(const std::string& state_key);

    // Flush cached process status info from `/proc/self/status`.
    static void refresh_proc_status();
    static void get_proc_status(ProcStatus* out);
    // Return the process actual physical memory in bytes.
    static inline int64_t get_vm_rss() { return _vm_rss; }
    static inline std::string get_vm_rss_str() { return _vm_rss_str; }
    static inline int64_t get_vm_hwm() { return _vm_hwm; }
    static inline int64_t get_vm_size() { return _vm_size; }
    static inline int64_t get_vm_peak() { return _vm_peak; }

private:
    // Copy constructor and assignment not allowed
    PerfCounters(const PerfCounters&);
    PerfCounters& operator=(const PerfCounters&);

    bool init_sys_counter(Counter counter);
    bool init_proc_self_io_counter(Counter counter);
    bool init_proc_self_status_counter(Counter counter);

    bool get_sys_counters(std::vector<int64_t>& snapshot);
    bool get_proc_self_io_counters(std::vector<int64_t>& snapshot);
    bool get_proc_self_status_counters(std::vector<int64_t>& snapshot);

    enum DataSource {
        SYS_PERF_COUNTER,
        PROC_SELF_IO,
        PROC_SELF_STATUS,
    };

    struct CounterData {
        Counter counter;
        DataSource source;
        TUnit::type type;

        // DataSource specific data.  This is used to pull the counter values.
        union {
            // For SYS_PERF_COUNTER. File descriptor where the counter value is stored.
            int64_t fd;
            // For PROC_SELF_IO.  Line number from /proc/self/io file with this counter's value
            int proc_io_line_number;
        };
        // For PROC_SELF_STATUS.  Field name for counter
        std::string proc_status_field;
    };

    std::vector<CounterData> _counters;
    std::vector<std::string> _counter_names;
    std::vector<std::string> _snapshot_names;
    std::vector<std::vector<int64_t>> _snapshots;
    // System perf counters can be grouped together.  The OS will update all grouped counters
    // at the same time.  This is useful to better correlate counter values.
    int64_t _group_fd;

    static int64_t _vm_rss;
    static std::string _vm_rss_str;
    static int64_t _vm_hwm;
    static int64_t _vm_size;
    static int64_t _vm_peak;
};

} // namespace doris
