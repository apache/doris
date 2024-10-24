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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/perf-counters.cpp
// and modified by Doris

#include "util/perf_counters.h"

#include <linux/perf_event.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <boost/algorithm/string/trim.hpp>
#include <fstream> // IWYU pragma: keep
#include <iomanip>
#include <iostream>
#include <unordered_map>
#include <utility>

#include "gutil/stringprintf.h"
#include "gutil/strings/substitute.h"
#include "util/pretty_printer.h"
#include "util/string_parser.hpp"
#include "util/string_util.h"

namespace doris {

#define COUNTER_SIZE (sizeof(void*))
#define PRETTY_PRINT_WIDTH 13

static std::unordered_map<std::string, std::string> _process_state;

int64_t PerfCounters::_vm_rss = 0;
std::string PerfCounters::_vm_rss_str = "";
int64_t PerfCounters::_vm_hwm = 0;
int64_t PerfCounters::_vm_size = 0;
int64_t PerfCounters::_vm_peak = 0;

// This is the order of the counters in /proc/self/io
enum PERF_IO_IDX {
    PROC_IO_READ = 0,
    PROC_IO_WRITE,
    PROC_IO_SYS_RREAD,
    PROC_IO_SYS_WRITE,
    PROC_IO_DISK_READ,
    PROC_IO_DISK_WRITE,
    PROC_IO_CANCELLED_WRITE,
    PROC_IO_LAST_COUNTER,
};

// Wrapper around sys call.  This syscall is hard to use and this is how it is recommended
// to be used.
static inline int sys_perf_event_open(struct perf_event_attr* attr, pid_t pid, int cpu,
                                      int group_fd, unsigned long flags) {
    attr->size = sizeof(*attr);
    return syscall(__NR_perf_event_open, attr, pid, cpu, group_fd, flags);
}

// Remap PerfCounters::Counter to Linux kernel enums
static bool init_event_attr(perf_event_attr* attr, PerfCounters::Counter counter) {
    memset(attr, 0, sizeof(perf_event_attr));

    switch (counter) {
    case PerfCounters::PERF_COUNTER_SW_CPU_CLOCK:
        attr->type = PERF_TYPE_SOFTWARE;
        attr->config = PERF_COUNT_SW_CPU_CLOCK;
        break;

    case PerfCounters::PERF_COUNTER_SW_PAGE_FAULTS:
        attr->type = PERF_TYPE_SOFTWARE;
        attr->config = PERF_COUNT_SW_PAGE_FAULTS;
        break;

    case PerfCounters::PERF_COUNTER_SW_CONTEXT_SWITCHES:
        attr->type = PERF_TYPE_SOFTWARE;
        attr->config = PERF_COUNT_SW_PAGE_FAULTS;
        break;

    case PerfCounters::PERF_COUNTER_SW_CPU_MIGRATIONS:
        attr->type = PERF_TYPE_SOFTWARE;
        attr->config = PERF_COUNT_SW_CPU_MIGRATIONS;
        break;

    case PerfCounters::PERF_COUNTER_HW_CPU_CYCLES:
        attr->type = PERF_TYPE_HARDWARE;
        attr->config = PERF_COUNT_HW_CPU_CYCLES;
        break;

    case PerfCounters::PERF_COUNTER_HW_INSTRUCTIONS:
        attr->type = PERF_TYPE_HARDWARE;
        attr->config = PERF_COUNT_HW_INSTRUCTIONS;
        break;

    case PerfCounters::PERF_COUNTER_HW_CACHE_HIT:
        attr->type = PERF_TYPE_HARDWARE;
        attr->config = PERF_COUNT_HW_CACHE_REFERENCES;
        break;

    case PerfCounters::PERF_COUNTER_HW_CACHE_MISSES:
        attr->type = PERF_TYPE_HARDWARE;
        attr->config = PERF_COUNT_HW_CACHE_MISSES;
        break;

    case PerfCounters::PERF_COUNTER_HW_BRANCHES:
        attr->type = PERF_TYPE_HARDWARE;
        attr->config = PERF_COUNT_HW_BRANCH_INSTRUCTIONS;
        break;

    case PerfCounters::PERF_COUNTER_HW_BRANCH_MISSES:
        attr->type = PERF_TYPE_HARDWARE;
        attr->config = PERF_COUNT_HW_BRANCH_MISSES;
        break;

    case PerfCounters::PERF_COUNTER_HW_BUS_CYCLES:
        attr->type = PERF_TYPE_HARDWARE;
        attr->config = PERF_COUNT_HW_BUS_CYCLES;
        break;

    default:
        return false;
    }

    return true;
}

static std::string get_counter_name(PerfCounters::Counter counter) {
    switch (counter) {
    case PerfCounters::PERF_COUNTER_SW_CPU_CLOCK:
        return "CPUTime";

    case PerfCounters::PERF_COUNTER_SW_PAGE_FAULTS:
        return "PageFaults";

    case PerfCounters::PERF_COUNTER_SW_CONTEXT_SWITCHES:
        return "ContextSwitches";

    case PerfCounters::PERF_COUNTER_SW_CPU_MIGRATIONS:
        return "CPUMigrations";

    case PerfCounters::PERF_COUNTER_HW_CPU_CYCLES:
        return "HWCycles";

    case PerfCounters::PERF_COUNTER_HW_INSTRUCTIONS:
        return "Instructions";

    case PerfCounters::PERF_COUNTER_HW_CACHE_HIT:
        return "CacheHit";

    case PerfCounters::PERF_COUNTER_HW_CACHE_MISSES:
        return "CacheMiss";

    case PerfCounters::PERF_COUNTER_HW_BRANCHES:
        return "Branches";

    case PerfCounters::PERF_COUNTER_HW_BRANCH_MISSES:
        return "BranchMiss";

    case PerfCounters::PERF_COUNTER_HW_BUS_CYCLES:
        return "BusCycles";

    case PerfCounters::PERF_COUNTER_VM_USAGE:
        return "VmUsage";

    case PerfCounters::PERF_COUNTER_VM_PEAK_USAGE:
        return "PeakVmUsage";

    case PerfCounters::PERF_COUNTER_RESIDENT_SET_SIZE:
        return "WorkingSet";

    case PerfCounters::PERF_COUNTER_BYTES_READ:
        return "BytesRead";

    case PerfCounters::PERF_COUNTER_BYTES_WRITE:
        return "BytesWritten";

    case PerfCounters::PERF_COUNTER_DISK_READ:
        return "DiskRead";

    case PerfCounters::PERF_COUNTER_DISK_WRITE:
        return "DiskWrite";

    default:
        return "";
    }
}

bool PerfCounters::init_sys_counter(Counter counter) {
    CounterData data;
    data.counter = counter;
    data.source = PerfCounters::SYS_PERF_COUNTER;
    data.fd = -1;
    perf_event_attr attr;

    if (!init_event_attr(&attr, counter)) {
        return false;
    }

    int fd = sys_perf_event_open(&attr, getpid(), -1, _group_fd, 0);

    if (fd < 0) {
        return false;
    }

    if (_group_fd == -1) {
        _group_fd = fd;
    }

    data.fd = fd;

    if (counter == PERF_COUNTER_SW_CPU_CLOCK) {
        data.type = TUnit::TIME_NS;
    } else {
        data.type = TUnit::UNIT;
    }

    _counters.push_back(data);
    return true;
}

bool PerfCounters::init_proc_self_io_counter(Counter counter) {
    CounterData data;
    data.counter = counter;
    data.source = PerfCounters::PROC_SELF_IO;
    data.type = TUnit::BYTES;

    switch (counter) {
    case PerfCounters::PERF_COUNTER_BYTES_READ:
        data.proc_io_line_number = PROC_IO_READ;
        break;

    case PerfCounters::PERF_COUNTER_BYTES_WRITE:
        data.proc_io_line_number = PROC_IO_WRITE;
        break;

    case PerfCounters::PERF_COUNTER_DISK_READ:
        data.proc_io_line_number = PROC_IO_DISK_READ;
        break;

    case PerfCounters::PERF_COUNTER_DISK_WRITE:
        data.proc_io_line_number = PROC_IO_DISK_WRITE;
        break;

    default:
        return false;
    }

    _counters.push_back(data);
    return true;
}

bool PerfCounters::init_proc_self_status_counter(Counter counter) {
    CounterData data {};
    data.counter = counter;
    data.source = PerfCounters::PROC_SELF_STATUS;
    data.type = TUnit::BYTES;

    switch (counter) {
    case PerfCounters::PERF_COUNTER_VM_USAGE:
        data.proc_status_field = "VmSize";
        break;

    case PerfCounters::PERF_COUNTER_VM_PEAK_USAGE:
        data.proc_status_field = "VmPeak";
        break;

    case PerfCounters::PERF_COUNTER_RESIDENT_SET_SIZE:
        data.proc_status_field = "VmRS";
        break;

    default:
        return false;
    }

    _counters.push_back(data);
    return true;
}

bool PerfCounters::get_sys_counters(std::vector<int64_t>& buffer) {
    for (int i = 0; i < _counters.size(); i++) {
        if (_counters[i].source == SYS_PERF_COUNTER) {
            int num_bytes = read(_counters[i].fd, &buffer[i], COUNTER_SIZE);

            if (num_bytes != COUNTER_SIZE) {
                return false;
            }

            if (_counters[i].type == TUnit::TIME_NS) {
                buffer[i] /= 1000000;
            }
        }
    }

    return true;
}

// Parse out IO counters from /proc/self/io.  The file contains a list of
// (name,byte) pairs.
// For example:
//    rchar: 210212
//    wchar: 94
//    syscr: 118
//    syscw: 3
//    read_bytes: 0
//    write_bytes: 0
//    cancelled_write_bytes: 0
bool PerfCounters::get_proc_self_io_counters(std::vector<int64_t>& buffer) {
    std::ifstream file("/proc/self/io", std::ios::in);
    std::string buf;
    int64_t values[PROC_IO_LAST_COUNTER];
    int ret = 0;

    for (int i = 0; i < PROC_IO_LAST_COUNTER; ++i) {
        if (!file) {
            ret = -1;
            break;
        }

        getline(file, buf);
        size_t colon = buf.find(':');

        if (colon == std::string::npos) {
            ret = -1;
            break;
        }

        buf = buf.substr(colon + 1);
        std::istringstream stream(buf);
        stream >> values[i];
    }

    if (ret == 0) {
        for (int i = 0; i < _counters.size(); ++i) {
            if (_counters[i].source == PROC_SELF_IO) {
                buffer[i] = values[_counters[i].proc_io_line_number];
            }
        }
    }

    if (file.is_open()) {
        file.close();
    }

    return true;
}

bool PerfCounters::get_proc_self_status_counters(std::vector<int64_t>& buffer) {
    std::ifstream file("/proc/self/status", std::ios::in);
    std::string buf;

    while (file) {
        getline(file, buf);

        for (int i = 0; i < _counters.size(); ++i) {
            if (_counters[i].source == PROC_SELF_STATUS) {
                size_t field = buf.find(_counters[i].proc_status_field);

                if (field == std::string::npos) {
                    continue;
                }

                size_t colon = field + _counters[i].proc_status_field.size() + 1;
                buf = buf.substr(colon + 1);
                std::istringstream stream(buf);
                int64_t value;
                stream >> value;
                buffer[i] = value * 1024; // values in file are in kb
            }
        }
    }

    if (file.is_open()) {
        file.close();
    }

    return true;
}

PerfCounters::PerfCounters() : _group_fd(-1) {}

// Close all fds for the counters
PerfCounters::~PerfCounters() {
    for (int i = 0; i < _counters.size(); ++i) {
        if (_counters[i].source == SYS_PERF_COUNTER) {
            close(_counters[i].fd);
        }
    }
}

// Add here the default ones that are most useful
bool PerfCounters::add_default_counters() {
    bool result = true;
    result &= add_counter(PERF_COUNTER_SW_CPU_CLOCK);
    // These hardware ones don't work on a vm, just ignore if they fail
    // TODO: these don't work reliably and aren't that useful.  Turn them off.
    //add_counter(PERF_COUNTER_HW_INSTRUCTIONS);
    //add_counter(PERF_COUNTER_HW_CPU_CYCLES);
    //add_counter(PERF_COUNTER_HW_BRANCHES);
    //add_counter(PERF_COUNTER_HW_BRANCH_MISSES);
    //add_counter(PERF_COUNTER_HW_CACHE_MISSES);
    add_counter(PERF_COUNTER_VM_USAGE);
    add_counter(PERF_COUNTER_VM_PEAK_USAGE);
    add_counter(PERF_COUNTER_RESIDENT_SET_SIZE);
    result &= add_counter(PERF_COUNTER_DISK_READ);
    return result;
}

// Add a specific counter
bool PerfCounters::add_counter(Counter counter) {
    // Ignore if it's already added.
    for (int i = 0; i < _counters.size(); ++i) {
        if (_counters[i].counter == counter) {
            return true;
        }
    }

    bool result = false;

    switch (counter) {
    case PerfCounters::PERF_COUNTER_SW_CPU_CLOCK:
    case PerfCounters::PERF_COUNTER_SW_PAGE_FAULTS:
    case PerfCounters::PERF_COUNTER_SW_CONTEXT_SWITCHES:
    case PerfCounters::PERF_COUNTER_SW_CPU_MIGRATIONS:
    case PerfCounters::PERF_COUNTER_HW_CPU_CYCLES:
    case PerfCounters::PERF_COUNTER_HW_INSTRUCTIONS:
    case PerfCounters::PERF_COUNTER_HW_CACHE_HIT:
    case PerfCounters::PERF_COUNTER_HW_CACHE_MISSES:
    case PerfCounters::PERF_COUNTER_HW_BRANCHES:
    case PerfCounters::PERF_COUNTER_HW_BRANCH_MISSES:
    case PerfCounters::PERF_COUNTER_HW_BUS_CYCLES:
        result = init_sys_counter(counter);
        break;

    case PerfCounters::PERF_COUNTER_BYTES_READ:
    case PerfCounters::PERF_COUNTER_BYTES_WRITE:
    case PerfCounters::PERF_COUNTER_DISK_READ:
    case PerfCounters::PERF_COUNTER_DISK_WRITE:
        result = init_proc_self_io_counter(counter);
        break;

    case PerfCounters::PERF_COUNTER_VM_USAGE:
    case PerfCounters::PERF_COUNTER_VM_PEAK_USAGE:
    case PerfCounters::PERF_COUNTER_RESIDENT_SET_SIZE:
        result = init_proc_self_status_counter(counter);
        break;

    default:
        return false;
    }

    if (result) {
        _counter_names.push_back(get_counter_name(counter));
    }

    return result;
}

// Query all the counters right now and store the values in results
void PerfCounters::snapshot(const std::string& name) {
    if (_counters.size() == 0) {
        return;
    }

    std::string fixed_name = name;

    if (fixed_name.size() == 0) {
        std::stringstream ss;
        ss << _snapshots.size() + 1;
        fixed_name = ss.str();
    }

    std::vector<int64_t> buffer(_counters.size());

    get_sys_counters(buffer);
    get_proc_self_io_counters(buffer);
    get_proc_self_status_counters(buffer);

    _snapshots.push_back(buffer);
    _snapshot_names.push_back(fixed_name);
}

const std::vector<int64_t>* PerfCounters::counters(int snapshot) const {
    if (snapshot < 0 || snapshot >= _snapshots.size()) {
        return nullptr;
    }

    return &_snapshots[snapshot];
}

void PerfCounters::pretty_print(std::ostream* s) const {
    std::ostream& stream = *s;
    stream << std::setw(8) << "snapshot";

    for (int i = 0; i < _counter_names.size(); ++i) {
        stream << std::setw(PRETTY_PRINT_WIDTH) << _counter_names[i];
    }

    stream << std::endl;

    for (int s = 0; s < _snapshots.size(); s++) {
        stream << std::setw(8) << _snapshot_names[s];
        const std::vector<int64_t>& snapshot = _snapshots[s];

        for (int i = 0; i < snapshot.size(); ++i) {
            stream << std::setw(PRETTY_PRINT_WIDTH)
                   << PrettyPrinter::print(snapshot[i], _counters[i].type);
        }

        stream << std::endl;
    }

    stream << std::endl;
}

// Refactor below

int PerfCounters::parse_int(const string& state_key) {
    auto it = _process_state.find(state_key);
    if (it != _process_state.end()) return atoi(it->second.c_str());
    return -1;
}

int64_t PerfCounters::parse_int64(const string& state_key) {
    auto it = _process_state.find(state_key);
    if (it != _process_state.end()) {
        StringParser::ParseResult result;
        int64_t state_value =
                StringParser::string_to_int<int64_t>(it->second.data(), it->second.size(), &result);
        if (result == StringParser::PARSE_SUCCESS) return state_value;
    }
    return -1;
}

string PerfCounters::parse_string(const string& state_key) {
    auto it = _process_state.find(state_key);
    if (it != _process_state.end()) return it->second;
    return string();
}

int64_t PerfCounters::parse_bytes(const string& state_key) {
    auto it = _process_state.find(state_key);
    if (it != _process_state.end()) {
        vector<string> fields = split(it->second, " ");
        // We expect state_value such as, e.g., '16129508', '16129508 kB', '16129508 mB'
        StringParser::ParseResult result;
        int64_t state_value =
                StringParser::string_to_int<int64_t>(fields[0].data(), fields[0].size(), &result);
        if (result == StringParser::PARSE_SUCCESS) {
            if (fields.size() < 2) return state_value;
            if (fields[1].compare("kB") == 0) return state_value * 1024L;
        }
    }
    return -1;
}

void PerfCounters::refresh_proc_status() {
    std::ifstream statusinfo("/proc/self/status", std::ios::in);
    std::string line;
    while (statusinfo.good() && !statusinfo.eof()) {
        getline(statusinfo, line);
        std::vector<std::string> fields = split(line, "\t");
        if (fields.size() < 2) continue;
        boost::algorithm::trim(fields[1]);
        std::string key = fields[0].substr(0, fields[0].size() - 1);
        _process_state[strings::Substitute("status/$0", key)] = fields[1];
    }

    if (statusinfo.is_open()) statusinfo.close();

    _vm_size = parse_bytes("status/VmSize");
    _vm_peak = parse_bytes("status/VmPeak");
    _vm_rss = parse_bytes("status/VmRSS");
#ifdef ADDRESS_SANITIZER
    _vm_rss_str = "[ASAN]" + PrettyPrinter::print(_vm_rss, TUnit::BYTES);
#else
    _vm_rss_str = PrettyPrinter::print(_vm_rss, TUnit::BYTES);
#endif
    _vm_hwm = parse_bytes("status/VmHWM");
}

void PerfCounters::get_proc_status(ProcStatus* out) {
    out->vm_size = parse_bytes("status/VmSize");
    out->vm_peak = parse_bytes("status/VmPeak");
    out->vm_rss = parse_bytes("status/VmRSS");
    out->vm_hwm = parse_bytes("status/VmHWM");
}

} // namespace doris
