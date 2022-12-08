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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/cpu-info.cpp
// and modified by Doris

#include "util/cpu_info.h"

#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
/* GCC-compatible compiler, targeting x86/x86-64 */
#include <x86intrin.h>
#elif defined(__GNUC__) && defined(__ARM_NEON__)
/* GCC-compatible compiler, targeting ARM with NEON */
#include <arm_neon.h>
#elif defined(__GNUC__) && defined(__IWMMXT__)
/* GCC-compatible compiler, targeting ARM with WMMX */
#include <mmintrin.h>
#elif (defined(__GNUC__) || defined(__xlC__)) && (defined(__VEC__) || defined(__ALTIVEC__))
/* XLC or GCC-compatible compiler, targeting PowerPC with VMX/VSX */
#include <altivec.h>
#elif defined(__GNUC__) && defined(__SPE__)
/* GCC-compatible compiler, targeting PowerPC with SPE */
#include <spe.h>
#endif

#ifndef __APPLE__
#include <sys/sysinfo.h>
#else
#include <sys/sysctl.h>
#endif

#include <sched.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/env_config.h"
#include "gflags/gflags.h"
#include "gutil/strings/substitute.h"
#include "util/pretty_printer.h"
#include "util/string_parser.hpp"

using boost::algorithm::contains;
using boost::algorithm::trim;
namespace fs = std::filesystem;
using std::max;

DECLARE_bool(abort_on_config_error);
DEFINE_int32(num_cores, 0,
             "(Advanced) If > 0, it sets the number of cores available to"
             " Impala. Setting it to 0 means Impala will use all available cores on the machine"
             " according to /proc/cpuinfo.");

namespace doris {
// Helper function to warn if a given file does not contain an expected string as its
// first line. If the file cannot be opened, no error is reported.
void WarnIfFileNotEqual(const string& filename, const string& expected,
                        const string& warning_text) {
    std::ifstream file(filename);
    if (!file) return;
    string line;
    getline(file, line);
    if (line != expected) {
        LOG(ERROR) << "Expected " << expected << ", actual " << line << std::endl << warning_text;
    }
}
} // namespace doris

namespace doris {

bool CpuInfo::initialized_ = false;
int64_t CpuInfo::hardware_flags_ = 0;
int64_t CpuInfo::original_hardware_flags_;
int64_t CpuInfo::cycles_per_ms_;
int CpuInfo::num_cores_ = 1;
int CpuInfo::max_num_cores_ = 1;
std::string CpuInfo::model_name_ = "unknown";
int CpuInfo::max_num_numa_nodes_;
std::unique_ptr<int[]> CpuInfo::core_to_numa_node_;
std::vector<vector<int>> CpuInfo::numa_node_to_cores_;
std::vector<int> CpuInfo::numa_node_core_idx_;

static struct {
    string name;
    int64_t flag;
} flag_mappings[] = {
        {"ssse3", CpuInfo::SSSE3},   {"sse4_1", CpuInfo::SSE4_1}, {"sse4_2", CpuInfo::SSE4_2},
        {"popcnt", CpuInfo::POPCNT}, {"avx", CpuInfo::AVX},       {"avx2", CpuInfo::AVX2},
};
static const long num_flags = sizeof(flag_mappings) / sizeof(flag_mappings[0]);

// Helper function to parse for hardware flags.
// values contains a list of space-separated flags.  check to see if the flags we
// care about are present.
// Returns a bitmap of flags.
int64_t ParseCPUFlags(const string& values) {
    int64_t flags = 0;
    for (int i = 0; i < num_flags; ++i) {
        if (contains(values, flag_mappings[i].name)) {
            flags |= flag_mappings[i].flag;
        }
    }
    return flags;
}

void CpuInfo::init() {
    if (initialized_) return;
    string line;
    string name;
    string value;

    float max_mhz = 0;
    int num_cores = 0;

    // Read from /proc/cpuinfo
    std::ifstream cpuinfo("/proc/cpuinfo");
    while (cpuinfo) {
        getline(cpuinfo, line);
        size_t colon = line.find(':');
        if (colon != string::npos) {
            name = line.substr(0, colon - 1);
            value = line.substr(colon + 1, string::npos);
            trim(name);
            trim(value);
            if (name.compare("flags") == 0) {
                hardware_flags_ |= ParseCPUFlags(value);
            } else if (name.compare("cpu MHz") == 0) {
                // Every core will report a different speed.  We'll take the max, assuming
                // that when impala is running, the core will not be in a lower power state.
                // TODO: is there a more robust way to do this, such as
                // Window's QueryPerformanceFrequency()
                float mhz = atof(value.c_str());
                max_mhz = max(mhz, max_mhz);
            } else if (name.compare("processor") == 0) {
                ++num_cores;
            } else if (name.compare("model name") == 0) {
                model_name_ = value;
            }
        }
    }

    if (max_mhz != 0) {
        cycles_per_ms_ = max_mhz * 1000;
    } else {
        cycles_per_ms_ = 1000000;
    }
    original_hardware_flags_ = hardware_flags_;

    if (num_cores > 0) {
        num_cores_ = num_cores;
    } else {
        num_cores_ = 1;
    }
    if (config::num_cores > 0) num_cores_ = config::num_cores;

#ifdef __APPLE__
    size_t len = sizeof(max_num_cores_);
    sysctlbyname("hw.logicalcpu", &max_num_cores_, &len, nullptr, 0);
#else
    max_num_cores_ = get_nprocs_conf();
#endif

    // Print a warning if something is wrong with sched_getcpu().
#ifdef HAVE_SCHED_GETCPU
    if (sched_getcpu() == -1) {
        LOG(WARNING) << "Kernel does not support sched_getcpu(). Performance may be impacted.";
    }
#else
    LOG(WARNING) << "Built on a system without sched_getcpu() support. Performance may"
                 << " be impacted.";
#endif

    _init_numa();
    initialized_ = true;
}

void CpuInfo::_init_numa() {
    // Use the NUMA info in the /sys filesystem. which is part of the Linux ABI:
    // see https://www.kernel.org/doc/Documentation/ABI/stable/sysfs-devices-node and
    // https://www.kernel.org/doc/Documentation/ABI/testing/sysfs-devices-system-cpu
    // The filesystem entries are only present if the kernel was compiled with NUMA support.
    core_to_numa_node_.reset(new int[max_num_cores_]);

    if (!fs::is_directory("/sys/devices/system/node")) {
        LOG(WARNING) << "/sys/devices/system/node is not present - no NUMA support";
        // Assume a single NUMA node.
        max_num_numa_nodes_ = 1;
        std::fill_n(core_to_numa_node_.get(), max_num_cores_, 0);
        _init_numa_node_to_cores();
        return;
    }

    // Search for node subdirectories - node0, node1, node2, etc to determine possible
    // NUMA nodes.
    fs::directory_iterator dir_it("/sys/devices/system/node");
    max_num_numa_nodes_ = 0;
    for (; dir_it != fs::directory_iterator(); ++dir_it) {
        const string filename = dir_it->path().filename().string();
        if (filename.find("node") == 0) ++max_num_numa_nodes_;
    }
    if (max_num_numa_nodes_ == 0) {
        LOG(WARNING) << "Could not find nodes in /sys/devices/system/node";
        max_num_numa_nodes_ = 1;
    }

    // Check which NUMA node each core belongs to based on the existence of a symlink
    // to the node subdirectory.
    for (int core = 0; core < max_num_cores_; ++core) {
        bool found_numa_node = false;
        for (int node = 0; node < max_num_numa_nodes_; ++node) {
            if (fs::exists(
                        strings::Substitute("/sys/devices/system/cpu/cpu$0/node$1", core, node))) {
                core_to_numa_node_[core] = node;
                found_numa_node = true;
                break;
            }
        }
        if (!found_numa_node) {
            LOG(WARNING) << "Could not determine NUMA node for core " << core
                         << " from /sys/devices/system/cpu/";
            core_to_numa_node_[core] = 0;
        }
    }
    _init_numa_node_to_cores();
}

void CpuInfo::_init_fake_numa_for_test(int max_num_numa_nodes,
                                       const std::vector<int>& core_to_numa_node) {
    DCHECK_EQ(max_num_cores_, core_to_numa_node.size());
    max_num_numa_nodes_ = max_num_numa_nodes;
    for (int i = 0; i < max_num_cores_; ++i) {
        core_to_numa_node_[i] = core_to_numa_node[i];
    }
    numa_node_to_cores_.clear();
    _init_numa_node_to_cores();
}

void CpuInfo::_init_numa_node_to_cores() {
    DCHECK(numa_node_to_cores_.empty());
    numa_node_to_cores_.resize(max_num_numa_nodes_);
    numa_node_core_idx_.resize(max_num_cores_);
    for (int core = 0; core < max_num_cores_; ++core) {
        std::vector<int>* cores_of_node = &numa_node_to_cores_[core_to_numa_node_[core]];
        numa_node_core_idx_[core] = cores_of_node->size();
        cores_of_node->push_back(core);
    }
}

void CpuInfo::verify_cpu_requirements() {
    if (!CpuInfo::is_supported(CpuInfo::SSSE3)) {
        LOG(ERROR) << "CPU does not support the Supplemental SSE3 (SSSE3) instruction set. "
                   << "This setup is generally unsupported and Impala might be unstable.";
    }
}

void CpuInfo::verify_performance_governor() {
    for (int cpu_id = 0; cpu_id < CpuInfo::num_cores(); ++cpu_id) {
        const string governor_file = strings::Substitute(
                "/sys/devices/system/cpu/cpu$0/cpufreq/scaling_governor", cpu_id);
        const string warning_text = strings::Substitute(
                "WARNING: CPU $0 is not using 'performance' governor. Note that changing the "
                "governor to 'performance' will reset the no_turbo setting to 0.",
                cpu_id);
        WarnIfFileNotEqual(governor_file, "performance", warning_text);
    }
}

void CpuInfo::verify_turbo_disabled() {
    WarnIfFileNotEqual(
            "/sys/devices/system/cpu/intel_pstate/no_turbo", "1",
            "WARNING: CPU turbo is enabled. This setting can change the clock frequency of CPU "
            "cores during the benchmark run, which can lead to inaccurate results. You can "
            "disable CPU turbo by writing a 1 to "
            "/sys/devices/system/cpu/intel_pstate/no_turbo. Note that changing the governor to "
            "'performance' will reset this to 0.");
}

void CpuInfo::enable_feature(long flag, bool enable) {
    DCHECK(initialized_);
    if (!enable) {
        hardware_flags_ &= ~flag;
    } else {
        // Can't turn something on that can't be supported
        DCHECK((original_hardware_flags_ & flag) != 0);
        hardware_flags_ |= flag;
    }
}

int CpuInfo::get_current_core() {
    // sched_getcpu() is not supported on some old kernels/glibcs (like the versions that
    // shipped with CentOS 5). In that case just pretend we're always running on CPU 0
    // so that we can build and run with degraded perf.
#ifdef HAVE_SCHED_GETCPU
    int cpu = sched_getcpu();
    if (cpu < 0) return 0;
    if (cpu >= max_num_cores_) {
        LOG_FIRST_N(WARNING, 5) << "sched_getcpu() return value " << cpu
                                << ", which is greater than get_nprocs_conf() retrun value "
                                << max_num_cores_ << ", now is " << get_nprocs_conf();
        cpu %= max_num_cores_;
    }
    return cpu;
#else
    return 0;
#endif
}

void CpuInfo::_get_cache_info(long cache_sizes[NUM_CACHE_LEVELS],
                              long cache_line_sizes[NUM_CACHE_LEVELS]) {
#ifdef __APPLE__
    // On Mac OS X use sysctl() to get the cache sizes
    size_t len = 0;
    sysctlbyname("hw.cachesize", nullptr, &len, nullptr, 0);
    uint64_t* data = static_cast<uint64_t*>(malloc(len));
    sysctlbyname("hw.cachesize", data, &len, nullptr, 0);
#ifndef __arm64__
    DCHECK(len / sizeof(uint64_t) >= 3);
    for (size_t i = 0; i < NUM_CACHE_LEVELS; ++i) {
        cache_sizes[i] = data[i];
    }
#else
    for (size_t i = 0; i < NUM_CACHE_LEVELS; ++i) {
        cache_sizes[i] = data[i + 1];
    }
#endif
    size_t linesize;
    size_t sizeof_linesize = sizeof(linesize);
    sysctlbyname("hw.cachelinesize", &linesize, &sizeof_linesize, nullptr, 0);
    for (size_t i = 0; i < NUM_CACHE_LEVELS; ++i) cache_line_sizes[i] = linesize;
#else
    // Call sysconf to query for the cache sizes
    // Note: on some systems (e.g. RHEL 5 on AWS EC2), this returns 0 instead of the
    // actual cache line size.
    cache_sizes[L1_CACHE] = sysconf(_SC_LEVEL1_DCACHE_SIZE);
    cache_sizes[L2_CACHE] = sysconf(_SC_LEVEL2_CACHE_SIZE);
    cache_sizes[L3_CACHE] = sysconf(_SC_LEVEL3_CACHE_SIZE);

    cache_line_sizes[L1_CACHE] = sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
    cache_line_sizes[L2_CACHE] = sysconf(_SC_LEVEL2_CACHE_LINESIZE);
    cache_line_sizes[L3_CACHE] = sysconf(_SC_LEVEL3_CACHE_LINESIZE);
#endif
}

std::string CpuInfo::debug_string() {
    DCHECK(initialized_);
    std::stringstream stream;
    long cache_sizes[NUM_CACHE_LEVELS];
    long cache_line_sizes[NUM_CACHE_LEVELS];
    _get_cache_info(cache_sizes, cache_line_sizes);

    string L1 = strings::Substitute(
            "L1 Cache: $0 (Line: $1)",
            PrettyPrinter::print(static_cast<int64_t>(cache_sizes[L1_CACHE]), TUnit::BYTES),
            PrettyPrinter::print(static_cast<int64_t>(cache_line_sizes[L1_CACHE]), TUnit::BYTES));
    string L2 = strings::Substitute(
            "L2 Cache: $0 (Line: $1)",
            PrettyPrinter::print(static_cast<int64_t>(cache_sizes[L2_CACHE]), TUnit::BYTES),
            PrettyPrinter::print(static_cast<int64_t>(cache_line_sizes[L2_CACHE]), TUnit::BYTES));
    string L3 =
            cache_sizes[L3_CACHE]
                    ? strings::Substitute(
                              "L3 Cache: $0 (Line: $1)",
                              PrettyPrinter::print(static_cast<int64_t>(cache_sizes[L3_CACHE]),
                                                   TUnit::BYTES),
                              PrettyPrinter::print(static_cast<int64_t>(cache_line_sizes[L3_CACHE]),
                                                   TUnit::BYTES))
                    : "";
    stream << "Cpu Info:" << std::endl
           << "  Model: " << model_name_ << std::endl
           << "  Cores: " << num_cores_ << std::endl
           << "  Max Possible Cores: " << max_num_cores_ << std::endl
           << "  " << L1 << std::endl
           << "  " << L2 << std::endl
           << "  " << L3 << std::endl
           << "  Hardware Supports:" << std::endl;
    for (int i = 0; i < num_flags; ++i) {
        if (is_supported(flag_mappings[i].flag)) {
            stream << "    " << flag_mappings[i].name << std::endl;
        }
    }
    stream << "  Numa Nodes: " << max_num_numa_nodes_ << std::endl;
    stream << "  Numa Nodes of Cores:";
    for (int core = 0; core < max_num_cores_; ++core) {
        stream << " " << core << "->" << core_to_numa_node_[core] << " |";
    }
    stream << std::endl;
    return stream.str();
}

} // namespace doris
