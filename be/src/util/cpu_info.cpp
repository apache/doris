// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#include "util/cpu_info.h"

#include <stdlib.h>
#include <string.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <unistd.h>

#include <boost/algorithm/string.hpp>

#include "util/debug_util.h"
#include "util/pretty_printer.h"

namespace palo {

bool CpuInfo::_s_initialized = false;
int64_t CpuInfo::_s_hardware_flags = 0;
int64_t CpuInfo::_s_original_hardware_flags;
long CpuInfo::_s_cache_sizes[L3_CACHE + 1];
int64_t CpuInfo::_s_cycles_per_ms;
int CpuInfo::_s_num_cores = 1;
std::string CpuInfo::_s_model_name = "unknown";

static struct {
    std::string name;
    int64_t flag;
} flag_mappings[] = {
    { "ssse3",  CpuInfo::SSE3 },
    { "sse4_1", CpuInfo::SSE4_1 },
    { "sse4_2", CpuInfo::SSE4_2 },
    { "popcnt", CpuInfo::POPCNT },
};
static const long num_flags = sizeof(flag_mappings) / sizeof(flag_mappings[0]);

// Helper function to parse for hardware flags.
// values contains a list of space-seperated flags.  check to see if the flags we
// care about are present.
// Returns a bitmap of flags.
int64_t parse_cpu_flags(const std::string& values) {
    int64_t flags = 0;

    for (int i = 0; i < num_flags; ++i) {
        if (boost::contains(values, flag_mappings[i].name)) {
            flags |= flag_mappings[i].flag;
        }
    }

    return flags;
}

void CpuInfo::init() {
    std::string line;
    std::string name;
    std::string value;

    float max_mhz = 0;
    int num_cores = 0;

    memset(&_s_cache_sizes, 0, sizeof(_s_cache_sizes));

    // Read from /proc/cpuinfo
    std::ifstream cpuinfo("/proc/cpuinfo", std::ios::in);

    while (cpuinfo) {
        getline(cpuinfo, line);
        size_t colon = line.find(':');

        if (colon != std::string::npos) {
            name = line.substr(0, colon - 1);
            value = line.substr(colon + 1, std::string::npos);
            boost::trim(name);
            boost::trim(value);

            if (name.compare("flags") == 0) {
                _s_hardware_flags |= parse_cpu_flags(value);
            } else if (name.compare("cpu MHz") == 0) {
                // Every core will report a different speed.  We'll take the max, assuming
                // that when impala is running, the core will not be in a lower power state.
                // TODO: is there a more robust way to do this, such as
                // Window's QueryPerformanceFrequency()
                float mhz = atof(value.c_str());
                max_mhz = std::max(mhz, max_mhz);
            } else if (name.compare("processor") == 0) {
                ++num_cores;
            } else if (name.compare("model name") == 0) {
                _s_model_name = value;
            }
        }
    }

    if (cpuinfo.is_open()) {
        cpuinfo.close();
    }

    // Call sysconf to query for the cache sizes
    _s_cache_sizes[0] = sysconf(_SC_LEVEL1_DCACHE_SIZE);
    _s_cache_sizes[1] = sysconf(_SC_LEVEL2_CACHE_SIZE);
    _s_cache_sizes[2] = sysconf(_SC_LEVEL3_CACHE_SIZE);

    if (max_mhz > 0.0) {
        _s_cycles_per_ms = max_mhz * 1000;
    } else {
        _s_cycles_per_ms = 1000000;
    }

    _s_original_hardware_flags = _s_hardware_flags;

    if (num_cores > 0) {
        _s_num_cores = num_cores;
    } else {
        _s_num_cores = 1;
    }

    _s_initialized = true;
}

void CpuInfo::enable_feature(long flag, bool enable) {
    DCHECK(_s_initialized);

    if (!enable) {
        _s_hardware_flags &= ~flag;
    } else {
        // Can't turn something on that can't be supported
        DCHECK((_s_original_hardware_flags & flag) != 0);
        _s_hardware_flags |= flag;
    }
}

std::string CpuInfo::debug_string() {
    DCHECK(_s_initialized);
    std::stringstream stream;
    int64_t l1 = cache_size(L1_CACHE);
    int64_t l2 = cache_size(L2_CACHE);
    int64_t l3 = cache_size(L3_CACHE);
    stream << "Cpu Info:" << std::endl
           << "  Model: " << _s_model_name << std::endl
           << "  Cores: " << _s_num_cores << std::endl
           << "  L1 Cache: " << PrettyPrinter::print(l1, TUnit::BYTES) << std::endl
           << "  L2 Cache: " << PrettyPrinter::print(l2, TUnit::BYTES) << std::endl
           << "  L3 Cache: " << PrettyPrinter::print(l3, TUnit::BYTES) << std::endl
           << "  Hardware Supports:" << std::endl;

    for (int i = 0; i < num_flags; ++i) {
        if (is_supported(flag_mappings[i].flag)) {
            stream << "    " << flag_mappings[i].name << std::endl;
        }
    }

    return stream.str();
}

}
