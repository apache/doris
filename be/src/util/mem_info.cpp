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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/mem-info.cc
// and modified by Doris

#include "mem_info.h"

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <sstream>

#include "common/config.h"
#include "gutil/strings/split.h"
#include "util/cgroup_util.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/string_parser.hpp"

namespace doris {

bool MemInfo::_s_initialized = false;
int64_t MemInfo::_s_physical_mem = -1;
int64_t MemInfo::_s_mem_limit = -1;
std::string MemInfo::_s_mem_limit_str = "";
int64_t MemInfo::_s_hard_mem_limit = -1;
size_t MemInfo::_s_allocator_physical_mem = 0;
size_t MemInfo::_s_pageheap_unmapped_bytes = 0;
size_t MemInfo::_s_tcmalloc_pageheap_free_bytes = 0;
size_t MemInfo::_s_tcmalloc_central_bytes = 0;
size_t MemInfo::_s_tcmalloc_transfer_bytes = 0;
size_t MemInfo::_s_tcmalloc_thread_bytes = 0;
size_t MemInfo::_s_allocator_cache_mem = 0;
std::string MemInfo::_s_allocator_cache_mem_str = "";
size_t MemInfo::_s_virtual_memory_used = 0;
int64_t MemInfo::_s_proc_mem_no_allocator_cache = -1;

#ifndef __APPLE__
void MemInfo::init() {
    // Read from /proc/meminfo
    std::ifstream meminfo("/proc/meminfo", std::ios::in);
    std::string line;

    while (meminfo.good() && !meminfo.eof()) {
        getline(meminfo, line);
        std::vector<std::string> fields = strings::Split(line, " ", strings::SkipWhitespace());

        // We expect lines such as, e.g., 'MemTotal: 16129508 kB'
        if (fields.size() < 3) {
            continue;
        }

        if (fields[0].compare("MemTotal:") != 0) {
            continue;
        }

        StringParser::ParseResult result;
        int64_t mem_total_kb =
                StringParser::string_to_int<int64_t>(fields[1].data(), fields[1].size(), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            // Entries in /proc/meminfo are in KB.
            _s_physical_mem = mem_total_kb * 1024L;
        }

        break;
    }

    int64_t cgroup_mem_limit = 0;
    Status status = CGroupUtil::find_cgroup_mem_limit(&cgroup_mem_limit);
    if (status.ok() && cgroup_mem_limit > 0) {
        _s_physical_mem = std::min(_s_physical_mem, cgroup_mem_limit);
    }

    if (meminfo.is_open()) {
        meminfo.close();
    }

    if (_s_physical_mem == -1) {
        LOG(WARNING) << "Could not determine amount of physical memory on this machine.";
    }

    bool is_percent = true;
    _s_mem_limit = ParseUtil::parse_mem_spec(config::mem_limit, -1, _s_physical_mem, &is_percent);
    _s_mem_limit_str = PrettyPrinter::print(_s_mem_limit, TUnit::BYTES);
    _s_hard_mem_limit =
            _s_physical_mem - std::max<int64_t>(209715200L, _s_physical_mem / 10); // 200M

    LOG(INFO) << "Physical Memory: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES);
    _s_initialized = true;
}
#else
void MemInfo::init() {
    size_t size = sizeof(_s_physical_mem);
    if (sysctlbyname("hw.memsize", &_s_physical_mem, &size, nullptr, 0) != 0) {
        LOG(WARNING) << "Could not determine amount of physical memory on this machine.";
        _s_physical_mem = -1;
    }

    bool is_percent = true;
    _s_mem_limit = ParseUtil::parse_mem_spec(config::mem_limit, -1, _s_physical_mem, &is_percent);
    _s_mem_limit_str = PrettyPrinter::print(_s_mem_limit, TUnit::BYTES);
    _s_hard_mem_limit =
            _s_physical_mem - std::max<int64_t>(209715200L, _s_physical_mem / 10); // 200M

    LOG(INFO) << "Physical Memory: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES);
    _s_initialized = true;
}
#endif

std::string MemInfo::debug_string() {
    DCHECK(_s_initialized);
    CGroupUtil util;
    std::stringstream stream;
    stream << "Physical Memory: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES)
           << std::endl;
    stream << "Memory Limt: " << PrettyPrinter::print(_s_mem_limit, TUnit::BYTES) << std::endl;
    stream << "Current Usage: "
           << PrettyPrinter::print(static_cast<uint64_t>(_s_allocator_physical_mem), TUnit::BYTES)
           << std::endl;
    stream << "CGroup Info: " << util.debug_string() << std::endl;
    return stream.str();
}

} // namespace doris
