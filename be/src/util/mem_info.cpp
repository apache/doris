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
int64_t MemInfo::_s_soft_mem_limit = -1;

int64_t MemInfo::_s_allocator_cache_mem = 0;
std::string MemInfo::_s_allocator_cache_mem_str = "";
int64_t MemInfo::_s_virtual_memory_used = 0;
int64_t MemInfo::_s_proc_mem_no_allocator_cache = -1;

static std::unordered_map<std::string, int64_t> _mem_info_bytes;
int64_t MemInfo::_s_sys_mem_available = 0;
std::string MemInfo::_s_sys_mem_available_str = "";
int64_t MemInfo::_s_sys_mem_available_low_water_mark = 0;
int64_t MemInfo::_s_sys_mem_available_warning_water_mark = 0;

void MemInfo::refresh_allocator_mem() {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    LOG(INFO) << "Memory tracking is not available with address sanitizer builds.";
#elif defined(USE_JEMALLOC)
    uint64_t epoch = 0;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);

    // https://jemalloc.net/jemalloc.3.html
    _s_allocator_cache_mem =
            get_je_metrics(fmt::format("stats.arenas.{}.tcache_bytes", MALLCTL_ARENAS_ALL));
    _s_allocator_cache_mem_str =
            PrettyPrinter::print(static_cast<uint64_t>(_s_allocator_cache_mem), TUnit::BYTES);
    _s_virtual_memory_used = get_je_metrics("stats.mapped");
#else
    _s_allocator_cache_mem = get_tc_metrics("tcmalloc.pageheap_free_bytes") +
                             get_tc_metrics("tcmalloc.central_cache_free_bytes") +
                             get_tc_metrics("tcmalloc.transfer_cache_free_bytes") +
                             get_tc_metrics("tcmalloc.thread_cache_free_bytes");
    _s_allocator_cache_mem_str =
            PrettyPrinter::print(static_cast<uint64_t>(_s_allocator_cache_mem), TUnit::BYTES);
    _s_virtual_memory_used = get_tc_metrics("generic.total_physical_bytes") +
                             get_tc_metrics("tcmalloc.pageheap_unmapped_bytes");
#endif
}

#ifndef __APPLE__
void MemInfo::refresh_proc_meminfo() {
    std::ifstream meminfo("/proc/meminfo", std::ios::in);
    std::string line;

    while (meminfo.good() && !meminfo.eof()) {
        getline(meminfo, line);
        std::vector<std::string> fields = strings::Split(line, " ", strings::SkipWhitespace());
        if (fields.size() < 2) continue;
        std::string key = fields[0].substr(0, fields[0].size() - 1);

        StringParser::ParseResult result;
        int64_t mem_value =
                StringParser::string_to_int<int64_t>(fields[1].data(), fields[1].size(), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            if (fields.size() == 2) {
                _mem_info_bytes[key] = mem_value;
            } else if (fields[2].compare("kB") == 0) {
                _mem_info_bytes[key] = mem_value * 1024L;
            }
        }
    }
    if (meminfo.is_open()) meminfo.close();

    _s_sys_mem_available = _mem_info_bytes["MemAvailable"];
    _s_sys_mem_available_str = PrettyPrinter::print(_s_sys_mem_available, TUnit::BYTES);
}

void MemInfo::init() {
    refresh_proc_meminfo();
    _s_physical_mem = _mem_info_bytes["MemTotal"];

    int64_t cgroup_mem_limit = 0;
    Status status = CGroupUtil::find_cgroup_mem_limit(&cgroup_mem_limit);
    if (status.ok() && cgroup_mem_limit > 0) {
        _s_physical_mem = std::min(_s_physical_mem, cgroup_mem_limit);
    }

    if (_s_physical_mem == -1) {
        LOG(WARNING) << "Could not determine amount of physical memory on this machine.";
    }

    bool is_percent = true;
    _s_mem_limit = ParseUtil::parse_mem_spec(config::mem_limit, -1, _s_physical_mem, &is_percent);
    if (_s_mem_limit <= 0) {
        LOG(WARNING) << "Failed to parse mem limit from '" + config::mem_limit + "'.";
    }
    if (_s_mem_limit > _s_physical_mem) {
        LOG(WARNING) << "Memory limit " << PrettyPrinter::print(_s_mem_limit, TUnit::BYTES)
                     << " exceeds physical memory of "
                     << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES)
                     << ". Using physical memory instead";
        _s_mem_limit = _s_physical_mem;
    }
    _s_mem_limit_str = PrettyPrinter::print(_s_mem_limit, TUnit::BYTES);
    _s_soft_mem_limit = _s_mem_limit * config::soft_mem_limit_frac;

    std::string line;
    int64_t _s_vm_min_free_kbytes = 0;
    std::ifstream vminfo("/proc/sys/vm/min_free_kbytes", std::ios::in);
    if (vminfo.good() && !vminfo.eof()) {
        getline(vminfo, line);
        boost::algorithm::trim(line);
        StringParser::ParseResult result;
        int64_t mem_value = StringParser::string_to_int<int64_t>(line.data(), line.size(), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            _s_vm_min_free_kbytes = mem_value * 1024L;
        }
    }
    if (vminfo.is_open()) vminfo.close();

    // MemAvailable = MemFree - LowWaterMark + (PageCache - min(PageCache / 2, LowWaterMark))
    // LowWaterMark = /proc/sys/vm/min_free_kbytes
    // Ref:
    // https://serverfault.com/questions/940196/why-is-memavailable-a-lot-less-than-memfreebufferscached
    // https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=34e431b0ae398fc54ea69ff85ec700722c9da773
    //
    // available_low_water_mark = p1 - p2
    // p1: upper sys_mem_available_low_water_mark, avoid wasting too much memory.
    // p2: vm/min_free_kbytes is usually 0.4% - 5% of the total memory, some cloud machines vm/min_free_kbytes is 5%,
    //     in order to avoid wasting too much memory, available_low_water_mark minus 1% at most.
    int64_t p1 = std::min<int64_t>(
            std::min<int64_t>(_s_physical_mem - _s_mem_limit, _s_physical_mem * 0.1),
            config::max_sys_mem_available_low_water_mark_bytes);
    int64_t p2 = std::max<int64_t>(_s_vm_min_free_kbytes - _s_physical_mem * 0.01, 0);
    _s_sys_mem_available_low_water_mark = std::max<int64_t>(p1 - p2, 0);
    _s_sys_mem_available_warning_water_mark = _s_sys_mem_available_low_water_mark + p1 * 2;

    LOG(INFO) << "Physical Memory: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES)
              << ", Mem Limit: " << _s_mem_limit_str
              << ", origin config value: " << config::mem_limit
              << ", System Mem Available Min Reserve: "
              << PrettyPrinter::print(_s_sys_mem_available_low_water_mark, TUnit::BYTES)
              << ", Vm Min Free KBytes: "
              << PrettyPrinter::print(_s_vm_min_free_kbytes, TUnit::BYTES);
    _s_initialized = true;
}
#else
void MemInfo::refresh_proc_meminfo() {}

void MemInfo::init() {
    size_t size = sizeof(_s_physical_mem);
    if (sysctlbyname("hw.memsize", &_s_physical_mem, &size, nullptr, 0) != 0) {
        LOG(WARNING) << "Could not determine amount of physical memory on this machine.";
        _s_physical_mem = -1;
    }

    bool is_percent = true;
    _s_mem_limit = ParseUtil::parse_mem_spec(config::mem_limit, -1, _s_physical_mem, &is_percent);
    _s_mem_limit_str = PrettyPrinter::print(_s_mem_limit, TUnit::BYTES);
    _s_soft_mem_limit = _s_mem_limit * config::soft_mem_limit_frac;

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
    stream << "CGroup Info: " << util.debug_string() << std::endl;
    return stream.str();
}

} // namespace doris
