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

#include <bvar/bvar.h>
#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/segment_v2.pb.h>
#include <jemalloc/jemalloc.h>

#include <algorithm>
#include <boost/algorithm/string/trim.hpp>
#include <fstream>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "util/cgroup_util.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/string_parser.hpp"

namespace doris {

bool MemInfo::_s_initialized = false;
std::atomic<int64_t> MemInfo::_s_physical_mem = std::numeric_limits<int64_t>::max();
std::atomic<int64_t> MemInfo::_s_mem_limit = std::numeric_limits<int64_t>::max();
std::atomic<int64_t> MemInfo::_s_soft_mem_limit = std::numeric_limits<int64_t>::max();

std::atomic<int64_t> MemInfo::_s_allocator_cache_mem = 0;
std::atomic<int64_t> MemInfo::_s_virtual_memory_used = 0;

int64_t MemInfo::_s_cgroup_mem_limit = std::numeric_limits<int64_t>::max();
int64_t MemInfo::_s_cgroup_mem_usage = std::numeric_limits<int64_t>::min();
static std::unordered_map<std::string, int64_t> _s_cgroup_mem_info_bytes;
bool MemInfo::_s_cgroup_mem_refresh_state = false;
int64_t MemInfo::_s_cgroup_mem_refresh_wait_times = 0;

static std::unordered_map<std::string, int64_t> _mem_info_bytes;
std::atomic<int64_t> MemInfo::_s_sys_mem_available = -1;
int64_t MemInfo::_s_sys_mem_available_low_water_mark = std::numeric_limits<int64_t>::min();
int64_t MemInfo::_s_sys_mem_available_warning_water_mark = std::numeric_limits<int64_t>::min();
std::atomic<int64_t> MemInfo::_s_process_minor_gc_size = -1;
std::atomic<int64_t> MemInfo::_s_process_full_gc_size = -1;
std::mutex MemInfo::je_purge_dirty_pages_lock;
std::condition_variable MemInfo::je_purge_dirty_pages_cv;
std::atomic<bool> MemInfo::je_purge_dirty_pages_notify {false};

void MemInfo::refresh_allocator_mem() {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
#elif defined(USE_JEMALLOC)
    // 'epoch' is a special mallctl -- it updates the statistics. Without it, all
    // the following calls will return stale values. It increments and returns
    // the current epoch number, which might be useful to log as a sanity check.
    uint64_t epoch = 0;
    size_t sz = sizeof(epoch);
#ifdef USE_JEMALLOC_HOOK
    jemallctl("epoch", &epoch, &sz, &epoch, sz);
#else
    mallctl("epoch", &epoch, &sz, &epoch, sz);
#endif

    // https://jemalloc.net/jemalloc.3.html
    // https://www.bookstack.cn/read/aliyun-rds-core/4a0cdf677f62feb3.md
    _s_allocator_cache_mem.store(get_je_all_arena_metrics("tcache_bytes") +
                                         get_je_metrics("stats.metadata") +
                                         get_je_all_arena_metrics("pdirty") * get_page_size(),
                                 std::memory_order_relaxed);
    _s_virtual_memory_used.store(get_je_metrics("stats.mapped"), std::memory_order_relaxed);
#else
    _s_allocator_cache_mem.store(get_tc_metrics("tcmalloc.pageheap_free_bytes") +
                                         get_tc_metrics("tcmalloc.central_cache_free_bytes") +
                                         get_tc_metrics("tcmalloc.transfer_cache_free_bytes") +
                                         get_tc_metrics("tcmalloc.thread_cache_free_bytes"),
                                 std::memory_order_relaxed);
    _s_virtual_memory_used.store(get_tc_metrics("generic.total_physical_bytes") +
                                         get_tc_metrics("tcmalloc.pageheap_unmapped_bytes"),
                                 std::memory_order_relaxed);
#endif
}

#ifndef __APPLE__
void MemInfo::refresh_proc_meminfo() {
    std::ifstream meminfo("/proc/meminfo", std::ios::in);
    std::string line;

    while (meminfo.good() && !meminfo.eof()) {
        getline(meminfo, line);
        std::vector<std::string> fields = strings::Split(line, " ", strings::SkipWhitespace());
        if (fields.size() < 2) {
            continue;
        }
        std::string key = fields[0].substr(0, fields[0].size() - 1);

        StringParser::ParseResult result;
        auto mem_value =
                StringParser::string_to_int<int64_t>(fields[1].data(), fields[1].size(), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            if (fields.size() == 2) {
                _mem_info_bytes[key] = mem_value;
            } else if (fields[2].compare("kB") == 0) {
                _mem_info_bytes[key] = mem_value * 1024L;
            }
        }
    }
    if (meminfo.is_open()) {
        meminfo.close();
    }

    // refresh cgroup memory
    if (_s_cgroup_mem_refresh_wait_times >= 0 && config::enable_use_cgroup_memory_info) {
        int64_t cgroup_mem_limit = -1;
        int64_t cgroup_mem_usage = -1;
        std::string cgroup_mem_info_file_path;
        _s_cgroup_mem_refresh_state = true;
        Status status = CGroupUtil::find_cgroup_mem_limit(&cgroup_mem_limit);
        if (!status.ok() || cgroup_mem_limit <= 0) {
            _s_cgroup_mem_refresh_state = false;
        }
        status = CGroupUtil::find_cgroup_mem_usage(&cgroup_mem_usage);
        if (!status.ok() || cgroup_mem_usage <= 0) {
            _s_cgroup_mem_refresh_state = false;
        }
        status = CGroupUtil::find_cgroup_mem_info(&cgroup_mem_info_file_path);
        if (status.ok()) {
            std::ifstream cgroup_meminfo(cgroup_mem_info_file_path, std::ios::in);
            std::string line;

            while (cgroup_meminfo.good() && !cgroup_meminfo.eof()) {
                getline(cgroup_meminfo, line);
                std::vector<std::string> fields =
                        strings::Split(line, " ", strings::SkipWhitespace());
                if (fields.size() < 2) {
                    continue;
                }
                std::string key = fields[0].substr(0, fields[0].size() - 1);

                StringParser::ParseResult result;
                auto mem_value = StringParser::string_to_int<int64_t>(fields[1].data(),
                                                                      fields[1].size(), &result);

                if (result == StringParser::PARSE_SUCCESS) {
                    if (fields.size() == 2) {
                        _s_cgroup_mem_info_bytes[key] = mem_value;
                    } else if (fields[2] == "kB") {
                        _s_cgroup_mem_info_bytes[key] = mem_value * 1024L;
                    }
                }
            }
            if (cgroup_meminfo.is_open()) {
                cgroup_meminfo.close();
            }
        } else {
            _s_cgroup_mem_refresh_state = false;
        }

        if (_s_cgroup_mem_refresh_state) {
            _s_cgroup_mem_limit = cgroup_mem_limit;
            // https://serverfault.com/questions/902009/the-memory-usage-reported-in-cgroup-differs-from-the-free-command
            // memory.usage_in_bytes ~= free.used + free.(buff/cache) - (buff)
            // so, memory.usage_in_bytes - memory.meminfo["Cached"]
            _s_cgroup_mem_usage = cgroup_mem_usage - _s_cgroup_mem_info_bytes["Cached"];
            // wait 10s, 100 * 100ms, avoid too frequently.
            _s_cgroup_mem_refresh_wait_times = -100;
            LOG(INFO) << "Refresh cgroup memory win, refresh again after 10s, cgroup mem limit: "
                      << _s_cgroup_mem_limit << ", cgroup mem usage: " << _s_cgroup_mem_usage
                      << ", cgroup mem info cached: " << _s_cgroup_mem_info_bytes["Cached"];
        } else {
            // find cgroup failed, wait 300s, 1000 * 100ms.
            _s_cgroup_mem_refresh_wait_times = -3000;
            LOG(INFO)
                    << "Refresh cgroup memory failed, refresh again after 300s, cgroup mem limit: "
                    << _s_cgroup_mem_limit << ", cgroup mem usage: " << _s_cgroup_mem_usage
                    << ", cgroup mem info cached: " << _s_cgroup_mem_info_bytes["Cached"];
        }
    } else {
        if (config::enable_use_cgroup_memory_info) {
            _s_cgroup_mem_refresh_wait_times++;
        } else {
            _s_cgroup_mem_refresh_state = false;
        }
    }

    // 1. calculate physical_mem
    int64_t physical_mem = -1;

    physical_mem = _mem_info_bytes["MemTotal"];
    if (_s_cgroup_mem_refresh_state) {
        // In theory, always cgroup_mem_limit < physical_mem
        if (physical_mem < 0) {
            physical_mem = _s_cgroup_mem_limit;
        } else {
            physical_mem = std::min(physical_mem, _s_cgroup_mem_limit);
        }
    }

    if (physical_mem <= 0) {
        LOG(WARNING)
                << "Could not determine amount of physical memory on this machine, physical_mem: "
                << physical_mem;
    }

    // 2. if physical_mem changed, refresh mem limit and gc size.
    if (physical_mem > 0 && _s_physical_mem.load(std::memory_order_relaxed) != physical_mem) {
        _s_physical_mem.store(physical_mem);

        bool is_percent = true;
        _s_mem_limit.store(
                ParseUtil::parse_mem_spec(config::mem_limit, -1, _s_physical_mem, &is_percent));
        if (_s_mem_limit <= 0) {
            LOG(WARNING) << "Failed to parse mem limit from '" + config::mem_limit + "'.";
        }
        if (_s_mem_limit > _s_physical_mem) {
            LOG(WARNING) << "Memory limit " << PrettyPrinter::print(_s_mem_limit, TUnit::BYTES)
                         << " exceeds physical memory of "
                         << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES)
                         << ". Using physical memory instead";
            _s_mem_limit.store(_s_physical_mem);
        }
        _s_soft_mem_limit.store(int64_t(_s_mem_limit * config::soft_mem_limit_frac));

        _s_process_minor_gc_size.store(ParseUtil::parse_mem_spec(config::process_minor_gc_size, -1,
                                                                 _s_mem_limit, &is_percent));
        _s_process_full_gc_size.store(ParseUtil::parse_mem_spec(config::process_full_gc_size, -1,
                                                                _s_mem_limit, &is_percent));
    }

    // 3. refresh process available memory
    int64_t mem_available = -1;
    if (_mem_info_bytes.find("MemAvailable") != _mem_info_bytes.end()) {
        mem_available = _mem_info_bytes["MemAvailable"];
    }
    if (_s_cgroup_mem_refresh_state) {
        if (mem_available < 0) {
            mem_available = _s_cgroup_mem_limit - _s_cgroup_mem_usage;
        } else {
            mem_available = std::min(mem_available, _s_cgroup_mem_limit - _s_cgroup_mem_usage);
        }
    }
    if (mem_available < 0) {
        LOG(WARNING) << "Failed to get available memory, set MAX_INT.";
        mem_available = std::numeric_limits<int64_t>::max();
    }
    if (_s_sys_mem_available.load(std::memory_order_relaxed) != mem_available) {
        _s_sys_mem_available.store(mem_available);
    }
}

void MemInfo::init() {
    refresh_proc_meminfo();

    std::string line;
    int64_t _s_vm_min_free_kbytes = 0;
    std::ifstream vminfo("/proc/sys/vm/min_free_kbytes", std::ios::in);
    if (vminfo.good() && !vminfo.eof()) {
        getline(vminfo, line);
        boost::algorithm::trim(line);
        StringParser::ParseResult result;
        auto mem_value = StringParser::string_to_int<int64_t>(line.data(), line.size(), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            _s_vm_min_free_kbytes = mem_value * 1024L;
        }
    }
    if (vminfo.is_open()) {
        vminfo.close();
    }

    // Redhat 4.x OS, `/proc/meminfo` has no `MemAvailable`.
    if (_mem_info_bytes.find("MemAvailable") != _mem_info_bytes.end()) {
        // MemAvailable = MemFree - LowWaterMark + (PageCache - min(PageCache / 2, LowWaterMark))
        // LowWaterMark = /proc/sys/vm/min_free_kbytes
        // Ref:
        // https://serverfault.com/questions/940196/why-is-memavailable-a-lot-less-than-memfreebufferscached
        // https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=34e431b0ae398fc54ea69ff85ec700722c9da773
        //
        // upper sys_mem_available_low_water_mark, avoid wasting too much memory.
        _s_sys_mem_available_low_water_mark = std::max<int64_t>(
                std::min<int64_t>(std::min<int64_t>(_s_physical_mem - _s_mem_limit,
                                                    int64_t(_s_physical_mem * 0.1)),
                                  config::max_sys_mem_available_low_water_mark_bytes),
                0);
        _s_sys_mem_available_warning_water_mark = _s_sys_mem_available_low_water_mark * 2;
    }

    std::ifstream sys_transparent_hugepage("/sys/kernel/mm/transparent_hugepage/enabled",
                                           std::ios::in);
    std::string hugepage_enable;
    // If file not exist, getline returns an empty string.
    getline(sys_transparent_hugepage, hugepage_enable);
    if (sys_transparent_hugepage.is_open()) {
        sys_transparent_hugepage.close();
    }
    if (hugepage_enable == "[always] madvise never") {
        std::cout << "[WARNING!] /sys/kernel/mm/transparent_hugepage/enabled: " << hugepage_enable
                  << ", Doris not recommend turning on THP, which may cause the BE process to use "
                     "more memory and cannot be freed in time. Turn off THP: `echo madvise | sudo "
                     "tee /sys/kernel/mm/transparent_hugepage/enabled`"
                  << std::endl;
    }

    // Expect vm overcommit memory value to be 1, system will no longer throw bad_alloc, memory alloc are always accepted,
    // memory limit check is handed over to Doris Allocator, make sure throw exception position is controllable,
    // otherwise bad_alloc can be thrown anywhere and it will be difficult to achieve exception safety.
    std::ifstream sys_vm("/proc/sys/vm/overcommit_memory", std::ios::in);
    std::string vm_overcommit;
    getline(sys_vm, vm_overcommit);
    if (sys_vm.is_open()) {
        sys_vm.close();
    }
    if (!vm_overcommit.empty() && std::stoi(vm_overcommit) == 2) {
        std::cout << "[WARNING!] /proc/sys/vm/overcommit_memory: " << vm_overcommit
                  << ", expect is 1, memory limit check is handed over to Doris Allocator, "
                     "otherwise BE may crash even with remaining memory"
                  << std::endl;
    }

    LOG(INFO) << "Physical Memory: " << _mem_info_bytes["MemTotal"]
              << ", BE Available Physical Memory(consider cgroup): "
              << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES) << ", Mem Limit: "
              << PrettyPrinter::print(_s_mem_limit.load(std::memory_order_relaxed), TUnit::BYTES)
              << ", origin config value: " << config::mem_limit
              << ", System Mem Available Min Reserve: "
              << PrettyPrinter::print(_s_sys_mem_available_low_water_mark, TUnit::BYTES)
              << ", Vm Min Free KBytes: "
              << PrettyPrinter::print(_s_vm_min_free_kbytes, TUnit::BYTES)
              << ", Vm Overcommit Memory: " << vm_overcommit;
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
    _s_soft_mem_limit = static_cast<int64_t>(_s_mem_limit * config::soft_mem_limit_frac);

    LOG(INFO) << "Physical Memory: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES);
    _s_initialized = true;
}
#endif

std::string MemInfo::debug_string() {
    DCHECK(_s_initialized);
    std::stringstream stream;
    stream << "Physical Memory: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES)
           << std::endl;
    stream << "Memory Limt: " << PrettyPrinter::print(_s_mem_limit, TUnit::BYTES) << std::endl;
    stream << "CGroup Info: " << doris::CGroupUtil::debug_string() << std::endl;
    return stream.str();
}

} // namespace doris
