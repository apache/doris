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

#include "runtime/memory/heap_profiler.h"

#ifdef USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif
#include "agent/utils.h"
#include "common/config.h"
#include "io/fs/local_file_system.h"

namespace doris {

void HeapProfiler::set_prof_active(bool prof) {
#ifdef USE_JEMALLOC
    std::lock_guard guard(_mutex);
    try {
        int err = jemallctl("prof.active", nullptr, nullptr, &prof, 1);
        err |= jemallctl("prof.thread_active_init", nullptr, nullptr, &prof, 1);
        if (err) {
            LOG(WARNING) << "jemalloc heap profiling start failed, " << err;
        } else {
            LOG(WARNING) << "jemalloc heap profiling started";
        }
    } catch (...) {
        LOG(WARNING) << "jemalloc heap profiling start failed";
    }
#endif
}

bool HeapProfiler::get_prof_dump(const std::string& profile_file_name) {
#ifdef USE_JEMALLOC
    std::lock_guard guard(_mutex);
    const char* file_name_ptr = profile_file_name.c_str();
    try {
        int err = jemallctl("prof.dump", nullptr, nullptr, &file_name_ptr, sizeof(const char*));
        if (err) {
            LOG(WARNING) << "dump heap profile failed, " << err;
            return false;
        } else {
            LOG(INFO) << "dump heap profile to " << profile_file_name;
            return true;
        }
    } catch (...) {
        LOG(WARNING) << "dump heap profile failed";
        return false;
    }
#else
    return false;
#endif
}

static std::string jeprof_profile_to_dot(const std::string& profile_file_name) {
    AgentUtils util;
    const static std::string jeprof_path = fmt::format("{}/bin/jeprof", std::getenv("DORIS_HOME"));
    const static std::string binary_path =
            fmt::format("{}/lib/doris_be", std::getenv("DORIS_HOME"));
    // https://doris.apache.org/community/developer-guide/debug-tool/#3-jeprof-parses-heap-profile
    std::string jeprof_cmd =
            fmt::format("{} --dot {} {}", jeprof_path, binary_path, profile_file_name);
    std::string msg;
    bool rc = util.exec_cmd(jeprof_cmd, &msg);
    if (!rc) {
        LOG(WARNING) << "jeprof profile to dot failed: " << msg;
    }
    return msg;
}

void HeapProfiler::heap_profiler_start() {
    set_prof_active(true);
}

void HeapProfiler::heap_profiler_stop() {
    set_prof_active(false);
}

bool HeapProfiler::check_heap_profiler() {
#ifdef USE_JEMALLOC
    size_t value = 0;
    size_t sz = sizeof(value);
    jemallctl("prof.active", &value, &sz, nullptr, 0);
    return value;
#else
    return false;
#endif
}

std::string HeapProfiler::dump_heap_profile() {
    if (!config::jeprofile_dir.empty()) {
        auto st = io::global_local_filesystem()->create_directory(config::jeprofile_dir);
        if (!st.ok()) {
            LOG(WARNING) << "create jeprofile dir failed.";
            return "";
        }
    }
    std::string profile_file_name =
            fmt::format("{}/jeheap_dump.{}.{}.{}.heap", config::jeprofile_dir, std::time(nullptr),
                        getpid(), rand());
    if (get_prof_dump(profile_file_name)) {
        return profile_file_name;
    } else {
        return "";
    }
}

std::string HeapProfiler::dump_heap_profile_to_dot() {
    std::string profile_file_name = dump_heap_profile();
    if (!profile_file_name.empty()) {
        return jeprof_profile_to_dot(profile_file_name);
    } else {
        return "";
    }
}

} // namespace doris
