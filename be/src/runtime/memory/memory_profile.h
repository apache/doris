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

#pragma once

#include <common/multi_version.h>

#include "util/runtime_profile.h"

namespace doris {

class MemoryProfile {
public:
    MemoryProfile();

    void refresh_memory_overview_profile();
    void refresh_tasks_memory_profile();

    void make_memory_profile(RuntimeProfile* profile) const;

    std::string print_memory_overview_profile() const {
        std::stringstream ss;
        auto version_ptr = _memory_overview_profile.get();
        version_ptr->pretty_print(&ss);
        return ss.str();
    }

    std::string print_global_memory_profile() const {
        std::stringstream ss;
        auto version_ptr = _global_memory_profile.get();
        version_ptr->pretty_print(&ss);
        return ss.str();
    }

    std::string print_top_memory_tasks_profile() const {
        std::stringstream ss;
        auto version_ptr = _top_memory_tasks_profile.get();
        version_ptr->pretty_print(&ss);
        return ss.str();
    }

    std::string print_tasks_memory_profile() const {
        std::stringstream ss;
        auto version_ptr = _tasks_memory_profile.get();
        version_ptr->pretty_print(&ss);
        return ss.str();
    }

    static int64_t query_current_usage();
    static int64_t load_current_usage();
    static int64_t compaction_current_usage();
    static int64_t schema_change_current_usage();
    static int64_t other_current_usage();

    // process memory changes more than 256M, or the GC ends
    void enable_print_log_process_usage() { _enable_print_log_process_usage = true; }
    void print_log_process_usage();

private:
    MultiVersion<RuntimeProfile> _memory_overview_profile;
    MultiVersion<RuntimeProfile> _global_memory_profile;
    MultiVersion<RuntimeProfile> _top_memory_tasks_profile;
    MultiVersion<RuntimeProfile> _tasks_memory_profile;

    std::atomic<bool> _enable_print_log_process_usage {true};
};

} // namespace doris
