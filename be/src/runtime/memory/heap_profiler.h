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

#include "runtime/exec_env.h"

namespace doris {

class HeapProfiler {
public:
    static HeapProfiler* create_global_instance() { return new HeapProfiler(); }
    static HeapProfiler* instance() { return ExecEnv::GetInstance()->get_heap_profiler(); }
    HeapProfiler() = default;

    void heap_profiler_start();
    void heap_profiler_stop();
    bool check_heap_profiler();
    std::string dump_heap_profile();
    std::string dump_heap_profile_to_dot();

private:
    void set_prof_active(bool prof);
    bool get_prof_dump(const std::string& profile_file_name);

    std::mutex _mutex;
};

} // namespace doris
