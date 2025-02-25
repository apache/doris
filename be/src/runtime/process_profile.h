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

#include "runtime/exec_env.h"
#include "runtime/memory/memory_profile.h"
#include "util/runtime_profile.h"

namespace doris {

class ProcessProfile {
public:
    static ProcessProfile* create_global_instance() { return new ProcessProfile(); }
    static ProcessProfile* instance() { return ExecEnv::GetInstance()->get_process_profile(); }
    ProcessProfile();

    void refresh_profile();

    std::string print_process_profile() const {
        auto version_ptr = _process_profile.get();
        std::stringstream ss;
        version_ptr->pretty_print(&ss);
        return ss.str();
    }

    std::string print_process_profile_no_root() const {
        std::stringstream ss;
        std::vector<RuntimeProfile*> profiles;
        auto version_ptr = _process_profile.get();
        auto* process_profile = const_cast<doris::RuntimeProfile*>(version_ptr.get());
        process_profile->get_children(&profiles);
        for (auto* profile : profiles) {
            profile->pretty_print(&ss);
        }
        return ss.str();
    }

    MemoryProfile* memory_profile() { return _memory_profile.get(); }

private:
    MultiVersion<RuntimeProfile> _process_profile;
    std::unique_ptr<MemoryProfile> _memory_profile;
};

} // namespace doris
