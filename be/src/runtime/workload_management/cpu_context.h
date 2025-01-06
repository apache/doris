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

#include "common/factory_creator.h"
#include "util/runtime_profile.h"

namespace doris {

class CPUContext : public std::enable_shared_from_this<CPUContext> {
    ENABLE_FACTORY_CREATOR(CPUContext);

public:
    /*
    * --------------------------------
    * |          Property            |
    * --------------------------------
    * 1. operate them thread-safe.
    * 2. all tasks are unified.
    * 3. should not be operated frequently, use local variables to update Counter.
    */

    RuntimeProfile::Counter* cpu_cost_ms_counter_;

    RuntimeProfile* profile() { return profile_.get(); }
    std::string debug_string() { return profile_->pretty_print(); }

    /*
    * --------------------------------
    * |           Action             |
    * --------------------------------
    */

    // Bind current thread to cgroup, only some load thread should do this.
    void bind_workload_group() {
        // TODO: Call workload group method to bind current thread to cgroup
    }

protected:
    CPUContext() { init_profile(); }
    virtual ~CPUContext() = default;

private:
    void init_profile() {
        profile_ = std::make_unique<RuntimeProfile>("MemoryContext");
        cpu_cost_ms_counter_ = ADD_COUNTER(profile_, "RevokeWaitTimeMs", TUnit::TIME_MS);
    }

    // Used to collect memory execution stats.
    std::unique_ptr<RuntimeProfile> profile_;
};

class QueryCPUContext : public CPUContext {
    QueryCPUContext() = default;
};

class LoadCPUContext : public CPUContext {
    LoadCPUContext() = default;
};

class CompactionCPUContext : public CPUContext {
    CompactionCPUContext() = default;
};

} // namespace doris
