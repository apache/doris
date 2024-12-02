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

#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>

#include "common/status.h"

namespace doris {

class CPUContext : public std::enable_shared_from_this<CPUContext> {
    ENABLE_FACTORY_CREATOR(CPUContext);

public:
    // Used to collect cpu execution stats.
    // The stats is not thread safe.
    // For example, you should use a seperate object for every scanner and do merge and reset
    class CPUStats {
    public:
        // Should add some cpu stats relared method here.
        void reset();
        void merge(CPUStats& stats);
        std::string debug_string();
    };

public:
    CPUContext() {}
    virtual ~CPUContext() = default;
    // Bind current thread to cgroup, only some load thread should do this.
    void bind_workload_group() {
        // Call workload group method to bind current thread to cgroup
    }
    CPUStats* cpu_stats() { return &stats_; }

private:
    CPUStats stats_;
};

} // namespace doris
