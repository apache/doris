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

#include <cstdint>
#include <string>

#include "common/bvars.h"

namespace doris::cloud {

struct MsStressMetrics {
    int64_t fdb_commit_latency_ns {BVAR_FDB_INVALID_VALUE};
    int64_t fdb_read_latency_ns {BVAR_FDB_INVALID_VALUE};
    int64_t fdb_performance_limited_by_name {BVAR_FDB_INVALID_VALUE};
    int64_t fdb_client_thread_busyness_percent {BVAR_FDB_INVALID_VALUE};
    int64_t ms_cpu_usage_percent {-1};
    int64_t ms_memory_usage_percent {-1};
};

struct MsStressDecision {
    bool fdb_cluster_under_pressure {false};
    bool fdb_client_thread_under_pressure {false};
    bool ms_resource_under_pressure {false};
    bool rate_limit_injected_for_test {false};
    int64_t fdb_commit_latency_ns {BVAR_FDB_INVALID_VALUE};
    int64_t fdb_read_latency_ns {BVAR_FDB_INVALID_VALUE};
    int64_t fdb_performance_limited_by_name {BVAR_FDB_INVALID_VALUE};
    int64_t fdb_client_thread_busyness_percent {BVAR_FDB_INVALID_VALUE};
    double fdb_client_thread_busyness_avg_percent {-1};
    int64_t ms_cpu_usage_percent {-1};
    double ms_cpu_usage_avg_percent {-1};
    int64_t ms_memory_usage_percent {-1};
    double ms_memory_usage_avg_percent {-1};
    int32_t rate_limit_injected_random_value {-1};

    [[nodiscard]] bool under_greate_stress() const {
        return fdb_cluster_under_pressure || fdb_client_thread_under_pressure ||
               ms_resource_under_pressure || rate_limit_injected_for_test;
    }

    [[nodiscard]] std::string debug_string() const;
};

MsStressDecision get_ms_stress_decision();
bool check_ms_if_under_greate_stress();
MsStressDecision update_ms_stress_detector_for_test(int64_t now_ms, const MsStressMetrics& metrics,
                                                    bool reset = false,
                                                    int32_t rate_limit_injected_random_value = -1);

} // namespace doris::cloud
