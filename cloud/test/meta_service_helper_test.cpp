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

#include <gtest/gtest.h>

#include "common/config.h"
#include "meta-service/meta_service_rate_limit_helper.h"

namespace doris::cloud {
namespace {
struct MsRateLimitInjectionConfigGuard {
    ~MsRateLimitInjectionConfigGuard() {
        config::enable_ms_rate_limit_injection = original_enable;
        config::ms_rate_limit_injection_probability = original_probability;
    }

    bool original_enable {config::enable_ms_rate_limit_injection};
    int32_t original_probability {config::ms_rate_limit_injection_probability};
};
} // namespace

TEST(MetaServiceHelperTest, FdbClusterPressureNeedsLatencyAndNonWorkload) {
    MsStressMetrics metrics;
    metrics.fdb_commit_latency_ns = 51L * 1000 * 1000;
    metrics.fdb_performance_limited_by_name = -1;

    auto decision = update_ms_stress_detector_for_test(0, metrics, true);
    ASSERT_TRUE(decision.fdb_cluster_under_pressure);
    ASSERT_TRUE(decision.under_greate_stress());
    std::cout << decision.debug_string() << std::endl;
    ASSERT_NE(decision.debug_string().find("fdb_cluster"), std::string::npos);

    metrics.fdb_performance_limited_by_name = 0;
    decision = update_ms_stress_detector_for_test(1000, metrics, true);
    ASSERT_FALSE(decision.fdb_cluster_under_pressure);
    ASSERT_FALSE(decision.under_greate_stress());
}

TEST(MetaServiceHelperTest, FdbClientThreadPressureNeedsWindowAverageAndInstantValue) {
    MsStressMetrics metrics;
    for (int second = 0; second < 60; ++second) {
        metrics.fdb_client_thread_busyness_percent = 71;
        auto decision = update_ms_stress_detector_for_test(second * 1000, metrics, second == 0);
        ASSERT_FALSE(decision.fdb_client_thread_under_pressure);
    }

    metrics.fdb_client_thread_busyness_percent = 91;
    auto decision = update_ms_stress_detector_for_test(60 * 1000, metrics);
    ASSERT_TRUE(decision.fdb_client_thread_under_pressure);
    ASSERT_TRUE(decision.under_greate_stress());
    std::cout << decision.debug_string() << std::endl;
    ASSERT_NE(decision.debug_string().find("fdb_client_thread"), std::string::npos);
}

TEST(MetaServiceHelperTest, MsResourcePressureNeedsCurrentAndWindowAverageHigh) {
    MsStressMetrics metrics;
    for (int second = 0; second < 59; ++second) {
        metrics.ms_cpu_usage_percent = 96;
        auto decision = update_ms_stress_detector_for_test(second * 1000, metrics, second == 0);
        ASSERT_FALSE(decision.ms_resource_under_pressure);
    }

    metrics.ms_cpu_usage_percent = 96;
    auto decision = update_ms_stress_detector_for_test(59 * 1000, metrics);
    ASSERT_TRUE(decision.ms_resource_under_pressure);
    ASSERT_TRUE(decision.under_greate_stress());
    std::cout << decision.debug_string() << std::endl;
    ASSERT_NE(decision.debug_string().find("ms_resource"), std::string::npos);

    metrics.ms_cpu_usage_percent = 50;
    decision = update_ms_stress_detector_for_test(60 * 1000, metrics);
    ASSERT_FALSE(decision.ms_resource_under_pressure);
}

TEST(MetaServiceHelperTest, MsRateLimitInjectionRequiresSwitchAndProbabilityHit) {
    MsRateLimitInjectionConfigGuard guard;

    MsStressMetrics metrics;
    config::enable_ms_rate_limit_injection = false;
    config::ms_rate_limit_injection_probability = 100;
    auto decision = update_ms_stress_detector_for_test(0, metrics, true, 0);
    ASSERT_FALSE(decision.rate_limit_injected_for_test);
    ASSERT_FALSE(decision.under_greate_stress());

    config::enable_ms_rate_limit_injection = true;
    config::ms_rate_limit_injection_probability = 30;
    decision = update_ms_stress_detector_for_test(1000, metrics, true, 30);
    ASSERT_FALSE(decision.rate_limit_injected_for_test);
    ASSERT_FALSE(decision.under_greate_stress());

    decision = update_ms_stress_detector_for_test(2000, metrics, true, 29);
    ASSERT_TRUE(decision.rate_limit_injected_for_test);
    ASSERT_TRUE(decision.under_greate_stress());
    ASSERT_NE(decision.debug_string().find("test_injection"), std::string::npos);
}
} // namespace doris::cloud
