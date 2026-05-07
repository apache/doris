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
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <string_view>

#include "common/config.h"
#include "meta-service/meta_service_rate_limit_helper.h"

namespace doris::cloud {
namespace internal {
int parse_cpuset_cpu_count(std::string_view cpuset_line);
std::optional<double> parse_cgroup_v2_cpu_limit(std::string_view cpu_max_line);
std::optional<double> parse_cgroup_v1_cpu_limit(int64_t quota_us, int64_t period_us);
int64_t calculate_usage_percent(int64_t usage_bytes, int64_t limit_bytes);
int64_t calculate_cpu_usage_percent(double delta_cpu_ns, double delta_wall_ns, double cpu_limit);
} // namespace internal

namespace {
struct MsRateLimitInjectionConfigGuard {
    ~MsRateLimitInjectionConfigGuard() {
        config::enable_ms_rate_limit_injection = original_enable;
        config::ms_rate_limit_injection_probability = original_probability;
    }

    bool original_enable {config::enable_ms_rate_limit_injection};
    int32_t original_probability {config::ms_rate_limit_injection_probability};
};

class CgroupTestRoot {
public:
    CgroupTestRoot() // NOLINT(modernize-use-equals-default): creates temporary cgroup tree.
            : root_(std::filesystem::temp_directory_path() /
                    ("meta_service_cgroup_test_" + std::to_string(::getpid()) + "_" +
                     std::to_string(id_++))) {
        std::filesystem::create_directories(root_ / "proc/self");
        std::filesystem::create_directories(root_ / "sys/fs/cgroup");
    }

    ~CgroupTestRoot() { // NOLINT(modernize-use-equals-default): removes temporary cgroup tree.
        std::filesystem::remove_all(root_);
    }

    [[nodiscard]] internal::CgroupPaths paths() const {
        return {.proc_self_cgroup_path = root_ / "proc/self/cgroup",
                .proc_self_mountinfo_path = root_ / "proc/self/mountinfo",
                .cgroup_root_path = root_ / "sys/fs/cgroup"};
    }

    void write_file(const std::filesystem::path& relative_path, std::string_view content) const {
        auto file_path = root_ / relative_path;
        std::filesystem::create_directories(file_path.parent_path());
        std::ofstream stream(file_path);
        stream << content;
    }

    [[nodiscard]] std::filesystem::path root() const { return root_; }

private:
    inline static int id_ = 0;
    std::filesystem::path root_;
};
} // namespace

TEST(MetaServiceHelperTest, FdbClusterPressureNeedsLatencyAndNonWorkload) {
    MsStressMetrics metrics;
    metrics.fdb_commit_latency_ns = 51L * 1000 * 1000;
    metrics.fdb_performance_limited_by_name = -1;

    auto decision = update_ms_stress_detector_for_test(0, metrics, true);
    ASSERT_TRUE(decision.fdb_cluster_under_pressure);
    ASSERT_TRUE(decision.under_great_stress());
    std::cout << decision.debug_string() << std::endl;
    ASSERT_NE(decision.debug_string().find("fdb_cluster"), std::string::npos);

    metrics.fdb_performance_limited_by_name = 0;
    decision = update_ms_stress_detector_for_test(1000, metrics, true);
    ASSERT_FALSE(decision.fdb_cluster_under_pressure);
    ASSERT_FALSE(decision.under_great_stress());
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
    ASSERT_TRUE(decision.under_great_stress());
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
    ASSERT_TRUE(decision.under_great_stress());
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
    ASSERT_FALSE(decision.under_great_stress());

    config::enable_ms_rate_limit_injection = true;
    config::ms_rate_limit_injection_probability = 30;
    decision = update_ms_stress_detector_for_test(1000, metrics, true, 30);
    ASSERT_FALSE(decision.rate_limit_injected_for_test);
    ASSERT_FALSE(decision.under_great_stress());

    decision = update_ms_stress_detector_for_test(2000, metrics, true, 29);
    ASSERT_TRUE(decision.rate_limit_injected_for_test);
    ASSERT_TRUE(decision.under_great_stress());
    ASSERT_NE(decision.debug_string().find("test_injection"), std::string::npos);
}

TEST(MetaServiceHelperTest, ParseCpusetCpuCount) {
    ASSERT_EQ(internal::parse_cpuset_cpu_count("0-3,5,7-8"), 7);
    ASSERT_EQ(internal::parse_cpuset_cpu_count("2"), 1);
    ASSERT_EQ(internal::parse_cpuset_cpu_count(""), -1);
    ASSERT_EQ(internal::parse_cpuset_cpu_count("3-1"), -1);
}

TEST(MetaServiceHelperTest, ParseCgroupCpuQuota) {
    auto v2_limit = internal::parse_cgroup_v2_cpu_limit("50000 100000");
    ASSERT_TRUE(v2_limit.has_value());
    ASSERT_DOUBLE_EQ(*v2_limit, 0.5);
    ASSERT_FALSE(internal::parse_cgroup_v2_cpu_limit("max 100000").has_value());

    auto v1_limit = internal::parse_cgroup_v1_cpu_limit(150000, 100000);
    ASSERT_TRUE(v1_limit.has_value());
    ASSERT_DOUBLE_EQ(*v1_limit, 1.5);
    ASSERT_FALSE(internal::parse_cgroup_v1_cpu_limit(-1, 100000).has_value());
}

TEST(MetaServiceHelperTest, EffectiveCpuLimitDisabledWhenV2CannotBeUsed) {
    CgroupTestRoot test_root;
    test_root.write_file("proc/self/cgroup", "0::/docker/test\n");
    test_root.write_file("sys/fs/cgroup/cgroup.controllers", "cpu memory\n");
    test_root.write_file("sys/fs/cgroup/docker/test/cpu.max", "max 100000\n");

    ASSERT_FALSE(internal::get_effective_process_cpu_limit(test_root.paths()).has_value());
}

TEST(MetaServiceHelperTest, EffectiveCpuLimitDisabledWhenV1AndV2CannotBeUsed) {
    CgroupTestRoot test_root;
    test_root.write_file("proc/self/cgroup", "2:cpu:/docker/test\n");
    test_root.write_file("proc/self/mountinfo",
                         "36 25 0:32 / /sys/fs/cgroup/cpu rw,nosuid,nodev,noexec,relatime - "
                         "cgroup cgroup rw,cpu\n");
    test_root.write_file("sys/fs/cgroup/cpu/docker/test/cpu.cfs_quota_us", "-1\n");
    test_root.write_file("sys/fs/cgroup/cpu/docker/test/cpu.cfs_period_us", "100000\n");

    ASSERT_FALSE(internal::get_effective_process_cpu_limit(test_root.paths()).has_value());
}

TEST(MetaServiceHelperTest, UsagePercentCalculationUsesEffectiveLimit) {
    ASSERT_EQ(internal::calculate_usage_percent(512, 1024), 50);
    ASSERT_EQ(internal::calculate_usage_percent(-1, 1024), -1);
    ASSERT_EQ(internal::calculate_usage_percent(512, std::numeric_limits<int64_t>::max()), 0);

    ASSERT_EQ(internal::calculate_cpu_usage_percent(5e8, 1e9, 0.5), 100);
    ASSERT_EQ(internal::calculate_cpu_usage_percent(15e8, 1e9, 2.0), 75);
    ASSERT_EQ(internal::calculate_cpu_usage_percent(1, 0, 2.0), -1);
}
} // namespace doris::cloud
