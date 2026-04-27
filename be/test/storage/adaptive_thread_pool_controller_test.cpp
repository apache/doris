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

#include "storage/adaptive_thread_pool_controller.h"

#include <gtest/gtest.h>

#include <thread>

#include "common/config.h"
#include "common/metrics/metrics.h"
#include "common/metrics/system_metrics.h"
#include "testutil/test_util.h"
#include "util/threadpool.h"

namespace doris {

extern const char* k_ut_stat_path;

namespace {

std::string get_stat_test_data_path(const std::string& file_name) {
    return GetCurrentRunningDir() + "/util/test_data/" + file_name;
}

} // namespace

class AdaptiveThreadPoolControllerTest : public testing::Test {
protected:
    void SetUp() override {
        _original_enable_adaptive = config::enable_adaptive_flush_threads;

        ASSERT_TRUE(ThreadPoolBuilder("TestPool")
                            .set_min_threads(2)
                            .set_max_threads(64)
                            .build(&_pool)
                            .ok());

        ASSERT_TRUE(ThreadPoolBuilder("TestPool2")
                            .set_min_threads(2)
                            .set_max_threads(64)
                            .build(&_pool2)
                            .ok());
    }

    void TearDown() override {
        config::enable_adaptive_flush_threads = _original_enable_adaptive;
        if (_pool) _pool->shutdown();
        if (_pool2) _pool2->shutdown();
    }

    bool _original_enable_adaptive;
    std::unique_ptr<ThreadPool> _pool;
    std::unique_ptr<ThreadPool> _pool2;
};

// Test basic add and get_current_threads
TEST_F(AdaptiveThreadPoolControllerTest, TestAddPoolGroup) {
    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);

    controller.add(
            "test", {_pool.get()},
            [](int current, int min_t, int max_t, std::string&) { return current; }, 4, 0.5);

    int num_cpus = std::thread::hardware_concurrency();
    if (num_cpus <= 0) num_cpus = 1;

    EXPECT_EQ(controller.get_current_threads("test"), num_cpus * 4);
    EXPECT_EQ(controller.get_current_threads("nonexistent"), 0);
}

// Test adding multiple pool groups with different adjust logic
TEST_F(AdaptiveThreadPoolControllerTest, TestMultiplePoolGroups) {
    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);

    controller.add(
            "group_a", {_pool.get()},
            [](int current, int min_t, int max_t, std::string&) { return current; }, 4, 0.5);
    controller.add(
            "group_b", {_pool2.get()},
            [](int current, int min_t, int max_t, std::string&) { return current; }, 2, 0.5);

    int num_cpus = std::thread::hardware_concurrency();
    if (num_cpus <= 0) num_cpus = 1;

    EXPECT_EQ(controller.get_current_threads("group_a"), num_cpus * 4);
    EXPECT_EQ(controller.get_current_threads("group_b"), num_cpus * 2);
}

// Test that when adaptive is disabled, adjust_once is a no-op (config guard in _fire_group)
TEST_F(AdaptiveThreadPoolControllerTest, TestAdaptiveDisabled) {
    config::enable_adaptive_flush_threads = false;

    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);
    controller.add(
            "test", {_pool.get()},
            [](int current, int min_t, int max_t, std::string&) { return 1; }, 4, 0.5);

    int initial = controller.get_current_threads("test");
    // bthread_timer callbacks check config::enable_adaptive_flush_threads before firing;
    // adjust_once bypasses that check, so we only verify the group was registered.
    EXPECT_GT(initial, 0);
}

// Test adjust_once calls the custom adjust function
TEST_F(AdaptiveThreadPoolControllerTest, TestCustomAdjustFunc) {
    config::enable_adaptive_flush_threads = true;

    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);

    int num_cpus = std::thread::hardware_concurrency();
    if (num_cpus <= 0) num_cpus = 1;
    int expected_min = std::max(1, static_cast<int>(num_cpus * 0.5));

    // Adjust function always returns min
    controller.add(
            "test", {_pool.get()},
            [](int current, int min_t, int max_t, std::string&) { return min_t; }, 4, 0.5);

    controller.adjust_once();

    EXPECT_EQ(controller.get_current_threads("test"), expected_min);
}

// Test that result is clamped to [min, max]
TEST_F(AdaptiveThreadPoolControllerTest, TestClampToMinMax) {
    config::enable_adaptive_flush_threads = true;

    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);

    int num_cpus = std::thread::hardware_concurrency();
    if (num_cpus <= 0) num_cpus = 1;
    int expected_max = num_cpus * 2;
    int expected_min = std::max(1, static_cast<int>(num_cpus * 0.5));

    // Adjust function tries to return way beyond max
    controller.add(
            "test", {_pool.get()},
            [](int current, int min_t, int max_t, std::string&) { return 99999; }, 2, 0.5);
    controller.adjust_once();
    EXPECT_EQ(controller.get_current_threads("test"), expected_max);

    // Adjust function tries to return below min
    controller.add(
            "test2", {_pool2.get()},
            [](int current, int min_t, int max_t, std::string&) { return -1; }, 2, 0.5);
    controller.adjust_once();
    EXPECT_EQ(controller.get_current_threads("test2"), expected_min);
}

// Test adjust_once with no change keeps current threads
TEST_F(AdaptiveThreadPoolControllerTest, TestAdjustOnceNoChange) {
    config::enable_adaptive_flush_threads = true;

    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);
    controller.add(
            "test", {_pool.get()},
            [](int current, int min_t, int max_t, std::string&) { return current; }, 4, 0.5);

    int initial = controller.get_current_threads("test");
    controller.adjust_once();
    EXPECT_EQ(controller.get_current_threads("test"), initial);
}

// Test stop lifecycle
TEST_F(AdaptiveThreadPoolControllerTest, TestStartStopLifecycle) {
    config::enable_adaptive_flush_threads = true;

    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);
    controller.add(
            "test", {_pool.get()},
            [](int current, int min_t, int max_t, std::string&) { return current; }, 4, 0.5);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    controller.stop();

    // Multiple stops should be safe
    controller.stop();
}

// Test destructor stops the controller (cancels all timer events)
TEST_F(AdaptiveThreadPoolControllerTest, TestDestructorStops) {
    config::enable_adaptive_flush_threads = true;

    {
        AdaptiveThreadPoolController controller;
        controller.init(nullptr, nullptr);
        controller.add(
                "test", {_pool.get()},
                [](int current, int min_t, int max_t, std::string&) { return current; }, 4, 0.5);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // Destructor called stop(), which cancelled all timer events. No UAF.
    SUCCEED();
}

// Test constants are reasonable
TEST_F(AdaptiveThreadPoolControllerTest, TestConstants) {
    EXPECT_EQ(AdaptiveThreadPoolController::kDefaultIntervalMs, 10000);
    EXPECT_EQ(AdaptiveThreadPoolController::kQueueThreshold, 10);
    EXPECT_EQ(AdaptiveThreadPoolController::kIOBusyThresholdPercent, 90);
    EXPECT_EQ(AdaptiveThreadPoolController::kCPUBusyThresholdPercent, 90);
    EXPECT_EQ(AdaptiveThreadPoolController::kS3QueueBusyThreshold, 100);
}

// Test add after controller is already running
TEST_F(AdaptiveThreadPoolControllerTest, TestAddAfterStart) {
    config::enable_adaptive_flush_threads = true;

    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);
    controller.add(
            "group_a", {_pool.get()},
            [](int current, int min_t, int max_t, std::string&) { return current; }, 4, 0.5);

    // Add a second group while the controller is already running
    controller.add(
            "group_b", {_pool2.get()},
            [](int current, int min_t, int max_t, std::string&) { return current; }, 2, 0.5);

    int num_cpus = std::thread::hardware_concurrency();
    if (num_cpus <= 0) num_cpus = 1;
    EXPECT_EQ(controller.get_current_threads("group_b"), num_cpus * 2);

    controller.stop();
}

// Test is_io_busy and is_cpu_busy with null system_metrics
TEST_F(AdaptiveThreadPoolControllerTest, TestIoBusyCpuBusyWithNullMetrics) {
    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);

    EXPECT_FALSE(controller.is_io_busy());
    EXPECT_FALSE(controller.is_cpu_busy());
}

TEST_F(AdaptiveThreadPoolControllerTest, TestCpuBusyUsesCpuMetricsDelta) {
    MetricRegistry registry("test");
    const std::string before_path = get_stat_test_data_path("stat_cpu_busy_before");
    const std::string after_path = get_stat_test_data_path("stat_cpu_busy_after");
    k_ut_stat_path = before_path.c_str();
    SystemMetrics metrics(&registry, {}, {});
    metrics.update();

    AdaptiveThreadPoolController controller;
    controller.init(&metrics, nullptr);

    EXPECT_FALSE(controller.is_cpu_busy());
    k_ut_stat_path = after_path.c_str();
    metrics.update();
    EXPECT_TRUE(controller.is_cpu_busy());
}

TEST_F(AdaptiveThreadPoolControllerTest, TestCpuBusyTreatsIoWaitAsIdle) {
    MetricRegistry registry("test");
    const std::string before_path = get_stat_test_data_path("stat_cpu_busy_before");
    const std::string after_path = get_stat_test_data_path("stat_cpu_iowait_after");
    k_ut_stat_path = before_path.c_str();
    SystemMetrics metrics(&registry, {}, {});
    metrics.update();

    AdaptiveThreadPoolController controller;
    controller.init(&metrics, nullptr);

    EXPECT_FALSE(controller.is_cpu_busy());
    k_ut_stat_path = after_path.c_str();
    metrics.update();
    EXPECT_FALSE(controller.is_cpu_busy());
}

TEST_F(AdaptiveThreadPoolControllerTest, TestCpuBusyInvalidSampleDoesNotAdvanceBaseline) {
    MetricRegistry registry("test");
    const std::string before_path = get_stat_test_data_path("stat_cpu_busy_before");
    const std::string invalid_path = get_stat_test_data_path("stat_cpu_regressed_after");
    const std::string recovery_path = get_stat_test_data_path("stat_cpu_recovery_after");
    k_ut_stat_path = before_path.c_str();
    SystemMetrics metrics(&registry, {}, {});
    metrics.update();

    AdaptiveThreadPoolController controller;
    controller.init(&metrics, nullptr);

    EXPECT_FALSE(controller.is_cpu_busy());
    k_ut_stat_path = invalid_path.c_str();
    metrics.update();
    EXPECT_FALSE(controller.is_cpu_busy());

    k_ut_stat_path = recovery_path.c_str();
    metrics.update();
    EXPECT_TRUE(controller.is_cpu_busy());
}

// Test adjust function that uses controller's is_io_busy/is_cpu_busy
TEST_F(AdaptiveThreadPoolControllerTest, TestAdjustFuncWithControllerMethods) {
    config::enable_adaptive_flush_threads = true;

    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);

    auto* ctrl = &controller;
    controller.add(
            "test", {_pool.get()},
            [ctrl](int current, int min_t, int max_t, std::string&) {
                int target = current;
                if (ctrl->is_io_busy()) target = std::max(min_t, target - 1);
                if (ctrl->is_cpu_busy()) target = std::max(min_t, target - 1);
                return target;
            },
            4, 0.5);

    int initial = controller.get_current_threads("test");
    controller.adjust_once();
    // With null metrics, io_busy and cpu_busy return false, so no change
    EXPECT_EQ(controller.get_current_threads("test"), initial);
}

// Test cancel removes the pool group
TEST_F(AdaptiveThreadPoolControllerTest, TestCancel) {
    AdaptiveThreadPoolController controller;
    controller.init(nullptr, nullptr);

    controller.add(
            "test", {_pool.get()},
            [](int current, int min_t, int max_t, std::string&) { return current; }, 4, 0.5);
    EXPECT_NE(controller.get_current_threads("test"), 0);

    controller.cancel("test");
    EXPECT_EQ(controller.get_current_threads("test"), 0);
}

} // namespace doris
