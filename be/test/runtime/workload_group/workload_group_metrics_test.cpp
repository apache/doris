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

#include "runtime/workload_group/workload_group_metrics.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "runtime/workload_group/workload_group.h"
#include "util/time.h"

namespace doris {

class WorkloadGroupMetricsTest : public testing::Test {
protected:
    void SetUp() override {
        // Use a unique id for each test instance to avoid metric entity conflicts
        static std::atomic<uint64_t> next_id {1};
        uint64_t id = next_id.fetch_add(1);
        WorkloadGroupInfo wg_info {.id = id, .name = "test_wg_" + std::to_string(id)};
        _wg = std::make_shared<WorkloadGroup>(wg_info);
        _metrics = _wg->get_metrics();
    }

    void TearDown() override {
        _metrics.reset();
        _wg.reset();
    }

    std::shared_ptr<WorkloadGroup> _wg;
    std::shared_ptr<WorkloadGroupMetrics> _metrics;
};

// Test that refresh_metrics uses real elapsed time to compute per-second rates.
// After sleeping for a known interval, the per-second CPU rate should reflect
// the actual elapsed time rather than the config-based fixed interval.
TEST_F(WorkloadGroupMetricsTest, refresh_uses_real_elapsed_time) {
    // First call to refresh_metrics to initialize _last_refresh_time_ms
    _metrics->refresh_metrics();

    // Add known CPU time: 2,000,000,000 nanos = 2 seconds of CPU
    const uint64_t cpu_delta_nanos = 2000000000ULL;
    _metrics->update_cpu_time_nanos(cpu_delta_nanos);

    // Sleep for ~1.1 seconds so the real interval is ~1 second
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    // Refresh metrics — should compute rate based on real ~1 second interval
    _metrics->refresh_metrics();

    // Expected: 2,000,000,000 nanos / 1 second = ~2,000,000,000 nanos per second
    // Allow generous tolerance for timing imprecision in CI
    uint64_t cpu_per_sec = _metrics->get_cpu_time_nanos_per_second();
    EXPECT_GT(cpu_per_sec, 500000000ULL) << "CPU per-second rate too low: " << cpu_per_sec;
    EXPECT_LT(cpu_per_sec, 4000000000ULL) << "CPU per-second rate too high: " << cpu_per_sec;
}

// Test that when interval is less than 1 second, refresh_metrics does not
// cause division by zero and preserves previous rates.
TEST_F(WorkloadGroupMetricsTest, refresh_skips_when_interval_less_than_one_second) {
    // First call to initialize _last_refresh_time_ms
    _metrics->refresh_metrics();

    // Add some CPU time
    _metrics->update_cpu_time_nanos(1000000000ULL); // 1B nanos

    // Call refresh immediately (< 1 second elapsed) — should not crash
    // and should not update per-second rates (interval_second == 0 → early return)
    _metrics->refresh_metrics();

    // Per-second rate should still be 0 (from the initial state)
    // because the sub-second refresh skips the rate calculation
    uint64_t cpu_per_sec = _metrics->get_cpu_time_nanos_per_second();
    EXPECT_EQ(cpu_per_sec, 0) << "CPU per-second rate should remain unchanged when interval < 1s";
}

// Test that different real intervals produce proportionally different rates.
// A shorter interval with the same delta should yield a higher per-second rate.
TEST_F(WorkloadGroupMetricsTest, shorter_interval_yields_higher_rate) {
    // --- First measurement: 1 second interval ---
    _metrics->refresh_metrics();
    _metrics->update_cpu_time_nanos(1000000000ULL); // 1B nanos

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    _metrics->refresh_metrics();

    uint64_t rate_1s = _metrics->get_cpu_time_nanos_per_second();

    // --- Second measurement: add same delta, wait 2 seconds ---
    _metrics->update_cpu_time_nanos(1000000000ULL); // another 1B nanos

    std::this_thread::sleep_for(std::chrono::milliseconds(2100));
    _metrics->refresh_metrics();

    uint64_t rate_2s = _metrics->get_cpu_time_nanos_per_second();

    // With the same absolute delta (1B nanos) but double the interval,
    // the per-second rate should be roughly half.
    // Allow generous tolerance for timing jitter
    EXPECT_GT(rate_1s, rate_2s) << "1s interval rate (" << rate_1s
                                << ") should be higher than 2s interval rate (" << rate_2s << ")";
}

// Test that memory metrics are correctly reported
TEST_F(WorkloadGroupMetricsTest, memory_used_reported_correctly) {
    const int64_t mem_used = 1024L * 1024 * 512; // 512 MB
    _metrics->update_memory_used_bytes(mem_used);
    _metrics->refresh_metrics();

    // Need to wait > 1 second for refresh to take effect
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    _metrics->refresh_metrics();

    EXPECT_EQ(_metrics->get_memory_used(), mem_used);
}

// Test that the first refresh (from _last_refresh_time_ms == 0) does not produce
// unreasonable rates since the interval is very large (time since boot).
TEST_F(WorkloadGroupMetricsTest, first_refresh_produces_near_zero_rate) {
    // Add some CPU time before the first refresh
    _metrics->update_cpu_time_nanos(5000000000ULL); // 5B nanos

    // First refresh: interval = current_time_ms / 1000 (time since boot in seconds)
    // For a system with uptime > 5 seconds, rate = 5B / uptime_seconds
    // This should be small relative to the delta
    _metrics->refresh_metrics();

    uint64_t cpu_per_sec = _metrics->get_cpu_time_nanos_per_second();
    // With system uptime of at least 60 seconds (reasonable assumption),
    // rate = 5B / 60+ < 84M nanos/sec
    EXPECT_LT(cpu_per_sec, 1000000000ULL)
            << "First refresh rate should be modest since interval is system uptime";
}

} // namespace doris
