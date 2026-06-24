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

#include <bthread/unstable.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace doris {

class ThreadPool;
class SystemMetrics;
class AdaptiveThreadPoolController;

// Each pool group's timer state. Heap-allocated; shared between the controller
// and the brpc TimerThread callback.
struct TimerArg {
    AdaptiveThreadPoolController* ctrl; // never null
    std::string name;
    int64_t interval_ms;

    // Set by cancel() before calling bthread_timer_del. The callback checks
    // this flag after acquiring `mu` and skips re-registration when true.
    std::atomic<bool> stopped {false};

    // Tracks the most recently registered timer id. Updated under `mu` by the
    // callback after each re-registration; read by cancel() to call
    // bthread_timer_del on the latest pending timer.
    std::atomic<bthread_timer_t> timer_id {0};

    // Held for the entire duration of the callback (fire + re-registration).
    // cancel() acquires it after bthread_timer_del to wait for any in-flight
    // invocation to complete before freeing `this`.
    std::mutex mu;
};

// AdaptiveThreadPoolController dynamically adjusts thread pool sizes based on
// system load (IO utilisation, CPU utilisation, flush queue depth).
//
// Each registered pool group runs as a one-shot bthread_timer_add chain: the
// callback fires, adjusts the pool, then re-registers the next one-shot timer.
// All groups share the single brpc TimerThread, keeping the overhead minimal.
//
// Usage:
//   AdaptiveThreadPoolController ctrl;
//   ctrl.init(system_metrics, s3_pool);
//   ctrl.add("flush", {pool1, pool2},
//       AdaptiveThreadPoolController::make_flush_adjust_func(&ctrl, pool1),
//       max_per_cpu, min_per_cpu);
//   // ... later ...
//   ctrl.cancel("flush");   // or ctrl.stop()
class AdaptiveThreadPoolController {
public:
    using AdjustFunc =
            std::function<int(int current, int min_threads, int max_threads, std::string& reason)>;

    static constexpr int kDefaultIntervalMs = 10000;

    static constexpr int kQueueThreshold = 10;
    static constexpr int kIOBusyThresholdPercent = 90;
    static constexpr int kCPUBusyThresholdPercent = 90;
    static constexpr int kS3QueueBusyThreshold = 100;

    AdaptiveThreadPoolController() = default;
    ~AdaptiveThreadPoolController() { stop(); }

    // Initialize with system-level dependencies.
    void init(SystemMetrics* system_metrics, ThreadPool* s3_file_upload_pool);

    // Cancel all registered pool groups. Must be called before the pools are destroyed.
    void stop();

    // Register a pool group and start a recurring bthread_timer_add chain.
    void add(std::string name, std::vector<ThreadPool*> pools, AdjustFunc adjust_func,
             double max_threads_per_cpu, double min_threads_per_cpu,
             int64_t interval_ms = kDefaultIntervalMs);

    // Cancel the timer chain and remove the pool group. Blocks until any
    // in-flight callback finishes, then returns. Safe to call before pool teardown.
    void cancel(const std::string& name);

    // Fire all registered groups once, ignoring the schedule. For testing.
    void adjust_once();

    // Get current thread count for a named group. For testing/debugging.
    int get_current_threads(const std::string& name) const;

    // System-state helpers; safe to call from inside an AdjustFunc.
    bool is_io_busy();
    bool is_cpu_busy();

    // Factory: standard flush-pool adjust function.
    static AdjustFunc make_flush_adjust_func(AdaptiveThreadPoolController* controller,
                                             ThreadPool* flush_pool);

    // Callback registered with bthread_timer_add. Public only for the C linkage
    // requirement; do not call directly.
    static void _on_timer(void* arg);

private:
    struct PoolGroup {
        std::string name;
        std::vector<ThreadPool*> pools;
        AdjustFunc adjust_func;
        double max_threads_per_cpu = 4.0;
        double min_threads_per_cpu = 0.5;
        int current_threads = 0;
        TimerArg* timer_arg = nullptr; // owned; freed by cancel()

        int get_max_threads() const;
        int get_min_threads() const;
    };

    // Run one group's adjustment. Called from _on_timer (no lock on entry).
    void _fire_group(const std::string& name);

    void _apply_thread_count(PoolGroup& group, int target_threads, const std::string& reason);

private:
    SystemMetrics* _system_metrics = nullptr;
    ThreadPool* _s3_file_upload_pool = nullptr;

    mutable std::mutex _mutex;
    mutable std::mutex _metrics_state_mutex;
    std::map<std::string, PoolGroup> _pool_groups;

    // Last successfully computed IO-busy result. Returned as-is when the
    // measurement interval is too short to produce a valid new delta.
    bool _last_io_busy = false;

    // For disk IO util calculation (used by is_io_busy).
    std::map<std::string, int64_t> _last_disk_io_time;
    int64_t _last_check_time_sec = 0;

    // For CPU util calculation (used by is_cpu_busy). The counters come from
    // SystemMetrics' existing cpu_* metrics and are compared as deltas.
    bool _last_cpu_busy = false;
    int64_t _last_cpu_total_time = -1;
    int64_t _last_cpu_idle_time = -1;
};

} // namespace doris
