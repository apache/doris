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
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "util/timer.h"

namespace doris {

class ThreadPool;
class SystemMetrics;

// AdaptiveThreadPoolController dynamically adjusts thread pool sizes based on
// system load (IO utilisation, CPU load average, flush queue depth).
//
// Scheduling is fully delegated to a Timer (be/src/util/timer.h): each
// registered pool group becomes a recurring Timer event. The controller itself
// contains no threads or sleep loops — it only holds adjustment logic and
// pool-group metadata.
//
// Usage:
//   Timer timer;
//   timer.start();
//   AdaptiveThreadPoolController ctrl;
//   ctrl.init(system_metrics, s3_pool, &timer);
//   ctrl.add("flush", {pool1, pool2},
//       AdaptiveThreadPoolController::make_flush_adjust_func(&ctrl, pool1),
//       max_per_cpu, min_per_cpu);
//   // ... later ...
//   ctrl.cancel("flush");
//   timer.stop();
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
    ~AdaptiveThreadPoolController() = default;

    // Initialize with system-level dependencies. `timer` must outlive this controller.
    void init(SystemMetrics* system_metrics, ThreadPool* s3_file_upload_pool, Timer* timer);

    // Cancel all registered pool groups. Must be called before the pools are destroyed.
    void stop();

    // Register a pool group and schedule a recurring adjustment event on the timer.
    void add(std::string name, std::vector<ThreadPool*> pools, AdjustFunc adjust_func,
             double max_threads_per_cpu, double min_threads_per_cpu,
             Timer::Duration interval = std::chrono::milliseconds(kDefaultIntervalMs));

    // Cancel the recurring event and remove the pool group.
    // Must be called before the pools are destroyed to prevent UAF.
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

private:
    struct PoolGroup {
        std::string name;
        std::vector<ThreadPool*> pools;
        AdjustFunc adjust_func;
        double max_threads_per_cpu = 4.0;
        double min_threads_per_cpu = 0.5;
        int current_threads = 0;

        int get_max_threads() const;
        int get_min_threads() const;
    };

    // Run one group's adjustment. Called from the Timer thread (no lock held on entry).
    void _fire_group(const std::string& name);

    void _apply_thread_count(PoolGroup& group, int target_threads, const std::string& reason);

private:
    SystemMetrics* _system_metrics = nullptr;
    ThreadPool* _s3_file_upload_pool = nullptr;
    Timer* _timer = nullptr;

    mutable std::mutex _mutex;
    std::map<std::string, PoolGroup> _pool_groups;
    std::map<std::string, Timer::EventId> _event_ids; // name → Timer event, for cancel()

    // Last successfully computed IO-busy result. Returned as-is when the
    // measurement interval is too short to produce a valid new delta
    // (e.g. two groups fire back-to-back and interval_sec ≈ 0).
    bool _last_io_busy = false;

    // For disk IO util calculation (used by is_io_busy).
    std::map<std::string, int64_t> _last_disk_io_time;
    int64_t _last_check_time_sec = 0;
};

} // namespace doris
