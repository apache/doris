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

#include <butil/time.h>

#include <algorithm>
#include <thread>

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/system_metrics.h"
#include "common/status.h"
#include "util/threadpool.h"
#include "util/time.h"

namespace doris {

int AdaptiveThreadPoolController::PoolGroup::get_max_threads() const {
    int num_cpus = std::thread::hardware_concurrency();
    if (num_cpus <= 0) num_cpus = 1;
    return static_cast<int>(num_cpus * max_threads_per_cpu);
}

int AdaptiveThreadPoolController::PoolGroup::get_min_threads() const {
    int num_cpus = std::thread::hardware_concurrency();
    if (num_cpus <= 0) num_cpus = 1;
    return std::max(1, static_cast<int>(num_cpus * min_threads_per_cpu));
}

// Static callback registered with bthread_timer_add.
// Runs in brpc TimerThread. Must be fast and non-blocking.
void AdaptiveThreadPoolController::_on_timer(void* raw) {
    auto* arg = static_cast<TimerArg*>(raw);

    // Hold mu for the entire callback (fire + re-registration).
    // cancel() acquires mu after bthread_timer_del, so this provides
    // cancel-with-wait semantics without a dedicated thread.
    std::lock_guard<std::mutex> lk(arg->mu);

    if (arg->stopped.load(std::memory_order_acquire)) {
        // cancel() set stopped before we took the lock.
        // cancel() owns arg and will delete it after taking mu.
        return;
    }

    arg->ctrl->_fire_group(arg->name);

    if (arg->stopped.load(std::memory_order_acquire)) {
        return; // cancel() will clean up
    }

    // Re-register the next one-shot timer.
    bthread_timer_t tid;
    if (bthread_timer_add(&tid, butil::milliseconds_from_now(arg->interval_ms), _on_timer, arg) ==
        0) {
        arg->timer_id.store(tid, std::memory_order_release);
    } else {
        LOG(WARNING) << "Adaptive: failed to re-register timer for group '" << arg->name << "'";
    }
}

void AdaptiveThreadPoolController::init(SystemMetrics* system_metrics,
                                        ThreadPool* s3_file_upload_pool) {
    _system_metrics = system_metrics;
    _s3_file_upload_pool = s3_file_upload_pool;
}

void AdaptiveThreadPoolController::stop() {
    std::vector<std::string> names;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        for (const auto& [name, _] : _pool_groups) {
            names.push_back(name);
        }
    }
    for (const auto& name : names) {
        cancel(name);
    }
}

void AdaptiveThreadPoolController::add(std::string name, std::vector<ThreadPool*> pools,
                                       AdjustFunc adjust_func, double max_threads_per_cpu,
                                       double min_threads_per_cpu, int64_t interval_ms) {
    PoolGroup group;
    group.name = name;
    group.pools = std::move(pools);
    group.adjust_func = std::move(adjust_func);
    group.max_threads_per_cpu = max_threads_per_cpu;
    group.min_threads_per_cpu = min_threads_per_cpu;
    group.current_threads = group.get_max_threads();

    int log_max = group.get_max_threads();
    int log_min = group.get_min_threads();

    auto* arg = new TimerArg();
    arg->ctrl = this;
    arg->name = name;
    arg->interval_ms = interval_ms;

    bthread_timer_t tid;
    if (bthread_timer_add(&tid, butil::milliseconds_from_now(interval_ms), _on_timer, arg) == 0) {
        arg->timer_id.store(tid, std::memory_order_release);
    } else {
        LOG(WARNING) << "Adaptive: failed to register timer for pool group '" << name << "'";
    }
    group.timer_arg = arg;

    {
        std::lock_guard<std::mutex> lk(_mutex);
        _pool_groups[name] = std::move(group);
    }

    LOG(INFO) << "Adaptive: added pool group '" << name << "'"
              << ", max_threads=" << log_max << ", min_threads=" << log_min
              << ", interval_ms=" << interval_ms;
}

void AdaptiveThreadPoolController::cancel(const std::string& name) {
    TimerArg* arg = nullptr;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        auto it = _pool_groups.find(name);
        if (it != _pool_groups.end()) {
            arg = it->second.timer_arg;
            _pool_groups.erase(it);
        }
    }

    if (arg == nullptr) {
        return;
    }

    // Signal the callback to stop re-registering.
    arg->stopped.store(true, std::memory_order_release);

    // Try to cancel a pending (not yet fired) timer. Read timer_id after
    // setting stopped so any re-registration in a concurrent callback has
    // already stored the latest id by now (it holds mu, which we haven't
    // taken yet).
    bthread_timer_t tid = arg->timer_id.load(std::memory_order_acquire);
    bthread_timer_del(tid); // returns non-zero if already fired; that's fine

    // Wait for any in-flight callback to finish. The callback holds mu while
    // running _fire_group and re-registering, so acquiring mu here ensures
    // we don't free arg while the callback is still executing.
    { std::lock_guard<std::mutex> lk(arg->mu); }

    delete arg;
    LOG(INFO) << "Adaptive: cancelled pool group '" << name << "'";
}

// Called from _on_timer. No lock held on entry.
void AdaptiveThreadPoolController::_fire_group(const std::string& name) {
    if (!config::enable_adaptive_flush_threads) {
        return;
    }
    // Phase 1: snapshot parameters under the lock.
    AdjustFunc fn;
    int current, min_t, max_t;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        auto it = _pool_groups.find(name);
        if (it == _pool_groups.end()) return;
        const PoolGroup& g = it->second;
        fn = g.adjust_func;
        current = g.current_threads;
        min_t = g.get_min_threads();
        max_t = g.get_max_threads();
    }

    // Phase 2: compute target — no lock held (adjust_func may call is_io_busy etc.).
    std::string reason;
    int target = fn(current, min_t, max_t, reason);

    // Phase 3: apply under lock; recheck in case cancel() raced with us.
    std::lock_guard<std::mutex> lk(_mutex);
    auto it = _pool_groups.find(name);
    if (it == _pool_groups.end()) return;
    _apply_thread_count(it->second, target, reason);
}

// Fire all groups once regardless of schedule. For testing.
void AdaptiveThreadPoolController::adjust_once() {
    std::vector<std::string> names;
    {
        std::lock_guard<std::mutex> lk(_mutex);
        for (const auto& [name, _] : _pool_groups) {
            names.push_back(name);
        }
    }
    for (const auto& name : names) {
        _fire_group(name);
    }
}

void AdaptiveThreadPoolController::_apply_thread_count(PoolGroup& group, int target_threads,
                                                       const std::string& reason) {
    int max_threads = group.get_max_threads();
    int min_threads = group.get_min_threads();
    target_threads = std::max(min_threads, std::min(max_threads, target_threads));
    if (target_threads == group.current_threads) return;

    LOG(INFO) << "Adaptive[" << group.name << "]: adjusting threads from " << group.current_threads
              << " to " << target_threads << " (min=" << min_threads << ", max=" << max_threads
              << ")" << (reason.empty() ? "" : " reason=[" + reason + "]");

    bool all_success = true;
    for (auto* pool : group.pools) {
        if (pool == nullptr) continue;
        // Always sync min_threads to guard against races with update_memtable_flush_threads().
        // Order matters: when increasing, set max first so max >= min is always satisfied;
        // when decreasing, set min first so the new max is never below min.
        Status st;
        if (target_threads >= group.current_threads) {
            st = pool->set_max_threads(target_threads);
            if (st.ok()) static_cast<void>(pool->set_min_threads(min_threads));
        } else {
            st = pool->set_min_threads(min_threads);
            if (st.ok()) st = pool->set_max_threads(target_threads);
        }
        if (!st.ok()) {
            all_success = false;
            LOG(WARNING) << "Adaptive[" << group.name << "]: failed to set threads: " << st;
        }
    }
    if (all_success) {
        group.current_threads = target_threads;
    }
}

int AdaptiveThreadPoolController::get_current_threads(const std::string& name) const {
    std::lock_guard<std::mutex> lk(_mutex);
    auto it = _pool_groups.find(name);
    return it != _pool_groups.end() ? it->second.current_threads : 0;
}

bool AdaptiveThreadPoolController::is_io_busy() {
    std::lock_guard<std::mutex> lk(_metrics_state_mutex);

    if (config::is_cloud_mode()) {
        if (_s3_file_upload_pool == nullptr) return false;
        int queue_size = _s3_file_upload_pool->get_queue_size();
        return queue_size > kS3QueueBusyThreshold;
    }

    if (_system_metrics == nullptr) return false;

    int64_t current_time_sec = MonotonicSeconds();
    int64_t interval_sec = current_time_sec - _last_check_time_sec;
    if (interval_sec <= 0) {
        return _last_io_busy;
    }

    int64_t max_io_util = _system_metrics->get_max_io_util(_last_disk_io_time, interval_sec);
    _system_metrics->get_disks_io_time(&_last_disk_io_time);
    _last_check_time_sec = current_time_sec;

    _last_io_busy = max_io_util > kIOBusyThresholdPercent;
    return _last_io_busy;
}

bool AdaptiveThreadPoolController::is_cpu_busy() {
    std::lock_guard<std::mutex> lk(_metrics_state_mutex);

    if (_system_metrics == nullptr) return false;

    int64_t total_time = 0;
    int64_t idle_time = 0;
    if (!_system_metrics->get_aggregate_cpu_time(&total_time, &idle_time)) {
        return _last_cpu_busy;
    }

    if (_last_cpu_total_time < 0) {
        _last_cpu_total_time = total_time;
        _last_cpu_idle_time = idle_time;
        _last_cpu_busy = false;
        return false;
    }

    int64_t total_time_delta = total_time - _last_cpu_total_time;
    int64_t idle_time_delta = idle_time - _last_cpu_idle_time;
    if (total_time_delta <= 0 || idle_time_delta < 0 || idle_time_delta > total_time_delta) {
        // Keep the previous baseline so a transient invalid sample does not
        // poison the next interval's delta calculation.
        return _last_cpu_busy;
    }

    _last_cpu_total_time = total_time;
    _last_cpu_idle_time = idle_time;
    double cpu_busy_percent =
            static_cast<double>(total_time_delta - idle_time_delta) * 100.0 / total_time_delta;
    _last_cpu_busy = cpu_busy_percent > kCPUBusyThresholdPercent;
    return _last_cpu_busy;
}

AdaptiveThreadPoolController::AdjustFunc AdaptiveThreadPoolController::make_flush_adjust_func(
        AdaptiveThreadPoolController* controller, ThreadPool* flush_pool) {
    return [controller, flush_pool](int current, int min_t, int max_t, std::string& reason) {
        int target = current;
        int queue_size = flush_pool->get_queue_size();
        if (queue_size > kQueueThreshold) {
            target = std::min(max_t, target + 1);
            reason += "queue_size=" + std::to_string(queue_size) + ">" +
                      std::to_string(kQueueThreshold) + " -> target=" + std::to_string(target) +
                      "; ";
        }
        if (controller->is_io_busy()) {
            target = std::max(min_t, target - 2);
            reason += "io_busy -> target=" + std::to_string(target) + "; ";
        }
        if (controller->is_cpu_busy()) {
            target = std::max(min_t, target - 2);
            reason += "cpu_busy -> target=" + std::to_string(target) + "; ";
        }
        return target;
    };
}

} // namespace doris
