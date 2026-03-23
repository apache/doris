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

#include "meta-service/meta_service_rate_limit_helper.h"

#include <fmt/format.h>
#include <sys/resource.h>
#include <sys/sysinfo.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <fstream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/logging.h"

namespace doris::cloud {
namespace {
constexpr int64_t kNanosecondsPerMillisecond = 1000 * 1000;
constexpr int64_t kInvalidPercent = -1;

struct WindowSample {
    int64_t second {0};
    int64_t fdb_client_thread_busyness_percent {BVAR_FDB_INVALID_VALUE};
    int64_t ms_cpu_usage_percent {kInvalidPercent};
    int64_t ms_memory_usage_percent {kInvalidPercent};
};

struct ProcessResourceSample {
    int64_t cpu_usage_percent {kInvalidPercent};
    int64_t memory_usage_percent {kInvalidPercent};
};

class ProcessResourceSampler {
public:
    ProcessResourceSample sample() {
        using namespace std::chrono;

        const auto now = steady_clock::now();
        const int64_t current_cpu_time_ns = get_process_cpu_time_ns();
        ProcessResourceSample sample;
        sample.memory_usage_percent = get_process_memory_usage_percent();

        const auto current_wall_time_ns =
                duration_cast<nanoseconds>(now.time_since_epoch()).count();
        std::lock_guard lock(mutex_);
        if (last_cpu_time_ns_ != kInvalidPercent && current_cpu_time_ns != kInvalidPercent &&
            current_wall_time_ns > last_wall_time_ns_) {
            const double delta_cpu_ns = current_cpu_time_ns - last_cpu_time_ns_;
            const double delta_wall_ns = current_wall_time_ns - last_wall_time_ns_;
            const uint32_t cpu_cores = std::max<uint32_t>(1, std::thread::hardware_concurrency());
            sample.cpu_usage_percent =
                    static_cast<int64_t>(delta_cpu_ns * 100.0 / delta_wall_ns / cpu_cores);
        }
        last_cpu_time_ns_ = current_cpu_time_ns;
        last_wall_time_ns_ = current_wall_time_ns;
        return sample;
    }

private:
    static int64_t get_process_cpu_time_ns() {
        rusage usage {};
        if (getrusage(RUSAGE_SELF, &usage) != 0) {
            return kInvalidPercent;
        }
        return usage.ru_utime.tv_sec * 1000L * 1000 * 1000 + usage.ru_utime.tv_usec * 1000L +
               usage.ru_stime.tv_sec * 1000L * 1000 * 1000 + usage.ru_stime.tv_usec * 1000L;
    }

    static int64_t get_process_memory_usage_percent() {
        std::ifstream status("/proc/self/status");
        if (!status.is_open()) {
            return kInvalidPercent;
        }

        int64_t rss_kb = kInvalidPercent;
        std::string line;
        while (std::getline(status, line)) {
            if (!line.starts_with("VmRSS:")) {
                continue;
            }
            size_t pos = std::string("VmRSS:").size();
            while (pos < line.size() && line[pos] == ' ') {
                ++pos;
            }
            rss_kb = std::stoll(line.substr(pos));
            break;
        }
        if (rss_kb == kInvalidPercent) {
            return kInvalidPercent;
        }

        struct sysinfo info {};
        if (sysinfo(&info) != 0) {
            return kInvalidPercent;
        }
        const double total_memory_bytes =
                static_cast<double>(info.totalram) * static_cast<double>(info.mem_unit);
        if (total_memory_bytes <= 0) {
            return kInvalidPercent;
        }
        const double rss_bytes = static_cast<double>(rss_kb) * 1024.0;
        return static_cast<int64_t>(rss_bytes * 100.0 / total_memory_bytes);
    }

    std::mutex mutex_;
    int64_t last_cpu_time_ns_ {kInvalidPercent};
    int64_t last_wall_time_ns_ {0};
};

MsStressMetrics collect_ms_stress_metrics(ProcessResourceSampler* sampler) {
    MsStressMetrics metrics;
    metrics.fdb_commit_latency_ns = g_bvar_fdb_latency_probe_commit_ns.get_value();
    metrics.fdb_read_latency_ns = g_bvar_fdb_latency_probe_read_ns.get_value();
    metrics.fdb_performance_limited_by_name = g_bvar_fdb_performance_limited_by_name.get_value();
    metrics.fdb_client_thread_busyness_percent =
            g_bvar_fdb_client_thread_busyness_percent.get_value();
    const auto resource_sample = sampler->sample();
    metrics.ms_cpu_usage_percent = resource_sample.cpu_usage_percent;
    metrics.ms_memory_usage_percent = resource_sample.memory_usage_percent;
    return metrics;
}

class MsStressDetector {
public:
    ~MsStressDetector() { stop(); }

    // Compute decision from metrics and store it in latest_decision_.
    // Called by the background thread or synchronously in tests.
    void update(int64_t now_ms, const MsStressMetrics& metrics) {
        auto decision = std::make_shared<MsStressDecision>();
        decision->fdb_commit_latency_ns = metrics.fdb_commit_latency_ns;
        decision->fdb_read_latency_ns = metrics.fdb_read_latency_ns;
        decision->fdb_performance_limited_by_name = metrics.fdb_performance_limited_by_name;
        decision->fdb_client_thread_busyness_percent = metrics.fdb_client_thread_busyness_percent;
        decision->ms_cpu_usage_percent = metrics.ms_cpu_usage_percent;
        decision->ms_memory_usage_percent = metrics.ms_memory_usage_percent;
        const bool commit_latency_high =
                metrics.fdb_commit_latency_ns != BVAR_FDB_INVALID_VALUE &&
                metrics.fdb_commit_latency_ns >
                        config::ms_rate_limit_fdb_commit_latency_ms * kNanosecondsPerMillisecond;
        const bool read_latency_high =
                metrics.fdb_read_latency_ns != BVAR_FDB_INVALID_VALUE &&
                metrics.fdb_read_latency_ns >
                        config::ms_rate_limit_fdb_read_latency_ms * kNanosecondsPerMillisecond;
        decision->fdb_cluster_under_pressure =
                (commit_latency_high || read_latency_high) &&
                metrics.fdb_performance_limited_by_name != BVAR_FDB_INVALID_VALUE &&
                metrics.fdb_performance_limited_by_name != 0;

        const int64_t current_second = now_ms / 1000;
        // No mutex needed: update() is only called from a single thread
        // (background thread in production, test thread in tests).
        record_sample(current_second, metrics);

        const double avg_busyness =
                get_window_avg(current_second, &WindowSample::fdb_client_thread_busyness_percent,
                               BVAR_FDB_INVALID_VALUE);
        decision->fdb_client_thread_busyness_avg_percent = avg_busyness;
        if (avg_busyness >= 0 &&
            metrics.fdb_client_thread_busyness_percent != BVAR_FDB_INVALID_VALUE) {
            decision->fdb_client_thread_under_pressure =
                    avg_busyness > config::ms_rate_limit_fdb_client_thread_busyness_avg_percent &&
                    metrics.fdb_client_thread_busyness_percent >
                            config::ms_rate_limit_fdb_client_thread_busyness_instant_percent;
        }

        const double avg_cpu = get_window_avg(current_second, &WindowSample::ms_cpu_usage_percent,
                                              kInvalidPercent);
        const double avg_memory = get_window_avg(
                current_second, &WindowSample::ms_memory_usage_percent, kInvalidPercent);
        decision->ms_cpu_usage_avg_percent = avg_cpu;
        decision->ms_memory_usage_avg_percent = avg_memory;
        if (avg_cpu >= 0 && metrics.ms_cpu_usage_percent != kInvalidPercent) {
            decision->ms_resource_under_pressure =
                    metrics.ms_cpu_usage_percent > config::ms_rate_limit_cpu_usage_percent &&
                    avg_cpu > config::ms_rate_limit_cpu_usage_percent;
        }
        if (avg_memory >= 0 && metrics.ms_memory_usage_percent != kInvalidPercent) {
            decision->ms_resource_under_pressure =
                    decision->ms_resource_under_pressure ||
                    (metrics.ms_memory_usage_percent > config::ms_rate_limit_memory_usage_percent &&
                     avg_memory > config::ms_rate_limit_memory_usage_percent);
        }
        latest_decision_.store(std::move(decision));
    }

    // Lock-free read of the latest decision. Returns nullptr before first update.
    std::shared_ptr<const MsStressDecision> get_latest_decision() const {
        return latest_decision_.load();
    }

    void reset() { samples_.clear(); }

    // Start the background thread that periodically collects metrics and updates.
    void start() {
        std::unique_lock lock(mtx_);
        if (running_.load() != 0) {
            return;
        }
        running_.store(1);
        bg_thread_ = std::make_unique<std::thread>([this] {
            pthread_setname_np(pthread_self(), "ms_stress_det");
            LOG(INFO) << "MsStressDetector background thread started";
            ProcessResourceSampler sampler;
            while (running_.load() == 1) {
                const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                            std::chrono::steady_clock::now().time_since_epoch())
                                            .count();
                const auto metrics = collect_ms_stress_metrics(&sampler);
                update(now_ms, metrics);
                std::unique_lock l(mtx_);
                cv_.wait_for(l, std::chrono::seconds(1), [this]() { return running_.load() != 1; });
            }
            LOG(INFO) << "MsStressDetector background thread stopped";
        });
    }

    void stop() {
        {
            std::unique_lock lock(mtx_);
            if (running_.load() != 1) {
                return;
            }
            running_.store(2);
            cv_.notify_all();
        }
        if (bg_thread_ && bg_thread_->joinable()) {
            bg_thread_->join();
            bg_thread_.reset();
        }
    }

private:
    using SampleField = int64_t WindowSample::*;

    void record_sample(int64_t current_second, const MsStressMetrics& metrics) {
        WindowSample sample;
        sample.second = current_second;
        sample.fdb_client_thread_busyness_percent = metrics.fdb_client_thread_busyness_percent;
        sample.ms_cpu_usage_percent = metrics.ms_cpu_usage_percent;
        sample.ms_memory_usage_percent = metrics.ms_memory_usage_percent;
        if (!samples_.empty() && samples_.back().second == current_second) {
            samples_.back() = sample;
        } else {
            samples_.push_back(sample);
        }

        const int64_t window_start =
                current_second - std::max<int64_t>(1, config::ms_rate_limit_window_seconds) + 1;
        while (!samples_.empty() && samples_.front().second < window_start) {
            samples_.pop_front();
        }
    }

    double get_window_avg(int64_t current_second, SampleField field, int64_t invalid_value) const {
        if (samples_.empty()) {
            return -1;
        }
        const int64_t required_span =
                std::max<int64_t>(1, config::ms_rate_limit_window_seconds) - 1;
        if (samples_.back().second != current_second ||
            current_second - samples_.front().second < required_span) {
            return -1;
        }

        double sum = 0;
        int64_t valid_count = 0;
        for (const auto& sample : samples_) {
            if (sample.*field == invalid_value) {
                continue;
            }
            sum += sample.*field;
            ++valid_count;
        }
        if (valid_count == 0) {
            return -1;
        }
        return sum / valid_count;
    }

    std::atomic<std::shared_ptr<const MsStressDecision>> latest_decision_;
    std::deque<WindowSample> samples_;

    // Background thread lifecycle
    std::atomic<int> running_ {0};
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::unique_ptr<std::thread> bg_thread_;
};

MsStressDetector& global_ms_stress_detector() {
    static MsStressDetector detector;
    // Auto-start background thread on first access.
    // start() is idempotent: subsequent calls are no-ops.
    detector.start();
    return detector;
}

int32_t get_ms_rate_limit_injection_random_value() {
    thread_local std::mt19937 gen(std::random_device {}());
    thread_local std::uniform_int_distribution<int32_t> dist(0, 99);
    return dist(gen);
}

void maybe_apply_ms_rate_limit_injection(MsStressDecision* decision, int32_t random_value) {
    if (!config::enable_ms_rate_limit_injection) {
        return;
    }
    if (random_value < 0 || random_value >= config::ms_rate_limit_injection_probability) {
        return;
    }
    decision->rate_limit_injected_for_test = true;
    decision->rate_limit_injected_random_value = random_value;
}
} // namespace

std::string MsStressDecision::debug_string() const {
    if (!under_great_stress()) {
        return "meta service rate limited: no stress condition matched";
    }

    std::vector<std::string> reasons;
    if (fdb_cluster_under_pressure) {
        reasons.push_back(fmt::format(
                "fdb_cluster(commit_latency_ms={}, read_latency_ms={}, performance_limited_by={})",
                fdb_commit_latency_ns == BVAR_FDB_INVALID_VALUE ? -1
                                                                : fdb_commit_latency_ns / 1000000,
                fdb_read_latency_ns == BVAR_FDB_INVALID_VALUE ? -1 : fdb_read_latency_ns / 1000000,
                fdb_performance_limited_by_name));
    }
    if (fdb_client_thread_under_pressure) {
        reasons.push_back(fmt::format(
                "fdb_client_thread(busyness_avg={:.2f}%, busyness_instant={}%, thresholds=avg>{}% "
                "and instant>{}%)",
                fdb_client_thread_busyness_avg_percent, fdb_client_thread_busyness_percent,
                config::ms_rate_limit_fdb_client_thread_busyness_avg_percent,
                config::ms_rate_limit_fdb_client_thread_busyness_instant_percent));
    }
    if (ms_resource_under_pressure) {
        reasons.push_back(
                fmt::format("ms_resource(cpu_current={}%, cpu_avg={:.2f}%, memory_current={}%, "
                            "memory_avg={:.2f}%, thresholds=cpu>{}% or memory>{}%)",
                            ms_cpu_usage_percent, ms_cpu_usage_avg_percent, ms_memory_usage_percent,
                            ms_memory_usage_avg_percent, config::ms_rate_limit_cpu_usage_percent,
                            config::ms_rate_limit_memory_usage_percent));
    }
    if (rate_limit_injected_for_test) {
        reasons.push_back(fmt::format("test_injection(random_value={}, probability<{}%)",
                                      rate_limit_injected_random_value,
                                      config::ms_rate_limit_injection_probability));
    }
    return fmt::format("meta service rate limited by {}", fmt::join(reasons, "; "));
}

MsStressDecision get_ms_stress_decision() {
    auto decision_ptr = global_ms_stress_detector().get_latest_decision();
    MsStressDecision decision;
    if (decision_ptr) {
        decision = *decision_ptr;
    }
    // Rate limit injection is per-request (random), so apply it here, not in the background thread.
    maybe_apply_ms_rate_limit_injection(&decision, get_ms_rate_limit_injection_random_value());
    return decision;
}

MsStressDecision update_ms_stress_detector_for_test(int64_t now_ms, const MsStressMetrics& metrics,
                                                    bool reset,
                                                    int32_t rate_limit_injected_random_value) {
    // Separate detector instance for tests — no background thread, synchronous updates.
    static MsStressDetector detector;
    if (reset) {
        detector.reset();
    }
    detector.update(now_ms, metrics);
    auto decision_ptr = detector.get_latest_decision();
    MsStressDecision decision;
    if (decision_ptr) {
        decision = *decision_ptr;
    }
    maybe_apply_ms_rate_limit_injection(&decision, rate_limit_injected_random_value);
    return decision;
}

RpcRateLimitWhitelist& RpcRateLimitWhitelist::instance() {
    static RpcRateLimitWhitelist inst;
    static std::once_flag init_flag;
    std::call_once(init_flag, []() {
        inst.set_whitelist({"prepare_rowset", "commit_rowset", "update_tmp_rowset",
                            "update_delete_bitmap", "update_packed_file_info"});
    });
    return inst;
}

bool RpcRateLimitWhitelist::should_rate_limit(const std::string& rpc_name) const {
    std::lock_guard lock(mutex_);
    return whitelist_.empty() || whitelist_.contains(rpc_name);
}

void RpcRateLimitWhitelist::set_whitelist(const std::vector<std::string>& rpcs) {
    std::lock_guard lock(mutex_);
    whitelist_.clear();
    whitelist_.insert(rpcs.begin(), rpcs.end());
}

std::vector<std::string> RpcRateLimitWhitelist::get_whitelist() const {
    std::lock_guard lock(mutex_);
    return {whitelist_.begin(), whitelist_.end()};
}

} // namespace doris::cloud
