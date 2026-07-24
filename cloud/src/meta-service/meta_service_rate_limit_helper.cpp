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
#include <sched.h>
#include <sys/resource.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/logging.h"

namespace doris::cloud {
namespace internal {
namespace {
constexpr std::string_view kProcSelfCgroupPath = "/proc/self/cgroup";
constexpr std::string_view kProcSelfMountInfoPath = "/proc/self/mountinfo";
constexpr std::string_view kCgroupRootPath = "/sys/fs/cgroup";
} // namespace

struct CgroupMemoryInfo {
    int64_t limit_bytes;
    int64_t usage_bytes;
};

std::optional<std::string> read_first_line(const std::filesystem::path& path) {
    std::ifstream stream(path);
    if (!stream.is_open()) {
        return std::nullopt;
    }
    std::string line;
    std::getline(stream, line);
    if (stream.fail() || stream.bad()) {
        return std::nullopt;
    }
    return line;
}

std::optional<int64_t> read_int64_line(const std::filesystem::path& path) {
    auto line = read_first_line(path);
    if (!line.has_value() || line->empty()) {
        return std::nullopt;
    }
    try {
        return std::stoll(*line);
    } catch (...) {
        return std::nullopt;
    }
}

std::unordered_map<std::string, int64_t> read_metrics_map(const std::filesystem::path& path) {
    std::unordered_map<std::string, int64_t> metrics;
    std::ifstream stream(path);
    if (!stream.is_open()) {
        return metrics;
    }

    std::string key;
    int64_t value = 0;
    while (stream >> key >> value) {
        metrics[key] = value;
    }
    return metrics;
}

std::vector<std::string> split(std::string_view text, char delimiter) {
    std::vector<std::string> parts;
    size_t start = 0;
    while (start <= text.size()) {
        size_t end = text.find(delimiter, start);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        parts.emplace_back(text.substr(start, end - start));
        start = end + 1;
    }
    return parts;
}

bool cgroups_v2_enabled() {
    return std::filesystem::exists(std::filesystem::path(kCgroupRootPath) / "cgroup.controllers");
}

std::optional<std::string> cgroup_v2_relative_path_of_process() {
    auto line = read_first_line(std::filesystem::path(kProcSelfCgroupPath));
    if (!line.has_value() || !line->starts_with("0::")) {
        return std::nullopt;
    }
    return line->substr(3);
}

std::optional<std::filesystem::path> get_cgroup_v2_dir(const std::string& subsystem_file) {
    if (!cgroups_v2_enabled()) {
        return std::nullopt;
    }
    auto relative_path = cgroup_v2_relative_path_of_process();
    if (!relative_path.has_value()) {
        return std::nullopt;
    }

    const std::filesystem::path cgroup_root(kCgroupRootPath);
    std::filesystem::path current =
            cgroup_root / relative_path->substr(relative_path->starts_with('/') ? 1 : 0);
    while (current != cgroup_root.parent_path()) {
        if (std::filesystem::exists(current / subsystem_file)) {
            return current;
        }
        current = current.parent_path();
    }
    return std::nullopt;
}

std::optional<std::string> get_cgroup_v1_process_path(const std::string& subsystem) {
    std::ifstream stream(kProcSelfCgroupPath.data());
    if (!stream.is_open()) {
        return std::nullopt;
    }

    std::string line;
    while (std::getline(stream, line)) {
        auto fields = split(line, ':');
        if (fields.size() != 3) {
            continue;
        }
        auto controllers = split(fields[1], ',');
        if (std::find(controllers.begin(), controllers.end(), subsystem) != controllers.end()) {
            return fields[2];
        }
    }
    return std::nullopt;
}

std::optional<std::pair<std::string, std::string>> get_cgroup_v1_mount(
        const std::string& subsystem) {
    std::ifstream stream(kProcSelfMountInfoPath.data());
    if (!stream.is_open()) {
        return std::nullopt;
    }

    std::string line;
    while (std::getline(stream, line)) {
        auto separator = line.find(" - ");
        if (separator == std::string::npos) {
            continue;
        }
        auto left = split(std::string_view(line).substr(0, separator), ' ');
        auto right = split(std::string_view(line).substr(separator + 3), ' ');
        if (left.size() < 5 || right.size() < 3 || right[0] != "cgroup") {
            continue;
        }
        auto options = split(right[2], ',');
        if (std::find(options.begin(), options.end(), subsystem) == options.end()) {
            continue;
        }
        std::string system_path = left[3];
        if (system_path.size() > 1 && system_path.back() == '/') {
            system_path.pop_back();
        }
        return std::make_pair(left[4], system_path);
    }
    return std::nullopt;
}

std::optional<std::filesystem::path> get_cgroup_v1_dir(const std::string& subsystem) {
    auto process_path = get_cgroup_v1_process_path(subsystem);
    auto mount = get_cgroup_v1_mount(subsystem);
    if (!process_path.has_value() || !mount.has_value()) {
        return std::nullopt;
    }

    const auto& [mount_path, system_path] = *mount;
    if (!process_path->starts_with(system_path)) {
        return std::nullopt;
    }

    std::string absolute = *process_path;
    absolute.replace(0, system_path.size(), mount_path);
    return std::filesystem::path(absolute);
}

int parse_cpuset_cpu_count(std::string_view cpuset_line) {
    if (cpuset_line.empty()) {
        return -1;
    }

    int cpu_count = 0;
    for (const auto& range : split(cpuset_line, ',')) {
        if (range.empty()) {
            return -1;
        }
        auto cpu_values = split(range, '-');
        try {
            if (cpu_values.size() == 2) {
                const int start = std::stoi(cpu_values[0]);
                const int end = std::stoi(cpu_values[1]);
                if (end < start) {
                    return -1;
                }
                cpu_count += end - start + 1;
            } else if (cpu_values.size() == 1) {
                static_cast<void>(std::stoi(cpu_values[0]));
                cpu_count += 1;
            } else {
                return -1;
            }
        } catch (...) {
            return -1;
        }
    }
    return cpu_count;
}

std::optional<double> parse_cgroup_v2_cpu_limit(std::string_view cpu_max_line) {
    std::istringstream input {std::string(cpu_max_line)};
    std::string quota;
    double period = 0;
    if (!(input >> quota >> period) || quota == "max" || period <= 0) {
        return std::nullopt;
    }
    try {
        const double quota_value = std::stod(quota);
        if (quota_value <= 0) {
            return std::nullopt;
        }
        return quota_value / period;
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<double> parse_cgroup_v1_cpu_limit(int64_t quota_us, int64_t period_us) {
    if (quota_us <= 0 || period_us <= 0) {
        return std::nullopt;
    }
    return static_cast<double>(quota_us) / static_cast<double>(period_us);
}

double get_process_affinity_cpu_limit() {
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    if (sched_getaffinity(0, sizeof(cpu_set), &cpu_set) == 0) {
        const int cpu_count = CPU_COUNT(&cpu_set);
        if (cpu_count > 0) {
            return static_cast<double>(cpu_count);
        }
    }
    const uint32_t fallback = std::max<uint32_t>(1, std::thread::hardware_concurrency());
    return static_cast<double>(fallback);
}

std::optional<double> get_cgroup_cpu_quota_limit() {
    if (auto dir = get_cgroup_v2_dir("cpu.max"); dir.has_value()) {
        const std::filesystem::path cgroup_root(kCgroupRootPath);
        std::optional<double> limit;
        auto current = *dir;
        while (current != cgroup_root.parent_path()) {
            if (auto line = read_first_line(current / "cpu.max"); line.has_value()) {
                if (auto quota = parse_cgroup_v2_cpu_limit(*line); quota.has_value()) {
                    limit = limit.has_value() ? std::min(*limit, *quota) : quota;
                }
            }
            current = current.parent_path();
        }
        if (limit.has_value()) {
            return limit;
        }
    }

    if (auto dir = get_cgroup_v1_dir("cpu"); dir.has_value()) {
        std::optional<double> limit;
        auto current = *dir;
        while (current != current.parent_path()) {
            auto quota = read_int64_line(current / "cpu.cfs_quota_us");
            auto period = read_int64_line(current / "cpu.cfs_period_us");
            if (quota.has_value() && period.has_value()) {
                if (auto parsed = parse_cgroup_v1_cpu_limit(*quota, *period); parsed.has_value()) {
                    limit = limit.has_value() ? std::min(*limit, *parsed) : parsed;
                }
            }
            current = current.parent_path();
        }
        if (limit.has_value()) {
            return limit;
        }
    }

    return std::nullopt;
}

double get_effective_process_cpu_limit() {
    double limit = get_process_affinity_cpu_limit();
    if (auto quota = get_cgroup_cpu_quota_limit(); quota.has_value() && *quota > 0) {
        limit = std::min(limit, *quota);
    }
    return std::max(0.001, limit);
}

int64_t calculate_usage_percent(int64_t usage_bytes, int64_t limit_bytes) {
    if (usage_bytes < 0 || limit_bytes <= 0) {
        return -1;
    }
    return static_cast<int64_t>(static_cast<double>(usage_bytes) * 100.0 /
                                static_cast<double>(limit_bytes));
}

int64_t calculate_cpu_usage_percent(double delta_cpu_ns, double delta_wall_ns, double cpu_limit) {
    if (delta_cpu_ns < 0 || delta_wall_ns <= 0 || cpu_limit <= 0) {
        return -1;
    }
    return static_cast<int64_t>(delta_cpu_ns * 100.0 / delta_wall_ns / cpu_limit);
}

std::optional<CgroupMemoryInfo> get_cgroup_memory_info() {
    if (auto dir = get_cgroup_v2_dir("memory.current"); dir.has_value()) {
        auto limit_line = read_first_line(*dir / "memory.max");
        auto usage = read_int64_line(*dir / "memory.current");
        if (limit_line.has_value() && usage.has_value()) {
            int64_t limit_bytes = std::numeric_limits<int64_t>::max();
            if (*limit_line != "max") {
                try {
                    limit_bytes = std::stoll(*limit_line);
                } catch (...) {
                    return std::nullopt;
                }
            }
            auto metrics = read_metrics_map(*dir / "memory.stat");
            int64_t adjusted_usage = *usage;
            adjusted_usage -= metrics["inactive_file"] + metrics["active_file"];
            adjusted_usage -= metrics["slab_reclaimable"];
            adjusted_usage = std::max<int64_t>(0, adjusted_usage);
            return CgroupMemoryInfo {limit_bytes, adjusted_usage};
        }
    }

    if (auto dir = get_cgroup_v1_dir("memory"); dir.has_value()) {
        auto limit = read_int64_line(*dir / "memory.limit_in_bytes");
        if (limit.has_value()) {
            auto metrics = read_metrics_map(*dir / "memory.stat");
            return CgroupMemoryInfo {*limit, metrics["rss"]};
        }
    }

    return std::nullopt;
}
} // namespace internal

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

class LatestDecisionStorage {
public:
    void store(const MsStressDecision& decision) {
        version_.fetch_add(1, std::memory_order_acq_rel);

        fdb_cluster_under_pressure_.store(decision.fdb_cluster_under_pressure,
                                          std::memory_order_relaxed);
        fdb_client_thread_under_pressure_.store(decision.fdb_client_thread_under_pressure,
                                                std::memory_order_relaxed);
        ms_resource_under_pressure_.store(decision.ms_resource_under_pressure,
                                          std::memory_order_relaxed);
        rate_limit_injected_for_test_.store(decision.rate_limit_injected_for_test,
                                            std::memory_order_relaxed);
        fdb_commit_latency_ns_.store(decision.fdb_commit_latency_ns, std::memory_order_relaxed);
        fdb_read_latency_ns_.store(decision.fdb_read_latency_ns, std::memory_order_relaxed);
        fdb_performance_limited_by_name_.store(decision.fdb_performance_limited_by_name,
                                               std::memory_order_relaxed);
        fdb_client_thread_busyness_percent_.store(decision.fdb_client_thread_busyness_percent,
                                                  std::memory_order_relaxed);
        fdb_client_thread_busyness_avg_percent_bits_.store(
                encode_double(decision.fdb_client_thread_busyness_avg_percent),
                std::memory_order_relaxed);
        ms_cpu_usage_percent_.store(decision.ms_cpu_usage_percent, std::memory_order_relaxed);
        ms_cpu_usage_avg_percent_bits_.store(encode_double(decision.ms_cpu_usage_avg_percent),
                                             std::memory_order_relaxed);
        ms_memory_usage_percent_.store(decision.ms_memory_usage_percent, std::memory_order_relaxed);
        ms_memory_usage_avg_percent_bits_.store(encode_double(decision.ms_memory_usage_avg_percent),
                                                std::memory_order_relaxed);
        rate_limit_injected_random_value_.store(decision.rate_limit_injected_random_value,
                                                std::memory_order_relaxed);

        version_.fetch_add(1, std::memory_order_release);
    }

    MsStressDecision load() const {
        MsStressDecision decision;
        while (true) {
            const uint64_t version_before = version_.load(std::memory_order_acquire);
            if ((version_before & 1) != 0) {
                continue;
            }

            decision.fdb_cluster_under_pressure =
                    fdb_cluster_under_pressure_.load(std::memory_order_relaxed);
            decision.fdb_client_thread_under_pressure =
                    fdb_client_thread_under_pressure_.load(std::memory_order_relaxed);
            decision.ms_resource_under_pressure =
                    ms_resource_under_pressure_.load(std::memory_order_relaxed);
            decision.rate_limit_injected_for_test =
                    rate_limit_injected_for_test_.load(std::memory_order_relaxed);
            decision.fdb_commit_latency_ns = fdb_commit_latency_ns_.load(std::memory_order_relaxed);
            decision.fdb_read_latency_ns = fdb_read_latency_ns_.load(std::memory_order_relaxed);
            decision.fdb_performance_limited_by_name =
                    fdb_performance_limited_by_name_.load(std::memory_order_relaxed);
            decision.fdb_client_thread_busyness_percent =
                    fdb_client_thread_busyness_percent_.load(std::memory_order_relaxed);
            decision.fdb_client_thread_busyness_avg_percent = decode_double(
                    fdb_client_thread_busyness_avg_percent_bits_.load(std::memory_order_relaxed));
            decision.ms_cpu_usage_percent = ms_cpu_usage_percent_.load(std::memory_order_relaxed);
            decision.ms_cpu_usage_avg_percent =
                    decode_double(ms_cpu_usage_avg_percent_bits_.load(std::memory_order_relaxed));
            decision.ms_memory_usage_percent =
                    ms_memory_usage_percent_.load(std::memory_order_relaxed);
            decision.ms_memory_usage_avg_percent = decode_double(
                    ms_memory_usage_avg_percent_bits_.load(std::memory_order_relaxed));
            decision.rate_limit_injected_random_value =
                    rate_limit_injected_random_value_.load(std::memory_order_relaxed);

            const uint64_t version_after = version_.load(std::memory_order_acquire);
            if (version_before == version_after) {
                return decision;
            }
        }
    }

private:
    static constexpr uint64_t encode_double(double value) { return std::bit_cast<uint64_t>(value); }

    static constexpr double decode_double(uint64_t bits) { return std::bit_cast<double>(bits); }

    std::atomic<uint64_t> version_ {0};
    std::atomic<bool> fdb_cluster_under_pressure_ {false};
    std::atomic<bool> fdb_client_thread_under_pressure_ {false};
    std::atomic<bool> ms_resource_under_pressure_ {false};
    std::atomic<bool> rate_limit_injected_for_test_ {false};
    std::atomic<int64_t> fdb_commit_latency_ns_ {BVAR_FDB_INVALID_VALUE};
    std::atomic<int64_t> fdb_read_latency_ns_ {BVAR_FDB_INVALID_VALUE};
    std::atomic<int64_t> fdb_performance_limited_by_name_ {BVAR_FDB_INVALID_VALUE};
    std::atomic<int64_t> fdb_client_thread_busyness_percent_ {BVAR_FDB_INVALID_VALUE};
    std::atomic<uint64_t> fdb_client_thread_busyness_avg_percent_bits_ {encode_double(-1)};
    std::atomic<int64_t> ms_cpu_usage_percent_ {-1};
    std::atomic<uint64_t> ms_cpu_usage_avg_percent_bits_ {encode_double(-1)};
    std::atomic<int64_t> ms_memory_usage_percent_ {-1};
    std::atomic<uint64_t> ms_memory_usage_avg_percent_bits_ {encode_double(-1)};
    std::atomic<int32_t> rate_limit_injected_random_value_ {-1};
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
            sample.cpu_usage_percent = internal::calculate_cpu_usage_percent(
                    delta_cpu_ns, delta_wall_ns, internal::get_effective_process_cpu_limit());
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
        if (auto cgroup_memory = internal::get_cgroup_memory_info(); cgroup_memory.has_value()) {
            if (cgroup_memory->limit_bytes > 0 &&
                cgroup_memory->limit_bytes < std::numeric_limits<int64_t>::max()) {
                return internal::calculate_usage_percent(cgroup_memory->usage_bytes,
                                                         cgroup_memory->limit_bytes);
            }
        }

        return kInvalidPercent;
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
        MsStressDecision decision;
        decision.fdb_commit_latency_ns = metrics.fdb_commit_latency_ns;
        decision.fdb_read_latency_ns = metrics.fdb_read_latency_ns;
        decision.fdb_performance_limited_by_name = metrics.fdb_performance_limited_by_name;
        decision.fdb_client_thread_busyness_percent = metrics.fdb_client_thread_busyness_percent;
        decision.ms_cpu_usage_percent = metrics.ms_cpu_usage_percent;
        decision.ms_memory_usage_percent = metrics.ms_memory_usage_percent;
        const bool commit_latency_high =
                metrics.fdb_commit_latency_ns != BVAR_FDB_INVALID_VALUE &&
                metrics.fdb_commit_latency_ns >
                        config::ms_rate_limit_fdb_commit_latency_ms * kNanosecondsPerMillisecond;
        const bool read_latency_high =
                metrics.fdb_read_latency_ns != BVAR_FDB_INVALID_VALUE &&
                metrics.fdb_read_latency_ns >
                        config::ms_rate_limit_fdb_read_latency_ms * kNanosecondsPerMillisecond;
        decision.fdb_cluster_under_pressure =
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
        decision.fdb_client_thread_busyness_avg_percent = avg_busyness;
        if (avg_busyness >= 0 &&
            metrics.fdb_client_thread_busyness_percent != BVAR_FDB_INVALID_VALUE) {
            decision.fdb_client_thread_under_pressure =
                    avg_busyness > config::ms_rate_limit_fdb_client_thread_busyness_avg_percent &&
                    metrics.fdb_client_thread_busyness_percent >
                            config::ms_rate_limit_fdb_client_thread_busyness_instant_percent;
        }

        const double avg_cpu = get_window_avg(current_second, &WindowSample::ms_cpu_usage_percent,
                                              kInvalidPercent);
        const double avg_memory = get_window_avg(
                current_second, &WindowSample::ms_memory_usage_percent, kInvalidPercent);
        decision.ms_cpu_usage_avg_percent = avg_cpu;
        decision.ms_memory_usage_avg_percent = avg_memory;
        if (avg_cpu >= 0 && metrics.ms_cpu_usage_percent != kInvalidPercent) {
            decision.ms_resource_under_pressure =
                    metrics.ms_cpu_usage_percent > config::ms_rate_limit_cpu_usage_percent &&
                    avg_cpu > config::ms_rate_limit_cpu_usage_percent;
        }
        if (avg_memory >= 0 && metrics.ms_memory_usage_percent != kInvalidPercent) {
            decision.ms_resource_under_pressure =
                    decision.ms_resource_under_pressure ||
                    (metrics.ms_memory_usage_percent > config::ms_rate_limit_memory_usage_percent &&
                     avg_memory > config::ms_rate_limit_memory_usage_percent);
        }
        latest_decision_.store(decision);
    }

    MsStressDecision get_latest_decision() const { return latest_decision_.load(); }

    void reset() {
        samples_.clear();
        latest_decision_.store(MsStressDecision {});
    }

    // Start the background thread that periodically collects metrics and updates.
    void start() {
        if (running_.load() != 0) {
            return;
        }

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

    LatestDecisionStorage latest_decision_;
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
    MsStressDecision decision = global_ms_stress_detector().get_latest_decision();
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
    MsStressDecision decision = detector.get_latest_decision();
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
