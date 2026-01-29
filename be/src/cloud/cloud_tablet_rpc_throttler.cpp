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

#include "cloud/cloud_tablet_rpc_throttler.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <cmath>

#include "cloud/config.h"

namespace doris::cloud {

// Global bvar metrics
bvar::Adder<uint64_t> g_backpressure_upgrade_count("ms_rpc_backpressure_upgrade_count");
bvar::Window<bvar::Adder<uint64_t>> g_backpressure_upgrade_qpm("ms_rpc_backpressure_upgrade_qpm",
                                                               &g_backpressure_upgrade_count, 60);
bvar::Adder<uint64_t> g_backpressure_downgrade_count("ms_rpc_backpressure_downgrade_count");
bvar::Window<bvar::Adder<uint64_t>> g_backpressure_downgrade_qpm(
        "ms_rpc_backpressure_downgrade_qpm", &g_backpressure_downgrade_count, 60);
bvar::LatencyRecorder g_table_throttle_wait_us("ms_rpc_backpressure_throttle_wait");
bvar::Adder<uint64_t> g_ms_busy_count("ms_rpc_backpressure_ms_busy_count");
bvar::Window<bvar::Adder<uint64_t>> g_ms_busy_qpm("ms_rpc_backpressure_ms_busy_qpm",
                                                  &g_ms_busy_count, 60);

// Display names for LoadRelatedRpc types
static constexpr std::string_view LOAD_RELATED_RPC_NAMES[] = {
        "prepare_rowset", "commit_rowset", "update_tmp_rowset", "update_packed_file_info",
        "update_delete_bitmap"};

std::string_view load_related_rpc_name(LoadRelatedRpc rpc) {
    size_t idx = static_cast<size_t>(rpc);
    if (idx < static_cast<size_t>(LoadRelatedRpc::COUNT)) {
        return LOAD_RELATED_RPC_NAMES[idx];
    }
    return "unknown";
}

// ============== StrictQpsLimiter ==============

StrictQpsLimiter::StrictQpsLimiter(double qps) {
    if (qps <= 0) {
        qps = 1.0;
    }
    _interval_ns = static_cast<int64_t>(1e9 / qps);
    _next_allowed_time = Clock::now();
}

StrictQpsLimiter::Clock::time_point StrictQpsLimiter::reserve() {
    std::lock_guard lock(_mtx);
    auto now = Clock::now();
    if (_next_allowed_time <= now) {
        _next_allowed_time = now + std::chrono::nanoseconds(_interval_ns);
        return now;
    }
    auto result = _next_allowed_time;
    _next_allowed_time += std::chrono::nanoseconds(_interval_ns);
    return result;
}

void StrictQpsLimiter::update_qps(double new_qps) {
    if (new_qps <= 0) {
        new_qps = 1.0;
    }
    std::lock_guard lock(_mtx);
    _interval_ns = static_cast<int64_t>(1e9 / new_qps);
}

double StrictQpsLimiter::get_qps() const {
    std::lock_guard lock(_mtx);
    if (_interval_ns <= 0) {
        return 0;
    }
    return 1e9 / _interval_ns;
}

// ============== TableRpcQpsCounter ==============

TableRpcQpsCounter::TableRpcQpsCounter(int64_t table_id, LoadRelatedRpc rpc_type)
        : _table_id(table_id), _rpc_type(rpc_type) {
    std::string bvar_name =
            fmt::format("ms_rpc_table_qps_{}_{}", load_related_rpc_name(rpc_type), table_id);
    _counter = std::make_unique<bvar::Adder<int64_t>>();
    _qps = std::make_unique<bvar::PerSecond<bvar::Adder<int64_t>>>(bvar_name, _counter.get());
}

void TableRpcQpsCounter::increment() {
    (*_counter) << 1;
}

double TableRpcQpsCounter::get_qps() const {
    return _qps->get_value();
}

// ============== TableRpcQpsRegistry ==============

TableRpcQpsRegistry::TableRpcQpsRegistry() = default;

void TableRpcQpsRegistry::record(LoadRelatedRpc rpc_type, int64_t table_id) {
    auto* counter = get_or_create_counter(rpc_type, table_id);
    if (counter) {
        counter->increment();
    }
}

TableRpcQpsCounter* TableRpcQpsRegistry::get_or_create_counter(LoadRelatedRpc rpc_type,
                                                               int64_t table_id) {
    size_t idx = static_cast<size_t>(rpc_type);
    if (idx >= static_cast<size_t>(LoadRelatedRpc::COUNT)) {
        return nullptr;
    }

    {
        std::shared_lock lock(_mutex);
        auto it = _counters[idx].find(table_id);
        if (it != _counters[idx].end()) {
            return it->second.get();
        }
    }

    std::unique_lock lock(_mutex);
    // Double check after acquiring exclusive lock
    auto it = _counters[idx].find(table_id);
    if (it != _counters[idx].end()) {
        return it->second.get();
    }

    // Check if we've exceeded the maximum number of tracked tables
    if (static_cast<int32_t>(_counters[idx].size()) >= config::ms_rpc_max_tracked_tables_per_rpc) {
        LOG_EVERY_N(WARNING, 1000) << "Exceeded max tracked tables per RPC ("
                                   << config::ms_rpc_max_tracked_tables_per_rpc << ") for "
                                   << load_related_rpc_name(rpc_type);
        return nullptr;
    }

    auto counter = std::make_unique<TableRpcQpsCounter>(table_id, rpc_type);
    auto* ptr = counter.get();
    _counters[idx][table_id] = std::move(counter);
    return ptr;
}

std::vector<std::pair<int64_t, double>> TableRpcQpsRegistry::get_top_k_tables(LoadRelatedRpc rpc_type,
                                                                              int k) const {
    size_t idx = static_cast<size_t>(rpc_type);
    if (idx >= static_cast<size_t>(LoadRelatedRpc::COUNT) || k <= 0) {
        return {};
    }

    std::vector<std::pair<int64_t, double>> result;

    {
        std::shared_lock lock(_mutex);
        result.reserve(_counters[idx].size());
        for (const auto& [table_id, counter] : _counters[idx]) {
            double qps = counter->get_qps();
            if (qps > 0) {
                result.emplace_back(table_id, qps);
            }
        }
    }

    // Sort by QPS in descending order
    std::sort(result.begin(), result.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    // Keep only top-k
    if (static_cast<int>(result.size()) > k) {
        result.resize(k);
    }

    return result;
}

double TableRpcQpsRegistry::get_qps(LoadRelatedRpc rpc_type, int64_t table_id) const {
    size_t idx = static_cast<size_t>(rpc_type);
    if (idx >= static_cast<size_t>(LoadRelatedRpc::COUNT)) {
        return 0;
    }

    std::shared_lock lock(_mutex);
    auto it = _counters[idx].find(table_id);
    if (it != _counters[idx].end()) {
        return it->second->get_qps();
    }
    return 0;
}

void TableRpcQpsRegistry::cleanup_inactive_tables() {
    std::unique_lock lock(_mutex);

    for (size_t idx = 0; idx < static_cast<size_t>(LoadRelatedRpc::COUNT); ++idx) {
        auto& counter_map = _counters[idx];
        for (auto it = counter_map.begin(); it != counter_map.end();) {
            // Remove counters with zero QPS for a long time
            if (it->second->get_qps() < 0.01) {
                it = counter_map.erase(it);
            } else {
                ++it;
            }
        }
    }
}

// ============== TableRpcThrottler ==============

TableRpcThrottler::TableRpcThrottler() {
    // Initialize bvar for throttled table counts
    for (size_t i = 0; i < static_cast<size_t>(LoadRelatedRpc::COUNT); ++i) {
        std::string bvar_name = fmt::format("ms_rpc_backpressure_throttled_tables_{}",
                                            load_related_rpc_name(static_cast<LoadRelatedRpc>(i)));
        _throttled_table_counts[i] = std::make_unique<bvar::Status<size_t>>(bvar_name, 0);
    }
}

std::chrono::steady_clock::time_point TableRpcThrottler::throttle(LoadRelatedRpc rpc_type,
                                                                  int64_t table_id) {
    std::shared_lock lock(_mutex);
    auto it = _limiters.find({rpc_type, table_id});
    if (it == _limiters.end()) {
        return std::chrono::steady_clock::now();
    }
    return it->second->reserve();
}

void TableRpcThrottler::set_qps_limit(LoadRelatedRpc rpc_type, int64_t table_id, double qps_limit) {
    if (qps_limit <= 0) {
        return;
    }

    std::unique_lock lock(_mutex);
    auto key = std::make_pair(rpc_type, table_id);
    auto it = _limiters.find(key);
    if (it != _limiters.end()) {
        it->second->update_qps(qps_limit);
    } else {
        _limiters[key] = std::make_unique<StrictQpsLimiter>(qps_limit);
        // Update bvar count
        size_t idx = static_cast<size_t>(rpc_type);
        if (idx < static_cast<size_t>(LoadRelatedRpc::COUNT)) {
            size_t count = 0;
            for (const auto& [k, _] : _limiters) {
                if (k.first == rpc_type) {
                    ++count;
                }
            }
            _throttled_table_counts[idx]->set_value(count);
        }
    }

    LOG(INFO) << "Set table QPS limit: rpc=" << load_related_rpc_name(rpc_type)
              << ", table_id=" << table_id << ", qps_limit=" << qps_limit;
}

void TableRpcThrottler::remove_qps_limit(LoadRelatedRpc rpc_type, int64_t table_id) {
    std::unique_lock lock(_mutex);
    auto key = std::make_pair(rpc_type, table_id);
    auto it = _limiters.find(key);
    if (it != _limiters.end()) {
        _limiters.erase(it);
        // Update bvar count
        size_t idx = static_cast<size_t>(rpc_type);
        if (idx < static_cast<size_t>(LoadRelatedRpc::COUNT)) {
            size_t count = 0;
            for (const auto& [k, _] : _limiters) {
                if (k.first == rpc_type) {
                    ++count;
                }
            }
            _throttled_table_counts[idx]->set_value(count);
        }

        LOG(INFO) << "Removed table QPS limit: rpc=" << load_related_rpc_name(rpc_type)
                  << ", table_id=" << table_id;
    }
}

double TableRpcThrottler::get_qps_limit(LoadRelatedRpc rpc_type, int64_t table_id) const {
    std::shared_lock lock(_mutex);
    auto it = _limiters.find({rpc_type, table_id});
    if (it != _limiters.end()) {
        return it->second->get_qps();
    }
    return 0;
}

bool TableRpcThrottler::has_limit(LoadRelatedRpc rpc_type, int64_t table_id) const {
    std::shared_lock lock(_mutex);
    return _limiters.find({rpc_type, table_id}) != _limiters.end();
}

size_t TableRpcThrottler::get_throttled_table_count(LoadRelatedRpc rpc_type) const {
    size_t idx = static_cast<size_t>(rpc_type);
    if (idx >= static_cast<size_t>(LoadRelatedRpc::COUNT)) {
        return 0;
    }
    return _throttled_table_counts[idx]->get_value();
}

// ============== MSBackpressureHandler ==============

MSBackpressureHandler::MSBackpressureHandler(TableRpcQpsRegistry* qps_registry,
                                             TableRpcThrottler* throttler)
        : _qps_registry(qps_registry), _throttler(throttler) {
    _last_ms_busy_time = std::chrono::steady_clock::time_point::min();
    _last_upgrade_time = std::chrono::steady_clock::time_point::min();
}

bool MSBackpressureHandler::on_ms_busy() {
    g_ms_busy_count << 1;

    if (!config::enable_ms_backpressure_handling) {
        return false;
    }

    auto now = std::chrono::steady_clock::now();

    std::lock_guard lock(_mutex);
    _last_ms_busy_time = now;

    // Check if enough time has passed since the last upgrade
    auto elapsed_sec =
            std::chrono::duration_cast<std::chrono::seconds>(now - _last_upgrade_time).count();
    if (elapsed_sec < config::ms_backpressure_upgrade_interval_sec) {
        LOG(INFO) << "Received MS_BUSY but skipping upgrade (last upgrade was " << elapsed_sec
                  << "s ago, need " << config::ms_backpressure_upgrade_interval_sec << "s)";
        return false;
    }

    LOG(INFO) << "Received MS_BUSY, triggering throttle upgrade";
    upgrade_throttle();
    _last_upgrade_time = now;
    return true;
}

void MSBackpressureHandler::upgrade_throttle() {
    ThrottleUpgradeRecord record;
    record.timestamp_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                    .count();

    int top_k = config::ms_backpressure_upgrade_top_k;
    double ratio = config::ms_backpressure_throttle_ratio;
    double floor_qps = config::ms_rpc_table_qps_limit_floor;

    // For each load-related RPC type, find top-k tables and apply throttling
    for (size_t i = 0; i < static_cast<size_t>(LoadRelatedRpc::COUNT); ++i) {
        LoadRelatedRpc rpc_type = static_cast<LoadRelatedRpc>(i);
        auto top_tables = _qps_registry->get_top_k_tables(rpc_type, top_k);

        for (const auto& [table_id, current_qps] : top_tables) {
            double old_limit = _throttler->get_qps_limit(rpc_type, table_id);
            double new_limit;

            if (old_limit > 0) {
                // Already has a limit, reduce it further
                new_limit = old_limit * ratio;
            } else {
                // No limit yet, set based on current QPS
                new_limit = current_qps * ratio;
            }

            // Ensure we don't go below the floor
            new_limit = std::max(new_limit, floor_qps);

            // Only apply if it's actually limiting
            if (new_limit < current_qps || old_limit > 0) {
                _throttler->set_qps_limit(rpc_type, table_id, new_limit);
                record.changes[{rpc_type, table_id}] = {old_limit, new_limit};

                LOG(INFO) << "Throttle upgrade: rpc=" << load_related_rpc_name(rpc_type)
                          << ", table_id=" << table_id << ", current_qps=" << current_qps
                          << ", old_limit=" << old_limit << ", new_limit=" << new_limit;
            }
        }
    }

    if (!record.changes.empty()) {
        _upgrade_history.push_back(std::move(record));
        g_backpressure_upgrade_count << 1;
    }
}

void MSBackpressureHandler::try_downgrade() {
    if (!config::enable_ms_backpressure_handling) {
        return;
    }

    auto now = std::chrono::steady_clock::now();

    std::lock_guard lock(_mutex);

    // Check if enough time has passed since the last MS_BUSY
    auto elapsed_sec =
            std::chrono::duration_cast<std::chrono::seconds>(now - _last_ms_busy_time).count();
    if (elapsed_sec < config::ms_backpressure_downgrade_interval_sec) {
        return;
    }

    // No MS_BUSY for a while, try to downgrade
    if (!_upgrade_history.empty()) {
        LOG(INFO) << "No MS_BUSY for " << elapsed_sec << "s, triggering throttle downgrade";
        downgrade_throttle();
    }
}

void MSBackpressureHandler::downgrade_throttle() {
    if (_upgrade_history.empty()) {
        return;
    }

    // Undo the most recent upgrade
    const auto& record = _upgrade_history.back();

    for (const auto& [key, limits] : record.changes) {
        const auto& [rpc_type, table_id] = key;
        double old_limit = limits.first;

        if (old_limit > 0) {
            // Restore the previous limit
            _throttler->set_qps_limit(rpc_type, table_id, old_limit);
            LOG(INFO) << "Throttle downgrade: rpc=" << load_related_rpc_name(rpc_type)
                      << ", table_id=" << table_id << ", restored_limit=" << old_limit;
        } else {
            // No previous limit, remove it entirely
            _throttler->remove_qps_limit(rpc_type, table_id);
            LOG(INFO) << "Throttle downgrade: rpc=" << load_related_rpc_name(rpc_type)
                      << ", table_id=" << table_id << ", removed limit";
        }
    }

    _upgrade_history.pop_back();
    g_backpressure_downgrade_count << 1;

    // Reset last_ms_busy_time to allow future downgrades to happen sooner
    // (if no new MS_BUSY is received)
    _last_ms_busy_time = std::chrono::steady_clock::now();
}

std::chrono::steady_clock::time_point MSBackpressureHandler::before_rpc(LoadRelatedRpc rpc_type,
                                                                        int64_t table_id) {
    if (!config::enable_ms_backpressure_handling) {
        return std::chrono::steady_clock::now();
    }

    return _throttler->throttle(rpc_type, table_id);
}

void MSBackpressureHandler::after_rpc(LoadRelatedRpc rpc_type, int64_t table_id) {
    if (!config::enable_ms_backpressure_handling) {
        return;
    }

    _qps_registry->record(rpc_type, table_id);
}

std::chrono::steady_clock::time_point MSBackpressureHandler::last_ms_busy_time() const {
    std::lock_guard lock(_mutex);
    return _last_ms_busy_time;
}

std::chrono::steady_clock::time_point MSBackpressureHandler::last_upgrade_time() const {
    std::lock_guard lock(_mutex);
    return _last_upgrade_time;
}

int64_t MSBackpressureHandler::seconds_since_last_ms_busy() const {
    std::lock_guard lock(_mutex);
    if (_last_ms_busy_time == std::chrono::steady_clock::time_point::min()) {
        return -1; // Never received MS_BUSY
    }
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() -
                                                            _last_ms_busy_time)
            .count();
}

} // namespace doris::cloud
