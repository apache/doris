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

#include "cloud/cloud_ms_backpressure_handler.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <queue>
#include <thread>

#include "cloud/config.h"
#include "common/status.h"
#include "util/thread.h"

namespace doris::cloud {

// Global bvar metrics
bvar::Adder<uint64_t> g_backpressure_upgrade_count("ms_rpc_backpressure_upgrade_count");
bvar::Window<bvar::Adder<uint64_t>> g_backpressure_upgrade_60s("ms_rpc_backpressure_upgrade_60s",
                                                               &g_backpressure_upgrade_count, 60);
bvar::Adder<uint64_t> g_backpressure_downgrade_count("ms_rpc_backpressure_downgrade_count");
bvar::Window<bvar::Adder<uint64_t>> g_backpressure_downgrade_60s(
        "ms_rpc_backpressure_downgrade_60s", &g_backpressure_downgrade_count, 60);
bvar::LatencyRecorder g_throttle_wait_prepare_rowset(
        "ms_rpc_backpressure_throttle_wait_prepare_rowset");
bvar::LatencyRecorder g_throttle_wait_commit_rowset(
        "ms_rpc_backpressure_throttle_wait_commit_rowset");
bvar::LatencyRecorder g_throttle_wait_update_tmp_rowset(
        "ms_rpc_backpressure_throttle_wait_update_tmp_rowset");
bvar::LatencyRecorder g_throttle_wait_update_packed_file_info(
        "ms_rpc_backpressure_throttle_wait_update_packed_file_info");
bvar::LatencyRecorder g_throttle_wait_update_delete_bitmap(
        "ms_rpc_backpressure_throttle_wait_update_delete_bitmap");
bvar::Adder<uint64_t> g_ms_busy_count("ms_rpc_backpressure_ms_busy_count");
bvar::Window<bvar::Adder<uint64_t>> g_ms_busy_60s("ms_rpc_backpressure_ms_busy_60s",
                                                  &g_ms_busy_count, 60);

static bvar::LatencyRecorder* s_throttle_wait_recorders[] = {
        &g_throttle_wait_prepare_rowset,       &g_throttle_wait_commit_rowset,
        &g_throttle_wait_update_tmp_rowset,    &g_throttle_wait_update_packed_file_info,
        &g_throttle_wait_update_delete_bitmap,
};

bvar::LatencyRecorder* get_throttle_wait_recorder(LoadRelatedRpc rpc) {
    size_t idx = static_cast<size_t>(rpc);
    if (idx >= static_cast<size_t>(LoadRelatedRpc::COUNT)) {
        return nullptr;
    }
    return s_throttle_wait_recorders[idx];
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

TableRpcQpsCounter::TableRpcQpsCounter(int64_t table_id, LoadRelatedRpc rpc_type, int window_sec)
        : _table_id(table_id), _rpc_type(rpc_type) {
    _counter = std::make_unique<bvar::Adder<int64_t>>();
    _counter->hide();
    _qps = std::make_unique<bvar::PerSecond<bvar::Adder<int64_t>>>(_counter.get(), window_sec);
    _qps->hide();
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

    auto counter = std::make_unique<TableRpcQpsCounter>(table_id, rpc_type,
                                                        config::ms_rpc_table_qps_window_sec);
    auto* ptr = counter.get();
    _counters[idx][table_id] = std::move(counter);
    return ptr;
}

std::vector<std::pair<int64_t, double>> TableRpcQpsRegistry::get_top_k_tables(
        LoadRelatedRpc rpc_type, int k) const {
    size_t idx = static_cast<size_t>(rpc_type);
    if (idx >= static_cast<size_t>(LoadRelatedRpc::COUNT) || k <= 0) {
        return {};
    }

    // Use a min-heap of size k to find top-k without allocating a vector for all tables.
    // The heap top is the smallest among the k largest elements seen so far.
    using Entry = std::pair<int64_t, double>; // (table_id, qps)
    auto min_cmp = [](const Entry& a, const Entry& b) { return a.second > b.second; };
    std::priority_queue<Entry, std::vector<Entry>, decltype(min_cmp)> min_heap(min_cmp);

    {
        std::shared_lock lock(_mutex);
        for (const auto& [table_id, counter] : _counters[idx]) {
            double qps = counter->get_qps();
            if (qps > 0) {
                if (static_cast<int>(min_heap.size()) < k) {
                    min_heap.push({table_id, qps});
                } else if (qps > min_heap.top().second) {
                    min_heap.pop();
                    min_heap.push({table_id, qps});
                }
            }
        }
    }

    // Extract results from heap (comes out in ascending order, reverse to descending)
    std::vector<Entry> result;
    result.reserve(min_heap.size());
    while (!min_heap.empty()) {
        result.push_back(min_heap.top());
        min_heap.pop();
    }
    std::reverse(result.begin(), result.end());

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

    LOG(INFO) << "[ms-throttle] set table QPS limit: rpc=" << load_related_rpc_name(rpc_type)
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

        LOG(INFO) << "[ms-throttle] removed table QPS limit: rpc="
                  << load_related_rpc_name(rpc_type) << ", table_id=" << table_id;
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

std::vector<TableRpcThrottler::ThrottleEntry> TableRpcThrottler::get_all_throttled_entries() const {
    std::shared_lock lock(_mutex);
    std::vector<ThrottleEntry> entries;
    entries.reserve(_limiters.size());
    for (const auto& [key, limiter] : _limiters) {
        entries.push_back({key.first, key.second, limiter->get_qps()});
    }
    return entries;
}

// ============== MSBackpressureHandler ==============

MSBackpressureHandler::MSBackpressureHandler(TableRpcQpsRegistry* qps_registry,
                                             TableRpcThrottler* throttler)
        : _qps_registry(qps_registry),
          _throttler(throttler),
          _stop_latch(1),
          _last_ms_busy_time(std::chrono::steady_clock::time_point::min()) {
    // Initialize state machine with config values
    RpcThrottleParams throttle_params {
            .top_k = config::ms_backpressure_upgrade_top_k,
            .ratio = config::ms_backpressure_throttle_ratio,
            .floor_qps = config::ms_rpc_table_qps_limit_floor,
    };
    _state_machine = std::make_unique<RpcThrottleStateMachine>(throttle_params);

    // Initialize coordinator with config values
    // Coordinator uses ticks where 1 tick = 1 millisecond (fixed unit)
    // This allows tick_interval_ms to change at runtime without affecting correctness
    ThrottleCoordinatorParams coordinator_params {
            .upgrade_cooldown_ticks = config::ms_backpressure_upgrade_interval_ms,
            .downgrade_after_ticks = config::ms_backpressure_downgrade_interval_ms,
    };
    _coordinator = std::make_unique<RpcThrottleCoordinator>(coordinator_params);

    auto st = Thread::create(
            "MSBackpressureHandler", "tick_thread", [this]() { this->_tick_thread_callback(); },
            &_tick_thread);
    if (!st.ok()) {
        LOG(WARNING) << "[ms-throttle] failed to create tick thread: " << st;
    } else {
        LOG(INFO) << "[ms-throttle] handler started: upgrade_cooldown="
                  << config::ms_backpressure_upgrade_interval_ms
                  << "ms, downgrade_interval=" << config::ms_backpressure_downgrade_interval_ms
                  << "ms";
    }
}

MSBackpressureHandler::~MSBackpressureHandler() {
    _stop_latch.count_down();
    if (_tick_thread) {
        _tick_thread->join();
    }
}

void MSBackpressureHandler::_tick_thread_callback() {
    // Fixed tick interval: 1 second. Since 1 tick = 1 ms, advance by 1000 ticks each iteration.
    constexpr int kTickIntervalMs = 1000;
    while (!_stop_latch.wait_for(std::chrono::milliseconds(kTickIntervalMs))) {
        _advance_time(kTickIntervalMs);
    }
}

void MSBackpressureHandler::_advance_time(int ticks) {
    if (!config::enable_ms_backpressure_handling) {
        return;
    }

    // Advance coordinator time; if downgrade is triggered, handle it
    if (_coordinator->tick(ticks)) {
        LOG(INFO) << "[ms-throttle] triggering downgrade, upgrade_level="
                  << _state_machine->upgrade_level();

        auto actions = _state_machine->on_downgrade();
        _apply_actions(actions);
        _coordinator->set_has_pending_upgrades(_state_machine->upgrade_level() > 0);

        g_backpressure_downgrade_count << 1;
    }
}

bool MSBackpressureHandler::on_ms_busy() {
    g_ms_busy_count << 1;

    if (!config::enable_ms_backpressure_handling) {
        return false;
    }

    {
        std::lock_guard lock(_mutex);
        _last_ms_busy_time = std::chrono::steady_clock::now();
    }

    // Check with coordinator if upgrade should be triggered
    if (!_coordinator->report_ms_busy()) {
        return false;
    }

    LOG(INFO) << "[ms-throttle] received MS_BUSY, triggering upgrade";

    auto snapshot = _build_qps_snapshot();
    auto actions = _state_machine->on_upgrade(snapshot);
    _apply_actions(actions);
    _coordinator->set_has_pending_upgrades(_state_machine->upgrade_level() > 0);

    g_backpressure_upgrade_count << 1;
    return true;
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

void MSBackpressureHandler::update_throttle_params(RpcThrottleParams params) {
    _state_machine->update_params(params);
}

void MSBackpressureHandler::update_coordinator_params(ThrottleCoordinatorParams params) {
    _coordinator->update_params(params);
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

size_t MSBackpressureHandler::upgrade_level() const {
    return _state_machine->upgrade_level();
}

int MSBackpressureHandler::ticks_since_last_ms_busy() const {
    return _coordinator->ticks_since_last_ms_busy();
}

int MSBackpressureHandler::ticks_since_last_upgrade() const {
    return _coordinator->ticks_since_last_upgrade();
}

void MSBackpressureHandler::_apply_actions(const std::vector<RpcThrottleAction>& actions) {
    for (const auto& action : actions) {
        switch (action.type) {
        case RpcThrottleAction::Type::SET_LIMIT:
            _throttler->set_qps_limit(action.rpc_type, action.table_id, action.qps_limit);
            break;
        case RpcThrottleAction::Type::REMOVE_LIMIT:
            _throttler->remove_qps_limit(action.rpc_type, action.table_id);
            break;
        }
    }
}

std::vector<RpcQpsSnapshot> MSBackpressureHandler::_build_qps_snapshot() const {
    std::vector<RpcQpsSnapshot> snapshot;

    // For each RPC type, get top-k tables
    int top_k = _state_machine->get_params().top_k;

    for (size_t i = 0; i < static_cast<size_t>(LoadRelatedRpc::COUNT); ++i) {
        LoadRelatedRpc rpc_type = static_cast<LoadRelatedRpc>(i);
        auto top_tables = _qps_registry->get_top_k_tables(rpc_type, top_k);

        for (const auto& [table_id, qps] : top_tables) {
            snapshot.push_back({rpc_type, table_id, qps});
        }
    }

    return snapshot;
}

} // namespace doris::cloud
