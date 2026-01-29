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

#include <bvar/bvar.h>

#include <array>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace doris::cloud {

// Load-related RPC types that need table-level QPS statistics
enum class LoadRelatedRpc : size_t {
    PREPARE_ROWSET,
    COMMIT_ROWSET,
    UPDATE_TMP_ROWSET,
    UPDATE_PACKED_FILE_INFO,
    UPDATE_DELETE_BITMAP,
    COUNT
};

// Get the name string for a LoadRelatedRpc type
std::string_view load_related_rpc_name(LoadRelatedRpc rpc);

// Strict QPS limiter that doesn't allow burst
// Unlike token bucket, it strictly enforces fixed intervals between requests
class StrictQpsLimiter {
public:
    using Clock = std::chrono::steady_clock;

    explicit StrictQpsLimiter(double qps);

    // Returns the time point when the request is allowed to execute
    // Caller should sleep until this time point
    Clock::time_point reserve();

    // Dynamically update the QPS limit
    void update_qps(double new_qps);

    // Get current QPS limit
    double get_qps() const;

private:
    mutable std::mutex _mtx;
    int64_t _interval_ns;
    Clock::time_point _next_allowed_time;
};

// QPS counter for a single (table, RPC type) pair using bvar
class TableRpcQpsCounter {
public:
    TableRpcQpsCounter(int64_t table_id, LoadRelatedRpc rpc_type);
    ~TableRpcQpsCounter() = default;

    // Record one RPC call
    void increment();

    // Get current QPS (average over the past 1 second)
    double get_qps() const;

    int64_t table_id() const { return _table_id; }
    LoadRelatedRpc rpc_type() const { return _rpc_type; }

private:
    int64_t _table_id;
    LoadRelatedRpc _rpc_type;

    // bvar name: "ms_rpc_table_qps_{rpc_name}_{table_id}"
    std::unique_ptr<bvar::Adder<int64_t>> _counter;
    std::unique_ptr<bvar::PerSecond<bvar::Adder<int64_t>>> _qps;
};

// Registry managing QPS counters for all tables
class TableRpcQpsRegistry {
public:
    TableRpcQpsRegistry();
    ~TableRpcQpsRegistry() = default;

    // Record one RPC call for the given table
    void record(LoadRelatedRpc rpc_type, int64_t table_id);

    // Get the top-k tables with highest QPS for the given RPC type
    // Returns: [(table_id, qps), ...] sorted by qps in descending order
    std::vector<std::pair<int64_t, double>> get_top_k_tables(LoadRelatedRpc rpc_type, int k) const;

    // Get QPS for a specific table on a specific RPC type
    double get_qps(LoadRelatedRpc rpc_type, int64_t table_id) const;

    // Clean up counters for tables that have been inactive for a long time
    void cleanup_inactive_tables();

private:
    // Get or create counter for (rpc_type, table_id)
    TableRpcQpsCounter* get_or_create_counter(LoadRelatedRpc rpc_type, int64_t table_id);

    mutable std::shared_mutex _mutex;

    // rpc_type -> (table_id -> counter)
    std::array<std::unordered_map<int64_t, std::unique_ptr<TableRpcQpsCounter>>,
               static_cast<size_t>(LoadRelatedRpc::COUNT)>
            _counters;
};

// Table-level throttler managing StrictQpsLimiter for each (RPC type, table) pair
class TableRpcThrottler {
public:
    TableRpcThrottler();
    ~TableRpcThrottler() = default;

    // Called before RPC execution, returns the time point when execution is allowed
    // Returns now if no limit is set
    std::chrono::steady_clock::time_point throttle(LoadRelatedRpc rpc_type, int64_t table_id);

    // Set or update the QPS limit for a table
    void set_qps_limit(LoadRelatedRpc rpc_type, int64_t table_id, double qps_limit);

    // Remove the QPS limit for a table
    void remove_qps_limit(LoadRelatedRpc rpc_type, int64_t table_id);

    // Get current QPS limit (returns 0 if not set)
    double get_qps_limit(LoadRelatedRpc rpc_type, int64_t table_id) const;

    // Check if a limit exists for the given (rpc_type, table_id)
    bool has_limit(LoadRelatedRpc rpc_type, int64_t table_id) const;

    // Get the number of throttled tables for a given RPC type
    size_t get_throttled_table_count(LoadRelatedRpc rpc_type) const;

private:
    mutable std::shared_mutex _mutex;
    // (rpc_type, table_id) -> StrictQpsLimiter
    std::map<std::pair<LoadRelatedRpc, int64_t>, std::unique_ptr<StrictQpsLimiter>> _limiters;

    // bvar: current throttled table count per RPC type
    std::array<std::unique_ptr<bvar::Status<size_t>>, static_cast<size_t>(LoadRelatedRpc::COUNT)>
            _throttled_table_counts;
};

// Record of a throttle upgrade operation, used for downgrade rollback
struct ThrottleUpgradeRecord {
    int64_t timestamp_ms;
    // (rpc_type, table_id) -> (old_qps_limit, new_qps_limit)
    // old_qps_limit = 0 means no previous limit
    std::map<std::pair<LoadRelatedRpc, int64_t>, std::pair<double, double>> changes;
};

// MS backpressure handler that coordinates QPS statistics, throttle upgrade and downgrade
class MSBackpressureHandler {
public:
    MSBackpressureHandler(TableRpcQpsRegistry* qps_registry, TableRpcThrottler* throttler);
    ~MSBackpressureHandler() = default;

    // Called when receiving MS_BUSY response
    // Returns true if throttle upgrade was triggered
    bool on_ms_busy();

    // Called periodically to check if throttle should be downgraded
    void try_downgrade();

    // Called before RPC execution, performs throttle wait
    // Returns the time point to wait until
    std::chrono::steady_clock::time_point before_rpc(LoadRelatedRpc rpc_type, int64_t table_id);

    // Called after RPC execution, records QPS statistics
    void after_rpc(LoadRelatedRpc rpc_type, int64_t table_id);

    // Get the time of the last MS_BUSY response
    std::chrono::steady_clock::time_point last_ms_busy_time() const;

    // Get the time of the last throttle upgrade
    std::chrono::steady_clock::time_point last_upgrade_time() const;

    // Get seconds since last MS_BUSY
    int64_t seconds_since_last_ms_busy() const;

private:
    // Perform throttle upgrade
    void upgrade_throttle();

    // Perform throttle downgrade (undo the most recent upgrade)
    void downgrade_throttle();

    TableRpcQpsRegistry* _qps_registry;
    TableRpcThrottler* _throttler;

    mutable std::mutex _mutex;
    std::chrono::steady_clock::time_point _last_ms_busy_time;
    std::chrono::steady_clock::time_point _last_upgrade_time;

    // Upgrade history for downgrade rollback
    std::vector<ThrottleUpgradeRecord> _upgrade_history;
};

// Global bvar metrics for backpressure handling
extern bvar::Adder<uint64_t> g_backpressure_upgrade_count;
extern bvar::Window<bvar::Adder<uint64_t>> g_backpressure_upgrade_qpm;
extern bvar::Adder<uint64_t> g_backpressure_downgrade_count;
extern bvar::Window<bvar::Adder<uint64_t>> g_backpressure_downgrade_qpm;
extern bvar::LatencyRecorder g_table_throttle_wait_us;
extern bvar::Adder<uint64_t> g_ms_busy_count;
extern bvar::Window<bvar::Adder<uint64_t>> g_ms_busy_qpm;

} // namespace doris::cloud
