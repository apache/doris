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

#include <chrono>
#include <map>
#include <mutex>
#include <utility>
#include <vector>

#include "cloud/cloud_tablet_rpc_throttler.h"

namespace doris::cloud {

// ============== Data Structures ==============

// QPS snapshot: the current QPS of a table on a specific RPC type
struct RpcQpsSnapshot {
    LoadRelatedRpc rpc_type;
    int64_t table_id;
    double current_qps;
};

// Throttle action: describes what action should be taken
struct RpcThrottleAction {
    enum class Type { SET_LIMIT, REMOVE_LIMIT };

    Type type;
    LoadRelatedRpc rpc_type;
    int64_t table_id;
    double qps_limit;  // only meaningful for SET_LIMIT
};

// ============== ThrottleStateMachine ==============

// Parameters for throttle state machine
struct RpcThrottleParams {
    int top_k = 3;            // Number of top tables to throttle on each upgrade
    double ratio = 0.5;       // Decay ratio for throttle upgrade
    double floor_qps = 1.0;   // Floor value for table-level QPS limit

    bool operator==(const RpcThrottleParams& other) const {
        return top_k == other.top_k && ratio == other.ratio && floor_qps == other.floor_qps;
    }
};

// Pure state machine for throttle upgrade/downgrade decisions
// - No time awareness: caller drives events via on_upgrade/on_downgrade
// - No config dependency: all parameters passed via constructor/update_params
// - No side effects: only returns action descriptions, doesn't touch throttler
// - Deterministically testable: same event sequence -> same output
class RpcThrottleStateMachine {
public:
    explicit RpcThrottleStateMachine(RpcThrottleParams params);

    // Runtime update parameters, takes effect on next on_upgrade
    // Note: existing upgrade history is NOT recalculated
    void update_params(RpcThrottleParams params);

    // Process a throttle upgrade event
    // qps_snapshot: current QPS snapshot for each (rpc, table), provided by caller
    // Returns: list of actions to execute
    std::vector<RpcThrottleAction> on_upgrade(const std::vector<RpcQpsSnapshot>& qps_snapshot);

    // Process a throttle downgrade event (undo the most recent upgrade)
    // Returns: list of actions to execute
    std::vector<RpcThrottleAction> on_downgrade();

    // Query current state
    size_t upgrade_level() const;  // Current upgrade level
    double get_current_limit(LoadRelatedRpc rpc_type, int64_t table_id) const;  // 0 = no limit
    RpcThrottleParams get_params() const;

private:
    mutable std::mutex _mtx;

    RpcThrottleParams _params;

    // Upgrade history for downgrade rollback
    // changes: (rpc_type, table_id) -> (old_limit, new_limit)
    struct UpgradeRecord {
        std::map<std::pair<LoadRelatedRpc, int64_t>, std::pair<double, double>> changes;
    };
    std::vector<UpgradeRecord> _upgrade_history;

    // Current active limits for all (rpc, table)
    std::map<std::pair<LoadRelatedRpc, int64_t>, double> _current_limits;
};

// ============== ThrottleCoordinator ==============

// Coordinator parameters
struct ThrottleCoordinatorParams {
    // Minimum ticks between upgrades
    int upgrade_cooldown_ticks = 10;
    // Ticks after last MS_BUSY to trigger downgrade
    int downgrade_after_ticks = 60;

    bool operator==(const ThrottleCoordinatorParams& other) const {
        return upgrade_cooldown_ticks == other.upgrade_cooldown_ticks &&
               downgrade_after_ticks == other.downgrade_after_ticks;
    }
};

// Pure timing control for upgrade/downgrade triggers
// - No time awareness: based on tick count, driven by caller
// - No config dependency: all parameters passed via constructor/update_params
//
// Tick to time ratio:
// - 1 tick = 1 second is the default interpretation
// - The caller can decide the actual time interval between ticks
// - For example, if calling tick() every 100ms, then 10 ticks = 1 second
//   and params should be adjusted accordingly
class RpcThrottleCoordinator {
public:
    explicit RpcThrottleCoordinator(ThrottleCoordinatorParams params);

    // Runtime update parameters, takes effect on subsequent report_ms_busy/tick calls
    // Note: existing tick counts are NOT reset
    void update_params(ThrottleCoordinatorParams params);

    // Report a MS_BUSY event
    // Returns true if upgrade should be triggered
    bool report_ms_busy();

    // Advance one tick (caller decides actual time between ticks)
    // Returns true if downgrade should be triggered
    bool tick();

    // Tell coordinator whether there are pending upgrades that can be downgraded
    // Called by the state machine consumer after upgrade/downgrade
    void set_has_pending_upgrades(bool has);

    // Query state
    int ticks_since_last_ms_busy() const;
    int ticks_since_last_upgrade() const;
    ThrottleCoordinatorParams get_params() const;

private:
    mutable std::mutex _mtx;

    ThrottleCoordinatorParams _params;
    int _ticks_since_last_ms_busy = -1;  // -1 means never received
    int _ticks_since_last_upgrade = -1;  // -1 means never upgraded
    bool _has_pending_upgrades = false;  // Whether there are upgrade records to downgrade
};

} // namespace doris::cloud
