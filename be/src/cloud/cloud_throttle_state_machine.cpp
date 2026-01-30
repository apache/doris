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

#include "cloud/cloud_throttle_state_machine.h"

#include <glog/logging.h>

#include <algorithm>

namespace doris::cloud {

// ============== ThrottleStateMachine ==============

ThrottleStateMachine::ThrottleStateMachine(ThrottleParams params) : _params(params) {
    LOG(INFO) << "ThrottleStateMachine initialized: top_k=" << params.top_k
              << ", ratio=" << params.ratio << ", floor_qps=" << params.floor_qps;
}

void ThrottleStateMachine::update_params(ThrottleParams params) {
    std::lock_guard lock(_mtx);
    _params = params;
    LOG(INFO) << "ThrottleStateMachine params updated: top_k=" << params.top_k
              << ", ratio=" << params.ratio << ", floor_qps=" << params.floor_qps;
}

std::vector<ThrottleAction> ThrottleStateMachine::on_upgrade(
        const std::vector<QpsSnapshot>& qps_snapshot) {
    std::lock_guard lock(_mtx);

    ThrottleUpgradeRecord record;
    std::vector<ThrottleAction> actions;

    int top_k = _params.top_k;
    double ratio = _params.ratio;
    double floor_qps = _params.floor_qps;

    // Group snapshot by rpc_type
    std::map<LoadRelatedRpc, std::vector<QpsSnapshot>> snapshot_by_rpc;
    for (const auto& snapshot : qps_snapshot) {
        snapshot_by_rpc[snapshot.rpc_type].push_back(snapshot);
    }

    // For each RPC type, find top-k tables by QPS and apply throttling
    for (size_t i = 0; i < static_cast<size_t>(LoadRelatedRpc::COUNT); ++i) {
        LoadRelatedRpc rpc_type = static_cast<LoadRelatedRpc>(i);

        auto it = snapshot_by_rpc.find(rpc_type);
        if (it == snapshot_by_rpc.end()) {
            continue;
        }

        auto& snapshots = it->second;

        // Sort by QPS in descending order
        std::sort(snapshots.begin(), snapshots.end(),
                  [](const QpsSnapshot& a, const QpsSnapshot& b) {
                      return a.current_qps > b.current_qps;
                  });

        // Take top-k
        int k = std::min(top_k, static_cast<int>(snapshots.size()));
        for (int j = 0; j < k; ++j) {
            const auto& snapshot = snapshots[j];
            auto key = std::make_pair(rpc_type, snapshot.table_id);

            double old_limit = 0.0;
            auto limit_it = _current_limits.find(key);
            if (limit_it != _current_limits.end()) {
                old_limit = limit_it->second;
            }

            double new_limit;
            if (old_limit > 0) {
                // Already has a limit, reduce it further
                new_limit = old_limit * ratio;
            } else {
                // No limit yet, set based on current QPS
                new_limit = snapshot.current_qps * ratio;
            }

            // Apply floor
            new_limit = std::max(new_limit, floor_qps);

            // Only apply if it's actually limiting
            if (new_limit < snapshot.current_qps || old_limit > 0) {
                ThrottleAction action;
                action.type = ThrottleAction::Type::SET_LIMIT;
                action.rpc_type = rpc_type;
                action.table_id = snapshot.table_id;
                action.qps_limit = new_limit;

                actions.push_back(action);
                record.changes[key] = {old_limit, new_limit};
                _current_limits[key] = new_limit;

                LOG(INFO) << "Throttle upgrade: rpc=" << load_related_rpc_name(rpc_type)
                          << ", table_id=" << snapshot.table_id
                          << ", current_qps=" << snapshot.current_qps
                          << ", old_limit=" << old_limit << ", new_limit=" << new_limit;
            }
        }
    }

    if (!record.changes.empty()) {
        _upgrade_history.push_back(std::move(record));
    }

    return actions;
}

std::vector<ThrottleAction> ThrottleStateMachine::on_downgrade() {
    std::lock_guard lock(_mtx);

    std::vector<ThrottleAction> actions;

    if (_upgrade_history.empty()) {
        return actions;
    }

    // Undo the most recent upgrade
    const auto& record = _upgrade_history.back();

    for (const auto& [key, limits] : record.changes) {
        const auto& [rpc_type, table_id] = key;
        double old_limit = limits.first;

        if (old_limit > 0) {
            // Restore the previous limit
            ThrottleAction action;
            action.type = ThrottleAction::Type::SET_LIMIT;
            action.rpc_type = rpc_type;
            action.table_id = table_id;
            action.qps_limit = old_limit;

            actions.push_back(action);
            _current_limits[key] = old_limit;

            LOG(INFO) << "Throttle downgrade: rpc=" << load_related_rpc_name(rpc_type)
                      << ", table_id=" << table_id << ", restored_limit=" << old_limit;
        } else {
            // No previous limit, remove it entirely
            ThrottleAction action;
            action.type = ThrottleAction::Type::REMOVE_LIMIT;
            action.rpc_type = rpc_type;
            action.table_id = table_id;

            actions.push_back(action);
            _current_limits.erase(key);

            LOG(INFO) << "Throttle downgrade: rpc=" << load_related_rpc_name(rpc_type)
                      << ", table_id=" << table_id << ", removed limit";
        }
    }

    _upgrade_history.pop_back();

    return actions;
}

size_t ThrottleStateMachine::upgrade_level() const {
    std::lock_guard lock(_mtx);
    return _upgrade_history.size();
}

double ThrottleStateMachine::get_current_limit(LoadRelatedRpc rpc_type, int64_t table_id) const {
    std::lock_guard lock(_mtx);
    auto it = _current_limits.find({rpc_type, table_id});
    if (it != _current_limits.end()) {
        return it->second;
    }
    return 0.0;
}

ThrottleParams ThrottleStateMachine::get_params() const {
    std::lock_guard lock(_mtx);
    return _params;
}

// ============== UpgradeDowngradeCoordinator ==============

UpgradeDowngradeCoordinator::UpgradeDowngradeCoordinator(CoordinatorParams params)
        : _params(params) {
    LOG(INFO) << "UpgradeDowngradeCoordinator initialized: upgrade_cooldown_ticks="
              << params.upgrade_cooldown_ticks
              << ", downgrade_after_ticks=" << params.downgrade_after_ticks;
}

void UpgradeDowngradeCoordinator::update_params(CoordinatorParams params) {
    std::lock_guard lock(_mtx);
    _params = params;
    LOG(INFO) << "UpgradeDowngradeCoordinator params updated: upgrade_cooldown_ticks="
              << params.upgrade_cooldown_ticks
              << ", downgrade_after_ticks=" << params.downgrade_after_ticks;
}

bool UpgradeDowngradeCoordinator::report_ms_busy() {
    std::lock_guard lock(_mtx);

    // Reset tick counter since last MS_BUSY
    _ticks_since_last_ms_busy = 0;

    // Check if cooldown has passed
    if (_ticks_since_last_upgrade == -1 ||
        _ticks_since_last_upgrade >= _params.upgrade_cooldown_ticks) {
        // Reset upgrade counter
        _ticks_since_last_upgrade = 0;
        _has_pending_upgrades = true;

        LOG(INFO) << "Upgrade triggered: ticks_since_last_upgrade=" << _ticks_since_last_upgrade
                  << ", cooldown=" << _params.upgrade_cooldown_ticks;
        return true;  // Should trigger upgrade
    }

    LOG(INFO) << "Upgrade skipped (cooling down): ticks_since_last_upgrade="
              << _ticks_since_last_upgrade << ", cooldown=" << _params.upgrade_cooldown_ticks;
    return false;  // Cooling down
}

bool UpgradeDowngradeCoordinator::tick() {
    std::lock_guard lock(_mtx);

    // Increment tick counters
    if (_ticks_since_last_ms_busy >= 0) {
        ++_ticks_since_last_ms_busy;
    }
    if (_ticks_since_last_upgrade >= 0) {
        ++_ticks_since_last_upgrade;
    }

    // Check if downgrade should be triggered
    if (_has_pending_upgrades &&
        _ticks_since_last_ms_busy >= _params.downgrade_after_ticks) {
        // Reset for next downgrade cycle
        _ticks_since_last_ms_busy = 0;

        LOG(INFO) << "Downgrade triggered: ticks_since_last_ms_busy="
                  << (_ticks_since_last_ms_busy + _params.downgrade_after_ticks)
                  << ", threshold=" << _params.downgrade_after_ticks;
        return true;  // Should trigger downgrade
    }

    return false;
}

void UpgradeDowngradeCoordinator::set_has_pending_upgrades(bool has) {
    std::lock_guard lock(_mtx);
    _has_pending_upgrades = has;
}

int UpgradeDowngradeCoordinator::ticks_since_last_ms_busy() const {
    std::lock_guard lock(_mtx);
    return _ticks_since_last_ms_busy;
}

int UpgradeDowngradeCoordinator::ticks_since_last_upgrade() const {
    std::lock_guard lock(_mtx);
    return _ticks_since_last_upgrade;
}

CoordinatorParams UpgradeDowngradeCoordinator::get_params() const {
    std::lock_guard lock(_mtx);
    return _params;
}

} // namespace doris::cloud
