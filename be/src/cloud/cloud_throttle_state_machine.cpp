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

// ============== RpcThrottleStateMachine ==============

RpcThrottleStateMachine::RpcThrottleStateMachine(RpcThrottleParams params) : _params(params) {
    LOG(INFO) << "[ms-throttle] state machine initialized: top_k=" << params.top_k
              << ", ratio=" << params.ratio << ", floor_qps=" << params.floor_qps;
}

void RpcThrottleStateMachine::update_params(RpcThrottleParams params) {
    std::lock_guard lock(_mtx);
    _params = params;
    LOG(INFO) << "[ms-throttle] state machine params updated: top_k=" << params.top_k
              << ", ratio=" << params.ratio << ", floor_qps=" << params.floor_qps;
}

std::vector<RpcThrottleAction> RpcThrottleStateMachine::on_upgrade(
        const std::vector<RpcQpsSnapshot>& qps_snapshot) {
    std::lock_guard lock(_mtx);

    UpgradeRecord record;
    std::vector<RpcThrottleAction> actions;

    double ratio = _params.ratio;
    double floor_qps = _params.floor_qps;

    // Caller is responsible for providing top-k snapshot per RPC type.
    // State machine simply applies throttling to every entry in the snapshot.
    for (const auto& snapshot : qps_snapshot) {
        auto key = std::make_pair(snapshot.rpc_type, snapshot.table_id);

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
            RpcThrottleAction action {
                    .type = RpcThrottleAction::Type::SET_LIMIT,
                    .rpc_type = snapshot.rpc_type,
                    .table_id = snapshot.table_id,
                    .qps_limit = new_limit,
            };
            actions.push_back(action);
            record.changes[key] = {old_limit, new_limit};
            _current_limits[key] = new_limit;

            LOG(INFO) << "[ms-throttle] upgrade: rpc=" << load_related_rpc_name(snapshot.rpc_type)
                      << ", table_id=" << snapshot.table_id
                      << ", current_qps=" << snapshot.current_qps << ", old_limit=" << old_limit
                      << ", new_limit=" << new_limit;
        }
    }

    if (!record.changes.empty()) {
        _upgrade_history.push_back(std::move(record));
    }

    LOG(INFO) << "[ms-throttle] on_upgrade done: actions=" << actions.size()
              << ", upgrade_level=" << _upgrade_history.size()
              << ", snapshot_size=" << qps_snapshot.size();

    return actions;
}

std::vector<RpcThrottleAction> RpcThrottleStateMachine::on_downgrade() {
    std::lock_guard lock(_mtx);

    std::vector<RpcThrottleAction> actions;

    if (_upgrade_history.empty()) {
        LOG(INFO) << "[ms-throttle] on_downgrade skipped: no upgrade history";
        return actions;
    }

    // Undo the most recent upgrade
    const auto& record = _upgrade_history.back();

    for (const auto& [key, limits] : record.changes) {
        const auto& [rpc_type, table_id] = key;
        double old_limit = limits.first;

        if (old_limit > 0) {
            // Restore the previous limit
            RpcThrottleAction action {
                    .type = RpcThrottleAction::Type::SET_LIMIT,
                    .rpc_type = rpc_type,
                    .table_id = table_id,
                    .qps_limit = old_limit,
            };

            actions.push_back(action);
            _current_limits[key] = old_limit;

            LOG(INFO) << "[ms-throttle] downgrade: rpc=" << load_related_rpc_name(rpc_type)
                      << ", table_id=" << table_id << ", restored_limit=" << old_limit;
        } else {
            // No previous limit, remove it entirely
            RpcThrottleAction action {
                    .type = RpcThrottleAction::Type::REMOVE_LIMIT,
                    .rpc_type = rpc_type,
                    .table_id = table_id,
            };

            actions.push_back(action);
            _current_limits.erase(key);

            LOG(INFO) << "[ms-throttle] downgrade: rpc=" << load_related_rpc_name(rpc_type)
                      << ", table_id=" << table_id << ", removed limit";
        }
    }

    _upgrade_history.pop_back();

    LOG(INFO) << "[ms-throttle] on_downgrade done: actions=" << actions.size()
              << ", upgrade_level=" << _upgrade_history.size();

    return actions;
}

size_t RpcThrottleStateMachine::upgrade_level() const {
    std::lock_guard lock(_mtx);
    return _upgrade_history.size();
}

double RpcThrottleStateMachine::get_current_limit(LoadRelatedRpc rpc_type, int64_t table_id) const {
    std::lock_guard lock(_mtx);
    auto it = _current_limits.find({rpc_type, table_id});
    if (it != _current_limits.end()) {
        return it->second;
    }
    return 0.0;
}

RpcThrottleParams RpcThrottleStateMachine::get_params() const {
    std::lock_guard lock(_mtx);
    return _params;
}

// ============== RpcThrottleCoordinator ==============

RpcThrottleCoordinator::RpcThrottleCoordinator(ThrottleCoordinatorParams params) : _params(params) {
    LOG(INFO) << "[ms-throttle] coordinator initialized: upgrade_cooldown_ticks="
              << params.upgrade_cooldown_ticks
              << ", downgrade_after_ticks=" << params.downgrade_after_ticks;
}

void RpcThrottleCoordinator::update_params(ThrottleCoordinatorParams params) {
    std::lock_guard lock(_mtx);
    _params = params;
    LOG(INFO) << "[ms-throttle] coordinator params updated: upgrade_cooldown_ticks="
              << params.upgrade_cooldown_ticks
              << ", downgrade_after_ticks=" << params.downgrade_after_ticks;
}

bool RpcThrottleCoordinator::report_ms_busy() {
    std::lock_guard lock(_mtx);

    // Reset tick counter since last MS_BUSY
    _ticks_since_last_ms_busy = 0;

    // Check if cooldown has passed
    if (_ticks_since_last_upgrade == -1 ||
        _ticks_since_last_upgrade >= _params.upgrade_cooldown_ticks) {
        // Reset upgrade counter
        _ticks_since_last_upgrade = 0;
        _has_pending_upgrades = true;

        LOG(INFO) << "[ms-throttle] upgrade triggered: ticks_since_last_upgrade="
                  << _ticks_since_last_upgrade << ", cooldown=" << _params.upgrade_cooldown_ticks;
        return true; // Should trigger upgrade
    }
    return false; // Cooling down
}

bool RpcThrottleCoordinator::tick(int ticks) {
    std::lock_guard lock(_mtx);

    // Increment tick counters
    if (_ticks_since_last_ms_busy >= 0) {
        _ticks_since_last_ms_busy += ticks;
    }
    if (_ticks_since_last_upgrade >= 0) {
        _ticks_since_last_upgrade += ticks;
    }

    // Check if downgrade should be triggered
    if (_has_pending_upgrades && _ticks_since_last_ms_busy >= _params.downgrade_after_ticks) {
        // Reset for next downgrade cycle
        _ticks_since_last_ms_busy = 0;

        LOG(INFO) << "[ms-throttle] downgrade triggered: ticks_since_last_ms_busy="
                  << (_ticks_since_last_ms_busy + _params.downgrade_after_ticks)
                  << ", threshold=" << _params.downgrade_after_ticks;
        return true; // Should trigger downgrade
    }

    return false;
}

void RpcThrottleCoordinator::set_has_pending_upgrades(bool has) {
    std::lock_guard lock(_mtx);
    _has_pending_upgrades = has;
}

int RpcThrottleCoordinator::ticks_since_last_ms_busy() const {
    std::lock_guard lock(_mtx);
    return _ticks_since_last_ms_busy;
}

int RpcThrottleCoordinator::ticks_since_last_upgrade() const {
    std::lock_guard lock(_mtx);
    return _ticks_since_last_upgrade;
}

ThrottleCoordinatorParams RpcThrottleCoordinator::get_params() const {
    std::lock_guard lock(_mtx);
    return _params;
}

} // namespace doris::cloud
