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

#include <gtest/gtest.h>

namespace doris::cloud {

// ============== RpcThrottleStateMachine Tests ==============

class RpcThrottleStateMachineTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RpcThrottleStateMachineTest, SingleUpgradeAndDowngrade) {
    RpcThrottleParams params {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // Construct QPS snapshot: caller already provides top-k (top-2 here)
    std::vector<RpcQpsSnapshot> snapshot = {
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 200.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 100.0},
    };

    auto actions = sm.on_upgrade(snapshot);

    // Should produce 2 SET_LIMIT actions (top-2)
    ASSERT_EQ(actions.size(), 2);

    // table 100: new_limit = 200 * 0.5 = 100
    EXPECT_EQ(actions[0].type, RpcThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 100.0);

    // table 200: new_limit = 100 * 0.5 = 50
    EXPECT_EQ(actions[1].type, RpcThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[1].table_id, 200);
    EXPECT_DOUBLE_EQ(actions[1].qps_limit, 50.0);

    EXPECT_EQ(sm.upgrade_level(), 1);

    // Downgrade: undo the above upgrade
    auto downgrade_actions = sm.on_downgrade();
    ASSERT_EQ(downgrade_actions.size(), 2);

    // Both tables should be REMOVE_LIMIT (no previous limit)
    for (const auto& action : downgrade_actions) {
        EXPECT_EQ(action.type, RpcThrottleAction::Type::REMOVE_LIMIT);
    }

    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST_F(RpcThrottleStateMachineTest, MultipleUpgradesThenDowngrades) {
    RpcThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // First upgrade
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::COMMIT_ROWSET, 100, 80.0}});
    ASSERT_EQ(a1.size(), 1);
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 40.0); // 80 * 0.5

    // Second upgrade, same table, current limit is 40
    auto a2 = sm.on_upgrade({{LoadRelatedRpc::COMMIT_ROWSET, 100, 40.0}});
    ASSERT_EQ(a2.size(), 1);
    EXPECT_DOUBLE_EQ(a2[0].qps_limit, 20.0); // 40 * 0.5 (based on old limit)

    EXPECT_EQ(sm.upgrade_level(), 2);

    // First downgrade: undo second upgrade, restore to 40
    auto d1 = sm.on_downgrade();
    ASSERT_EQ(d1.size(), 1);
    EXPECT_EQ(d1[0].type, RpcThrottleAction::Type::SET_LIMIT);
    EXPECT_DOUBLE_EQ(d1[0].qps_limit, 40.0);

    // Second downgrade: undo first upgrade, remove limit
    auto d2 = sm.on_downgrade();
    ASSERT_EQ(d2.size(), 1);
    EXPECT_EQ(d2[0].type, RpcThrottleAction::Type::REMOVE_LIMIT);

    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST_F(RpcThrottleStateMachineTest, FloorQpsEnforced) {
    RpcThrottleParams params {.top_k = 1, .ratio = 0.1, .floor_qps = 5.0};
    RpcThrottleStateMachine sm(params);

    // current_qps=10, 10*0.1=1.0 < floor(5.0), should use floor
    auto actions = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 10.0}});
    ASSERT_EQ(actions.size(), 1);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 5.0);
}

TEST_F(RpcThrottleStateMachineTest, DowngradeOnEmptyHistoryIsNoop) {
    RpcThrottleParams params {};
    RpcThrottleStateMachine sm(params);

    auto actions = sm.on_downgrade();
    EXPECT_TRUE(actions.empty());
    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST_F(RpcThrottleStateMachineTest, UpdateTopKAtRuntime) {
    RpcThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // First upgrade, top_k=1, caller provides only 1 entry
    auto a1 = sm.on_upgrade({
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0},
    });
    ASSERT_EQ(a1.size(), 1);
    EXPECT_EQ(a1[0].table_id, 100);

    // Runtime update top_k=3
    sm.update_params({.top_k = 3, .ratio = 0.5, .floor_qps = 1.0});

    // Second upgrade, should now throttle both tables
    auto a2 = sm.on_upgrade({
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 50.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 40.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 300, 30.0},
    });
    ASSERT_EQ(a2.size(), 3);
    EXPECT_EQ(a2[0].table_id, 100);
    EXPECT_EQ(a2[1].table_id, 200);
    EXPECT_EQ(a2[2].table_id, 300);
}

TEST_F(RpcThrottleStateMachineTest, UpdateRatioAtRuntime) {
    RpcThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // First upgrade, ratio=0.5
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0}});
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 50.0);

    // Runtime update ratio=0.1 (more aggressive)
    sm.update_params({.top_k = 1, .ratio = 0.1, .floor_qps = 1.0});

    // Second upgrade, new ratio takes effect
    auto a2 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 50.0}});
    EXPECT_DOUBLE_EQ(a2[0].qps_limit, 5.0); // 50 * 0.1
}

TEST_F(RpcThrottleStateMachineTest, UpdateFloorQpsAtRuntime) {
    RpcThrottleParams params {.top_k = 1, .ratio = 0.01, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // First upgrade, qps=10, 10*0.01=0.1 < floor(1.0), use floor
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 10.0}});
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 1.0);

    // Runtime update floor_qps=5.0
    sm.update_params({.top_k = 1, .ratio = 0.01, .floor_qps = 5.0});

    // Second upgrade, new floor takes effect
    auto a2 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 1.0}});
    EXPECT_DOUBLE_EQ(a2[0].qps_limit, 5.0); // 1*0.01=0.01 < floor(5.0)
}

TEST_F(RpcThrottleStateMachineTest, MultipleRpcTypes) {
    RpcThrottleParams params {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // Multiple RPC types, caller provides top-2 per RPC type
    auto actions = sm.on_upgrade({
            // PREPARE_ROWSET: top-2
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 200.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 100.0},
            // COMMIT_ROWSET: top-2
            {LoadRelatedRpc::COMMIT_ROWSET, 100, 150.0},
            {LoadRelatedRpc::COMMIT_ROWSET, 400, 80.0},
    });

    // Should have 4 actions: 2 for each RPC type
    ASSERT_EQ(actions.size(), 4);

    // Check PREPARE_ROWSET actions
    int prepare_count = 0;
    int commit_count = 0;
    for (const auto& action : actions) {
        if (action.rpc_type == LoadRelatedRpc::PREPARE_ROWSET) {
            ++prepare_count;
        } else if (action.rpc_type == LoadRelatedRpc::COMMIT_ROWSET) {
            ++commit_count;
        }
    }
    EXPECT_EQ(prepare_count, 2);
    EXPECT_EQ(commit_count, 2);
}

TEST_F(RpcThrottleStateMachineTest, GetCurrentLimit) {
    RpcThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 0.0);

    sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0}});
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 50.0);

    sm.on_downgrade();
    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 0.0);
}

TEST_F(RpcThrottleStateMachineTest, GetParams) {
    RpcThrottleParams params {.top_k = 5, .ratio = 0.3, .floor_qps = 2.0};
    RpcThrottleStateMachine sm(params);

    auto got = sm.get_params();
    EXPECT_EQ(got.top_k, 5);
    EXPECT_DOUBLE_EQ(got.ratio, 0.3);
    EXPECT_DOUBLE_EQ(got.floor_qps, 2.0);

    sm.update_params({.top_k = 10, .ratio = 0.7, .floor_qps = 5.0});
    got = sm.get_params();
    EXPECT_EQ(got.top_k, 10);
    EXPECT_DOUBLE_EQ(got.ratio, 0.7);
    EXPECT_DOUBLE_EQ(got.floor_qps, 5.0);
}

TEST_F(RpcThrottleStateMachineTest, NoActionWhenNotLimiting) {
    RpcThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // If current_qps is already very low, applying ratio would increase it
    // In this case, no action should be produced
    auto actions = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 0.5}});
    EXPECT_TRUE(actions.empty()) << "Should not produce action when not actually limiting";
}

TEST_F(RpcThrottleStateMachineTest, OnlyLimitWhenQpsHighEnough) {
    RpcThrottleParams params {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // Table 100: high QPS, should be limited
    // Table 200: low QPS, should NOT be limited
    auto actions = sm.on_upgrade({
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 0.5},
            {LoadRelatedRpc::PREPARE_ROWSET, 300, 50.0},
    });

    // Should only limit table 100 and 300 (top-2 by QPS, but only those where new_limit < current_qps)
    ASSERT_EQ(actions.size(), 2);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_EQ(actions[1].table_id, 300);
}

// ============== Top-K Snapshot Pattern Tests ==============

TEST_F(RpcThrottleStateMachineTest, EmptySnapshot) {
    RpcThrottleParams params {.top_k = 3, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // Empty snapshot should produce no actions and no upgrade record
    auto actions = sm.on_upgrade({});
    EXPECT_TRUE(actions.empty());
    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST_F(RpcThrottleStateMachineTest, SingleEntrySnapshot) {
    RpcThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    auto actions = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 200.0}});
    ASSERT_EQ(actions.size(), 1);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 100.0);
    EXPECT_EQ(sm.upgrade_level(), 1);
}

TEST_F(RpcThrottleStateMachineTest, RepeatedUpgradeOnSameTable) {
    // Simulate caller providing the same table in consecutive top-k snapshots
    RpcThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // First upgrade: 100 * 0.5 = 50
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0}});
    ASSERT_EQ(a1.size(), 1);
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 50.0);

    // Second upgrade: existing limit 50 * 0.5 = 25
    auto a2 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 50.0}});
    ASSERT_EQ(a2.size(), 1);
    EXPECT_DOUBLE_EQ(a2[0].qps_limit, 25.0);

    // Third upgrade: existing limit 25 * 0.5 = 12.5
    auto a3 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 25.0}});
    ASSERT_EQ(a3.size(), 1);
    EXPECT_DOUBLE_EQ(a3[0].qps_limit, 12.5);

    EXPECT_EQ(sm.upgrade_level(), 3);

    // Downgrade 3 times, each restoring previous limit
    auto d1 = sm.on_downgrade();
    ASSERT_EQ(d1.size(), 1);
    EXPECT_DOUBLE_EQ(d1[0].qps_limit, 25.0);

    auto d2 = sm.on_downgrade();
    ASSERT_EQ(d2.size(), 1);
    EXPECT_DOUBLE_EQ(d2[0].qps_limit, 50.0);

    auto d3 = sm.on_downgrade();
    ASSERT_EQ(d3.size(), 1);
    EXPECT_EQ(d3[0].type, RpcThrottleAction::Type::REMOVE_LIMIT);

    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST_F(RpcThrottleStateMachineTest, TopKTablesChangeAcrossUpgrades) {
    // Different tables appear in top-k across successive upgrades
    RpcThrottleParams params {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // First upgrade: tables 100, 200
    auto a1 = sm.on_upgrade({
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 200.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 100.0},
    });
    ASSERT_EQ(a1.size(), 2);
    EXPECT_EQ(a1[0].table_id, 100);
    EXPECT_EQ(a1[1].table_id, 200);

    // Second upgrade: tables 200, 300 (100 dropped out of top-k, 300 is new)
    auto a2 = sm.on_upgrade({
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 50.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 300, 80.0},
    });
    ASSERT_EQ(a2.size(), 2);

    // table 200: already limited at 50, new_limit = 50 * 0.5 = 25
    // table 300: new, 80 * 0.5 = 40
    bool found_200 = false, found_300 = false;
    for (const auto& a : a2) {
        if (a.table_id == 200) {
            EXPECT_DOUBLE_EQ(a.qps_limit, 25.0);
            found_200 = true;
        } else if (a.table_id == 300) {
            EXPECT_DOUBLE_EQ(a.qps_limit, 40.0);
            found_300 = true;
        }
    }
    EXPECT_TRUE(found_200);
    EXPECT_TRUE(found_300);

    EXPECT_EQ(sm.upgrade_level(), 2);

    // Downgrade: undo second upgrade
    // table 200 restored to 50, table 300 removed
    auto d1 = sm.on_downgrade();
    ASSERT_EQ(d1.size(), 2);
    for (const auto& a : d1) {
        if (a.table_id == 200) {
            EXPECT_EQ(a.type, RpcThrottleAction::Type::SET_LIMIT);
            EXPECT_DOUBLE_EQ(a.qps_limit, 50.0);
        } else if (a.table_id == 300) {
            EXPECT_EQ(a.type, RpcThrottleAction::Type::REMOVE_LIMIT);
        }
    }

    // table 100 still has limit from first upgrade
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 100.0);
    // table 200 restored to first upgrade limit
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 200), 50.0);
    // table 300 no longer limited
    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 300), 0.0);
}

TEST_F(RpcThrottleStateMachineTest, MixedLimitedAndNewTables) {
    // Snapshot contains both already-limited and never-limited tables
    RpcThrottleParams params {.top_k = 3, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // First upgrade: only table 100
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::COMMIT_ROWSET, 100, 100.0}});
    ASSERT_EQ(a1.size(), 1);
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 50.0);

    // Second upgrade: table 100 (already limited) + table 200 (new)
    auto a2 = sm.on_upgrade({
            {LoadRelatedRpc::COMMIT_ROWSET, 100, 50.0},
            {LoadRelatedRpc::COMMIT_ROWSET, 200, 80.0},
    });
    ASSERT_EQ(a2.size(), 2);

    for (const auto& a : a2) {
        if (a.table_id == 100) {
            // Already has limit 50, new_limit = 50 * 0.5 = 25
            EXPECT_DOUBLE_EQ(a.qps_limit, 25.0);
        } else if (a.table_id == 200) {
            // New, 80 * 0.5 = 40
            EXPECT_DOUBLE_EQ(a.qps_limit, 40.0);
        }
    }

    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::COMMIT_ROWSET, 100), 25.0);
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::COMMIT_ROWSET, 200), 40.0);
}

TEST_F(RpcThrottleStateMachineTest, FloorQpsWithRepeatedUpgrades) {
    // Verify floor_qps is enforced even after many successive upgrades
    RpcThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 10.0};
    RpcThrottleStateMachine sm(params);

    // 100 * 0.5 = 50
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0}});
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 50.0);

    // 50 * 0.5 = 25
    auto a2 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 50.0}});
    EXPECT_DOUBLE_EQ(a2[0].qps_limit, 25.0);

    // 25 * 0.5 = 12.5
    auto a3 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 25.0}});
    EXPECT_DOUBLE_EQ(a3[0].qps_limit, 12.5);

    // 12.5 * 0.5 = 6.25 < floor(10), use floor
    // NOTE: the ratio is applied to old_limit (12.5), not current_qps
    auto a4 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 12.5}});
    EXPECT_DOUBLE_EQ(a4[0].qps_limit, 10.0);

    // Already at floor: 10 * 0.5 = 5 < floor(10), stays at floor
    auto a5 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 10.0}});
    EXPECT_DOUBLE_EQ(a5[0].qps_limit, 10.0);
}

TEST_F(RpcThrottleStateMachineTest, MultiRpcTypeTopKIndependence) {
    // Each RPC type's throttling is independent
    RpcThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(params);

    // Same table_id, different RPC types
    auto actions = sm.on_upgrade({
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 200.0},
            {LoadRelatedRpc::COMMIT_ROWSET, 100, 80.0},
    });
    ASSERT_EQ(actions.size(), 2);

    // Each (rpc_type, table_id) gets its own limit
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 100.0);
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::COMMIT_ROWSET, 100), 40.0);

    // Downgrade undoes both
    auto downgrade = sm.on_downgrade();
    ASSERT_EQ(downgrade.size(), 2);
    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 0.0);
    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::COMMIT_ROWSET, 100), 0.0);
}

// ============== RpcThrottleCoordinator Tests ==============

class RpcThrottleCoordinatorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RpcThrottleCoordinatorTest, UpgradeCooldown) {
    ThrottleCoordinatorParams params {.upgrade_cooldown_ticks = 10, .downgrade_after_ticks = 60};
    RpcThrottleCoordinator coord(params);

    // First MS_BUSY should trigger upgrade
    EXPECT_TRUE(coord.report_ms_busy());

    // MS_BUSY during cooldown should not trigger upgrade
    for (int i = 0; i < 9; i++) {
        coord.tick();
        EXPECT_FALSE(coord.report_ms_busy());
    }

    // After cooldown, can trigger again
    coord.tick(); // 10th tick
    EXPECT_TRUE(coord.report_ms_busy());
}

TEST_F(RpcThrottleCoordinatorTest, DowngradeAfterQuietPeriod) {
    ThrottleCoordinatorParams params {.upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 10};
    RpcThrottleCoordinator coord(params);

    // Trigger an upgrade
    coord.report_ms_busy();
    coord.set_has_pending_upgrades(true);

    // 9 ticks, should not trigger downgrade
    for (int i = 0; i < 9; i++) {
        EXPECT_FALSE(coord.tick());
    }

    // 10th tick, trigger downgrade
    EXPECT_TRUE(coord.tick());
}

TEST_F(RpcThrottleCoordinatorTest, MsBusyResetsDowngradeTimer) {
    ThrottleCoordinatorParams params {.upgrade_cooldown_ticks = 10, .downgrade_after_ticks = 10};
    RpcThrottleCoordinator coord(params);

    coord.report_ms_busy();
    coord.set_has_pending_upgrades(true);

    // 8 ticks
    for (int i = 0; i < 8; i++) {
        coord.tick();
    }

    // Another MS_BUSY, downgrade timer resets
    EXPECT_FALSE(coord.report_ms_busy()); // Still in cooldown

    // 9 more ticks, should not trigger downgrade (timer was reset)
    for (int i = 0; i < 9; i++) {
        EXPECT_FALSE(coord.tick());
    }

    // 10th tick, trigger downgrade
    EXPECT_TRUE(coord.tick());
}

TEST_F(RpcThrottleCoordinatorTest, NoDowngradeWithoutPendingUpgrades) {
    ThrottleCoordinatorParams params {.upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 3};
    RpcThrottleCoordinator coord(params);

    coord.report_ms_busy();
    // Explicitly clear pending upgrades to simulate the case where
    // the caller decided not to upgrade (report_ms_busy sets it internally)
    coord.set_has_pending_upgrades(false);

    for (int i = 0; i < 100; i++) {
        EXPECT_FALSE(coord.tick());
    }
}

TEST_F(RpcThrottleCoordinatorTest, UpdateUpgradeCooldownAtRuntime) {
    ThrottleCoordinatorParams params {.upgrade_cooldown_ticks = 10, .downgrade_after_ticks = 60};
    RpcThrottleCoordinator coord(params);

    EXPECT_TRUE(coord.report_ms_busy());

    // 4 ticks
    for (int i = 0; i < 4; i++) {
        coord.tick();
        EXPECT_FALSE(coord.report_ms_busy()); // Cooling down
    }

    // Runtime shorten cooldown to 5 ticks
    coord.update_params({.upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 60});

    // 1 more tick (5th total), cooldown ends
    coord.tick();
    EXPECT_TRUE(coord.report_ms_busy()); // Can trigger upgrade now
}

TEST_F(RpcThrottleCoordinatorTest, UpdateDowngradeIntervalAtRuntime) {
    ThrottleCoordinatorParams params {.upgrade_cooldown_ticks = 1, .downgrade_after_ticks = 20};
    RpcThrottleCoordinator coord(params);

    coord.report_ms_busy();
    coord.set_has_pending_upgrades(true);

    // 9 ticks, should not trigger downgrade (need 20)
    for (int i = 0; i < 9; i++) {
        EXPECT_FALSE(coord.tick());
    }

    // Runtime shorten downgrade interval to 10 ticks
    coord.update_params({.upgrade_cooldown_ticks = 1, .downgrade_after_ticks = 10});

    // 1 more tick (10th total), trigger downgrade
    EXPECT_TRUE(coord.tick());
}

TEST_F(RpcThrottleCoordinatorTest, UpdateBothParamsAtRuntime) {
    ThrottleCoordinatorParams params {.upgrade_cooldown_ticks = 100, .downgrade_after_ticks = 100};
    RpcThrottleCoordinator coord(params);

    EXPECT_TRUE(coord.report_ms_busy());
    coord.set_has_pending_upgrades(true);

    // 50 ticks, nothing should happen
    for (int i = 0; i < 50; i++) {
        coord.tick();
        EXPECT_FALSE(coord.report_ms_busy()); // Cooling down
        EXPECT_FALSE(coord.tick());           // Downgrade interval not reached
    }

    // Runtime drastically shorten both intervals
    coord.update_params({.upgrade_cooldown_ticks = 1, .downgrade_after_ticks = 1});

    // Next tick should trigger downgrade
    EXPECT_TRUE(coord.tick());

    // Next report_ms_busy should also allow upgrade
    EXPECT_TRUE(coord.report_ms_busy());
}

TEST_F(RpcThrottleCoordinatorTest, TicksSinceLastMsBusy) {
    ThrottleCoordinatorParams params {};
    RpcThrottleCoordinator coord(params);

    EXPECT_EQ(coord.ticks_since_last_ms_busy(), -1);

    coord.report_ms_busy();
    EXPECT_EQ(coord.ticks_since_last_ms_busy(), 0);

    coord.tick();
    EXPECT_EQ(coord.ticks_since_last_ms_busy(), 1);

    for (int i = 0; i < 5; i++) {
        coord.tick();
    }
    EXPECT_EQ(coord.ticks_since_last_ms_busy(), 6);
}

TEST_F(RpcThrottleCoordinatorTest, TicksSinceLastUpgrade) {
    ThrottleCoordinatorParams params {};
    RpcThrottleCoordinator coord(params);

    EXPECT_EQ(coord.ticks_since_last_upgrade(), -1);

    coord.report_ms_busy(); // Triggers upgrade
    EXPECT_EQ(coord.ticks_since_last_upgrade(), 0);

    coord.tick();
    EXPECT_EQ(coord.ticks_since_last_upgrade(), 1);

    // Second report_ms_busy doesn't trigger upgrade (cooldown)
    coord.tick();
    EXPECT_FALSE(coord.report_ms_busy());
    EXPECT_EQ(coord.ticks_since_last_upgrade(), 2);

    // After cooldown
    for (int i = 0; i < 100; i++) {
        coord.tick();
    }
    coord.report_ms_busy(); // Triggers upgrade again
    EXPECT_EQ(coord.ticks_since_last_upgrade(), 0);
}

TEST_F(RpcThrottleCoordinatorTest, GetParams) {
    ThrottleCoordinatorParams params {.upgrade_cooldown_ticks = 15, .downgrade_after_ticks = 30};
    RpcThrottleCoordinator coord(params);

    auto got = coord.get_params();
    EXPECT_EQ(got.upgrade_cooldown_ticks, 15);
    EXPECT_EQ(got.downgrade_after_ticks, 30);

    coord.update_params({.upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 10});
    got = coord.get_params();
    EXPECT_EQ(got.upgrade_cooldown_ticks, 5);
    EXPECT_EQ(got.downgrade_after_ticks, 10);
}

// ============== Integration Tests ==============

TEST(RpcThrottleIntegrationTest, FullUpgradeDowngradeCycle) {
    // Full in-memory upgrade/downgrade cycle, no time/config/bvar dependency
    RpcThrottleParams tp {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(tp);

    ThrottleCoordinatorParams cp {.upgrade_cooldown_ticks = 3, .downgrade_after_ticks = 5};
    RpcThrottleCoordinator coord(cp);

    // Simulate 2 tables with QPS data (caller provides top-k, here top-2)
    auto make_snapshot = [](double qps1, double qps2) {
        return std::vector<RpcQpsSnapshot> {
                {LoadRelatedRpc::PREPARE_ROWSET, 100, qps1},
                {LoadRelatedRpc::PREPARE_ROWSET, 200, qps2},
        };
    };

    // T=0: MS_BUSY, trigger first upgrade
    ASSERT_TRUE(coord.report_ms_busy());
    auto actions = sm.on_upgrade(make_snapshot(100, 50));
    // top-2: table 100 (limit=50), table 200 (limit=25)
    ASSERT_EQ(actions.size(), 2);
    EXPECT_EQ(actions[0].type, RpcThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 50.0);
    EXPECT_EQ(actions[1].type, RpcThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[1].table_id, 200);
    EXPECT_DOUBLE_EQ(actions[1].qps_limit, 25.0);
    coord.set_has_pending_upgrades(sm.upgrade_level() > 0);

    // T=1,2: tick, nothing happens
    EXPECT_FALSE(coord.tick());
    EXPECT_FALSE(coord.tick());

    // T=3: Another MS_BUSY (cooldown just passed), trigger second upgrade
    coord.tick();
    ASSERT_TRUE(coord.report_ms_busy());
    // table 100 already has limit=50, new_limit = 50*0.5 = 25
    // table 200 already has limit=25, new_limit = 25*0.5 = 12.5
    actions = sm.on_upgrade(make_snapshot(50, 25));
    ASSERT_EQ(actions.size(), 2);
    EXPECT_EQ(actions[0].type, RpcThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 25.0);
    EXPECT_EQ(actions[1].type, RpcThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[1].table_id, 200);
    EXPECT_DOUBLE_EQ(actions[1].qps_limit, 12.5);
    EXPECT_EQ(sm.upgrade_level(), 2);
    coord.set_has_pending_upgrades(true);

    // T=4..8: 5 ticks without MS_BUSY, trigger downgrade
    for (int i = 0; i < 4; i++) {
        EXPECT_FALSE(coord.tick());
    }
    ASSERT_TRUE(coord.tick()); // T=8: 5 ticks since T=3 MS_BUSY

    // Undo second upgrade: restore table 100 to 50, table 200 to 25
    actions = sm.on_downgrade();
    ASSERT_EQ(actions.size(), 2);
    for (const auto& a : actions) {
        EXPECT_EQ(a.type, RpcThrottleAction::Type::SET_LIMIT);
        if (a.table_id == 100) {
            EXPECT_DOUBLE_EQ(a.qps_limit, 50.0);
        } else if (a.table_id == 200) {
            EXPECT_DOUBLE_EQ(a.qps_limit, 25.0);
        } else {
            FAIL() << "Unexpected table_id: " << a.table_id;
        }
    }
    EXPECT_EQ(sm.upgrade_level(), 1);
    coord.set_has_pending_upgrades(true);

    // 5 more ticks, trigger second downgrade
    for (int i = 0; i < 4; i++) {
        EXPECT_FALSE(coord.tick());
    }
    ASSERT_TRUE(coord.tick());

    // Undo first upgrade: remove limits for both tables
    actions = sm.on_downgrade();
    ASSERT_EQ(actions.size(), 2);
    for (const auto& a : actions) {
        EXPECT_EQ(a.type, RpcThrottleAction::Type::REMOVE_LIMIT);
        EXPECT_TRUE(a.table_id == 100 || a.table_id == 200)
                << "Unexpected table_id: " << a.table_id;
    }
    EXPECT_EQ(sm.upgrade_level(), 0);
    coord.set_has_pending_upgrades(false);

    // No more downgrades even after many ticks
    for (int i = 0; i < 20; i++) {
        EXPECT_FALSE(coord.tick());
    }
}

TEST(RpcThrottleIntegrationTest, DynamicParamsDuringCycle) {
    RpcThrottleParams tp {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(tp);

    ThrottleCoordinatorParams cp {.upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 10};
    RpcThrottleCoordinator coord(cp);

    // T=0: First upgrade
    coord.report_ms_busy();
    auto actions = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0}});
    ASSERT_EQ(actions.size(), 1);
    EXPECT_EQ(actions[0].type, RpcThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[0].rpc_type, LoadRelatedRpc::PREPARE_ROWSET);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 50.0);
    coord.set_has_pending_upgrades(true);

    // T=1..3: ticks
    for (int i = 0; i < 3; i++) {
        coord.tick();
    }

    // Runtime update: change ratio to 0.1
    sm.update_params({.top_k = 1, .ratio = 0.1, .floor_qps = 1.0});

    // T=4,5: ticks
    coord.tick();
    coord.tick();

    // T=5: cooldown passed, second upgrade with new ratio
    EXPECT_TRUE(coord.report_ms_busy());
    actions = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 50.0}});
    ASSERT_EQ(actions.size(), 1);
    EXPECT_EQ(actions[0].type, RpcThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[0].rpc_type, LoadRelatedRpc::PREPARE_ROWSET);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 5.0); // 50 * 0.1, not 50 * 0.5
    coord.set_has_pending_upgrades(true);

    // Runtime update: shorten downgrade interval to 3 ticks
    coord.update_params({.upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 3});

    // 3 ticks without MS_BUSY, trigger downgrade
    for (int i = 0; i < 2; i++) {
        EXPECT_FALSE(coord.tick());
    }
    EXPECT_TRUE(coord.tick());

    // Undo second upgrade: restore table 100 to 50.0
    actions = sm.on_downgrade();
    ASSERT_EQ(actions.size(), 1);
    EXPECT_EQ(actions[0].type, RpcThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[0].rpc_type, LoadRelatedRpc::PREPARE_ROWSET);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 50.0);
}

TEST(RpcThrottleIntegrationTest, TopKTablesChangeDuringUpgradeDowngrade) {
    RpcThrottleParams tp {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    RpcThrottleStateMachine sm(tp);

    ThrottleCoordinatorParams cp {.upgrade_cooldown_ticks = 1, .downgrade_after_ticks = 5};
    RpcThrottleCoordinator coord(cp);

    // Upgrade 1: top-2 = {100, 200}
    ASSERT_TRUE(coord.report_ms_busy());
    auto actions = sm.on_upgrade({
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 200.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 100.0},
    });
    ASSERT_EQ(actions.size(), 2);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 100.0); // 200 * 0.5
    EXPECT_EQ(actions[1].table_id, 200);
    EXPECT_DOUBLE_EQ(actions[1].qps_limit, 50.0); // 100 * 0.5
    EXPECT_EQ(sm.upgrade_level(), 1);
    coord.set_has_pending_upgrades(true);

    // Upgrade 2: top-2 = {200, 300}, table 100 dropped out, table 300 is new
    coord.tick();
    ASSERT_TRUE(coord.report_ms_busy());
    actions = sm.on_upgrade({
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 50.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 300, 80.0},
    });
    ASSERT_EQ(actions.size(), 2);
    for (const auto& a : actions) {
        EXPECT_EQ(a.type, RpcThrottleAction::Type::SET_LIMIT);
        if (a.table_id == 200) {
            EXPECT_DOUBLE_EQ(a.qps_limit, 25.0); // old_limit=50, 50*0.5=25
        } else if (a.table_id == 300) {
            EXPECT_DOUBLE_EQ(a.qps_limit, 40.0); // new, 80*0.5=40
        } else {
            FAIL() << "Unexpected table_id: " << a.table_id;
        }
    }
    EXPECT_EQ(sm.upgrade_level(), 2);
    coord.set_has_pending_upgrades(true);

    // Upgrade 3: top-2 = {300, 400}, table 200 dropped out, table 400 is new
    coord.tick();
    ASSERT_TRUE(coord.report_ms_busy());
    actions = sm.on_upgrade({
            {LoadRelatedRpc::PREPARE_ROWSET, 300, 40.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 400, 60.0},
    });
    ASSERT_EQ(actions.size(), 2);
    for (const auto& a : actions) {
        EXPECT_EQ(a.type, RpcThrottleAction::Type::SET_LIMIT);
        if (a.table_id == 300) {
            EXPECT_DOUBLE_EQ(a.qps_limit, 20.0); // old_limit=40, 40*0.5=20
        } else if (a.table_id == 400) {
            EXPECT_DOUBLE_EQ(a.qps_limit, 30.0); // new, 60*0.5=30
        } else {
            FAIL() << "Unexpected table_id: " << a.table_id;
        }
    }
    EXPECT_EQ(sm.upgrade_level(), 3);
    // Verify all active limits
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 100.0);
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 200), 25.0);
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 300), 20.0);
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 400), 30.0);
    coord.set_has_pending_upgrades(true);

    // Downgrade 1: undo upgrade 3, restore {300→40, 400→removed}
    for (int i = 0; i < 4; i++) {
        EXPECT_FALSE(coord.tick());
    }
    ASSERT_TRUE(coord.tick());
    actions = sm.on_downgrade();
    ASSERT_EQ(actions.size(), 2);
    for (const auto& a : actions) {
        if (a.table_id == 300) {
            EXPECT_EQ(a.type, RpcThrottleAction::Type::SET_LIMIT);
            EXPECT_DOUBLE_EQ(a.qps_limit, 40.0);
        } else if (a.table_id == 400) {
            EXPECT_EQ(a.type, RpcThrottleAction::Type::REMOVE_LIMIT);
        } else {
            FAIL() << "Unexpected table_id: " << a.table_id;
        }
    }
    EXPECT_EQ(sm.upgrade_level(), 2);
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 100.0);
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 200), 25.0);
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 300), 40.0);
    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 400), 0.0);
    coord.set_has_pending_upgrades(true);

    // Downgrade 2: undo upgrade 2, restore {200→50, 300→removed}
    for (int i = 0; i < 4; i++) {
        EXPECT_FALSE(coord.tick());
    }
    ASSERT_TRUE(coord.tick());
    actions = sm.on_downgrade();
    ASSERT_EQ(actions.size(), 2);
    for (const auto& a : actions) {
        if (a.table_id == 200) {
            EXPECT_EQ(a.type, RpcThrottleAction::Type::SET_LIMIT);
            EXPECT_DOUBLE_EQ(a.qps_limit, 50.0);
        } else if (a.table_id == 300) {
            EXPECT_EQ(a.type, RpcThrottleAction::Type::REMOVE_LIMIT);
        } else {
            FAIL() << "Unexpected table_id: " << a.table_id;
        }
    }
    EXPECT_EQ(sm.upgrade_level(), 1);
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 100.0);
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 200), 50.0);
    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 300), 0.0);
    coord.set_has_pending_upgrades(true);

    // Downgrade 3: undo upgrade 1, restore {100→removed, 200→removed}
    for (int i = 0; i < 4; i++) {
        EXPECT_FALSE(coord.tick());
    }
    ASSERT_TRUE(coord.tick());
    actions = sm.on_downgrade();
    ASSERT_EQ(actions.size(), 2);
    for (const auto& a : actions) {
        EXPECT_EQ(a.type, RpcThrottleAction::Type::REMOVE_LIMIT);
        EXPECT_TRUE(a.table_id == 100 || a.table_id == 200)
                << "Unexpected table_id: " << a.table_id;
    }
    EXPECT_EQ(sm.upgrade_level(), 0);
    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 0.0);
    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 200), 0.0);
    coord.set_has_pending_upgrades(false);
}

} // namespace doris::cloud
