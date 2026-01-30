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

// ============== ThrottleStateMachine Tests ==============

class ThrottleStateMachineTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ThrottleStateMachineTest, SingleUpgradeAndDowngrade) {
    ThrottleParams params {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // Construct QPS snapshot: table 100 has QPS=200, table 200 has QPS=100
    std::vector<QpsSnapshot> snapshot = {
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 200.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 100.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 300, 10.0}, // Not in top-2
    };

    auto actions = sm.on_upgrade(snapshot);

    // Should produce 2 SET_LIMIT actions (top-2)
    ASSERT_EQ(actions.size(), 2);

    // table 100: new_limit = 200 * 0.5 = 100
    EXPECT_EQ(actions[0].type, ThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 100.0);

    // table 200: new_limit = 100 * 0.5 = 50
    EXPECT_EQ(actions[1].type, ThrottleAction::Type::SET_LIMIT);
    EXPECT_EQ(actions[1].table_id, 200);
    EXPECT_DOUBLE_EQ(actions[1].qps_limit, 50.0);

    EXPECT_EQ(sm.upgrade_level(), 1);

    // Downgrade: undo the above upgrade
    auto downgrade_actions = sm.on_downgrade();
    ASSERT_EQ(downgrade_actions.size(), 2);

    // Both tables should be REMOVE_LIMIT (no previous limit)
    for (const auto& action : downgrade_actions) {
        EXPECT_EQ(action.type, ThrottleAction::Type::REMOVE_LIMIT);
    }

    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST_F(ThrottleStateMachineTest, MultipleUpgradesThenDowngrades) {
    ThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

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
    EXPECT_EQ(d1[0].type, ThrottleAction::Type::SET_LIMIT);
    EXPECT_DOUBLE_EQ(d1[0].qps_limit, 40.0);

    // Second downgrade: undo first upgrade, remove limit
    auto d2 = sm.on_downgrade();
    ASSERT_EQ(d2.size(), 1);
    EXPECT_EQ(d2[0].type, ThrottleAction::Type::REMOVE_LIMIT);

    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST_F(ThrottleStateMachineTest, FloorQpsEnforced) {
    ThrottleParams params {.top_k = 1, .ratio = 0.1, .floor_qps = 5.0};
    ThrottleStateMachine sm(params);

    // current_qps=10, 10*0.1=1.0 < floor(5.0), should use floor
    auto actions = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 10.0}});
    ASSERT_EQ(actions.size(), 1);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 5.0);
}

TEST_F(ThrottleStateMachineTest, DowngradeOnEmptyHistoryIsNoop) {
    ThrottleParams params {};
    ThrottleStateMachine sm(params);

    auto actions = sm.on_downgrade();
    EXPECT_TRUE(actions.empty());
    EXPECT_EQ(sm.upgrade_level(), 0);
}

TEST_F(ThrottleStateMachineTest, UpdateTopKAtRuntime) {
    ThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // First upgrade, top_k=1, only table 100 is throttled
    auto a1 = sm.on_upgrade({
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 80.0},
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

TEST_F(ThrottleStateMachineTest, UpdateRatioAtRuntime) {
    ThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // First upgrade, ratio=0.5
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0}});
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 50.0);

    // Runtime update ratio=0.1 (more aggressive)
    sm.update_params({.top_k = 1, .ratio = 0.1, .floor_qps = 1.0});

    // Second upgrade, new ratio takes effect
    auto a2 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 50.0}});
    EXPECT_DOUBLE_EQ(a2[0].qps_limit, 5.0); // 50 * 0.1
}

TEST_F(ThrottleStateMachineTest, UpdateFloorQpsAtRuntime) {
    ThrottleParams params {.top_k = 1, .ratio = 0.01, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // First upgrade, qps=10, 10*0.01=0.1 < floor(1.0), use floor
    auto a1 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 10.0}});
    EXPECT_DOUBLE_EQ(a1[0].qps_limit, 1.0);

    // Runtime update floor_qps=5.0
    sm.update_params({.top_k = 1, .ratio = 0.01, .floor_qps = 5.0});

    // Second upgrade, new floor takes effect
    auto a2 = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 1.0}});
    EXPECT_DOUBLE_EQ(a2[0].qps_limit, 5.0); // 1*0.01=0.01 < floor(5.0)
}

TEST_F(ThrottleStateMachineTest, MultipleRpcTypes) {
    ThrottleParams params {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // Multiple RPC types
    auto actions = sm.on_upgrade({
            // PREPARE_ROWSET: table 100 (200 qps), table 200 (100 qps)
            {LoadRelatedRpc::PREPARE_ROWSET, 100, 200.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 200, 100.0},
            {LoadRelatedRpc::PREPARE_ROWSET, 300, 50.0},
            // COMMIT_ROWSET: table 100 (150 qps), table 400 (80 qps)
            {LoadRelatedRpc::COMMIT_ROWSET, 100, 150.0},
            {LoadRelatedRpc::COMMIT_ROWSET, 400, 80.0},
            {LoadRelatedRpc::COMMIT_ROWSET, 500, 30.0},
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

TEST_F(ThrottleStateMachineTest, GetCurrentLimit) {
    ThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 0.0);

    sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0}});
    EXPECT_DOUBLE_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 50.0);

    sm.on_downgrade();
    EXPECT_EQ(sm.get_current_limit(LoadRelatedRpc::PREPARE_ROWSET, 100), 0.0);
}

TEST_F(ThrottleStateMachineTest, GetParams) {
    ThrottleParams params {.top_k = 5, .ratio = 0.3, .floor_qps = 2.0};
    ThrottleStateMachine sm(params);

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

TEST_F(ThrottleStateMachineTest, NoActionWhenNotLimiting) {
    ThrottleParams params {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

    // If current_qps is already very low, applying ratio would increase it
    // In this case, no action should be produced
    auto actions = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 0.5}});
    EXPECT_TRUE(actions.empty()) << "Should not produce action when not actually limiting";
}

TEST_F(ThrottleStateMachineTest, OnlyLimitWhenQpsHighEnough) {
    ThrottleParams params {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(params);

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

// ============== UpgradeDowngradeCoordinator Tests ==============

class CoordinatorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(CoordinatorTest, UpgradeCooldown) {
    CoordinatorParams params {.upgrade_cooldown_ticks = 10, .downgrade_after_ticks = 60};
    UpgradeDowngradeCoordinator coord(params);

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

TEST_F(CoordinatorTest, DowngradeAfterQuietPeriod) {
    CoordinatorParams params {
            .upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 10};
    UpgradeDowngradeCoordinator coord(params);

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

TEST_F(CoordinatorTest, MsBusyResetsDowngradeTimer) {
    CoordinatorParams params {
            .upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 10};
    UpgradeDowngradeCoordinator coord(params);

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

TEST_F(CoordinatorTest, NoDowngradeWithoutPendingUpgrades) {
    CoordinatorParams params {
            .upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 3};
    UpgradeDowngradeCoordinator coord(params);

    coord.report_ms_busy();
    // Don't set has_pending_upgrades

    for (int i = 0; i < 100; i++) {
        EXPECT_FALSE(coord.tick());
    }
}

TEST_F(CoordinatorTest, UpdateUpgradeCooldownAtRuntime) {
    CoordinatorParams params {.upgrade_cooldown_ticks = 10, .downgrade_after_ticks = 60};
    UpgradeDowngradeCoordinator coord(params);

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

TEST_F(CoordinatorTest, UpdateDowngradeIntervalAtRuntime) {
    CoordinatorParams params {
            .upgrade_cooldown_ticks = 1, .downgrade_after_ticks = 20};
    UpgradeDowngradeCoordinator coord(params);

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

TEST_F(CoordinatorTest, UpdateBothParamsAtRuntime) {
    CoordinatorParams params {
            .upgrade_cooldown_ticks = 100, .downgrade_after_ticks = 100};
    UpgradeDowngradeCoordinator coord(params);

    EXPECT_TRUE(coord.report_ms_busy());
    coord.set_has_pending_upgrades(true);

    // 50 ticks, nothing should happen
    for (int i = 0; i < 50; i++) {
        coord.tick();
        EXPECT_FALSE(coord.report_ms_busy()); // Cooling down
        EXPECT_FALSE(coord.tick()); // Downgrade interval not reached
    }

    // Runtime drastically shorten both intervals
    coord.update_params({.upgrade_cooldown_ticks = 1, .downgrade_after_ticks = 1});

    // Next tick should trigger downgrade
    EXPECT_TRUE(coord.tick());

    // Next report_ms_busy should also allow upgrade
    EXPECT_TRUE(coord.report_ms_busy());
}

TEST_F(CoordinatorTest, TicksSinceLastMsBusy) {
    CoordinatorParams params {};
    UpgradeDowngradeCoordinator coord(params);

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

TEST_F(CoordinatorTest, TicksSinceLastUpgrade) {
    CoordinatorParams params {};
    UpgradeDowngradeCoordinator coord(params);

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

TEST_F(CoordinatorTest, GetParams) {
    CoordinatorParams params {.upgrade_cooldown_ticks = 15, .downgrade_after_ticks = 30};
    UpgradeDowngradeCoordinator coord(params);

    auto got = coord.get_params();
    EXPECT_EQ(got.upgrade_cooldown_ticks, 15);
    EXPECT_EQ(got.downgrade_after_ticks, 30);

    coord.update_params({.upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 10});
    got = coord.get_params();
    EXPECT_EQ(got.upgrade_cooldown_ticks, 5);
    EXPECT_EQ(got.downgrade_after_ticks, 10);
}

// ============== Integration Tests ==============

TEST(IntegrationTest, FullUpgradeDowngradeCycle) {
    // Full in-memory upgrade/downgrade cycle, no time/config/bvar dependency
    ThrottleParams tp {.top_k = 2, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(tp);

    CoordinatorParams cp {.upgrade_cooldown_ticks = 3, .downgrade_after_ticks = 5};
    UpgradeDowngradeCoordinator coord(cp);

    // Simulate 3 tables with QPS data
    auto make_snapshot = [](double qps1, double qps2, double qps3) {
        return std::vector<QpsSnapshot> {
                {LoadRelatedRpc::PREPARE_ROWSET, 100, qps1},
                {LoadRelatedRpc::PREPARE_ROWSET, 200, qps2},
                {LoadRelatedRpc::PREPARE_ROWSET, 300, qps3},
        };
    };

    std::vector<ThrottleAction> all_actions;

    // T=0: MS_BUSY, trigger first upgrade
    ASSERT_TRUE(coord.report_ms_busy());
    auto actions = sm.on_upgrade(make_snapshot(100, 50, 10));
    // top-2: table 100 (limit=50), table 200 (limit=25)
    ASSERT_EQ(actions.size(), 2);
    EXPECT_EQ(actions[0].table_id, 100);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 50.0);
    EXPECT_EQ(actions[1].table_id, 200);
    EXPECT_DOUBLE_EQ(actions[1].qps_limit, 25.0);
    coord.set_has_pending_upgrades(sm.upgrade_level() > 0);

    // T=1,2: tick, nothing happens
    EXPECT_FALSE(coord.tick());
    EXPECT_FALSE(coord.tick());

    // T=3: Another MS_BUSY (cooldown just passed), trigger second upgrade
    coord.tick();
    ASSERT_TRUE(coord.report_ms_busy());
    // table 100 already has limit=50, so new_limit = 50*0.5 = 25
    actions = sm.on_upgrade(make_snapshot(50, 25, 10));
    EXPECT_EQ(sm.upgrade_level(), 2);
    coord.set_has_pending_upgrades(true);

    // T=4..8: 5 ticks without MS_BUSY, trigger downgrade
    for (int i = 0; i < 4; i++) {
        EXPECT_FALSE(coord.tick());
    }
    ASSERT_TRUE(coord.tick()); // T=8: 5 ticks since T=3 MS_BUSY

    actions = sm.on_downgrade(); // Undo second upgrade
    EXPECT_EQ(sm.upgrade_level(), 1);
    coord.set_has_pending_upgrades(true);

    // 5 more ticks, trigger second downgrade
    for (int i = 0; i < 4; i++) {
        EXPECT_FALSE(coord.tick());
    }
    ASSERT_TRUE(coord.tick());

    actions = sm.on_downgrade(); // Undo first upgrade
    EXPECT_EQ(sm.upgrade_level(), 0);
    coord.set_has_pending_upgrades(false);

    // No more downgrades even after many ticks
    for (int i = 0; i < 20; i++) {
        EXPECT_FALSE(coord.tick());
    }
}

TEST(IntegrationTest, DynamicParamsDuringCycle) {
    ThrottleParams tp {.top_k = 1, .ratio = 0.5, .floor_qps = 1.0};
    ThrottleStateMachine sm(tp);

    CoordinatorParams cp {.upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 10};
    UpgradeDowngradeCoordinator coord(cp);

    // T=0: First upgrade
    coord.report_ms_busy();
    auto actions = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 100.0}});
    ASSERT_EQ(actions.size(), 1);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 50.0);
    coord.set_has_pending_upgrades(true);

    // T=1..3: ticks
    for (int i = 0; i < 3; i++) {
        coord.tick();
    }

    // Runtime update: change ratio to 0.1
    sm.update_params({.top_k = 1, .ratio = 0.1, .floor_qps = 1.0});

    // T=4: ticks
    coord.tick();

    // T=5: cooldown passed, second upgrade with new ratio
    EXPECT_TRUE(coord.report_ms_busy());
    actions = sm.on_upgrade({{LoadRelatedRpc::PREPARE_ROWSET, 100, 50.0}});
    ASSERT_EQ(actions.size(), 1);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 5.0); // 50 * 0.1, not 50 * 0.5
    coord.set_has_pending_upgrades(true);

    // Runtime update: shorten downgrade interval to 3 ticks
    coord.update_params({.upgrade_cooldown_ticks = 5, .downgrade_after_ticks = 3});

    // 3 ticks without MS_BUSY, trigger downgrade
    for (int i = 0; i < 2; i++) {
        EXPECT_FALSE(coord.tick());
    }
    EXPECT_TRUE(coord.tick());

    actions = sm.on_downgrade();
    EXPECT_EQ(actions.size(), 1);
    EXPECT_DOUBLE_EQ(actions[0].qps_limit, 50.0); // Restored to first upgrade value
}

} // namespace doris::cloud
