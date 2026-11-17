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

package org.apache.doris.nereids.spm.manager;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.SPMPlanTreeSupport;
import org.apache.doris.nereids.spm.SPMPlanner;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Tests cold-read deadlines, refreshed replay metadata and reservation lifecycle ordering. */
public class SPMManagerContractsTest {
    @Test
    public void testReservationSqlPrefersResolvedStateAtEqualTimestamp() throws Exception {
        BaselinePersistence.SeqReservation pending =
                new BaselinePersistence.SeqReservation(7L, 1_000L, true, false);
        BaselinePersistence.SeqReservation resolved =
                new BaselinePersistence.SeqReservation(7L, 1_000L, false, false);
        Assertions.assertTrue(BaselineManager.isNewerReservation(resolved, pending));
        for (String name : List.of("SELECT_PENDING_SEQ_SQL", "SELECT_COMPACT_SEQ_SQL",
                "SELECT_COMPACT_SEQ_BATCH_SQL")) {
            java.lang.reflect.Field field = BaselineManager.class.getDeclaredField(name);
            field.setAccessible(true);
            String sql = ((String) field.get(null)).replaceAll("\\$\\{[^}]+}", "0");
            Plan plan = new NereidsParser().parseSingle(sql);
            List<OrderKey> orderKeys = new ArrayList<>();
            SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
                if (node instanceof LogicalSort) {
                    orderKeys.addAll(((LogicalSort<?>) node).getOrderKeys());
                }
            });
            Assertions.assertFalse(orderKeys.isEmpty(), name);
            List<BaselinePersistence.SeqReservation> rows = new ArrayList<>(List.of(pending, resolved));
            rows.sort((left, right) -> {
                for (OrderKey key : orderKeys) {
                    String column = ((UnboundSlot) key.getExpr()).getName();
                    int compared = Long.compare(reservationField(left, column),
                            reservationField(right, column));
                    if (compared != 0) {
                        return key.isAsc() ? compared : -compared;
                    }
                }
                return 0;
            });
            Assertions.assertSame(resolved, rows.get(0),
                    name + " must agree with lifecycle reconciliation at equal id and timestamp");
        }
    }

    private static long reservationField(BaselinePersistence.SeqReservation row, String column) {
        switch (column) {
            case "id":
                return 1L;
            case "baseline_id":
                return row.id();
            case "reserve_time":
                return row.reserveTimeMicros();
            case "dropped":
                return row.dropped() ? 1L : 0L;
            case "unconfirmed":
                return row.unconfirmed() ? 1L : 0L;
            default:
                throw new AssertionError("unexpected reservation sort column: " + column);
        }
    }

    @Test
    public void testRefreshAdoptsPlanDigestWithoutRebuildingTrees() {
        BaselineManager manager = new BaselineManager(new BaselineTestDoubles.TestBaselineStore(),
                new BaselineTestDoubles.TestClusterGateway(), System::currentTimeMillis);
        BaselineManagerTestAccess.clear(manager);
        LogicalPlan tree = (LogicalPlan) new NereidsParser().parseSingle("SELECT 1");
        BaselinePlan row = new BaselinePlan();
        row.setId(1L);
        row.setBindSql("SELECT 1");
        row.setPlanSql("SELECT 1");
        row.setBindSqlDigest("bind");
        row.setBindSqlHash(7L);
        row.setPlanSqlDigest("old-plan-digest");
        row.setParameterizedBindPlan(tree);
        row.setParameterizedPlanPlan(tree);
        manager.applyRefreshedBaselines(Map.of(1L, row));

        BaselinePlan refreshed = row.copyPersistedScalars();
        refreshed.setPlanSqlDigest("new-plan-digest");
        refreshed.setTreesPending(true);
        manager.applyRefreshedBaselines(Map.of(1L, refreshed));
        BaselinePlan cached = manager.getBaseline(1L);
        Assertions.assertSame(row, cached);
        Assertions.assertEquals("new-plan-digest", cached.getPlanSqlDigest());
        Assertions.assertSame(tree, cached.getParameterizedBindPlan());
        Assertions.assertSame(tree, cached.getParameterizedPlanPlan());
    }

    @Test
    public void testPlannerBoundsColdReadByRewriteDeadline() {
        BaselineTestDoubles.TestBaselineStore store = new BaselineTestDoubles.TestBaselineStore();
        store.durableOverride = true;
        BaselineManager manager = new BaselineManager(store,
                new BaselineTestDoubles.TestClusterGateway(), System::currentTimeMillis);
        BaselineManagerTestAccess.clear(manager);
        String sql = "SELECT 1";
        Pair<String, Long> key = SPMPlanner.queryMatchKey(null, sql);
        Assertions.assertNotNull(key);
        BaselinePlan row = new BaselinePlan();
        row.setId(1L);
        row.setBindSql(sql);
        row.setPlanSql(sql);
        row.setBindSqlDigest(key.first);
        row.setBindSqlHash(key.second);
        manager.applyRefreshedBaselines(Map.of(1L, row));
        AtomicInteger timeout = new AtomicInteger(-1);
        store.byHashTimedReader = (hash, seconds) -> {
            timeout.set(seconds);
            return List.of();
        };
        LogicalPlan plan = (LogicalPlan) new NereidsParser().parseSingle(sql);
        try (MockedStatic<BaselineManager> mocked = Mockito.mockStatic(
                BaselineManager.class, Mockito.CALLS_REAL_METHODS)) {
            mocked.when(BaselineManager::getInstance).thenReturn(manager);
            Assertions.assertNull(new SPMPlanner().tryRewritePlan(plan,
                    System.currentTimeMillis() + 5_000L));
        }
        Assertions.assertTrue(timeout.get() >= 1 && timeout.get() <= 5,
                "a five-second rewrite must not use the default ten-second read timeout: "
                        + timeout.get());
    }
}
