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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlanShallowCopier;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Compare detached subtrees from real, post-processed plans with the pinned lookup identity. */
class PhysicalPlanStructInfoPlannerTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("physical_struct_info");
        useDatabase("physical_struct_info");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        executeSql("set enable_hbo_optimization=true");
        createTable("CREATE TABLE a (k INT, v BIGINT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE strings (k INT, s VARCHAR(32)) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES('replication_num'='1')");
        createTable("CREATE TABLE b (k INT, v BIGINT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 3 PROPERTIES('replication_num'='1')");
    }

    @Test
    void detachedCandidatesMatchPinnedLookup() {
        int compared = 0;
        for (String sql : List.of(
                "select * from a where v < 10",
                "select * from strings where substring(s,1,3)='abc'",
                "select * from strings where substring(s,1,3)='abc' and s>='aaa'",
                "select * from strings join b on strings.k=b.k where substring(s,1,3)='abc'",
                "select * from a join b on a.k=b.k where a.v < 10",
                "select * from a left join b on a.k=b.k and a.v < b.v",
                "select v, sum(k) from a group by v",
                "select v, sum(k) s from a group by v having sum(k) > 10",
                "select * from (select k+1 x from a) t where x < 10",
                "select * from (select * from a limit 5) t where v < 10",
                "select * from (select * from a order by v limit 5) t where k < 10")) {
            StatementContext context = new StatementContext(connectContext, new OriginStatement(sql, 0));
            connectContext.setStatementContext(context);
            LogicalPlanAdapter statement = new LogicalPlanAdapter(new NereidsParser().parseSingle(sql), context);
            context.setParsedStatement(statement);
            CapturingPlanner planner = new CapturingPlanner(context);
            planner.plan(statement);
            Assertions.assertNull(planner.getCascadesContext().getMemo());
            PhysicalPlan original = planner.getPhysicalPlan();
            PhysicalPlan copy = PhysicalPlanShallowCopier.copy(original);
            compared += compare(original, copy, planner.groups, sql);
        }
        Assertions.assertTrue(compared >= 6, "expected real filter/join/aggregation candidates");
    }

    @Test
    void entryKeyedByDetachedPlanIsAppliedByPinnedReadSide() {
        Assertions.assertTrue(connectContext.getSessionVariable().isEnableHboOptimization());
        String sql = "select * from strings where substring(s,1,3)='abc'";
        PhysicalPlan original = PlanChecker.from(connectContext).plan(sql).getPhysicalPlan();
        PhysicalFilter<?> filter = original.collectToList(PhysicalFilter.class::isInstance).stream()
                .map(node -> (PhysicalFilter<?>) node).findFirst().orElseThrow();
        Assertions.assertEquals(3, filter.getConjuncts().size(), "execution keeps both inferred bounds");
        GroupStructInfo info = GroupStructInfo.fromPhysicalPlan(PhysicalPlanShallowCopier.copy(filter));
        Assertions.assertTrue(info.isValid(), info.getSkipReason());
        GroupStructInfo.LiteralMode mode = GroupStructInfo.LiteralMode.WITH_LITERAL;
        String key = info.getFingerprint(mode);
        HboPlanStatisticsManager manager = Env.getCurrentEnv().getHboPlanStatisticsManager();
        boolean persist = Config.hbo_persist_pinned_to_internal_db;
        Config.hbo_persist_pinned_to_internal_db = false;
        try {
            manager.putPinnedPlanStatistics(key, 7, HboPlanStatisticsManager.PinnedType.EXACT,
                    info.getCanonical(mode), HboPlanStatisticsManager.LiteralMode.WITH_LITERAL);
            PhysicalPlan replanned = PlanChecker.from(connectContext).plan(sql).getPhysicalPlan();
            PhysicalFilter<?> learnedFilter = replanned.collectToList(PhysicalFilter.class::isInstance).stream()
                    .map(node -> (PhysicalFilter<?>) node).findFirst().orElseThrow();
            Assertions.assertEquals(7, learnedFilter.getStats().getRowCount());
        } finally {
            manager.removePinnedPlanStatistics(key);
            Config.hbo_persist_pinned_to_internal_db = persist;
        }
    }

    private static class CapturingPlanner extends NereidsPlanner {
        private final Map<Integer, Group> groups = new HashMap<>();

        private CapturingPlanner(StatementContext context) {
            super(context);
        }

        @Override
        protected void splitFragments(PhysicalPlan plan) {
            // The production path releases Memo after translation. Retain it only on the test's
            // reference side; the cloned-plan builder must compute the same key without this map.
            for (Group group : getCascadesContext().getMemo().getGroups()) {
                groups.put(group.getGroupId().asInt(), group);
            }
            super.splitFragments(plan);
        }
    }

    private int compare(Plan plan, PhysicalPlan copy, Map<Integer, Group> groups, String sql) {
        int compared = 0;
        boolean candidate = plan instanceof PhysicalFilter || plan instanceof AbstractPhysicalJoin
                || plan instanceof PhysicalHashAggregate
                        && !((PhysicalHashAggregate<?>) plan).getAggMode().productAggregateBuffer;
        if (candidate) {
            Optional<GroupStructInfo> expected = GroupStructInfo.structInfoOfPlanNode(
                    (AbstractPlan) plan, groups);
            if (expected.isPresent()) {
                GroupStructInfo actual = GroupStructInfo.fromPhysicalPlan(copy);
                Assertions.assertTrue(actual.isValid(), sql + "\n" + plan.treeString()
                        + "\n" + actual.getSkipReason());
                for (GroupStructInfo.LiteralMode mode : GroupStructInfo.LiteralMode.values()) {
                    Assertions.assertEquals(expected.get().getFingerprint(mode), actual.getFingerprint(mode),
                            sql + "\n" + plan.treeString() + "\nexpected: " + expected.get().getCanonical(mode)
                                    + "\nactual: " + actual.getCanonical(mode));
                }
                compared++;
            }
        }
        for (int i = 0; i < plan.arity(); i++) {
            compared += compare(plan.child(i), (PhysicalPlan) copy.child(i), groups, sql);
        }
        return compared;
    }
}
