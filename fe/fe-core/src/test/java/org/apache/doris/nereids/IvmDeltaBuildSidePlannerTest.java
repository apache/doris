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

package org.apache.doris.nereids;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.DistributionSpecReplicated;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.LogicalSubQueryAliasToLogicalProject;
import org.apache.doris.nereids.rules.rewrite.AddProjectForJoin;
import org.apache.doris.nereids.rules.rewrite.FindHashConditionForJoin;
import org.apache.doris.nereids.rules.rewrite.OperativeColumnDerive;
import org.apache.doris.nereids.rules.rewrite.OrExpansion;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.statistics.analysis.TableStatsMeta;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

class IvmDeltaBuildSidePlannerTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("ivm_build_side");
        for (String table : ImmutableList.of("delta_input", "snapshot_input", "unrelated_input")) {
            createTable("CREATE TABLE " + table
                    + " (p INT NOT NULL, k INT NOT NULL) DUPLICATE KEY(p) DISTRIBUTED BY HASH(p) BUCKETS 3"
                    + " PROPERTIES ('replication_num' = '1')");
        }
    }

    @Test
    void testLateInvalidNdvPutsDeltaOnBuildSide() throws Exception {
        String sql = "SELECT d.k, s.k FROM (SELECT k FROM delta_input WHERE k = 1) d"
                + " LEFT JOIN snapshot_input s ON d.k = s.k";
        checkFinalPlan(sql, "delta_input", 100_000, false, JoinType.RIGHT_OUTER_JOIN, "delta_input");
        checkFinalPlan(sql, "delta_input", 100_000, true, JoinType.LEFT_OUTER_JOIN, "snapshot_input");
    }

    @Test
    void testLateInvalidNdvPutsAntiJoinDeltaOnBuildSide() throws Exception {
        String sql = "SELECT d.k FROM (SELECT k FROM delta_input WHERE k = 1) d"
                + " LEFT ANTI JOIN snapshot_input s ON d.k = s.k";
        checkFinalPlan(sql, "delta_input", 100_000, false, JoinType.RIGHT_ANTI_JOIN, "delta_input");
        checkFinalPlan(sql, "delta_input", 100_000, true, JoinType.LEFT_ANTI_JOIN, "snapshot_input");
    }

    @Test
    void testInvalidJoinKeyKeepsAutomaticSnapshotBroadcast() throws Exception {
        assertAutomaticSnapshotBroadcast("SELECT d.k, s.k FROM delta_input d LEFT JOIN snapshot_input s ON d.k = s.k",
                JoinType.LEFT_OUTER_JOIN);
        assertAutomaticSnapshotBroadcast("SELECT d.k FROM delta_input d LEFT ANTI JOIN snapshot_input s ON d.k = s.k",
                JoinType.LEFT_ANTI_JOIN);
    }

    @Test
    void testUnreliableProbeKeepsAutomaticSnapshotBroadcast() throws Exception {
        assertAutomaticSnapshotBroadcast("SELECT d.k FROM (SELECT k FROM delta_input WHERE k = 1) d"
                + " LEFT ANTI JOIN snapshot_input s ON d.k = s.k", JoinType.LEFT_ANTI_JOIN);
    }

    @Test
    void testReliableInputsKeepOrderAboveBroadcastLimit() throws Exception {
        String sql = "SELECT d.k, s.k FROM delta_input d LEFT JOIN snapshot_input s ON d.k = s.k";
        checkFinalPlan(sql, "delta_input", 10_000, false, JoinType.LEFT_OUTER_JOIN, "snapshot_input");
    }

    @Test
    void testMaterializedOrExpansionInputKeepsOrder() throws Exception {
        String sql = "SELECT d.k, s.k FROM (SELECT k FROM delta_input WHERE k = 1) d"
                + " LEFT JOIN (SELECT s.k FROM snapshot_input s JOIN unrelated_input u"
                + " ON s.k = u.k OR s.p = u.p) s ON d.k = s.k";
        checkFinalPlan(sql, "delta_input", 100_000, false, JoinType.LEFT_OUTER_JOIN, "snapshot_input", true);
    }

    private void assertAutomaticSnapshotBroadcast(String sql, JoinType joinType) throws Exception {
        for (boolean disableFallback : ImmutableList.of(true, false)) {
            Assertions.assertEquals(DistributionSpecReplicated.class,
                    checkFinalPlan(sql, "delta_input", 10, disableFallback, joinType, "snapshot_input"));
        }
    }

    @Test
    void testBroadcastDecisionTakesPrecedence() throws Exception {
        String sql = "SELECT d.k, s.k FROM delta_input d LEFT JOIN [BROADCAST] snapshot_input s ON d.k = s.k";
        Class<? extends DistributionSpec> buildDistribution =
                checkFinalPlan(sql, "delta_input", 10, false, JoinType.LEFT_OUTER_JOIN, "snapshot_input");
        Assertions.assertEquals(DistributionSpecReplicated.class, buildDistribution);
    }

    @Test
    void testUnrelatedInvalidNdvKeepsReliableBuildSide() throws Exception {
        String sql = "SELECT d.k, s.k FROM delta_input d LEFT JOIN snapshot_input s ON d.k = s.k"
                + " UNION ALL SELECT k, k FROM unrelated_input";
        Class<? extends DistributionSpec> withFallback =
                checkFinalPlan(sql, "unrelated_input", 10, false, JoinType.LEFT_OUTER_JOIN, "snapshot_input");
        Class<? extends DistributionSpec> withoutFallback =
                checkFinalPlan(sql, "unrelated_input", 10, true, JoinType.LEFT_OUTER_JOIN, "snapshot_input");
        Assertions.assertEquals(withoutFallback, withFallback);
    }

    private Class<? extends DistributionSpec> checkFinalPlan(String sql, String invalidTable,
            long snapshotRows, boolean disableFallback,
            JoinType expectedType, String expectedBuildTable) throws Exception {
        return checkFinalPlan(sql, invalidTable, snapshotRows, disableFallback,
                expectedType, expectedBuildTable, false);
    }

    private Class<? extends DistributionSpec> checkFinalPlan(String sql, String invalidTable,
            long snapshotRows, boolean disableFallback,
            JoinType expectedType, String expectedBuildTable, boolean materializeOrExpansion) throws Exception {
        connectContext.getSessionVariable().setDisableJoinReorder(false);
        connectContext.getSessionVariable().setBroadcastRowCountLimit(100);
        if (materializeOrExpansion) {
            connectContext.getSessionVariable().cteInlineMode = -1;
        }
        connectContext.getSessionVariable().setDisableNereidsRules(
                disableFallback ? RuleType.PREFER_IVM_DELTA_BUILD_SIDE.name() : "");
        try (StatementContext statement = createStatementCtx(sql)) {
            CascadesContext context = CascadesContext.initContext(statement, new NereidsParser().parseSingle(sql),
                    PhysicalProperties.ANY);
            context.newAnalyzer().analyze();
            Rewriter.getCteChildrenRewriter(context, ImmutableList.of(
                    Rewriter.topDown(new LogicalSubQueryAliasToLogicalProject(), new FindHashConditionForJoin()),
                    Rewriter.custom(RuleType.ADD_PROJECT_FOR_JOIN, AddProjectForJoin::new),
                    Rewriter.custom(RuleType.OPERATIVE_COLUMN_DERIVE, OperativeColumnDerive::new))).execute();
            if (materializeOrExpansion) {
                Rewriter.getCteChildrenRewriter(context,
                        ImmutableList.of(Rewriter.custom(RuleType.OR_EXPANSION, () -> OrExpansion.INSTANCE))).execute();
            }
            // Inject stats into the already rewritten plan to exercise the planner's final check.
            List<LogicalOlapScan> scans = context.getRewritePlan().collectToList(LogicalOlapScan.class::isInstance);
            for (LogicalOlapScan scan : scans) {
                String tableName = scan.getTable().getName();
                boolean delta = tableName.equals("delta_input");
                installStats(scan.getTable(), delta ? 1000L : snapshotRows, tableName.equals(invalidTable));
                if (delta) {
                    statement.markIvmDeltaScan(scan.getRelationId());
                }
            }
            statement.setIvmRewriteContext(Optional.of(IvmRewriteContext.incrementalExplain(
                    Mockito.mock(MTMV.class), true)));
            NereidsPlanner planner = new NereidsPlanner(statement);
            Deencapsulation.setField(planner, "cascadesContext", context);
            boolean oldInternalSchema = FeConstants.enableInternalSchemaDb;
            boolean oldRunningUnitTest = FeConstants.runningUnitTest;
            try {
                FeConstants.enableInternalSchemaDb = true;
                FeConstants.runningUnitTest = false;
                Assertions.assertFalse(connectContext.getSessionVariable().isDisableJoinReorder());
                planner.optimize(false);
                Assertions.assertTrue(connectContext.getSessionVariable().isDisableJoinReorder());
                Assertions.assertTrue(statement.getDisableJoinReorderReason().orElseThrow()
                        .contains("slot k has invalid column stats"));
            } finally {
                FeConstants.runningUnitTest = oldRunningUnitTest;
                FeConstants.enableInternalSchemaDb = oldInternalSchema;
            }

            Plan physical = NereidsPlanner.chooseBestPlan(context.getMemo().getRoot(), PhysicalProperties.ANY, context);
            List<PhysicalHashJoin<?, ?>> joins = physical.collectToList(plan -> plan instanceof PhysicalHashJoin
                    && (((PhysicalHashJoin<?, ?>) plan).getJoinType().isOuterJoin()
                            || ((PhysicalHashJoin<?, ?>) plan).getJoinType().isAntiJoin()));
            Assertions.assertEquals(1, joins.size());
            PhysicalHashJoin<?, ?> join = joins.get(0);
            Assertions.assertEquals(expectedType, join.getJoinType());
            if (materializeOrExpansion) {
                Assertions.assertTrue(join.right().containsType(PhysicalCTEConsumer.class));
            } else {
                List<PhysicalOlapScan> buildScans = join.right().collectToList(PhysicalOlapScan.class::isInstance);
                Assertions.assertEquals(1, buildScans.size());
                Assertions.assertEquals(expectedBuildTable, buildScans.get(0).getTable().getName());
            }
            return ((PhysicalPlan) join.right()).getPhysicalProperties().getDistributionSpec().getClass();
        }
    }

    private void installStats(OlapTable table, long rows, boolean invalidNdv) {
        Env env = Env.getCurrentEnv();
        TableStatsMeta tableStats = Mockito.spy(new TableStatsMeta(table));
        tableStats.userInjected = true;
        Mockito.doReturn(rows).when(tableStats).getRowCount(table.getBaseIndexId());
        env.getAnalysisManager().replayUpdateTableStatsStatus(tableStats);
        env.getStatisticsCache().updateColStatsCache(table.getDatabase().getCatalog().getId(),
                table.getDatabase().getId(), table.getId(), -1, "k", new ColumnStatisticBuilder(rows)
                        .setNdv(invalidNdv ? 0 : rows)
                        .setMinExpr(new IntLiteral(1))
                        .setMaxExpr(new IntLiteral(rows))
                        .setMinValue(1)
                        .setMaxValue(rows)
                        .setAvgSizeByte(4)
                        .build());
    }
}
