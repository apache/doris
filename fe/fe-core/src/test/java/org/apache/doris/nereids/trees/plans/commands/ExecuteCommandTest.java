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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.mtmv.MTMVRewriteUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.UseMvHint;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecuteCommandTest {

    @Test
    public void testResolvedScanOptionsAreResetForEveryExecute() throws Exception {
        String sql = "select * from p@options('scan.mode'='latest')";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        UnboundRelation relation = logicalPlan.<UnboundRelation>collectToList(
                UnboundRelation.class::isInstance).get(0);
        TableScanParams scanParams = relation.getScanParams();
        AtomicInteger snapshotId = new AtomicInteger();

        Assertions.assertEquals("1", resolveNextSnapshot(scanParams, snapshotId));

        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = new StatementContext();
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        PreparedStatementContext preparedStatement = new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt");
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(connectContext.getPreparedStementContext("stmt")).thenReturn(preparedStatement);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);
        Assertions.assertEquals("2", resolveNextSnapshot(scanParams, snapshotId));

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);
        Assertions.assertEquals("3", resolveNextSnapshot(scanParams, snapshotId));
        Mockito.verify(executor, Mockito.times(2)).execute();
    }

    @Test
    public void testPartitionStateIsResetForEveryExecute() throws Exception {
        String sql = "select 1";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = new StatementContext();
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        PreparedStatementContext preparedStatement = new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt");
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(connectContext.getPreparedStementContext("stmt")).thenReturn(preparedStatement);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        List<String> tableQualifier = Collections.singletonList("table");
        AtomicInteger relationId = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            int currentRelationId = relationId.getAndIncrement();
            statementContext.getTableUsedPartitionNameMap().put(tableQualifier,
                    Pair.of(new RelationId(currentRelationId), Collections.singleton("p")));
            statementContext.getCommonTableIdToRelationIdMap().put(0, currentRelationId);
            return null;
        }).when(executor).execute();

        statementContext.getTableUsedPartitionNameMap().put(
                tableQualifier, Pair.of(new RelationId(100), Collections.singleton("old")));
        statementContext.getCommonTableIdToRelationIdMap().put(0, 100);

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);
        Assertions.assertEquals(1, statementContext.getTableUsedPartitionNameMap().size());
        Assertions.assertEquals(0, statementContext.getTableUsedPartitionNameMap()
                .get(tableQualifier).iterator().next().key().asInt());
        Assertions.assertEquals(Collections.singleton(0),
                statementContext.getCommonTableIdToRelationIdMap().get(0));

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);
        Assertions.assertEquals(1, statementContext.getTableUsedPartitionNameMap().size());
        Assertions.assertEquals(1, statementContext.getTableUsedPartitionNameMap()
                .get(tableQualifier).iterator().next().key().asInt());
        Assertions.assertEquals(Collections.singleton(1),
                statementContext.getCommonTableIdToRelationIdMap().get(0));
        Mockito.verify(executor, Mockito.times(2)).execute();
    }

    @Test
    public void testMaterializedViewStateIsResetForEveryExecute() throws Exception {
        String sql = "select 1";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statementContext);
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        PreparedStatementContext preparedStatement = new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt");
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        connectContext.addPreparedStatementContext("stmt", preparedStatement);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        Hint retainedHint = new Hint("Distribute");
        PlannerHook retainedHook = Mockito.mock(PlannerHook.class);
        statementContext.addHint(retainedHint);
        statementContext.addPlannerHook(retainedHook);
        statementContext.setForceRecordTmpPlan(true);
        AtomicInteger executionCount = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            Assertions.assertTrue(statementContext.getTableUsedPartitionNameMap().isEmpty());
            Assertions.assertTrue(statementContext.getCommonTableIdToRelationIdMap().isEmpty());
            Assertions.assertTrue(statementContext.getMvCanRewritePartitionsMap().isEmpty());
            Assertions.assertTrue(statementContext.getCandidateMTMVs().isEmpty());
            Assertions.assertTrue(statementContext.getCandidateMVs().isEmpty());
            Assertions.assertTrue(statementContext.getMtmvRelatedTables().isEmpty());
            Assertions.assertEquals(Collections.singleton(retainedHook), statementContext.getPlannerHooks());
            Assertions.assertEquals(0, statementContext.getMaterializedViewRewriteDuration());
            Assertions.assertEquals(Collections.singletonList(retainedHint), statementContext.getHints());
            Assertions.assertTrue(statementContext.getTmpPlanForMvRewrite().isEmpty());
            Assertions.assertTrue(statementContext.getRewrittenPlansByMv().isEmpty());
            Assertions.assertTrue(statementContext.getNeedPreMvRewriteRuleMasks().isEmpty());
            Assertions.assertFalse(statementContext.isNeedPreMvRewrite());
            Assertions.assertFalse(statementContext.isPreMvRewritten());
            Assertions.assertTrue(statementContext.getMaterializationRewrittenSuccessSet().isEmpty());
            Assertions.assertTrue(statementContext.getRelationIdToStatisticsMap().isEmpty());
            Assertions.assertTrue(statementContext.isForceRecordTmpPlan());
            NereidsPlanner planner = new NereidsPlanner(statementContext);
            planner.plan(new LogicalPlanAdapter(logicalPlan, statementContext));
            Assertions.assertNotNull(planner.getPhysicalPlan());
            populateMaterializedViewState(statementContext, logicalPlan);
            executionCount.incrementAndGet();
            return null;
        }).when(executor).execute();

        populateMaterializedViewState(statementContext, logicalPlan);
        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);
        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);

        Assertions.assertEquals(2, executionCount.get());
    }

    @Test
    public void testMvValidPartitionsAreRefreshedForEveryExecute() throws Exception {
        String sql = "select 1";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statementContext);
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        connectContext.addPreparedStatementContext("stmt", new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt"));
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        MTMV mtmv = Mockito.mock(MTMV.class);
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(mtmv.getId()).thenReturn(3L);
        Mockito.when(mtmv.getName()).thenReturn("mv");
        Mockito.when(mtmv.getDatabase()).thenReturn(database);
        Mockito.when(database.getId()).thenReturn(2L);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(catalog.getName()).thenReturn("internal");
        Partition firstPartition = Mockito.mock(Partition.class);
        Partition secondPartition = Mockito.mock(Partition.class);
        MTMVRelationManager relationManager = new MTMVRelationManager();

        try (MockedStatic<MTMVRewriteUtil> rewriteUtil = Mockito.mockStatic(MTMVRewriteUtil.class)) {
            rewriteUtil.when(() -> MTMVRewriteUtil.getMTMVCanRewritePartitions(
                    Mockito.eq(mtmv), Mockito.eq(connectContext), Mockito.anyLong(), Mockito.eq(false),
                    Mockito.anyMap())).thenReturn(Arrays.asList(firstPartition, secondPartition),
                            Collections.singleton(firstPartition));
            Mockito.doAnswer(invocation -> {
                relationManager.isMVPartitionValid(
                        mtmv, connectContext, false, Collections.emptyMap());
                return null;
            }).when(executor).execute();

            ExecuteCommand executeCommand = new ExecuteCommand("stmt", prepareCommand, statementContext);
            executeCommand.run(connectContext, executor);
            Assertions.assertEquals(Arrays.asList(firstPartition, secondPartition),
                    statementContext.getMvCanRewritePartitionsMap().get(new BaseTableInfo(mtmv)));

            executeCommand.run(connectContext, executor);
            Assertions.assertEquals(Collections.singleton(firstPartition),
                    statementContext.getMvCanRewritePartitionsMap().get(new BaseTableInfo(mtmv)));
        }
    }

    @Test
    public void testMaterializationHookFollowsRewriteSettingForEveryExecute() throws Exception {
        String sql = "select 1";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        StatementContext statementContext = new StatementContext(
                connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statementContext);
        connectContext.getState().setIsQuery(true);
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(true);
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        connectContext.addPreparedStatementContext("stmt", new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt"));
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getContext()).thenReturn(connectContext);
        Mockito.doAnswer(invocation -> {
            NereidsPlanner planner = new NereidsPlanner(statementContext);
            planner.plan(new LogicalPlanAdapter(logicalPlan, statementContext));
            return null;
        }).when(executor).execute();

        ExecuteCommand executeCommand = new ExecuteCommand("stmt", prepareCommand, statementContext);
        executeCommand.run(connectContext, executor);
        Assertions.assertTrue(statementContext.getPlannerHooks().stream()
                .anyMatch(InitMaterializationContextHook.class::isInstance));

        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        executeCommand.run(connectContext, executor);
        Assertions.assertFalse(statementContext.getPlannerHooks().stream()
                .anyMatch(InitMaterializationContextHook.class::isInstance));
    }

    @Test
    public void testResolvedScanOptionsAreResetForPreparedDeleteUsing() throws Exception {
        String sql = "delete from target using source@options('scan.mode'='latest') "
                + "where target.id = source.id";
        LogicalPlan command = new NereidsParser().parseSingle(sql);
        LogicalPlan relationRoot = ((DeleteFromUsingCommand) command).getLogicalQuery();

        assertPreparedCommandResetsScanOptions(sql, command, relationRoot);
    }

    @Test
    public void testResolvedScanOptionsAreResetForPreparedDeleteSubquery() throws Exception {
        String sql = "delete from target where id in "
                + "(select id from source@options('scan.mode'='latest'))";
        DeleteFromCommand command = (DeleteFromCommand) new NereidsParser().parseSingle(sql);
        SubqueryExpr subquery = command.logicalQuery.<LogicalPlan>collectToList(plan -> true).stream()
                .flatMap(plan -> plan.getExpressions().stream())
                .flatMap(expression -> expression.<SubqueryExpr>collectToList(
                        SubqueryExpr.class::isInstance).stream())
                .findFirst().orElseThrow(AssertionError::new);

        assertPreparedCommandResetsScanOptions(sql, command, subquery.getQueryPlan());
    }

    @Test
    public void testResolvedScanOptionsAreResetForPreparedMerge() throws Exception {
        String sql = "merge into target using source@options('scan.mode'='latest') "
                + "on target.id = source.id when matched then delete";
        MergeIntoCommand command = (MergeIntoCommand) new NereidsParser().parseSingle(sql);

        assertPreparedCommandResetsScanOptions(
                sql, command, command.getRelationRoots().get(0));
    }

    @Test
    public void testExecutePreparedCommandWithoutPlanChildren() throws Exception {
        String sql = "show variables";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);

        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = new StatementContext();
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        PreparedStatementContext preparedStatement = new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt");
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(connectContext.getPreparedStementContext("stmt")).thenReturn(preparedStatement);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);

        Mockito.verify(executor).execute();
    }

    @Test
    public void testPreparedConnectorUpdateRefreshesWriteDefaultEveryExecution() throws Exception {
        // Prepared UPDATE reuses one StatementContext. Model connector metadata changing from default 1 to 2
        // between executions: each planner callback pins the current schema only when no schema is already pinned,
        // then expands DEFAULT(v) and writes the resulting value.
        // MUTATION: resetConnectorStatementScope() not clearing connectorWriteSchemas makes execution two reuse
        // default 1, so the written values become [1, 1] instead of [1, 2].
        String sql = "update ext_catalog.db.t set v = default(v) where id = 1";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        Assertions.assertInstanceOf(UpdateCommand.class, logicalPlan);

        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = new StatementContext();
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        PreparedStatementContext preparedStatement = new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt");
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(connectContext.getPreparedStementContext("stmt")).thenReturn(preparedStatement);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        long tableId = 7L;
        AtomicInteger metadataDefault = new AtomicInteger(1);
        List<String> writtenValues = new ArrayList<>();
        Mockito.doAnswer(invocation -> {
            if (!statementContext.getConnectorWriteSchema(tableId).isPresent()) {
                Column column = new Column("v", ScalarType.createType(PrimitiveType.INT),
                        false, null, String.valueOf(metadataDefault.get()), "");
                statementContext.setConnectorWriteSchema(tableId, Collections.singletonList(column));
            }
            writtenValues.add(statementContext.getConnectorWriteSchema(tableId).get()
                    .get(0).getDefaultValueSql());
            return null;
        }).when(executor).execute();

        ExecuteCommand execute = new ExecuteCommand("stmt", prepareCommand, statementContext);
        execute.run(connectContext, executor);
        metadataDefault.set(2);
        execute.run(connectContext, executor);

        Assertions.assertEquals(Arrays.asList("1", "2"), writtenValues,
                "each prepared UPDATE must write the default from its freshly resolved connector schema");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMvccSnapshotsAreResetForEveryExecute() throws Exception {
        String sql = "select 1";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = new StatementContext();
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        PreparedStatementContext preparedStatement = new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt");
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(connectContext.getPreparedStementContext("stmt")).thenReturn(preparedStatement);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        MvccTable table = Mockito.mock(MvccTable.class);
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        MvccSnapshot first = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot second = Mockito.mock(MvccSnapshot.class);
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.empty())).thenReturn(first, second);

        statementContext.loadSnapshots(table, Optional.empty(), Optional.empty());
        Assertions.assertSame(first,
                statementContext.getSnapshot(table, Optional.empty(), Optional.empty()).orElse(null));

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);
        statementContext.loadSnapshots(table, Optional.empty(), Optional.empty());

        Assertions.assertSame(second,
                statementContext.getSnapshot(table, Optional.empty(), Optional.empty()).orElse(null));
        Mockito.verify(table, Mockito.times(2)).loadSnapshot(Optional.empty(), Optional.empty());
    }

    private String resolveNextSnapshot(TableScanParams scanParams, AtomicInteger snapshotId) {
        return scanParams.getOrResolveMapParams(ignored -> ImmutableMap.of(
                "scan.snapshot-id", String.valueOf(snapshotId.incrementAndGet())))
                .get("scan.snapshot-id");
    }

    private void populateMaterializedViewState(StatementContext statementContext, LogicalPlan logicalPlan) {
        MTMV candidateMTMV = Mockito.mock(MTMV.class);
        statementContext.getTableUsedPartitionNameMap().put(Collections.singletonList("table"),
                Pair.of(new RelationId(1), Collections.singleton("partition")));
        statementContext.getCommonTableIdToRelationIdMap().put(1, 1);
        statementContext.getMvCanRewritePartitionsMap().put(Mockito.mock(BaseTableInfo.class),
                Collections.singleton(Mockito.mock(Partition.class)));
        statementContext.getCandidateMTMVs().add(candidateMTMV);
        statementContext.getCandidateMVs().add(Mockito.mock(MaterializedIndexMeta.class));
        statementContext.getMtmvRelatedTables().put(Collections.singletonList("mv"), candidateMTMV);
        statementContext.addPlannerHook(InitMaterializationContextHook.INSTANCE);
        statementContext.addMaterializedViewRewriteDuration(1);
        statementContext.addHint(Mockito.mock(UseMvHint.class));
        statementContext.addTmpPlanForMvRewrite(logicalPlan);
        statementContext.addRewrittenPlanByMv(logicalPlan);
        statementContext.ruleSetApplied(RuleType.REORDER_JOIN);
        statementContext.setNeedPreMvRewrite(true);
        statementContext.setPreMvRewritten(true);
        statementContext.addMaterializationRewrittenSuccess(Collections.singletonList("mv"));
        statementContext.addStatistics(new RelationId(1), Mockito.mock(Statistics.class));
    }

    private void assertPreparedCommandResetsScanOptions(
            String sql, LogicalPlan command, LogicalPlan relationRoot) throws Exception {
        UnboundRelation relation = relationRoot.<UnboundRelation>collectToList(
                UnboundRelation.class::isInstance).stream()
                .filter(candidate -> candidate.getScanParams() != null)
                .findFirst().orElseThrow(AssertionError::new);
        TableScanParams scanParams = relation.getScanParams();
        AtomicInteger snapshotId = new AtomicInteger();
        Assertions.assertEquals("1", resolveNextSnapshot(scanParams, snapshotId));

        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = new StatementContext();
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", command, Collections.emptyList(), new OriginStatement(sql, 0));
        PreparedStatementContext preparedStatement = new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt");
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(connectContext.getPreparedStementContext("stmt")).thenReturn(preparedStatement);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);

        Assertions.assertEquals("2", resolveNextSnapshot(scanParams, snapshotId));

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);

        Assertions.assertEquals("3", resolveNextSnapshot(scanParams, snapshotId));
    }
}
