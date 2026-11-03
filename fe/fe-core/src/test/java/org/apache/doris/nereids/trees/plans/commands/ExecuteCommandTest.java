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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MysqlColType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.PointQueryExecutor;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShortCircuitQueryContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
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

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testExpressionBindingRefreshesCachedPointQuery() throws Exception {
        ConnectContext context = new ConnectContext();
        PreparedStatementContext prepared = prepareParameterStatement(context, "select * from tbl where k = ?");
        StatementContext initialContext = prepared.getStatementContext();
        initialContext.setShortCircuitQuery(true);
        ShortCircuitQueryContext cached = Mockito.mock(ShortCircuitQueryContext.class);
        Mockito.when(cached.isReusable(context)).thenReturn(true);
        prepared.shortCircuitQueryContext = Optional.of(cached);
        StmtExecutor executor = parameterExecutor(context);

        try (MockedStatic<PointQueryExecutor> pointQueries = Mockito.mockStatic(PointQueryExecutor.class)) {
            bindAndExecute(context, prepared, executor, new StringLiteral("ASCII"));
            pointQueries.verify(() -> PointQueryExecutor.directExecuteShortCircuitQuery(
                    executor, prepared, prepared.getStatementContext()));

            Expression binary = new Cast(new VarBinaryLiteral(new byte[] {(byte) 0xff}), StringType.INSTANCE);
            bindAndExecute(context, prepared, executor, binary);
            StatementContext refreshed = prepared.getStatementContext();
            Assertions.assertFalse(refreshed.isShortCircuitQuery());
            Assertions.assertNull(refreshed.getShortCircuitQueryContext());
            Assertions.assertTrue(prepared.shortCircuitQueryContext.isEmpty());
            Assertions.assertNotSame(initialContext, refreshed);
            Assertions.assertEquals(binary, refreshed.getIdToPlaceholderRealExpr().get(
                    prepared.command.getPlaceholders().get(0).getPlaceholderId()));
            Assertions.assertEquals(MysqlColType.MYSQL_TYPE_VARSTRING,
                    prepared.command.getPlaceholders().get(0).getMysqlColType());
            Mockito.verify(executor).execute();
            pointQueries.verifyNoMoreInteractions();

            bindAndExecute(context, prepared, executor, new StringLiteral("ASCII again"));
            Mockito.verify(executor, Mockito.times(2)).execute();
        }
    }

    @Test
    public void testExpressionBindingClearsUncachedPointQueryFlag() throws Exception {
        ConnectContext context = new ConnectContext();
        PreparedStatementContext prepared = prepareParameterStatement(context, "select * from tbl where k = ?");
        // A point-query execution can fail before publishing its cached context.
        prepared.getStatementContext().setShortCircuitQuery(true);
        StmtExecutor executor = parameterExecutor(context);

        bindAndExecute(context, prepared, executor,
                new Cast(new VarBinaryLiteral(new byte[] {(byte) 0xff}), StringType.INSTANCE));

        Assertions.assertFalse(prepared.getStatementContext().isShortCircuitQuery());
        Assertions.assertNull(prepared.getStatementContext().getShortCircuitQueryContext());
        Mockito.verify(executor).execute();
    }

    @Test
    public void testExpressionBindingSkipsPreparedGroupCommit() throws Exception {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().groupCommit = "sync_mode";
        Assertions.assertTrue(context.getSessionVariable().enableGroupCommitFullPrepare);
        PreparedStatementContext prepared = prepareParameterStatement(context, "insert into test.tbl values (?)");
        // A previous literal execution has already populated the full-prepare cache.
        GroupCommitPlanner cached = Mockito.mock(GroupCommitPlanner.class);
        prepared.groupCommitPlanner = Optional.of(cached);
        StmtExecutor executor = parameterExecutor(context);
        try (MockedStatic<GroupCommitPlanner> groupCommit = Mockito.mockStatic(GroupCommitPlanner.class)) {
            bindAndExecute(context, prepared, executor, new StringLiteral("ASCII"));
            groupCommit.verify(() -> GroupCommitPlanner.executeGroupCommitInsert(
                    context, prepared, prepared.getStatementContext()));
            groupCommit.clearInvocations();

            Expression binary = new Cast(new VarBinaryLiteral(new byte[] {(byte) 0xff}), StringType.INSTANCE);
            bindAndExecute(context, prepared, executor, binary);
            Mockito.verify(executor).execute();
            Assertions.assertEquals(binary, prepared.getStatementContext().getIdToPlaceholderRealExpr().get(
                    prepared.command.getPlaceholders().get(0).getPlaceholderId()));
            groupCommit.verifyNoInteractions();

            bindAndExecute(context, prepared, executor, new NullLiteral());
            groupCommit.verify(() -> GroupCommitPlanner.executeGroupCommitInsert(
                    context, prepared, prepared.getStatementContext()));
            Assertions.assertSame(cached, prepared.groupCommitPlanner.orElseThrow());
        }
    }

    private PreparedStatementContext prepareParameterStatement(ConnectContext context, String sql) throws Exception {
        context.setThreadLocalInfo();
        LogicalPlanAdapter parsed = (LogicalPlanAdapter) new NereidsParser().parseSQL(sql).get(0);
        StatementContext statementContext = parsed.getStatementContext();
        statementContext.setConnectContext(context);
        List<Placeholder> placeholders = statementContext.getPlaceholders().stream()
                .map(placeholder -> placeholder.withNewMysqlColType(MysqlColType.MYSQL_TYPE_VARSTRING.getCode()))
                .toList();
        statementContext.setPlaceholders(placeholders);
        PrepareCommand command = new PrepareCommand("stmt", parsed.getLogicalPlan(), placeholders,
                new OriginStatement(sql, 0));
        PreparedStatementContext prepared = new PreparedStatementContext(command, context, statementContext, "stmt");
        context.setStatementContext(statementContext);
        context.addPreparedStatementContext("stmt", prepared);
        return prepared;
    }

    private StmtExecutor parameterExecutor(ConnectContext context) {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getContext()).thenReturn(context);
        Mockito.doAnswer(invocation -> {
            context.setStatementContext(invocation.getArgument(0));
            return null;
        }).when(executor).setStatementContext(Mockito.any(StatementContext.class));
        return executor;
    }

    private void bindAndExecute(ConnectContext context, PreparedStatementContext prepared,
            StmtExecutor executor, Expression value) throws Exception {
        context.setGroupCommit(false);
        prepared.getStatementContext().getIdToPlaceholderRealExpr().put(
                prepared.command.getPlaceholders().get(0).getPlaceholderId(), value);
        new ExecuteCommand("stmt", prepared.command, prepared.getStatementContext()).run(context, executor);
    }

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
        // ExecuteCommand allocates a fresh StatementContext per EXECUTE. Model connector metadata changing from
        // default 1 to 2 between executions: each execution's planner callback pins the current schema only when
        // the fresh context has no schema pinned, then expands DEFAULT(v) and writes the resulting value.
        // MUTATION: reusing one StatementContext across executions without dropping connectorWriteSchemas makes
        // execution two reuse default 1, so the written values become [1, 1] instead of [1, 2].
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
            // Each execution plans through the fresh StatementContext allocated by ExecuteCommand, so resolve/pin
            // the connector writer schema on THAT context (a stale pin would make the second execution reuse
            // default 1).
            StatementContext currentContext = preparedStatement.getStatementContext();
            if (!currentContext.getConnectorWriteSchema(tableId).isPresent()) {
                Column column = new Column("v", ScalarType.createType(PrimitiveType.INT),
                        false, null, String.valueOf(metadataDefault.get()), "");
                currentContext.setConnectorWriteSchema(tableId, Collections.singletonList(column));
            }
            writtenValues.add(currentContext.getConnectorWriteSchema(tableId).get()
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

        // ExecuteCommand allocates a fresh StatementContext per EXECUTE, so the next execution must not reuse the
        // snapshot pinned on the previous context (a stale snapshot would make a later commit permanently
        // invisible).
        StatementContext nextContext = preparedStatement.getStatementContext();
        Assertions.assertNotSame(statementContext, nextContext,
                "ExecuteCommand allocates a fresh StatementContext per EXECUTE");
        nextContext.loadSnapshots(table, Optional.empty(), Optional.empty());

        Assertions.assertSame(second,
                nextContext.getSnapshot(table, Optional.empty(), Optional.empty()).orElse(null));
        Mockito.verify(table, Mockito.times(2)).loadSnapshot(Optional.empty(), Optional.empty());
    }

    @Test
    public void testFastPathInstallsCachedShortCircuitContextAcrossExecutions() throws Exception {
        // ExecuteCommand allocates a fresh StatementContext per EXECUTE. The fresh context carries the
        // short-circuit flag but not the cached plan, so the fast path must install the just-validated
        // ShortCircuitQueryContext before direct execution -- otherwise result sending falls back to
        // `new ShortCircuitQueryContext(planner, ...)` with a null planner (this path never plans) and
        // NPEs on planner.getDescTable(). Two executions exercise the second (reusable) EXECUTE that
        // hits the regression.
        // MUTATION: removing the install in ExecuteCommand.run() -> the fresh context has no
        // statement-level cache -> the assertSame below flips -> red.
        String sql = "select * from tbl";
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);

        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        StatementContext statementContext = new StatementContext();
        statementContext.setShortCircuitQuery(true);
        PrepareCommand prepareCommand = new PrepareCommand(
                "stmt", logicalPlan, Collections.emptyList(), new OriginStatement(sql, 0));
        PreparedStatementContext preparedStatement = new PreparedStatementContext(
                prepareCommand, connectContext, statementContext, "stmt");

        // A real ShortCircuitQueryContext (built from a mocked planner) that passes isReusable().
        Planner planner = Mockito.mock(Planner.class);
        Mockito.when(planner.getQueryOptions()).thenReturn(new TQueryOptions());
        DescriptorTable descriptorTable = new DescriptorTable();
        descriptorTable.createTupleDescriptor();
        Mockito.when(planner.getDescTable()).thenReturn(descriptorTable);
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn("tbl").when(table).getName();
        Mockito.doReturn(10).when(table).getBaseSchemaVersion();
        Mockito.when(scanNode.getPointQueryProjectList()).thenReturn(Collections.emptyList());
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getTableNameInPlan()).thenReturn("tbl");
        Mockito.when(scanNode.getConjuncts()).thenReturn(Collections.emptyList());
        Mockito.when(planner.getScanNodes()).thenReturn(Collections.singletonList(scanNode));
        ShortCircuitQueryContext cachedPlan = new ShortCircuitQueryContext(planner, Mockito.mock(Queriable.class));
        preparedStatement.shortCircuitQueryContext = Optional.of(cachedPlan);

        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(connectContext.getPreparedStementContext("stmt")).thenReturn(preparedStatement);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.enableGroupCommitFullPrepare = false;
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(executor.getContext()).thenReturn(connectContext);

        ExecuteCommand execute = new ExecuteCommand("stmt", prepareCommand, statementContext);
        execute.run(connectContext, executor);
        Assertions.assertSame(cachedPlan, preparedStatement.getStatementContext().getShortCircuitQueryContext(),
                "the fast path installs the validated cache on the fresh context (first EXECUTE)");
        Mockito.verify(executor, Mockito.times(1)).executeAndSendResult(Mockito.anyBoolean(), Mockito.anyBoolean(),
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());

        execute.run(connectContext, executor);
        Assertions.assertSame(cachedPlan, preparedStatement.getStatementContext().getShortCircuitQueryContext(),
                "the fast path installs the validated cache on the fresh context (second, reusable EXECUTE)");
        Mockito.verify(executor, Mockito.times(2)).executeAndSendResult(Mockito.anyBoolean(), Mockito.anyBoolean(),
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    private String resolveNextSnapshot(TableScanParams scanParams, AtomicInteger snapshotId) {
        return scanParams.getOrResolveMapParams(ignored -> ImmutableMap.of(
                "scan.snapshot-id", String.valueOf(snapshotId.incrementAndGet())))
                .get("scan.snapshot-id");
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
