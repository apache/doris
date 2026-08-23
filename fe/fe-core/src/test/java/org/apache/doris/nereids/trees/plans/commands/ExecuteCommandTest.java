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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalScanTaskCacheKey;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.PreparedStatementContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
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

    @Test
    public void testExternalScanTasksUseANewGenerationForEveryExecute() throws Exception {
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
        ExternalScanTaskCacheKey<String> key = new PreparedScanTaskCacheKey("same-scan");
        AtomicInteger loadCount = new AtomicInteger();

        StatementContext.ExternalScanTaskCache preparedGeneration =
                statementContext.getExternalScanTaskCache();
        preparedGeneration.getOrLoad(key,
                () -> Collections.singletonList("prepared-" + loadCount.incrementAndGet()));

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);
        StatementContext.ExternalScanTaskCache firstExecuteGeneration =
                statementContext.getExternalScanTaskCache();
        Assertions.assertNotSame(preparedGeneration, firstExecuteGeneration);
        Assertions.assertEquals(Collections.singletonList("execute-2"),
                firstExecuteGeneration.getOrLoad(key,
                        () -> Collections.singletonList("execute-" + loadCount.incrementAndGet())));

        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);
        StatementContext.ExternalScanTaskCache secondExecuteGeneration =
                statementContext.getExternalScanTaskCache();
        Assertions.assertNotSame(firstExecuteGeneration, secondExecuteGeneration);
        Assertions.assertEquals(Collections.singletonList("execute-3"),
                secondExecuteGeneration.getOrLoad(key,
                        () -> Collections.singletonList("execute-" + loadCount.incrementAndGet())));
        Assertions.assertEquals(3, loadCount.get());
    }

    private static final class PreparedScanTaskCacheKey implements ExternalScanTaskCacheKey<String> {
        private final String value;

        private PreparedScanTaskCacheKey(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object object) {
            return object instanceof PreparedScanTaskCacheKey
                    && value.equals(((PreparedScanTaskCacheKey) object).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    @Test
    public void testIcebergWriteSchemaContextIsResetForEveryExecute() throws Exception {
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

        statementContext.setIcebergWriteSchemaContext(Optional.of(Mockito.mock(
                org.apache.doris.datasource.iceberg.IcebergWriteSchemaContext.class)));
        new ExecuteCommand("stmt", prepareCommand, statementContext).run(connectContext, executor);

        Assertions.assertFalse(statementContext.getIcebergWriteSchemaContext().isPresent());
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
