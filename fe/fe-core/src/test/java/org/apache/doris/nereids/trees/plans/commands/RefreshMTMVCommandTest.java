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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshManager;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Tests for REFRESH MATERIALIZED VIEW command parsing with INCREMENTAL/PARTITIONS modes.
 */
public class RefreshMTMVCommandTest {

    private final NereidsParser parser = new NereidsParser();

    private RefreshMTMVInfo extractRefreshInfo(String sql) throws Exception {
        StatementBase statementBase = parser.parseSQL(sql).get(0);
        Assertions.assertTrue(statementBase instanceof LogicalPlanAdapter,
                "Parsed statement should be LogicalPlanAdapter");
        LogicalPlan plan = ((LogicalPlanAdapter) statementBase).getLogicalPlan();
        Assertions.assertTrue(plan instanceof RefreshMTMVCommand,
                "Parsed plan should be RefreshMTMVCommand");
        Field field = RefreshMTMVCommand.class.getDeclaredField("refreshMTMVInfo");
        field.setAccessible(true);
        return (RefreshMTMVInfo) field.get(plan);
    }

    private void assertParseFails(String sql) {
        try {
            parser.parseSQL(sql);
            Assertions.fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
    }

    // TC-8-1: REFRESH MV ... INCREMENTAL can be parsed
    @Test
    public void testParseRefreshIncremental() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertEquals(RefreshMode.INCREMENTAL, info.getRefreshMode());
        Assertions.assertTrue(info.getPartitions().isEmpty());
    }

    // TC-8-2: REFRESH MV ... AUTO still parseable
    @Test
    public void testParseRefreshAuto() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 AUTO");
        Assertions.assertEquals(RefreshMode.AUTO, info.getRefreshMode());
    }

    // TC-8-3: REFRESH MV ... COMPLETE still parseable
    @Test
    public void testParseRefreshComplete() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
        Assertions.assertEquals(RefreshMode.COMPLETE, info.getRefreshMode());
        Assertions.assertTrue(info.isComplete());
    }

    // TC-8-4: REFRESH MV ... PARTITIONS parsed as new refresh mode
    @Test
    public void testParseRefreshPartitionsMode() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS");
        Assertions.assertEquals(RefreshMode.PARTITIONS, info.getRefreshMode());
        Assertions.assertTrue(info.getPartitions().isEmpty());
    }

    // TC-8-extra: REFRESH MV without mode should be rejected (syntax error)
    @Test
    public void testParseRefreshWithoutModeFails() throws Exception {
        assertParseFails("REFRESH MATERIALIZED VIEW db1.mv1");
    }

    // TC-8-extra: REFRESH MV with old-style partitionSpec still works
    @Test
    public void testParseRefreshWithPartitionSpec() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS (p1, p2)");
        Assertions.assertEquals(RefreshMode.PARTITIONS, info.getRefreshMode());
        Assertions.assertEquals(2, info.getPartitions().size());
        Assertions.assertFalse(info.allowFallback());
    }

    @Test
    public void testParseRefreshWithFallback() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL FALLBACK");
        Assertions.assertEquals(RefreshMode.INCREMENTAL, info.getRefreshMode());
        Assertions.assertTrue(info.allowFallback());
    }

    @Test
    public void testParseRefreshPartitionsFallback() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS FALLBACK");
        Assertions.assertEquals(RefreshMode.PARTITIONS, info.getRefreshMode());
        Assertions.assertTrue(info.allowFallback());
    }

    @Test
    public void testParseRefreshCompleteFallback() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE FALLBACK");
        Assertions.assertEquals(RefreshMode.COMPLETE, info.getRefreshMode());
        Assertions.assertTrue(info.allowFallback());
    }

    @Test
    public void testParsePartitionSpecFallbackFails() {
        assertParseFails("REFRESH MATERIALIZED VIEW db1.mv1 PARTITIONS (p1) FALLBACK");
    }

    // TC-8-extra: isComplete() backward compat — only true for COMPLETE mode
    @Test
    public void testIsCompleteBackwardCompat() throws Exception {
        RefreshMTMVInfo completeInfo = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
        Assertions.assertTrue(completeInfo.isComplete());

        RefreshMTMVInfo incrementalInfo = extractRefreshInfo(
                "REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertFalse(incrementalInfo.isComplete());

        RefreshMTMVInfo autoInfo = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 AUTO");
        Assertions.assertFalse(autoInfo.isComplete());
    }

    @Test
    public void testDryRunStatementContextCarriesExcludedTriggerTables() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(info, true);
        MTMV mtmv = Mockito.mock(MTMV.class);
        Set<TableNameInfo> excluded = Collections.singleton(new TableNameInfo("internal", "db", "dup_t"));
        Mockito.when(mtmv.getQuerySql()).thenReturn("SELECT 1 AS k1");
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(excluded);

        StatementContext stmtCtx = command.createDryRunStatementContext(mtmv, new ConnectContext());

        Assertions.assertEquals(excluded, stmtCtx.getExcludedTriggerTables());
    }

    @Test
    public void testIncrementalDryRunParsesSetVarWithItsStatementContext() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(info, true);
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getQuerySql()).thenReturn("SELECT /*+ SET_VAR(query_timeout=10) */ 1 AS k1");
        ConnectContext internalCtx = new ConnectContext();
        internalCtx.setThreadLocalInfo();
        try {
            StatementContext stmtCtx = command.createDryRunStatementContext(mtmv, internalCtx);
            new IvmIncrRefreshManager().buildQueryPlan(mtmv);
            Assertions.assertSame(stmtCtx, internalCtx.getStatementContext());
            Assertions.assertEquals(10, internalCtx.getSessionVariable().getQueryTimeoutS());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testIncrementalExplainCarriesExcludedTriggerTables() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        IvmIncrRefreshManager manager = new IvmIncrRefreshManager() {
            @Override
            public InsertIntoTableCommand buildInsertCommand(MTMV mtmv) {
                return Mockito.mock(InsertIntoTableCommand.class);
            }
        };
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(info, manager);
        MTMV mtmv = Mockito.mock(MTMV.class);
        Set<TableNameInfo> excluded = Collections.singleton(new TableNameInfo("internal", "db", "dup_t"));
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(excluded);
        StatementContext stmtCtx = new StatementContext(new ConnectContext(), null);

        command.createRefreshCommand(mtmv, stmtCtx);

        Assertions.assertEquals(excluded, stmtCtx.getExcludedTriggerTables());
    }

    @Test
    public void testCompleteExplainPinsOneSnapshotPerExternalInput() throws Exception {
        RefreshMTMVInfo info = extractRefreshInfo("REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(info, false);
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo mvPartitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        Mockito.when(mvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.SELF_MANAGE);
        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(mvPartitionInfo);
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(Collections.emptySet());
        Mockito.when(mtmv.isIvm()).thenReturn(false);

        MvccTable firstInput = mockMvccTable("t1");
        MvccTable secondInput = mockMvccTable("t2");
        MvccSnapshot firstSnapshot = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot secondSnapshot = Mockito.mock(MvccSnapshot.class);
        Mockito.when(firstInput.loadSnapshot(Mockito.any(), Mockito.any())).thenReturn(firstSnapshot);
        Mockito.when(secondInput.loadSnapshot(Mockito.any(), Mockito.any())).thenReturn(secondSnapshot);
        Mockito.when(mtmv.getQuerySql()).thenReturn("SELECT 1 AS k1");

        StatementContext stmtCtx = new StatementContext(new ConnectContext(), null);
        UpdateMvByPartitionCommand refreshPlan = Mockito.mock(UpdateMvByPartitionCommand.class);
        Map<MvccTableInfo, MvccSnapshot> contextPins = new HashMap<>();
        try (MockedStatic<MTMVPlanUtil> planUtil =
                        Mockito.mockStatic(MTMVPlanUtil.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<MTMVUtil> util = Mockito.mockStatic(MTMVUtil.class);
                MockedStatic<MTMVRefreshContext> contexts = Mockito.mockStatic(MTMVRefreshContext.class);
                MockedStatic<UpdateMvByPartitionCommand> from =
                        Mockito.mockStatic(UpdateMvByPartitionCommand.class)) {
            // The inputs are resolved from the definition, not from the stored relation.
            planUtil.when(() -> MTMVPlanUtil.getBaseTableFromQuery(Mockito.anyString(), Mockito.any()))
                    .thenReturn(new MTMVPlanUtil.QueryAnalysisResult(
                            Sets.newHashSet(firstInput, secondInput),
                            Sets.newHashSet(firstInput, secondInput), false));
            util.when(() -> MTMVUtil.getTable(new BaseTableInfo(firstInput))).thenReturn(firstInput);
            util.when(() -> MTMVUtil.getTable(new BaseTableInfo(secondInput))).thenReturn(secondInput);
            contexts.when(() -> MTMVRefreshContext.buildContext(Mockito.eq(mtmv), Mockito.anyMap(),
                            Mockito.anyMap()))
                    .thenAnswer(invocation -> {
                        contextPins.putAll(invocation.getArgument(2));
                        return Mockito.mock(MTMVRefreshContext.class);
                    });
            from.when(() -> UpdateMvByPartitionCommand.from(Mockito.eq(mtmv), Mockito.anySet(),
                            Mockito.anyMap(), Mockito.same(stmtCtx), Mockito.any(), Mockito.any()))
                    .thenReturn(refreshPlan);

            Assertions.assertSame(refreshPlan, command.createRefreshCommand(mtmv, stmtCtx));
        }

        Mockito.verify(mtmv, Mockito.never()).getRelation();
        // One engine materialization per input, whatever number of MV partitions the scope is derived for.
        Mockito.verify(firstInput, Mockito.times(1)).loadSnapshot(Optional.empty(), Optional.empty());
        Mockito.verify(secondInput, Mockito.times(1)).loadSnapshot(Optional.empty(), Optional.empty());
        // The same pins are handed to the mapping derivation and installed for predicate/scan binding.
        Assertions.assertSame(firstSnapshot, contextPins.get(new MvccTableInfo(firstInput)));
        Assertions.assertSame(secondSnapshot, contextPins.get(new MvccTableInfo(secondInput)));
        Assertions.assertSame(firstSnapshot, stmtCtx.getSnapshot(firstInput).get());
        Assertions.assertSame(secondSnapshot, stmtCtx.getSnapshot(secondInput).get());
    }

    private static MvccTable mockMvccTable(String tableName) {
        MvccTable table = Mockito.mock(MvccTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db1");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        return table;
    }

    private static class TestRefreshMTMVCommand extends RefreshMTMVCommand {
        private final IvmIncrRefreshManager manager;

        TestRefreshMTMVCommand(RefreshMTMVInfo info, boolean dryRun) {
            super(info, false, dryRun, Optional.empty());
            this.manager = new IvmIncrRefreshManager();
        }

        TestRefreshMTMVCommand(RefreshMTMVInfo info, IvmIncrRefreshManager manager) {
            super(info, false, false, Optional.empty());
            this.manager = manager;
        }

        @Override
        IvmIncrRefreshManager createIvmIncrRefreshManager() {
            return manager;
        }

        @Override
        public LogicalPlan createRefreshCommand(MTMV mtmv, StatementContext statementContext) throws Exception {
            return super.createRefreshCommand(mtmv, statementContext);
        }
    }
}
