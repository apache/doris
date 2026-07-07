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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.ivm.IvmRefreshManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class ExplainRefreshMtmvCommandTest {
    private final NereidsParser parser = new NereidsParser();

    // ---------- parse tests ----------

    @Test
    public void testParseExplainRefreshIncrementalOverview() {
        LogicalPlan plan = extractLogicalPlan("EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");

        Assertions.assertInstanceOf(ExplainRefreshMtmvCommand.class, plan);
        ExplainRefreshMtmvCommand explain = (ExplainRefreshMtmvCommand) plan;
        Assertions.assertEquals(ExplainLevel.NORMAL, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshLogicalPlan() {
        LogicalPlan plan = extractLogicalPlan(
                "EXPLAIN LOGICAL PLAN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");

        Assertions.assertInstanceOf(ExplainRefreshMtmvCommand.class, plan);
        ExplainRefreshMtmvCommand explain = (ExplainRefreshMtmvCommand) plan;
        Assertions.assertEquals(ExplainLevel.REWRITTEN_PLAN, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshPhysicalPlan() {
        LogicalPlan plan = extractLogicalPlan(
                "EXPLAIN PHYSICAL PLAN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");

        Assertions.assertInstanceOf(ExplainRefreshMtmvCommand.class, plan);
        ExplainRefreshMtmvCommand explain = (ExplainRefreshMtmvCommand) plan;
        Assertions.assertEquals(ExplainLevel.OPTIMIZED_PLAN, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshLogicalPlanProcess() {
        LogicalPlan plan = extractLogicalPlan(
                "EXPLAIN LOGICAL PLAN PROCESS REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");

        Assertions.assertInstanceOf(ExplainRefreshMtmvCommand.class, plan);
        ExplainRefreshMtmvCommand explain = (ExplainRefreshMtmvCommand) plan;
        Assertions.assertEquals(ExplainLevel.REWRITTEN_PLAN, explain.getLevel());
        Assertions.assertTrue(explain.showPlanProcess());
    }

    @Test
    public void testParseExplainRefreshAllPlan() {
        LogicalPlan plan = extractLogicalPlan(
                "EXPLAIN ALL PLAN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");

        Assertions.assertInstanceOf(ExplainRefreshMtmvCommand.class, plan);
        ExplainRefreshMtmvCommand explain = (ExplainRefreshMtmvCommand) plan;
        Assertions.assertEquals(ExplainLevel.ALL_PLAN, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshDistributedPlan() {
        LogicalPlan plan = extractLogicalPlan(
                "EXPLAIN DISTRIBUTED PLAN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");

        Assertions.assertInstanceOf(ExplainRefreshMtmvCommand.class, plan);
        ExplainRefreshMtmvCommand explain = (ExplainRefreshMtmvCommand) plan;
        Assertions.assertEquals(ExplainLevel.DISTRIBUTED_PLAN, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshNormal() {
        LogicalPlan plan = extractLogicalPlan(
                "EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");

        Assertions.assertInstanceOf(ExplainRefreshMtmvCommand.class, plan);
        ExplainRefreshMtmvCommand explain = (ExplainRefreshMtmvCommand) plan;
        Assertions.assertEquals(ExplainLevel.NORMAL, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshComplete() {
        LogicalPlan plan = extractLogicalPlan(
                "EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");

        Assertions.assertInstanceOf(ExplainRefreshMtmvCommand.class, plan);
        ExplainRefreshMtmvCommand explain = (ExplainRefreshMtmvCommand) plan;
        Assertions.assertEquals(ExplainLevel.NORMAL, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshWithoutIncrementalFails() {
        assertParseFails("EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1");
    }

    // ---------- run tests ----------

    @Test
    public void testRunExplainRefreshOverviewSuccess() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        MTMVRefreshInfo refreshInfo = Mockito.mock(MTMVRefreshInfo.class);
        Mockito.when(refreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getRefreshInfo()).thenReturn(refreshInfo);
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        InsertIntoTableCommand insertCommand = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(manager.buildInsertCommand(mtmv)).thenReturn(insertCommand);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        RecordingExplainCommand explainCommand = newExplainCommand(RefreshMode.INCREMENTAL, ExplainLevel.NORMAL, manager);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            explainCommand.run(new ConnectContext(), executor);
        }

        Mockito.verify(manager).buildInsertCommand(mtmv);
        Assertions.assertSame(insertCommand, explainCommand.explainedCommand);
        Assertions.assertNotNull(explainCommand.explainConnectContext);
    }

    @Test
    public void testRunExplainRefreshAnalyzedDeltaSuccess() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        MTMVRefreshInfo refreshInfo = Mockito.mock(MTMVRefreshInfo.class);
        Mockito.when(refreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getRefreshInfo()).thenReturn(refreshInfo);
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        InsertIntoTableCommand insertCommand = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(manager.buildInsertCommand(mtmv)).thenReturn(insertCommand);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        RecordingExplainCommand explainCommand = newExplainCommand(
                RefreshMode.INCREMENTAL, ExplainLevel.ANALYZED_PLAN, manager);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            explainCommand.run(new ConnectContext(), executor);
        }

        Mockito.verify(manager).buildInsertCommand(mtmv);
        Assertions.assertSame(insertCommand, explainCommand.explainedCommand);
    }

    @Test
    public void testRunExplainRefreshMissingMvFails() throws Exception {
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMissingMtmv()) {
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> newExplainCommand(RefreshMode.INCREMENTAL, ExplainLevel.NORMAL, manager)
                            .run(new ConnectContext(), executor));
        }

        Mockito.verify(manager, Mockito.never()).buildInsertCommand(Mockito.any());
    }

    @Test
    public void testRunExplainRefreshNonIvmMvFails() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(false);
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> newExplainCommand(RefreshMode.INCREMENTAL, ExplainLevel.NORMAL, manager)
                            .run(new ConnectContext(), executor));
        }

        Mockito.verify(manager, Mockito.never()).buildInsertCommand(Mockito.any());
    }

    @Test
    public void testRunExplainRefreshLogicalPlanWorksWithoutDelta() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        MTMVRefreshInfo refreshInfo = Mockito.mock(MTMVRefreshInfo.class);
        Mockito.when(refreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getRefreshInfo()).thenReturn(refreshInfo);
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        InsertIntoTableCommand insertCommand = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(manager.buildInsertCommand(mtmv)).thenReturn(insertCommand);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        RecordingExplainCommand explainCommand = newExplainCommand(
                RefreshMode.INCREMENTAL, ExplainLevel.ANALYZED_PLAN, manager);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            explainCommand.run(new ConnectContext(), executor);
        }

        Mockito.verify(manager).buildInsertCommand(mtmv);
        Assertions.assertSame(insertCommand, explainCommand.explainedCommand);
    }

    @Test
    public void testRunExplainRefreshCompleteSuccess() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRefreshInfo refreshInfo = Mockito.mock(MTMVRefreshInfo.class);
        Mockito.when(refreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.COMPLETE);
        Mockito.when(mtmv.getRefreshInfo()).thenReturn(refreshInfo);
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        RecordingExplainCommand explainCommand = newExplainCommand(RefreshMode.COMPLETE, ExplainLevel.NORMAL, manager);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            explainCommand.run(new ConnectContext(), executor);
        }

        Assertions.assertNotNull(explainCommand.explainedCommand);
    }

    @Test
    public void testRunExplainRefreshPartitionsFails() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        MTMVRefreshInfo refreshInfo = Mockito.mock(MTMVRefreshInfo.class);
        Mockito.when(refreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.PARTITIONS);
        Mockito.when(mtmv.getRefreshInfo()).thenReturn(refreshInfo);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> new ExplainRefreshMtmvCommand(
                            new RefreshMTMVInfo(new TableNameInfo("internal", "db1", "mv1"),
                                    Collections.emptyList(), RefreshMode.PARTITIONS),
                            ExplainLevel.NORMAL, false).run(new ConnectContext(), executor));
        }
    }

    // ---------- helpers ----------

    private LogicalPlan extractLogicalPlan(String sql) {
        StatementBase statementBase = parser.parseSQL(sql).get(0);
        Assertions.assertTrue(statementBase instanceof LogicalPlanAdapter,
                "Parsed statement should be LogicalPlanAdapter");
        return ((LogicalPlanAdapter) statementBase).getLogicalPlan();
    }

    private void assertParseFails(String sql) {
        try {
            parser.parseSQL(sql);
            Assertions.fail("Expected Exception");
        } catch (Exception e) {
            // expected
        }
    }

    private RecordingExplainCommand newExplainCommand(RefreshMode refreshMode, ExplainLevel level,
            IvmRefreshManager manager) {
        RefreshMTMVInfo info = new RefreshMTMVInfo(
                new TableNameInfo("internal", "db1", "mv1"), Collections.emptyList(), refreshMode);
        return new RecordingExplainCommand(info, level, manager);
    }

    private static class RecordingExplainCommand extends ExplainRefreshMtmvCommand {
        private final IvmRefreshManager manager;
        private LogicalPlan explainedCommand;
        private ConnectContext explainConnectContext;

        private RecordingExplainCommand(RefreshMTMVInfo info, ExplainLevel level, IvmRefreshManager manager) {
            super(info, level, false);
            this.manager = manager;
        }

        @Override
        IvmRefreshManager createIvmRefreshManager() {
            return manager;
        }

        @Override
        protected ConnectContext createExplainConnectContext(MTMV mtmv) {
            return new ConnectContext();
        }

        @Override
        protected LogicalPlan createRefreshCommand(MTMV mtmv, StatementContext statementContext) {
            if (getRefreshMTMVInfo().getRefreshMode() == RefreshMode.INCREMENTAL) {
                return manager.buildInsertCommand(mtmv);
            }
            return Mockito.mock(UpdateMvByPartitionCommand.class);
        }

        @Override
        protected void runExplainCommand(ConnectContext planCtx, StmtExecutor executor,
                LogicalPlan command) {
            this.explainConnectContext = planCtx;
            this.explainedCommand = command;
        }
    }

    private MockedStatic<Env> mockEnvWithMtmv(MTMV mtmv) throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any())).thenReturn(true);
        Mockito.when(catalog.getDbOrDdlException("db1")).thenReturn(db);
        Mockito.when(catalog.getDbOrAnalysisException("db1")).thenReturn(db);
        Mockito.when(db.getTableOrMetaException("mv1", TableType.MATERIALIZED_VIEW)).thenReturn(mtmv);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return mockedEnv;
    }

    private MockedStatic<Env> mockEnvWithMissingMtmv() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any())).thenReturn(true);
        Mockito.when(catalog.getDbOrDdlException("db1")).thenReturn(db);
        Mockito.when(db.getTableOrMetaException("mv1", TableType.MATERIALIZED_VIEW))
                .thenThrow(new MetaNotFoundException("table not found"));
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return mockedEnv;
    }
}
