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
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

public class ExplainRefreshMTMVCommandTest {
    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testParseExplainRefreshIncrementalOverview() {
        ExplainCommand explain = extractExplainCommand("EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertEquals(ExplainLevel.NORMAL, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshLogicalPlan() {
        ExplainCommand explain = extractExplainCommand(
                "EXPLAIN LOGICAL PLAN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertEquals(ExplainLevel.REWRITTEN_PLAN, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshPhysicalPlan() {
        ExplainCommand explain = extractExplainCommand(
                "EXPLAIN PHYSICAL PLAN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertEquals(ExplainLevel.OPTIMIZED_PLAN, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshLogicalPlanProcess() {
        ExplainCommand explain = extractExplainCommand(
                "EXPLAIN LOGICAL PLAN PROCESS REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertEquals(ExplainLevel.REWRITTEN_PLAN, explain.getLevel());
        Assertions.assertTrue(explain.showPlanProcess());
    }

    @Test
    public void testParseExplainRefreshAllPlan() {
        ExplainCommand explain = extractExplainCommand(
                "EXPLAIN ALL PLAN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertEquals(ExplainLevel.ALL_PLAN, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshDistributedPlan() {
        ExplainCommand explain = extractExplainCommand(
                "EXPLAIN DISTRIBUTED PLAN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertEquals(ExplainLevel.DISTRIBUTED_PLAN, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshWithSessionVariable() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        try {
            StatementBase statementBase = parser.parseSQL(
                    "EXPLAIN SHAPE PLAN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL",
                    connectContext.getSessionVariable()).get(0);
            Assertions.assertInstanceOf(LogicalPlanAdapter.class, statementBase);
            LogicalPlan plan = ((LogicalPlanAdapter) statementBase).getLogicalPlan();
            Assertions.assertInstanceOf(ExplainCommand.class, plan);
            Assertions.assertInstanceOf(RefreshMTMVCommand.class, ((ExplainCommand) plan).getLogicalPlan());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testParseExplainRefreshNormal() {
        ExplainCommand explain = extractExplainCommand(
                "EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");
        Assertions.assertEquals(ExplainLevel.NORMAL, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshComplete() {
        ExplainCommand explain = extractExplainCommand(
                "EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
        Assertions.assertEquals(ExplainLevel.NORMAL, explain.getLevel());
    }

    @Test
    public void testParseExplainRefreshWithoutIncrementalFails() {
        assertParseFails("EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1");
    }

    @Test
    public void testExplainCommandUsesCustomConnectContext() throws Exception {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        Mockito.when(planner.getExplainString(Mockito.any())).thenReturn("plan");
        Mockito.when(planner.getScanNodes()).thenReturn(Collections.<ScanNode>emptyList());
        Mockito.when(planner.getCascadesContext()).thenReturn(Mockito.mock(org.apache.doris.nereids.CascadesContext.class));
        RecordingRefreshMTMVCommand command = new RecordingRefreshMTMVCommand(planner);
        ConnectContext originCtx = new ConnectContext();
        originCtx.setThreadLocalInfo();
        try {
            new ExplainCommand(ExplainLevel.NORMAL, command, false).run(originCtx, executor);

            Assertions.assertSame(command.explainCtx, command.explainPlanCtx);
            Assertions.assertSame(originCtx, ConnectContext.get());
            Mockito.verify(executor).handleExplainStmt("plan", true);
            Mockito.verify(planner).plan(Mockito.any(LogicalPlanAdapter.class), Mockito.any());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testExplainCommandRestoresPreviousThreadLocalContext() throws Exception {
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        Mockito.when(planner.getExplainString(Mockito.any())).thenReturn("plan");
        Mockito.when(planner.getScanNodes()).thenReturn(Collections.<ScanNode>emptyList());
        Mockito.when(planner.getCascadesContext()).thenReturn(Mockito.mock(org.apache.doris.nereids.CascadesContext.class));
        RecordingRefreshMTMVCommand command = new RecordingRefreshMTMVCommand(planner);
        ConnectContext previousCtx = new ConnectContext();
        previousCtx.setThreadLocalInfo();
        ConnectContext originCtx = new ConnectContext();
        try {
            new ExplainCommand(ExplainLevel.NORMAL, command, false).run(originCtx, executor);

            Assertions.assertSame(previousCtx, ConnectContext.get());
            Mockito.verify(executor).handleExplainStmt("plan", true);
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testExplainCommandRestoresPreviousThreadLocalContextWhenGetExplainContextFails() {
        ConnectContext previousCtx = new ConnectContext();
        previousCtx.setThreadLocalInfo();
        ConnectContext originCtx = new ConnectContext();
        ExplainCommand explainCommand = new ExplainCommand(
                ExplainLevel.NORMAL, new ThrowingExplainContextRefreshMTMVCommand(), false);
        try {
            Assertions.assertThrows(IllegalStateException.class,
                    new Executable() {
                        @Override
                        public void execute() throws Throwable {
                            explainCommand.run(originCtx, Mockito.mock(StmtExecutor.class));
                        }
                    });
            Assertions.assertSame(previousCtx, ConnectContext.get());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testExplainCommandRestoresPreviousThreadLocalContextWhenGetExplainPlanFails() {
        ConnectContext previousCtx = new ConnectContext();
        previousCtx.setThreadLocalInfo();
        ConnectContext originCtx = new ConnectContext();
        ExplainCommand explainCommand = new ExplainCommand(
                ExplainLevel.NORMAL, new ThrowingExplainPlanRefreshMTMVCommand(), false);
        try {
            Assertions.assertThrows(IllegalStateException.class,
                    new Executable() {
                        @Override
                        public void execute() throws Throwable {
                            explainCommand.run(originCtx, Mockito.mock(StmtExecutor.class));
                        }
                    });
            Assertions.assertSame(previousCtx, ConnectContext.get());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testGetExplainPlanIncrementalSuccess() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        MTMVRefreshInfo refreshInfo = Mockito.mock(MTMVRefreshInfo.class);
        Mockito.when(refreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getRefreshInfo()).thenReturn(refreshInfo);
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        InsertIntoTableCommand insertCommand = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(manager.buildInsertCommand(mtmv)).thenReturn(insertCommand);
        Mockito.when(insertCommand.getExplainPlan(Mockito.any())).thenReturn(insertCommand);
        Mockito.when(insertCommand.getExplainPlanner(Mockito.eq(insertCommand), Mockito.any()))
                .thenReturn(Optional.empty());
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(newRefreshInfo(RefreshMode.INCREMENTAL), manager);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            ConnectContext explainCtx = command.getExplainConnectContext(new ConnectContext());
            Plan explainPlan = command.getExplainPlan(explainCtx);
            Assertions.assertSame(insertCommand, explainPlan);
        }

        Mockito.verify(manager).buildInsertCommand(mtmv);
    }

    @Test
    public void testGetExplainPlanMissingMvFails() throws Exception {
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(
                newRefreshInfo(RefreshMode.INCREMENTAL), Mockito.mock(IvmRefreshManager.class));

        try (MockedStatic<Env> mockedEnv = mockEnvWithMissingMtmv()) {
            Assertions.assertThrows(AnalysisException.class,
                    new Executable() {
                        @Override
                        public void execute() throws Throwable {
                            command.getExplainConnectContext(new ConnectContext());
                        }
                    });
        }
    }

    @Test
    public void testGetExplainPlanNonIvmMvFails() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(false);
        MTMVRefreshInfo refreshInfo = Mockito.mock(MTMVRefreshInfo.class);
        Mockito.when(refreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getRefreshInfo()).thenReturn(refreshInfo);
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(
                newRefreshInfo(RefreshMode.INCREMENTAL), Mockito.mock(IvmRefreshManager.class));

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            ConnectContext explainCtx = command.getExplainConnectContext(new ConnectContext());
            Assertions.assertThrows(AnalysisException.class,
                    new Executable() {
                        @Override
                        public void execute() throws Throwable {
                            command.getExplainPlan(explainCtx);
                        }
                    });
        }
    }

    @Test
    public void testRefreshMtmvExplainBuildsRefreshCommandOnlyOnce() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        MTMVRefreshInfo refreshInfo = Mockito.mock(MTMVRefreshInfo.class);
        Mockito.when(refreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.INCREMENTAL);
        Mockito.when(mtmv.getRefreshInfo()).thenReturn(refreshInfo);
        InsertIntoTableCommand insertCommand = Mockito.mock(InsertIntoTableCommand.class);
        Mockito.when(insertCommand.getExplainPlan(Mockito.any())).thenReturn(insertCommand);
        Optional<NereidsPlanner> planner = Optional.of(Mockito.mock(NereidsPlanner.class));
        Mockito.when(insertCommand.getExplainPlanner(Mockito.eq(insertCommand), Mockito.any())).thenReturn(planner);
        CountingRefreshMTMVCommand command =
                new CountingRefreshMTMVCommand(newRefreshInfo(RefreshMode.INCREMENTAL), insertCommand);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            ConnectContext explainCtx = command.getExplainConnectContext(new ConnectContext());
            Plan explainPlan = command.getExplainPlan(explainCtx);
            Optional<NereidsPlanner> explainPlanner =
                    command.getExplainPlanner((LogicalPlan) explainPlan, explainCtx.getStatementContext());
            Assertions.assertSame(insertCommand, explainPlan);
            Assertions.assertSame(planner, explainPlanner);
        }

        Assertions.assertEquals(1, command.createRefreshCommandCount);
    }

    @Test
    public void testGetExplainPlanCompleteSuccess() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(false);
        MTMVRefreshInfo refreshInfo = Mockito.mock(MTMVRefreshInfo.class);
        Mockito.when(refreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.COMPLETE);
        Mockito.when(mtmv.getRefreshInfo()).thenReturn(refreshInfo);
        LogicalPlan completePlan = Mockito.mock(UpdateMvByPartitionCommand.class);
        Mockito.when(((UpdateMvByPartitionCommand) completePlan).getExplainPlan(Mockito.any())).thenReturn(completePlan);
        Mockito.when(((UpdateMvByPartitionCommand) completePlan).getExplainPlanner(Mockito.eq(completePlan), Mockito.any()))
                .thenReturn(Optional.empty());
        CompleteRefreshMTMVCommand command =
                new CompleteRefreshMTMVCommand(newRefreshInfo(RefreshMode.COMPLETE), completePlan);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            ConnectContext explainCtx = command.getExplainConnectContext(new ConnectContext());
            Plan explainPlan = command.getExplainPlan(explainCtx);
            Assertions.assertSame(completePlan, explainPlan);
        }
    }

    @Test
    public void testGetExplainPlanPartitionsFails() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(false);
        MTMVRefreshInfo refreshInfo = Mockito.mock(MTMVRefreshInfo.class);
        Mockito.when(refreshInfo.getRefreshMethod()).thenReturn(RefreshMethod.PARTITIONS);
        Mockito.when(mtmv.getRefreshInfo()).thenReturn(refreshInfo);
        TestRefreshMTMVCommand command = new TestRefreshMTMVCommand(
                newRefreshInfo(RefreshMode.PARTITIONS), Mockito.mock(IvmRefreshManager.class));

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            ConnectContext explainCtx = command.getExplainConnectContext(new ConnectContext());
            Assertions.assertThrows(AnalysisException.class,
                    new Executable() {
                        @Override
                        public void execute() throws Throwable {
                            command.getExplainPlan(explainCtx);
                        }
                    });
        }
    }

    private ExplainCommand extractExplainCommand(String sql) {
        LogicalPlan plan = extractLogicalPlan(sql);
        Assertions.assertInstanceOf(ExplainCommand.class, plan);
        ExplainCommand explain = (ExplainCommand) plan;
        Assertions.assertInstanceOf(RefreshMTMVCommand.class, explain.getLogicalPlan());
        return explain;
    }

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

    private RefreshMTMVInfo newRefreshInfo(RefreshMode refreshMode) {
        return new RefreshMTMVInfo(
                new TableNameInfo("internal", "db1", "mv1"), Collections.emptyList(), refreshMode);
    }

    private static class TestRefreshMTMVCommand extends RefreshMTMVCommand {
        private final IvmRefreshManager manager;

        private TestRefreshMTMVCommand(RefreshMTMVInfo info, IvmRefreshManager manager) {
            super(info);
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
    }

    private static class CompleteRefreshMTMVCommand extends RefreshMTMVCommand {
        private final LogicalPlan completePlan;

        private CompleteRefreshMTMVCommand(RefreshMTMVInfo info, LogicalPlan completePlan) {
            super(info);
            this.completePlan = completePlan;
        }

        @Override
        protected ConnectContext createExplainConnectContext(MTMV mtmv) {
            return new ConnectContext();
        }

        @Override
        protected LogicalPlan createRefreshCommand(MTMV mtmv, StatementContext statementContext) {
            return completePlan;
        }
    }

    private static class RecordingRefreshMTMVCommand extends RefreshMTMVCommand {
        private final NereidsPlanner planner;
        private ConnectContext explainCtx;
        private ConnectContext explainPlanCtx;

        private RecordingRefreshMTMVCommand(NereidsPlanner planner) {
            super(new RefreshMTMVInfo(
                    new TableNameInfo("internal", "db1", "mv1"), Collections.emptyList(), RefreshMode.INCREMENTAL));
            this.planner = planner;
        }

        @Override
        public ConnectContext getExplainConnectContext(ConnectContext ctx) {
            explainCtx = new ConnectContext();
            explainCtx.setThreadLocalInfo();
            StatementContext statementContext = new StatementContext(explainCtx, null);
            explainCtx.setStatementContext(statementContext);
            statementContext.setConnectContext(explainCtx);
            return explainCtx;
        }

        @Override
        public Plan getExplainPlan(ConnectContext ctx) {
            explainPlanCtx = ctx;
            return Mockito.mock(LogicalPlan.class);
        }

        @Override
        public Optional<NereidsPlanner> getExplainPlanner(LogicalPlan logicalPlan, StatementContext ctx) {
            return Optional.of(planner);
        }
    }

    private static class ThrowingExplainContextRefreshMTMVCommand extends RefreshMTMVCommand {
        private ThrowingExplainContextRefreshMTMVCommand() {
            super(new RefreshMTMVInfo(
                    new TableNameInfo("internal", "db1", "mv1"), Collections.emptyList(), RefreshMode.INCREMENTAL));
        }

        @Override
        public ConnectContext getExplainConnectContext(ConnectContext ctx) {
            ConnectContext explainCtx = new ConnectContext();
            explainCtx.setThreadLocalInfo();
            throw new IllegalStateException("getExplainConnectContext failed");
        }
    }

    private static class ThrowingExplainPlanRefreshMTMVCommand extends RefreshMTMVCommand {
        private ThrowingExplainPlanRefreshMTMVCommand() {
            super(new RefreshMTMVInfo(
                    new TableNameInfo("internal", "db1", "mv1"), Collections.emptyList(), RefreshMode.INCREMENTAL));
        }

        @Override
        public ConnectContext getExplainConnectContext(ConnectContext ctx) {
            ConnectContext explainCtx = new ConnectContext();
            explainCtx.setThreadLocalInfo();
            StatementContext statementContext = new StatementContext(explainCtx, null);
            explainCtx.setStatementContext(statementContext);
            statementContext.setConnectContext(explainCtx);
            return explainCtx;
        }

        @Override
        public Plan getExplainPlan(ConnectContext ctx) {
            throw new IllegalStateException("getExplainPlan failed");
        }
    }

    private static class CountingRefreshMTMVCommand extends RefreshMTMVCommand {
        private final InsertIntoTableCommand insertCommand;
        private int createRefreshCommandCount;

        private CountingRefreshMTMVCommand(RefreshMTMVInfo info, InsertIntoTableCommand insertCommand) {
            super(info);
            this.insertCommand = insertCommand;
        }

        @Override
        protected ConnectContext createExplainConnectContext(MTMV mtmv) {
            return new ConnectContext();
        }

        @Override
        protected LogicalPlan createRefreshCommand(MTMV mtmv, StatementContext statementContext) {
            createRefreshCommandCount++;
            return insertCommand;
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
                .thenThrow(new MetaNotFoundException("mv not found"));
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return mockedEnv;
    }
}
