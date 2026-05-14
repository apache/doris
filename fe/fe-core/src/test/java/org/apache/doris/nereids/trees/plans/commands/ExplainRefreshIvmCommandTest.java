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
import org.apache.doris.mtmv.ivm.IvmRefreshExplainResult;
import org.apache.doris.mtmv.ivm.IvmRefreshManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSet;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class ExplainRefreshIvmCommandTest {
    private final NereidsParser parser = new NereidsParser();

    @Test
    public void testParseExplainRefreshIncrementalOverview() {
        LogicalPlan plan = extractLogicalPlan("EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL");

        Assertions.assertInstanceOf(ExplainRefreshIvmCommand.class, plan);
        ExplainRefreshIvmCommand explain = (ExplainRefreshIvmCommand) plan;
        Assertions.assertEquals(ExplainLevel.NORMAL, explain.getLevel());
        Assertions.assertNull(explain.getDeltaId());
    }

    @Test
    public void testParseExplainRefreshForDelta() {
        LogicalPlan plan = extractLogicalPlan(
                "EXPLAIN LOGICAL PLAN REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL FOR DELTA 1");

        Assertions.assertInstanceOf(ExplainRefreshIvmCommand.class, plan);
        ExplainRefreshIvmCommand explain = (ExplainRefreshIvmCommand) plan;
        Assertions.assertEquals(ExplainLevel.REWRITTEN_PLAN, explain.getLevel());
        Assertions.assertEquals(1, explain.getDeltaId());
    }

    @Test
    public void testParseForDeltaWithoutExplainFails() {
        assertParseFails("REFRESH MATERIALIZED VIEW db1.mv1 INCREMENTAL FOR DELTA 1");
    }

    @Test
    public void testParseExplainRefreshCompleteFails() {
        assertParseFails("EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1 COMPLETE");
    }

    @Test
    public void testParseExplainRefreshWithoutIncrementalFails() {
        assertParseFails("EXPLAIN REFRESH MATERIALIZED VIEW db1.mv1");
    }

    @Test
    public void testRunExplainRefreshOverviewSuccess() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(true);
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        IvmRefreshExplainResult result = mockExplainResult();
        Mockito.when(manager.explainRefresh(mtmv)).thenReturn(result);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            newExplainCommand(RefreshMode.INCREMENTAL, ExplainLevel.NORMAL, null, manager)
                    .run(new ConnectContext(), executor);
        }

        Mockito.verify(manager).explainRefresh(mtmv);
        Mockito.verify(executor).sendResultSet(Mockito.any(ResultSet.class));
    }

    @Test
    public void testRunExplainRefreshMissingMvFails() throws Exception {
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMissingMtmv()) {
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> newExplainCommand(RefreshMode.INCREMENTAL, ExplainLevel.NORMAL, null, manager)
                            .run(new ConnectContext(), executor));
        }

        Mockito.verify(manager, Mockito.never()).explainRefresh(Mockito.any());
        Mockito.verify(executor, Mockito.never()).sendResultSet(Mockito.any(ResultSet.class));
    }

    @Test
    public void testRunExplainRefreshNonIvmMvFails() throws Exception {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.isIvm()).thenReturn(false);
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);

        try (MockedStatic<Env> mockedEnv = mockEnvWithMtmv(mtmv)) {
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> newExplainCommand(RefreshMode.INCREMENTAL, ExplainLevel.NORMAL, null, manager)
                            .run(new ConnectContext(), executor));
        }

        Mockito.verify(manager, Mockito.never()).explainRefresh(Mockito.any());
        Mockito.verify(executor, Mockito.never()).sendResultSet(Mockito.any(ResultSet.class));
    }

    @Test
    public void testRunExplainRefreshCompleteModeFailsBeforeMetadataAccess() throws Exception {
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> newExplainCommand(RefreshMode.COMPLETE, ExplainLevel.NORMAL, null, manager)
                        .run(new ConnectContext(), executor));

        Mockito.verify(manager, Mockito.never()).explainRefresh(Mockito.any());
        Mockito.verify(executor, Mockito.never()).sendResultSet(Mockito.any(ResultSet.class));
    }

    @Test
    public void testRunExplainLogicalRefreshRequiresDelta() throws Exception {
        IvmRefreshManager manager = Mockito.mock(IvmRefreshManager.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> newExplainCommand(RefreshMode.INCREMENTAL, ExplainLevel.REWRITTEN_PLAN, null, manager)
                        .run(new ConnectContext(), executor));

        Mockito.verify(manager, Mockito.never()).explainRefresh(Mockito.any());
        Mockito.verify(executor, Mockito.never()).sendResultSet(Mockito.any(ResultSet.class));
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

    private ExplainRefreshIvmCommand newExplainCommand(RefreshMode refreshMode, ExplainLevel level,
            Integer deltaId, IvmRefreshManager manager) {
        RefreshMTMVInfo info = new RefreshMTMVInfo(
                new TableNameInfo("internal", "db1", "mv1"), Collections.emptyList(), refreshMode);
        return new ExplainRefreshIvmCommand(info, level, false, deltaId) {
            @Override
            IvmRefreshManager createIvmRefreshManager() {
                return manager;
            }
        };
    }

    private IvmRefreshExplainResult mockExplainResult() {
        Plan normalizedPlan = Mockito.mock(Plan.class);
        Mockito.when(normalizedPlan.treeString()).thenReturn("normalized plan");
        return new IvmRefreshExplainResult(normalizedPlan, ImmutableList.of());
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
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return mockedEnv;
    }
}
