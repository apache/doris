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

package org.apache.doris.qe;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryStatistics;
import org.apache.doris.thrift.TQueryStatisticsResult;
import org.apache.doris.thrift.TReportWorkloadRuntimeStatusParams;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class StmtExecutorInternalQueryTest {
    @Test
    public void testEmbeddedShowAuditUsesActualExecutionIdForBeStatistics() throws Exception {
        String showSql = "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'";
        ConnectContext ctx = new ConnectContext();
        ctx.setStartTime();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        TUniqueId showId = new TUniqueId(1, 2);
        ctx.setQueryId(showId);
        StmtExecutor executor = new StmtExecutor(ctx, showSql);
        ctx.setExecutor(executor);
        NereidsParser parser = new NereidsParser();
        LogicalPlanAdapter showStmt = new LogicalPlanAdapter(parser.parseSingle(showSql), ctx.getStatementContext());
        executor.setParsedStmt(showStmt);

        Env env = Env.getCurrentEnv();
        WorkloadRuntimeStatusMgr originalMgr = env.getWorkloadRuntimeStatusMgr();
        AuditEventProcessor originalProcessor = env.getAuditEventProcessor();
        int originalAuditTimeout = Config.query_audit_log_timeout_ms;
        WorkloadRuntimeStatusMgr mgr = new WorkloadRuntimeStatusMgr();
        AuditEventProcessor processor = Mockito.mock(AuditEventProcessor.class);
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        TUniqueId[] executionId = new TUniqueId[1];
        Mockito.when(factory.createCoordinator(Mockito.eq(ctx), Mockito.any(NereidsPlanner.class), Mockito.any()))
                .thenAnswer(invocation -> {
                    executionId[0] = ctx.queryId().deepCopy();
                    Coordinator coordinator = Mockito.mock(Coordinator.class);
                    Mockito.when(coordinator.getQueryOptions()).thenReturn(new TQueryOptions());
                    Mockito.when(coordinator.getExecutionProfile())
                            .thenReturn(new ExecutionProfile(executionId[0], List.of(0)));
                    Mockito.when(coordinator.getDispatchedBackendIdsForAudit()).thenReturn(Set.of(10001L));
                    Mockito.when(coordinator.getNext()).thenReturn(new RowBatch());
                    return coordinator;
                });

        Deencapsulation.setField(env, "workloadRuntimeStatusMgr", mgr);
        Deencapsulation.setField(env, "auditEventProcessor", processor);
        Config.query_audit_log_timeout_ms = 10;
        try (MockedStatic<EnvFactory> factories = Mockito.mockStatic(EnvFactory.class);
                MockedConstruction<NereidsPlanner> planners = Mockito.mockConstruction(NereidsPlanner.class,
                        (planner, construction) -> Mockito.when(planner.handleQueryInFe(Mockito.any()))
                                .thenReturn(Optional.empty()))) {
            factories.when(EnvFactory::getInstance).thenReturn(factory);
            executor.executeEmbeddedQuery(parser.parseSingle("SELECT TABLE_NAME FROM information_schema.tables"));
            Assertions.assertNotNull(executionId[0]);
            Assertions.assertNotEquals(showId, executionId[0]);
            Assertions.assertEquals(showId, ctx.queryId());

            ctx.getState().setOk();
            AuditLogHelper.logAuditLog(ctx, showSql, showStmt, executor.getQueryStatisticsForAuditLog(), true);
            List<AuditEvent> events = Deencapsulation.getField(mgr, "queryAuditEventList");
            Assertions.assertEquals(1, events.size());
            AuditEvent event = events.get(0);
            Assertions.assertEquals(DebugUtil.printId(showId), event.queryId);
            Assertions.assertEquals("SHOW", event.stmtType);
            Assertions.assertEquals(DebugUtil.printId(executionId[0]), event.getStatisticsQueryId());

            TQueryStatistics statistics = new TQueryStatistics();
            statistics.setScanRows(17);
            statistics.setScanBytes(19);
            statistics.setCpuMs(23);
            statistics.setMaxPeakMemoryBytes(29);
            TQueryStatisticsResult result = new TQueryStatisticsResult();
            result.setStatistics(statistics);
            result.setQueryFinished(true);
            TReportWorkloadRuntimeStatusParams report = new TReportWorkloadRuntimeStatusParams();
            report.setBackendId(10001L);
            report.setQueryStatisticsResultMap(Map.of(DebugUtil.printId(executionId[0]), result));
            mgr.updateBeQueryStats(report);
            event.pushToAuditLogQueueTime = System.currentTimeMillis() - 20;
            Deencapsulation.invoke(mgr, "runAfterCatalogReady");

            Mockito.verify(processor).handleAuditEvent(event);
            Assertions.assertEquals(17, event.scanRows);
            Assertions.assertEquals(19, event.scanBytes);
            Assertions.assertEquals(23, event.cpuTimeMs);
            Assertions.assertEquals(29, event.peakMemoryBytes);
            Assertions.assertTrue(((List<?>) Deencapsulation.getField(mgr, "queryAuditEventList")).isEmpty());
        } finally {
            Config.query_audit_log_timeout_ms = originalAuditTimeout;
            Deencapsulation.setField(env, "workloadRuntimeStatusMgr", originalMgr);
            Deencapsulation.setField(env, "auditEventProcessor", originalProcessor);
        }
    }

    @Test
    public void testSetSqlHash() {
        StmtExecutor executor = new StmtExecutor(new ConnectContext(), "select * from table1");
        try (MockedConstruction<NereidsPlanner> mocked = Mockito.mockConstruction(NereidsPlanner.class,
                (mock, context) -> {
                    Mockito.when(mock.getStatementContext()).thenReturn(executor.getContext().getStatementContext());
                    Mockito.doThrow(new RuntimeException()).when(mock).plan(
                            Mockito.any(StatementBase.class), Mockito.any(TQueryOptions.class));
                })) {
            try {
                executor.executeInternalQuery();
            } catch (Exception e) {
                // do nothing
            }
        }
        Assertions.assertEquals("a8ec30e5ad0820f8c5bd16a82a4491ca", executor.getContext().getSqlHash());
    }

    @Test
    public void testExecuteInternalQuerySetsErrorStateOnFailure() {
        // Regression test for CIR-20019: when the internal SQL execution throws,
        // ConnectContext state must be set to ERR so AuditLogHelper records the failure
        // instead of misleadingly logging State=OK with empty error message.
        ConnectContext ctx = new ConnectContext();
        StmtExecutor executor = new StmtExecutor(ctx, "select * from table1");
        try (MockedConstruction<NereidsPlanner> mocked = Mockito.mockConstruction(NereidsPlanner.class,
                (mock, context) -> {
                    Mockito.when(mock.getStatementContext()).thenReturn(ctx.getStatementContext());
                    Mockito.doThrow(new RuntimeException("mock plan failure"))
                            .when(mock).plan(Mockito.any(StatementBase.class), Mockito.any(TQueryOptions.class));
                })) {
            Assertions.assertThrows(RuntimeException.class, executor::executeInternalQuery);
        }
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
        Assertions.assertEquals(ErrorCode.ERR_INTERNAL_ERROR, ctx.getState().getErrorCode());
        Assertions.assertNotNull(ctx.getState().getErrorMessage());
        Assertions.assertTrue(ctx.getState().getErrorMessage().contains("mock plan failure"), "error message should mention root cause, got: " + ctx.getState().getErrorMessage());
        Assertions.assertTrue(ctx.getState().isInternal(), "internal query should be flagged as internal in audit state");
        Assertions.assertTrue(ctx.getState().isQuery(), "internal query should be flagged as query in audit state");
    }

    @Test
    public void testExecuteInternalQuerySubmitsErrorAuditEventOnFailure() {
        ConnectContext ctx = new ConnectContext();
        StmtExecutor executor = new StmtExecutor(ctx, "select * from table1");
        Env env = Env.getCurrentEnv();
        WorkloadRuntimeStatusMgr originalWorkloadRuntimeStatusMgr = env.getWorkloadRuntimeStatusMgr();
        WorkloadRuntimeStatusMgr workloadRuntimeStatusMgr = Mockito.mock(WorkloadRuntimeStatusMgr.class);
        ArgumentCaptor<AuditEvent> auditEventCaptor = ArgumentCaptor.forClass(AuditEvent.class);

        Deencapsulation.setField(env, "workloadRuntimeStatusMgr", workloadRuntimeStatusMgr);
        try {
            try (MockedConstruction<NereidsPlanner> mockedPlanner = Mockito.mockConstruction(NereidsPlanner.class,
                    (mock, context) -> {
                        Mockito.when(mock.getStatementContext()).thenReturn(ctx.getStatementContext());
                        Mockito.doThrow(new RuntimeException("mock plan failure"))
                                .when(mock).plan(Mockito.any(StatementBase.class), Mockito.any(TQueryOptions.class));
                    })) {
                Assertions.assertThrows(RuntimeException.class, executor::executeInternalQuery);
            }

            Mockito.verify(workloadRuntimeStatusMgr).submitFinishQueryToAudit(auditEventCaptor.capture());
        } finally {
            Deencapsulation.setField(env, "workloadRuntimeStatusMgr", originalWorkloadRuntimeStatusMgr);
        }

        AuditEvent auditEvent = auditEventCaptor.getValue();
        Assertions.assertEquals(AuditEvent.EventType.AFTER_QUERY, auditEvent.type);
        Assertions.assertEquals("ERR", auditEvent.state);
        Assertions.assertEquals(ErrorCode.ERR_INTERNAL_ERROR.getCode(), auditEvent.errorCode);
        Assertions.assertNotNull(auditEvent.errorMessage);
        Assertions.assertTrue(auditEvent.errorMessage.contains("mock plan failure"), "error message should mention root cause, got: " + auditEvent.errorMessage);
        Assertions.assertTrue(auditEvent.isInternal, "audit event should be marked as internal");
        Assertions.assertTrue(auditEvent.isQuery, "audit event should be marked as query");
        Assertions.assertTrue(auditEvent.isNereids, "audit event should be marked as nereids");
        Assertions.assertEquals("select * from table1", auditEvent.stmt);
        Assertions.assertEquals("a8ec30e5ad0820f8c5bd16a82a4491ca", auditEvent.sqlHash);
    }

    @Test
    public void testExecuteInternalQueryWithoutAuditSkipsEventOnFailure() {
        ConnectContext ctx = new ConnectContext();
        StmtExecutor executor = new StmtExecutor(ctx, "select * from table1");
        Env env = Env.getCurrentEnv();
        WorkloadRuntimeStatusMgr originalWorkloadRuntimeStatusMgr = env.getWorkloadRuntimeStatusMgr();
        WorkloadRuntimeStatusMgr workloadRuntimeStatusMgr = Mockito.mock(WorkloadRuntimeStatusMgr.class);

        Deencapsulation.setField(env, "workloadRuntimeStatusMgr", workloadRuntimeStatusMgr);
        try {
            try (MockedConstruction<NereidsPlanner> mockedPlanner = Mockito.mockConstruction(NereidsPlanner.class,
                    (mock, context) -> {
                        Mockito.when(mock.getStatementContext()).thenReturn(ctx.getStatementContext());
                        Mockito.doThrow(new RuntimeException("mock plan failure"))
                                .when(mock).plan(Mockito.any(StatementBase.class), Mockito.any(TQueryOptions.class));
                    })) {
                Assertions.assertThrows(RuntimeException.class, executor::executeInternalQueryWithoutAudit);
            }
            Mockito.verify(workloadRuntimeStatusMgr, Mockito.never())
                    .submitFinishQueryToAudit(Mockito.any(AuditEvent.class));
        } finally {
            Deencapsulation.setField(env, "workloadRuntimeStatusMgr", originalWorkloadRuntimeStatusMgr);
        }
        Assertions.assertEquals(QueryState.MysqlStateType.ERR, ctx.getState().getStateType());
    }

    @Test
    public void testEmbeddedQueryRestoresShowAuditContext() {
        ConnectContext ctx = new ConnectContext();
        StmtExecutor executor = Mockito.spy(new StmtExecutor(ctx,
                "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'"));
        NereidsParser parser = new NereidsParser();
        LogicalPlanAdapter showStmt = new LogicalPlanAdapter(
                parser.parseSingle("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'"), ctx.getStatementContext());
        executor.setParsedStmt(showStmt);
        TUniqueId showQueryId = new TUniqueId(1, 2);
        ctx.setQueryId(showQueryId);
        ctx.setSqlHash("show-hash");
        ctx.getState().setNereids(true);
        Mockito.doAnswer(invocation -> {
            Assertions.assertEquals(StmtType.SELECT,
                    ((LogicalPlanAdapter) executor.getParsedStmt()).getLogicalPlan().stmtType());
            ctx.setQueryId(new TUniqueId(3, 4));
            ctx.setSqlHash("select-hash");
            ctx.getState().setInternal(true);
            ctx.getState().setIsQuery(true);
            return Collections.emptyList();
        }).when(executor).executeInternalQueryWithoutAudit();

        executor.executeEmbeddedQuery(parser.parseSingle("SELECT 1"));

        Assertions.assertSame(showStmt, executor.getParsedStmt());
        Assertions.assertEquals(showQueryId, ctx.queryId());
        Assertions.assertEquals("show-hash", ctx.getSqlHash());
        Assertions.assertFalse(ctx.getState().isInternal());
        Assertions.assertFalse(ctx.getState().isQuery());
        Assertions.assertTrue(ctx.getState().isNereids());
    }

    @Test
    public void testEmbeddedQueryRestoresShowAuditContextOnFailure() {
        ConnectContext ctx = new ConnectContext();
        StmtExecutor executor = Mockito.spy(new StmtExecutor(ctx,
                "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'"));
        NereidsParser parser = new NereidsParser();
        LogicalPlanAdapter showStmt = new LogicalPlanAdapter(
                parser.parseSingle("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'"), ctx.getStatementContext());
        executor.setParsedStmt(showStmt);
        TUniqueId showQueryId = new TUniqueId(1, 2);
        ctx.setQueryId(showQueryId);
        ctx.setSqlHash("show-hash");
        Mockito.doAnswer(invocation -> {
            ctx.setQueryId(new TUniqueId(3, 4));
            ctx.setSqlHash("select-hash");
            ctx.getState().setInternal(true);
            ctx.getState().setIsQuery(true);
            throw new IllegalStateException("internal SELECT failed");
        }).when(executor).executeInternalQueryWithoutAudit();

        Assertions.assertThrows(IllegalStateException.class,
                () -> executor.executeEmbeddedQuery(parser.parseSingle("SELECT 1")));

        Assertions.assertSame(showStmt, executor.getParsedStmt());
        Assertions.assertEquals(showQueryId, ctx.queryId());
        Assertions.assertEquals("show-hash", ctx.getSqlHash());
        Assertions.assertFalse(ctx.getState().isInternal());
        Assertions.assertFalse(ctx.getState().isQuery());
    }
}
