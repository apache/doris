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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;
import org.apache.doris.thrift.TQueryOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class StmtExecutorInternalQueryTest {
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
}
