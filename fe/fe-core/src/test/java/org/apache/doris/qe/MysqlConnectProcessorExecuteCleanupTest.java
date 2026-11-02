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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A COM_STMT_EXECUTE ends with what COM_QUERY's per-statement finally does: the query-finish callbacks of the
 * execution run and its connector scope closes, also when the execution fails after planning.
 */
public class MysqlConnectProcessorExecuteCleanupTest {

    @Test
    public void testExecuteRejectedAfterPlanningRunsItsQueryFinishCallbacks() throws Exception {
        // PluginDrivenScanNode.getSplits registers a query-finish callback while planning; a scan-level SQL block
        // rule then rejects the plan, so the execution never reaches unregisterQuery.
        ConnectContext context = newContext();
        PreparedStatementContext prepared = newPrepared(context);
        TUniqueId queryId = new TUniqueId(31L, 1L);
        AtomicInteger callbackRuns = new AtomicInteger();
        CloseCounter scopeValue = new CloseCounter();
        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                (executor, init) -> Mockito.doAnswer(invocation -> {
                    ConnectorStatementScope scope = startExecution(context, prepared, queryId, scopeValue);
                    QeProcessorImpl.INSTANCE.registerQueryFinishCallback(DebugUtil.printId(queryId), () -> {
                        callbackRuns.incrementAndGet();
                        scope.closeAll();
                    });
                    throw new AnalysisException("sql hits sql block rule");
                }).when(executor).execute());
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            execute(context, prepared);

            Assertions.assertEquals(MysqlStateType.ERR, context.getState().getStateType());
            Assertions.assertEquals(1, callbackRuns.get(), "the failed execution's query-finish callback must run");
            Assertions.assertEquals(1, scopeValue.closes.get(), "the failed execution's connector scope must close");
        } finally {
            // Keep the process-wide registry clean even when an assertion above failed.
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }
    }

    @Test
    public void testExecuteFailingBeforeAnyScanClosesItsConnectorScope() throws Exception {
        // A connector opened statement-scoped state during analysis and the execution failed before a scan
        // registered a query-finish callback: only the end of the execution can close the scope.
        ConnectContext context = newContext();
        PreparedStatementContext prepared = newPrepared(context);
        TUniqueId queryId = new TUniqueId(31L, 2L);
        CloseCounter scopeValue = new CloseCounter();
        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                (executor, init) -> Mockito.doAnswer(invocation -> {
                    startExecution(context, prepared, queryId, scopeValue);
                    throw new AnalysisException("analysis failed");
                }).when(executor).execute());
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            execute(context, prepared);

            Assertions.assertEquals(1, scopeValue.closes.get(), "the failed execution's connector scope must close");
        }
    }

    @Test
    public void testSuccessfulExecuteClosesItsScopeOnce() throws Exception {
        // finalizeQuery unregisters a finished execution, which runs its callbacks and closes its scope; the end of
        // the execution must then neither run the callbacks again nor close a value twice.
        ConnectContext context = newContext();
        PreparedStatementContext prepared = newPrepared(context);
        TUniqueId queryId = new TUniqueId(31L, 3L);
        AtomicInteger callbackRuns = new AtomicInteger();
        CloseCounter scopeValue = new CloseCounter();
        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                (executor, init) -> Mockito.doAnswer(invocation -> {
                    ConnectorStatementScope scope = startExecution(context, prepared, queryId, scopeValue);
                    QeProcessorImpl.INSTANCE.registerQueryFinishCallback(DebugUtil.printId(queryId), () -> {
                        callbackRuns.incrementAndGet();
                        scope.closeAll();
                    });
                    QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
                    return null;
                }).when(executor).execute());
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class)) {
            execute(context, prepared);

            Assertions.assertEquals(1, callbackRuns.get());
            Assertions.assertEquals(1, scopeValue.closes.get());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }
    }

    private static ConnectContext newContext() {
        ConnectContext context = new ConnectContext(null, false);
        context.setCommand(MysqlCommand.COM_STMT_EXECUTE);
        return context;
    }

    private static PreparedStatementContext newPrepared(ConnectContext context) {
        PrepareCommand command = Mockito.mock(PrepareCommand.class);
        Mockito.when(command.getOriginalStmt()).thenReturn(new OriginStatement("select 1", 0));
        return new PreparedStatementContext(
                command, context, new StatementContext(context, new OriginStatement("select 1", 0)), "select 1");
    }

    private static void execute(ConnectContext context, PreparedStatementContext prepared) {
        ByteBuffer packet = ByteBuffer.allocate(9);
        packet.position(packet.limit()); // COM_STMT_EXECUTE header consumed, no parameter payload.
        new MysqlConnectProcessor(context).handleExecute(prepared.command, 7, prepared, packet, null);
    }

    /** What ExecuteCommand.run and StmtExecutor set up for one execution, with a closeable value in its scope. */
    private static ConnectorStatementScope startExecution(ConnectContext context, PreparedStatementContext prepared,
            TUniqueId queryId, CloseCounter scopeValue) {
        StatementContext execution = prepared.nextStatementContext();
        context.setStatementContext(execution);
        context.setQueryId(queryId);
        ConnectorStatementScope scope = execution.getOrCreateConnectorStatementScope();
        scope.computeIfAbsent("metadata", () -> scopeValue);
        return scope;
    }

    private static final class CloseCounter implements AutoCloseable {
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }
}
