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

package org.apache.doris.nereids.trees.plans.commands.execute;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.procedure.ConnectorRewriteGroup;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.commands.insert.RewriteTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.exception.JobException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class ConnectorRewriteGroupTaskResourceTest {

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void planningFailureClosesTaskStatementScopeAndWorkerContext() {
        ConnectorRewriteGroup group = Mockito.mock(ConnectorRewriteGroup.class);
        Mockito.when(group.getDataFilePaths()).thenReturn(Collections.emptySet());
        ExternalTable table = Mockito.mock(ExternalTable.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(table.getCatalog().getName()).thenReturn("catalog");
        Mockito.when(table.getDbName()).thenReturn("db");
        Mockito.when(table.getName()).thenReturn("table");
        ConnectContext taskContext = new ConnectContext();
        StatementContext statementContext = new StatementContext(taskContext, null);
        taskContext.setStatementContext(statementContext);
        AtomicBoolean scopeClosed = new AtomicBoolean();
        AtomicReference<Boolean> callbackObservedClosedScope = new AtomicReference<>();

        ConnectorRewriteGroupTask task = new ConnectorRewriteGroupTask(
                group, 1L, Mockito.mock(ConnectorTransaction.class), table, new ConnectContext(),
                new ConnectorRewriteGroupTask.RewriteResultCallback() {
                    @Override
                    public void onTaskCompleted(Long taskId) {
                        callbackObservedClosedScope.set(scopeClosed.get());
                    }

                    @Override
                    public void onTaskFailed(Long taskId, Exception error) {
                        callbackObservedClosedScope.set(scopeClosed.get());
                    }
                }) {
            @Override
            protected ConnectContext buildConnectContext() {
                taskContext.setThreadLocalInfo();
                return taskContext;
            }

            @Override
            protected void executeGroup(ConnectContext context, RewriteTableCommand command,
                    StatementBase parsedStatement) throws Exception {
                ConnectorStatementScope scope = statementContext.getOrCreateConnectorStatementScope();
                scope.computeIfAbsent("iceberg-table", () -> (AutoCloseable) () -> scopeClosed.set(true));
                throw new IllegalStateException("sink planning failed after beginWrite");
            }
        };

        Assertions.assertThrows(JobException.class, task::execute);
        Assertions.assertTrue(scopeClosed.get());
        Assertions.assertEquals(Boolean.TRUE, callbackObservedClosedScope.get());
        Assertions.assertNull(ConnectContext.get());
    }
}
