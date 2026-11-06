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

package org.apache.doris.nereids;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.ConnectorStatementScopeImpl;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class StatementContextCleanupTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void failedPlannerReleaseStillClosesOwnedExecutionResources(boolean deferred) throws Exception {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.isReturnResultFromLocal()).thenReturn(!deferred);
        StatementContext statement = new StatementContext(context, null);
        ConnectorStatementScope scope = statement.getOrCreateConnectorStatementScope();
        AutoCloseable resource = Mockito.mock(AutoCloseable.class);
        scope.computeIfAbsent("resource", () -> resource);
        ScanNode scan = Mockito.mock(ScanNode.class);
        statement.stopScanNodeAtClose(scan);
        if (deferred) {
            statement.handOverScanNodesToDeferredCoordinator(Collections.singletonList(scan));
        }
        RuntimeException failure = new IllegalStateException("unlock failed");
        TableIf table = addTable(statement, 1L, failure);
        statement.lock();

        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, statement::close));
        Mockito.verify(table).readUnlock();
        Mockito.verify(scan, Mockito.times(deferred ? 0 : 1)).stop();
        Mockito.verify(resource, Mockito.times(deferred ? 0 : 1)).close();
        statement.close();
        Mockito.verify(scan, Mockito.times(deferred ? 0 : 1)).stop();
        Mockito.verify(resource, Mockito.times(deferred ? 0 : 1)).close();
        if (deferred) {
            // The deferred coordinator owns these resources after handoff, even when statement close fails.
            scan.stop();
            scope.closeAll();
            Mockito.verify(scan).stop();
            Mockito.verify(resource).close();
        }
    }

    @Test
    public void plannerReleaseRetainsLaterUnlockFailuresAndAttemptsEveryTable() {
        StatementContext statement = new StatementContext(null, null);
        RuntimeException first = new IllegalStateException("second table unlock failed");
        RuntimeException later = new IllegalStateException("first table unlock failed");
        TableIf firstTable = addTable(statement, 1L, later);
        TableIf secondTable = addTable(statement, 2L, first);
        TableIf healthyTable = addTable(statement, 0L, null);
        statement.lock();

        Assertions.assertSame(first, Assertions.assertThrows(RuntimeException.class, statement::close));
        Assertions.assertArrayEquals(new Throwable[] {later}, first.getSuppressed());
        Mockito.verify(firstTable).readUnlock();
        Mockito.verify(secondTable).readUnlock();
        Mockito.verify(healthyTable).readUnlock();
        statement.close();
        Mockito.verify(healthyTable).readUnlock();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void executionCleanupPreservesPrimaryFailure(boolean failPlanner) {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.isReturnResultFromLocal()).thenReturn(true);
        StatementContext statement = new StatementContext(context, null);
        RuntimeException plannerFailure = new IllegalStateException("unlock failed");
        RuntimeException executionFailure = new IllegalStateException("connector close failed");
        try (MockedConstruction<ConnectorStatementScopeImpl> scopes = Mockito.mockConstruction(
                ConnectorStatementScopeImpl.class, (scope, construction) -> {
                    Mockito.doThrow(executionFailure).when(scope).closeAll();
                })) {
            statement.getOrCreateConnectorStatementScope();
            addTable(statement, 1L, failPlanner ? plannerFailure : null);
            statement.lock();
            RuntimeException caught = Assertions.assertThrows(RuntimeException.class, statement::close);
            Assertions.assertSame(failPlanner ? plannerFailure : executionFailure, caught);
            Assertions.assertArrayEquals(failPlanner ? new Throwable[] {executionFailure} : new Throwable[0],
                    caught.getSuppressed());
            Mockito.verify(scopes.constructed().get(0)).closeAll();
        }
    }

    private TableIf addTable(StatementContext statement, long id, RuntimeException unlockFailure) {
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getId()).thenReturn(id);
        Mockito.when(table.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(table.tryReadLock(Mockito.anyLong(), Mockito.any(TimeUnit.class))).thenReturn(true);
        Mockito.when(table.getNameWithFullQualifiers()).thenReturn("internal.db.t" + id);
        if (unlockFailure != null) {
            Mockito.doThrow(unlockFailure).when(table).readUnlock();
        }
        statement.getTables().put(Arrays.asList("internal", "db", "t" + id), table);
        return table;
    }
}
