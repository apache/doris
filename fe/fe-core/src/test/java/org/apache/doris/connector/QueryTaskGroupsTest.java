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

package org.apache.doris.connector;

import org.apache.doris.common.util.FairTaskExecutor;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.RejectedExecutionException;

class QueryTaskGroupsTest {
    @Test
    void cancellationBeforeFirstSchedulerAcquisitionDoesNotCreateAGroup() {
        ConnectorStatementScopeImpl scope = new ConnectorStatementScopeImpl();
        scope.computeIfAbsent(QueryTaskGroups.SCOPE_KEY, () -> new QueryTaskGroups(stage -> {
            throw new AssertionError("a cancelled query must not create a task group");
        }));
        ConnectorSession session = ConnectorSessionBuilder.create().withQueryId("cancel-before-acquisition")
                .withCatalogId(1).withStatementScope(scope).build();
        scope.closeAll();
        for (ConnectorTaskScheduler.Stage stage : ConnectorTaskScheduler.Stage.values()) {
            Assertions.assertThrows(CancellationException.class, () -> session.getTaskScheduler(stage));
        }
    }

    @Test
    void scansAndCatalogsShareBothGroupsAndCannotResurrectFinishedQuery() {
        try (FairTaskExecutor split = new FairTaskExecutor(1, Thread::new);
                FairTaskExecutor metadata = new FairTaskExecutor(1, Thread::new)) {
            ConnectorStatementScopeImpl scope = new ConnectorStatementScopeImpl();
            scope.computeIfAbsent(QueryTaskGroups.SCOPE_KEY, () -> new QueryTaskGroups(
                    stage -> stage == ConnectorTaskScheduler.Stage.SPLIT ? split : metadata));
            ConnectorSession first = ConnectorSessionBuilder.create().withQueryId("q")
                    .withCatalogId(1).withStatementScope(scope).build();
            ConnectorSession second = ConnectorSessionBuilder.create().withQueryId("q")
                    .withCatalogId(2).withStatementScope(scope).build();
            for (ConnectorTaskScheduler.Stage stage : ConnectorTaskScheduler.Stage.values()) {
                Assertions.assertSame(first.getTaskScheduler(stage), second.getTaskScheduler(stage));
            }
            ConnectorTaskScheduler captured = first.getTaskScheduler(ConnectorTaskScheduler.Stage.SPLIT);
            Assertions.assertFalse(captured.isClosed());
            ConnectorTaskScheduler capturedMetadata = first.getTaskScheduler(ConnectorTaskScheduler.Stage.METADATA);
            Assertions.assertNotSame(captured, capturedMetadata);
            Assertions.assertFalse(capturedMetadata.isClosed());
            scope.closeAll();
            scope.closeAll();
            Assertions.assertTrue(captured.isClosed());
            Assertions.assertTrue(capturedMetadata.isClosed());
            Assertions.assertThrows(RejectedExecutionException.class,
                    () -> captured.execute(() -> Assertions.fail("finished query ran")));
            Assertions.assertThrows(RejectedExecutionException.class,
                    () -> captured.tryExecuteContinuation(() -> { }, failure -> Assertions.fail(failure), null));
            Assertions.assertThrows(CancellationException.class,
                    () -> second.getTaskScheduler(ConnectorTaskScheduler.Stage.METADATA));
        }
    }
}
