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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.procedure.ConnectorRewriteGroup;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.exception.JobException;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guards the cancellation/publication handoff of {@link ConnectorRewriteGroupTask}. The distributed write
 * path needs a live cluster, but the handoff itself is unit-testable: a queued task must report its
 * cancellation through the collector, and a cancel that lands after the executor is published must reach it.
 */
public class ConnectorRewriteGroupTaskTest {

    private ConnectorRewriteGroupTask newTask(
            AtomicBoolean completed,
            AtomicReference<Long> failedId,
            AtomicReference<Exception> failure) {
        ConnectorRewriteGroup group = new ConnectorRewriteGroup(
                ImmutableSet.of("s3://bucket/table/a.parquet"), 1, 1024L, 0);
        return new ConnectorRewriteGroupTask(
                group,
                7L,
                Mockito.mock(ConnectorTransaction.class),
                Mockito.mock(ExternalTable.class),
                Mockito.mock(ConnectContext.class),
                new ConnectorRewriteGroupTask.RewriteResultCallback() {
                    @Override
                    public void onTaskCompleted(Long taskId) {
                        completed.set(true);
                    }

                    @Override
                    public void onTaskFailed(Long taskId, Exception error) {
                        failedId.set(taskId);
                        failure.set(error);
                    }
                });
    }

    @Test
    public void planningFailureClosesPrivateContextAndRestoresWorker() {
        boolean savedUnitTest = FeConstants.runningUnitTest;
        ConnectContext previous = ConnectContext.get();
        FeConstants.runningUnitTest = true;
        try (MockedStatic<Env> environment = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
            InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
            Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(catalogs.getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME)).thenReturn(catalog);
            environment.when(Env::getCurrentEnv).thenReturn(env);
            ConnectContext parent = new ConnectContext();
            parent.setThreadLocalInfo();
            AtomicReference<ConnectContext> execution = new AtomicReference<>();
            ExternalTable table = Mockito.mock(ExternalTable.class);
            Mockito.when(table.getCatalog()).thenAnswer(call -> {
                execution.set(ConnectContext.get());
                throw new IllegalStateException("planning failed");
            });
            ConnectorRewriteGroupTask task = new ConnectorRewriteGroupTask(
                    new ConnectorRewriteGroup(ImmutableSet.of("s3://bucket/table/a.parquet"), 1, 1024L, 0),
                    7L, Mockito.mock(ConnectorTransaction.class), table, parent, null);
            Assertions.assertThrows(JobException.class, task::execute);
            Assertions.assertSame(parent, ConnectContext.get());
            Assertions.assertFalse(parent.isClosingOrClosed());
            Assertions.assertNotNull(execution.get());
            Assertions.assertNotSame(parent, execution.get());
            Assertions.assertTrue(execution.get().isClosingOrClosed());
            Assertions.assertNull(execution.get().getStatementContext());
            parent.cleanup();
        } finally {
            FeConstants.runningUnitTest = savedUnitTest;
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void databaseSetupFailureClosesUnpublishedContext() {
        ConnectContext parent = Mockito.mock(ConnectContext.class);
        Mockito.when(parent.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(parent.getDatabase()).thenReturn("db");
        ConnectorRewriteGroupTask task = new ConnectorRewriteGroupTask(
                new ConnectorRewriteGroup(ImmutableSet.of("s3://bucket/table/a.parquet"), 1, 1024L, 0),
                7L, Mockito.mock(ConnectorTransaction.class), Mockito.mock(ExternalTable.class), parent, null);
        ConnectContext previous = ConnectContext.get();
        try (MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class,
                (context, construction) -> Mockito.doThrow(new IllegalStateException("database setup failed"))
                        .when(context).setDatabase("db"))) {
            Assertions.assertThrows(JobException.class, task::execute);
            Assertions.assertEquals(1, contexts.constructed().size());
            Mockito.verify(contexts.constructed().get(0)).cleanup();
            Mockito.verify(parent, Mockito.never()).cleanup();
            Assertions.assertSame(previous, ConnectContext.get());
        }
    }

    @Test
    public void queuedCancellationIsReportedToTheCollector() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Long> failedId = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ConnectorRewriteGroupTask task = newTask(completed, failedId, failure);

        // Cancelled while still queued: the scheduler may still invoke execute().
        task.cancel();
        Assertions.assertThrows(JobException.class, task::execute);

        Assertions.assertFalse(completed.get(), "a cancelled task must never report completion");
        Assertions.assertEquals(task.getId(), failedId.get(), "the collector must be told this task failed");
        Assertions.assertNotNull(failure.get());
    }

    @Test
    public void cancelAfterExecutorPublicationCancelsTheExecutor() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Long> failedId = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ConnectorRewriteGroupTask task = newTask(completed, failedId, failure);

        // Simulate a running task whose executor has been published.
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
        Deencapsulation.setField(task, "stmtExecutor", stmtExecutor);

        task.cancel();

        Mockito.verify(stmtExecutor).cancel(Mockito.any(Status.class));
        Assertions.assertFalse(completed.get());
    }

    @Test
    public void cancelAfterFinishIsIgnored() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicReference<Long> failedId = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        ConnectorRewriteGroupTask task = newTask(completed, failedId, failure);
        ((AtomicBoolean) Deencapsulation.getField(task, "isFinished")).set(true);
        StmtExecutor stmtExecutor = Mockito.mock(StmtExecutor.class);
        Deencapsulation.setField(task, "stmtExecutor", stmtExecutor);

        task.cancel();

        Mockito.verify(stmtExecutor, Mockito.never()).cancel(Mockito.any(Status.class));
    }
}
