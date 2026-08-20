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

package org.apache.doris.job.executor;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TUniqueId;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskProcessorTest {
    @Test
    public void testReleaseStatementScopeAfterInsertTask() throws Exception {
        assertReleaseStatementScope(true);
    }

    @Test
    public void testReleaseStatementScopeAfterMTMVTask() throws Exception {
        assertReleaseStatementScope(false);
    }

    @Test
    public void testRemoveContextWhenStatementCloseFails() {
        TaskProcessor taskProcessor = new TaskProcessor(1, 1, Thread::new);
        ConnectContext connectContext = new ConnectContext();
        connectContext.setStatementContext(new StatementContext() {
            @Override
            public void close() {
                throw new RuntimeException("injected close failure");
            }
        });
        connectContext.setThreadLocalInfo();
        try {
            Deencapsulation.invoke(taskProcessor, "closeTaskContext");
            Assert.fail("closeTaskContext should propagate the close failure");
        } catch (RuntimeException e) {
            Assert.assertEquals("injected close failure", e.getMessage());
        } finally {
            Assert.assertNull(ConnectContext.get());
            taskProcessor.shutdown();
        }
    }

    @Test
    public void testRunQueryFinishCallbacksForTaskContext() {
        TaskProcessor taskProcessor = new TaskProcessor(1, 1, Thread::new);
        ConnectContext connectContext = new ConnectContext();
        connectContext.setStatementContext(new StatementContext());
        TUniqueId queryId = new TUniqueId(1L, 1L);
        connectContext.setQueryId(queryId);
        connectContext.setThreadLocalInfo();
        AtomicInteger callbackCount = new AtomicInteger();
        QeProcessorImpl.INSTANCE.registerQueryFinishCallback(
                DebugUtil.printId(queryId), callbackCount::incrementAndGet);
        try {
            Deencapsulation.invoke(taskProcessor, "closeTaskContext");
            Assert.assertEquals(1, callbackCount.get());
            Assert.assertNull(ConnectContext.get());
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
            ConnectContext.remove();
            taskProcessor.shutdown();
        }
    }

    @Test
    public void testReleaseEveryMTMVExecutionContext() {
        AtomicInteger closeCount = new AtomicInteger();
        MTMVTask task = new MTMVTask();
        try {
            for (int i = 0; i < 2; i++) {
                ConnectContext connectContext = new ConnectContext();
                connectContext.setThreadLocalInfo();
                StatementContext statementContext = new StatementContext();
                connectContext.setStatementContext(statementContext);
                ConnectorStatementScope scope = statementContext.getOrCreateConnectorStatementScope();
                scope.computeIfAbsent("closeable", () -> (AutoCloseable) closeCount::incrementAndGet);

                Deencapsulation.invoke(task, "closeExecutionContext", connectContext);
                Assert.assertNull(ConnectContext.get());
            }
            Assert.assertEquals(2, closeCount.get());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testRestorePinnedSnapshotAndFinishCallbacksBetweenMTMVChunks() {
        MTMVTask task = new MTMVTask();
        ConnectContext taskContext = new ConnectContext();
        StatementContext taskStatementContext = new StatementContext();
        taskContext.setStatementContext(taskStatementContext);
        taskContext.setThreadLocalInfo();

        MvccTable table = Mockito.mock(MvccTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getName()).thenReturn("external_table");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn("external_db");
        Mockito.when(catalog.getName()).thenReturn("external_catalog");
        MvccSnapshot pinnedSnapshot = Mockito.mock(MvccSnapshot.class);
        Map<MvccTableInfo, MvccSnapshot> snapshots = new HashMap<>();
        snapshots.put(new MvccTableInfo(table), pinnedSnapshot);
        Deencapsulation.setField(task, "snapshots", snapshots);
        Deencapsulation.invoke(task, "installTaskSnapshots", taskStatementContext);

        AtomicInteger callbackCount = new AtomicInteger();
        try {
            for (int i = 0; i < 2; i++) {
                ConnectContext executionContext = new ConnectContext();
                executionContext.setStatementContext(new StatementContext());
                TUniqueId queryId = new TUniqueId(1L, i + 1L);
                executionContext.setQueryId(queryId);
                executionContext.setThreadLocalInfo();
                QeProcessorImpl.INSTANCE.registerQueryFinishCallback(
                        DebugUtil.printId(queryId), callbackCount::incrementAndGet);

                Deencapsulation.invoke(task, "closeExecutionContext", executionContext, taskContext);

                Assert.assertSame(taskContext, ConnectContext.get());
                Assert.assertSame(pinnedSnapshot,
                        MvccUtil.getSnapshotFromContext(table).orElse(null));
            }
            Assert.assertEquals(2, callbackCount.get());
        } finally {
            taskStatementContext.close();
            ConnectContext.remove();
        }
    }

    @Test
    public void testCreateMTMVTaskContextInstallsStatementContext() {
        MTMVTask task = new MTMVTask();
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getSessionVariables()).thenReturn(Collections.emptyMap());
        Deencapsulation.setField(task, "mtmv", mtmv);

        ConnectContext taskContext = Deencapsulation.invoke(task, "createTaskContext");
        try {
            Assert.assertSame(taskContext, ConnectContext.get());
            Assert.assertNotNull(taskContext.getStatementContext());
        } finally {
            taskContext.getStatementContext().close();
            ConnectContext.remove();
        }
    }

    private void assertReleaseStatementScope(boolean insertTask) throws Exception {
        AtomicInteger closeCount = new AtomicInteger();
        CountDownLatch scopeInstalled = new CountDownLatch(1);
        CountDownLatch workerReused = new CountDownLatch(1);
        TaskProcessor taskProcessor = new TaskProcessor(1, 1, Thread::new);
        try {
            AbstractTask externalTask;
            if (insertTask) {
                externalTask = new InsertTask("external-insert", null, null, null) {
                    @Override
                    public void runTask() {
                        installStatementScope(closeCount, scopeInstalled);
                    }
                };
            } else {
                externalTask = new MTMVTask() {
                    @Override
                    public void runTask() {
                        installStatementScope(closeCount, scopeInstalled);
                    }
                };
            }
            externalTask.setStatus(TaskStatus.PENDING);
            externalTask.setTaskId(1L);
            Assert.assertTrue(taskProcessor.addTask(externalTask));
            Assert.assertTrue(scopeInstalled.await(10, TimeUnit.SECONDS));

            AbstractTask nextTask = new InsertTask("next-task", null, null, null) {
                @Override
                public void runTask() {
                    Assert.assertNull(ConnectContext.get());
                    workerReused.countDown();
                }
            };
            nextTask.setStatus(TaskStatus.PENDING);
            nextTask.setTaskId(2L);
            Assert.assertTrue(taskProcessor.addTask(nextTask));

            Assert.assertTrue(workerReused.await(10, TimeUnit.SECONDS));
            Assert.assertEquals(1, closeCount.get());
        } finally {
            taskProcessor.shutdown();
        }
    }

    private static void installStatementScope(AtomicInteger closeCount, CountDownLatch scopeInstalled) {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        StatementContext statementContext = new StatementContext();
        connectContext.setStatementContext(statementContext);
        ConnectorStatementScope scope = statementContext.getOrCreateConnectorStatementScope();
        scope.computeIfAbsent("closeable", () -> (AutoCloseable) closeCount::incrementAndGet);
        scopeInstalled.countDown();
    }
}
