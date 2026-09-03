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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.spi.ConnectorStatementScope;
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
    public void testCleanEveryMTMVExecutionContext() {
        MTMVTask task = new MTMVTask();
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicInteger closeCount = new AtomicInteger();
        ConnectContext taskContext = new ConnectContext();
        taskContext.setThreadLocalInfo();
        StatementContext taskStatementContext = new StatementContext();
        taskContext.setStatementContext(taskStatementContext);
        taskStatementContext.getOrCreateConnectorStatementScope().computeIfAbsent(
                "closeable", () -> (AutoCloseable) closeCount::incrementAndGet);
        try {
            for (int i = 0; i < 2; i++) {
                ConnectContext executionContext = new ConnectContext();
                executionContext.setThreadLocalInfo();
                StatementContext statementContext = new StatementContext();
                executionContext.setStatementContext(statementContext);
                TUniqueId queryId = new TUniqueId(2L, i + 1L);
                executionContext.setQueryId(queryId);
                statementContext.getOrCreateConnectorStatementScope().computeIfAbsent(
                        "closeable", () -> (AutoCloseable) closeCount::incrementAndGet);
                QeProcessorImpl.INSTANCE.registerQueryFinishCallback(
                        DebugUtil.printId(queryId), callbackCount::incrementAndGet);

                Deencapsulation.invoke(task, "closeExecutionContext", executionContext);

                Assert.assertSame(executionContext, ConnectContext.get());
            }
            Assert.assertEquals(2, callbackCount.get());
            Assert.assertEquals(2, closeCount.get());

            ConnectContext lastExecutionContext = ConnectContext.get();
            Deencapsulation.invoke(task, "closeExecutionContext", taskContext);
            Assert.assertSame(lastExecutionContext, ConnectContext.get());
            Assert.assertEquals(3, closeCount.get());

            Deencapsulation.invoke(task, "closeExecutionContext", new ConnectContext());
            Assert.assertSame(lastExecutionContext, ConnectContext.get());
        } finally {
            taskStatementContext.close();
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
