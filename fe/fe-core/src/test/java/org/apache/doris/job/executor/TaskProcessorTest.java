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
import org.apache.doris.datasource.ExternalScanTaskCacheKey;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TaskProcessorTest {
    @Test
    public void testReleaseExternalScanTasksAfterInsertTask() throws Exception {
        assertReleaseExternalScanTasks(true);
    }

    @Test
    public void testReleaseExternalScanTasksAfterMTMVTask() throws Exception {
        assertReleaseExternalScanTasks(false);
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

    private void assertReleaseExternalScanTasks(boolean insertTask) throws Exception {
        AtomicInteger loadCount = new AtomicInteger();
        AtomicReference<StatementContext> statementContext = new AtomicReference<>();
        CountDownLatch workerReused = new CountDownLatch(1);
        ExternalScanTaskCacheKey<String> cacheKey = new TestExternalScanTaskCacheKey();
        TaskProcessor taskProcessor = new TaskProcessor(1, 1, Thread::new);
        try {
            AbstractTask externalTask;
            if (insertTask) {
                externalTask = new InsertTask("external-insert", null, null, null) {
                    @Override
                    public void runTask() {
                        installExternalScanTaskContext(statementContext, cacheKey, loadCount);
                    }
                };
            } else {
                externalTask = new MTMVTask() {
                    @Override
                    public void runTask() {
                        installExternalScanTaskContext(statementContext, cacheKey, loadCount);
                    }
                };
            }
            externalTask.setStatus(TaskStatus.PENDING);
            externalTask.setTaskId(1L);

            Assert.assertTrue(taskProcessor.addTask(externalTask));

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

            StatementContext completedStatementContext = statementContext.get();
            Assert.assertNotNull(completedStatementContext);
            completedStatementContext.getExternalScanTaskCache().getOrLoad(
                    cacheKey, () -> {
                        loadCount.incrementAndGet();
                        return Collections.singletonList("reloaded");
                    });
            Assert.assertEquals(2, loadCount.get());
        } finally {
            taskProcessor.shutdown();
        }
    }

    private static void installExternalScanTaskContext(AtomicReference<StatementContext> statementContextReference,
            ExternalScanTaskCacheKey<String> cacheKey, AtomicInteger loadCount) {
        ConnectContext connectContext = new ConnectContext();
        StatementContext statementContext = new StatementContext();
        connectContext.setStatementContext(statementContext);
        connectContext.setThreadLocalInfo();
        statementContextReference.set(statementContext);
        try {
            statementContext.getExternalScanTaskCache().getOrLoad(
                    cacheKey,
                    () -> {
                        loadCount.incrementAndGet();
                        return Collections.singletonList("external-task");
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final class TestExternalScanTaskCacheKey implements ExternalScanTaskCacheKey<String> {
    }
}
