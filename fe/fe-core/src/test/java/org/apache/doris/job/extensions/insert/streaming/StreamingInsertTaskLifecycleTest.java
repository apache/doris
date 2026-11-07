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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.offset.s3.S3SourceOffsetProvider;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class StreamingInsertTaskLifecycleTest {
    @Test
    public void testEveryRetryClosesItsContextAndRestoresCaller() throws Exception {
        for (boolean failInBefore : new boolean[] {true, false}) {
            LifecycleTask task = new LifecycleTask();
            task.failInBefore = failInBefore;
            task.failures = 2;
            ConnectContext parent = new ConnectContext();
            try (AutoCloseConnectContext scope = new AutoCloseConnectContext(parent)) {
                task.execute();
                Assertions.assertEquals(3, task.contexts.size());
                Assertions.assertEquals(3, task.closed.get());
                for (ConnectContext context : task.contexts) {
                    Assertions.assertTrue(context.isClosingOrClosed());
                    Assertions.assertNull(context.getStatementContext());
                }
                Assertions.assertNull(task.getCtx());
                Assertions.assertSame(parent, ConnectContext.get());
                Assertions.assertFalse(parent.isClosingOrClosed());
            }
        }
    }

    @Test
    public void testCancelBeforeExecutionDoesNotCloseCaller() throws Exception {
        StreamingInsertTask task = new StreamingInsertTask(1L, 2L, "", new S3SourceOffsetProvider(),
                null, null, Collections.emptyMap(), UserIdentity.ROOT, null);
        task.setStatus(TaskStatus.PENDING);
        task.cancel(false);
        ConnectContext parent = new ConnectContext();
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(parent)) {
            task.execute();
            Assertions.assertNull(task.getCtx());
            Assertions.assertSame(parent, ConnectContext.get());
            Assertions.assertFalse(parent.isClosingOrClosed());
        }
    }

    @Test
    public void testCancelLeavesCleanupToExecutingThread() throws Exception {
        LifecycleTask task = new LifecycleTask();
        CountDownLatch executing = new CountDownLatch(1);
        CountDownLatch finishExecution = new CountDownLatch(1);
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        task.body = () -> {
            executing.countDown();
            try {
                Assertions.assertTrue(finishExecution.await(10, TimeUnit.SECONDS));
                Assertions.assertFalse(task.getCtx().isClosingOrClosed());
                Assertions.assertEquals(0, task.closed.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        };
        Thread worker = new Thread(() -> {
            try {
                task.execute();
                Assertions.assertNull(ConnectContext.get());
            } catch (Throwable e) {
                workerFailure.set(e);
            }
        });
        worker.start();
        try {
            Assertions.assertTrue(executing.await(10, TimeUnit.SECONDS));
            StreamingInsertJob job = new StreamingInsertJob();
            job.runningStreamTask = task;
            job.clearRunningStreamTask(JobStatus.STOPPED);
            Assertions.assertNotNull(task.getCtx());
            Assertions.assertFalse(task.getCtx().isClosingOrClosed());
            Assertions.assertEquals(0, task.closed.get());
        } finally {
            finishExecution.countDown();
            worker.join(TimeUnit.SECONDS.toMillis(10));
        }
        Assertions.assertFalse(worker.isAlive());
        Assertions.assertNull(workerFailure.get());
        Assertions.assertEquals(1, task.closed.get());
        Assertions.assertTrue(task.contexts.get(0).isClosingOrClosed());
        Assertions.assertNull(task.getCtx());
    }

    private static class LifecycleTask extends StreamingInsertTask {
        private final List<ConnectContext> contexts = new ArrayList<>();
        private final AtomicInteger closed = new AtomicInteger();
        private boolean failInBefore;
        private int failures;
        private Runnable body = () -> { };

        LifecycleTask() {
            super(1L, 2L, "", new S3SourceOffsetProvider(), null, null,
                    Collections.emptyMap(), UserIdentity.ROOT, null);
            setStatus(TaskStatus.PENDING);
        }

        @Override
        public void before() throws Exception {
            setStatus(TaskStatus.RUNNING);
            ConnectContext context = new ConnectContext();
            contexts.add(context);
            context.setThreadLocalInfo();
            StatementContext statement = new StatementContext(context, null);
            context.setStatementContext(statement);
            statement.getOrCreateConnectorStatementScope().computeIfAbsent(
                    "resource", () -> (AutoCloseable) closed::incrementAndGet);
            Deencapsulation.setField(this, "ctx", context);
            Deencapsulation.setField(this, "stmtExecutor", Mockito.mock(StmtExecutor.class));
            if (failInBefore && contexts.size() <= failures) {
                throw new JobException("injected preparation failure");
            }
        }

        @Override
        public void run() throws JobException {
            body.run();
            if (!failInBefore && contexts.size() <= failures) {
                throw new JobException("injected execution failure");
            }
        }

        @Override
        public boolean onSuccess() {
            if (!getIsCanceled().get()) {
                setStatus(TaskStatus.SUCCESS);
            }
            return true;
        }
    }
}
