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
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.disruptor.ExecuteTaskEvent;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskProcessorTest {
    @Test
    public void testEventReleasesItsTaskAndPreservesTheBorrowedContext() {
        ConnectContext parent = new ConnectContext();
        try (AutoCloseConnectContext parentScope = new AutoCloseConnectContext(parent)) {
            ExecuteTaskEvent<AbstractTask> event = new ExecuteTaskEvent<>();
            DefaultTaskExecutorHandler<AbstractTask> handler = new DefaultTaskExecutorHandler<>();
            handler.onEvent(event);
            Assertions.assertSame(parent, ConnectContext.get());
            Assertions.assertFalse(parent.isClosingOrClosed());

            event.setTask(countingRejections(2L, new AtomicInteger(), () -> {
                Assertions.assertSame(parent, ConnectContext.get());
                throw new IllegalStateException("injected task failure");
            }));
            handler.onEvent(event);
            Assertions.assertNull(event.getTask());
            Assertions.assertSame(parent, ConnectContext.get());
            Assertions.assertFalse(parent.isClosingOrClosed());
        }
    }

    @Test
    public void testCleanEveryMTMVExecutionContext() {
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicInteger closeCount = new AtomicInteger();
        ConnectContext taskContext = new ConnectContext();
        try (AutoCloseConnectContext taskScope = new AutoCloseConnectContext(taskContext)) {
            for (int i = 0; i < 2; i++) {
                ConnectContext executionContext = new ConnectContext();
                StatementContext statementContext = new StatementContext(executionContext, null);
                executionContext.setStatementContext(statementContext);
                TUniqueId queryId = new TUniqueId(2L, i + 1L);
                executionContext.setQueryId(queryId);
                statementContext.getOrCreateConnectorStatementScope().computeIfAbsent(
                        "closeable", () -> (AutoCloseable) closeCount::incrementAndGet);
                QeProcessorImpl.INSTANCE.registerQueryFinishCallback(DebugUtil.printId(queryId), () -> {
                    Assertions.assertFalse(executionContext.isClosingOrClosed());
                    Assertions.assertSame(executionContext, ConnectContext.get());
                    callbackCount.incrementAndGet();
                });

                try (AutoCloseConnectContext scope = Deencapsulation.invoke(
                        AbstractTask.class, "bindExecutionContext", executionContext)) {
                    Assertions.assertSame(executionContext, ConnectContext.get());
                }
                Assertions.assertTrue(executionContext.isClosingOrClosed());
                Assertions.assertNull(executionContext.getStatementContext());
                Assertions.assertSame(taskContext, ConnectContext.get());
                Assertions.assertFalse(taskContext.isClosingOrClosed());
            }
            Assertions.assertEquals(2, callbackCount.get());
            Assertions.assertEquals(2, closeCount.get());
        }
        Assertions.assertTrue(taskContext.isClosingOrClosed());
    }

    /** A task the full queue turns away is told so, and only that one: the others are accepted and run. */
    @Test
    public void testATaskTheFullQueueTurnsAwayIsToldSo() throws Exception {
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger rejections = new AtomicInteger();
        TaskProcessor taskProcessor = new TaskProcessor(1, 1, Thread::new);
        try {
            Assertions.assertTrue(taskProcessor.addTask(countingRejections(1L, rejections, () -> {
                running.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            })));
            Assertions.assertTrue(running.await(10, TimeUnit.SECONDS));
            // The only worker is busy, and the queue has room for one more task.
            Assertions.assertTrue(taskProcessor.addTask(countingRejections(2L, rejections, () -> { })));
            Assertions.assertEquals(0, rejections.get());

            Assertions.assertFalse(taskProcessor.addTask(countingRejections(3L, rejections, () -> { })));
            Assertions.assertEquals(1, rejections.get());
        } finally {
            release.countDown();
            taskProcessor.shutdown();
        }
    }

    private static AbstractTask countingRejections(long taskId, AtomicInteger rejections, Runnable body) {
        AbstractTask task = new InsertTask("task-" + taskId, null, null, null) {
            @Override
            public void runTask() {
                body.run();
            }

            @Override
            public void onRejected() {
                rejections.incrementAndGet();
            }
        };
        task.setStatus(TaskStatus.PENDING);
        task.setTaskId(taskId);
        return task;
    }

}
