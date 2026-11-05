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

package org.apache.doris.mysql;

import org.apache.doris.common.FeConstants;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.MysqlConnectProcessor;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.xnio.XnioIoThread;
import org.xnio.XnioWorker;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ReadListenerTest {
    private boolean savedRunningUnitTest;
    private final ExecutorService workers = Executors.newFixedThreadPool(2);

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
    }

    @AfterEach
    public void tearDown() throws Exception {
        workers.shutdownNow();
        Assertions.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        ConnectContext.remove();
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @Test
    public void testNextCommandStartsAfterPreviousFinallyAndBindingRestore() throws Exception {
        MysqlProtocolAdapter adapter = mysqlAdapter();
        ConnectContext ctx = new ConnectContext(adapter);
        ConnectContext previous = new ConnectContext();
        ConnectProcessor processor = Mockito.mock(ConnectProcessor.class);
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch finishFirst = new CountDownLatch(1);
        AtomicInteger commands = new AtomicInteger();
        AtomicBoolean firstReleased = new AtomicBoolean();
        AtomicBoolean firstFinallyFinished = new AtomicBoolean();
        AtomicBoolean secondSawFinally = new AtomicBoolean();
        AtomicReference<ConnectContext> bindingAtRearm = new AtomicReference<>();
        AtomicReference<Future<?>> second = new AtomicReference<>();
        Mockito.doAnswer(call -> {
            if (commands.incrementAndGet() == 1) {
                try {
                    firstEntered.countDown();
                    firstReleased.set(finishFirst.await(5, TimeUnit.SECONDS));
                } finally {
                    firstFinallyFinished.set(true);
                }
            } else {
                secondSawFinally.set(firstFinallyFinished.get());
                ctx.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
            }
            return null;
        }).when(processor).processOnce();
        Runnable task = enqueue(ctx, processor);
        Mockito.doAnswer(call -> {
            bindingAtRearm.set(ConnectContext.get());
            // Model XNIO dispatching the next read as soon as reads are resumed.
            second.set(workers.submit(task));
            return null;
        }).when(adapter).resumeAcceptQuery();
        Future<?> first = workers.submit(() -> {
            previous.setThreadLocalInfo();
            try {
                task.run();
            } finally {
                ConnectContext.remove();
            }
        });
        try {
            await(firstEntered);
            Mockito.verify(adapter, Mockito.never()).resumeAcceptQuery();
        } finally {
            finishFirst.countDown();
        }
        first.get(5, TimeUnit.SECONDS);
        second.get().get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(2, commands.get());
        Assertions.assertTrue(firstReleased.get());
        Assertions.assertTrue(secondSawFinally.get());
        Assertions.assertSame(previous, bindingAtRearm.get());
        Mockito.verify(adapter).resumeAcceptQuery();
        Mockito.verify(adapter).releaseSession(ctx);
        previous.cleanup();
    }

    @Test
    public void testExternalCloseWaitsForCommandAndToleratesReentrantCallbacks() throws Exception {
        MysqlProtocolAdapter adapter = mysqlAdapter();
        ConnectContext ctx = new ConnectContext(adapter);
        ConnectProcessor processor = Mockito.mock(ConnectProcessor.class);
        CountDownLatch commandEntered = new CountDownLatch(1);
        CountDownLatch finishCommand = new CountDownLatch(1);
        AtomicBoolean commandExited = new AtomicBoolean();
        AtomicBoolean commandReleased = new AtomicBoolean();
        AtomicBoolean exitedAtRelease = new AtomicBoolean();
        AtomicInteger releases = new AtomicInteger();
        Mockito.doAnswer(call -> {
            commandEntered.countDown();
            try {
                commandReleased.set(finishCommand.await(5, TimeUnit.SECONDS));
            } finally {
                commandExited.set(true);
            }
            return null;
        }).when(processor).processOnce();
        Mockito.doAnswer(call -> {
            ctx.cleanup(); // The socket close listener may run synchronously.
            return null;
        }).when(adapter).closeConnection(ctx);
        Mockito.doAnswer(call -> {
            releases.incrementAndGet();
            exitedAtRelease.set(commandExited.get());
            ctx.cleanup(); // Protocol cleanup must not recursively start another finalizer.
            return null;
        }).when(adapter).releaseSession(ctx);
        Future<?> command = workers.submit(enqueue(ctx, processor));
        try {
            await(commandEntered);
            ctx.cleanup();
            ctx.cleanup();
            Assertions.assertEquals(0, releases.get());
        } finally {
            finishCommand.countDown();
        }
        command.get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(1, releases.get());
        Assertions.assertTrue(commandReleased.get());
        Assertions.assertTrue(exitedAtRelease.get());
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter, Mockito.never()).resumeAcceptQuery();
    }

    @Test
    public void testRequestExitBeforeCancellationReturnsDoesNotReleaseResources() throws Exception {
        MysqlProtocolAdapter adapter = mysqlAdapter();
        ConnectContext ctx = new ConnectContext(adapter);
        ConnectProcessor processor = Mockito.mock(ConnectProcessor.class);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        CountDownLatch commandEntered = new CountDownLatch(1);
        CountDownLatch finishCommand = new CountDownLatch(1);
        CountDownLatch cancellationEntered = new CountDownLatch(1);
        CountDownLatch finishCancellation = new CountDownLatch(1);
        AtomicBoolean commandReleased = new AtomicBoolean();
        AtomicBoolean cancellationReleased = new AtomicBoolean();
        Mockito.doAnswer(call -> {
            ctx.setExecutor(executor);
            commandEntered.countDown();
            commandReleased.set(finishCommand.await(5, TimeUnit.SECONDS));
            return null;
        }).when(processor).processOnce();
        Mockito.doAnswer(call -> {
            cancellationEntered.countDown();
            cancellationReleased.set(finishCancellation.await(5, TimeUnit.SECONDS));
            return null;
        }).when(executor).cancel(Mockito.any(), Mockito.eq(false));
        Future<?> command = workers.submit(enqueue(ctx, processor));
        await(commandEntered);
        Future<?> closer = workers.submit(ctx::cleanup);
        try {
            await(cancellationEntered);
            finishCommand.countDown();
            command.get(5, TimeUnit.SECONDS);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        } finally {
            finishCommand.countDown();
            finishCancellation.countDown();
        }
        closer.get(5, TimeUnit.SECONDS);
        Assertions.assertTrue(commandReleased.get());
        Assertions.assertTrue(cancellationReleased.get());
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(adapter, Mockito.never()).resumeAcceptQuery();
    }

    @Test
    public void testQueuedReadDoesNotOwnResourcesAndCannotRunAfterIdleClose() throws Exception {
        MysqlProtocolAdapter adapter = mysqlAdapter();
        ConnectContext ctx = new ConnectContext(adapter);
        ConnectProcessor processor = Mockito.mock(ConnectProcessor.class);
        Runnable queued = enqueue(ctx, processor);
        ctx.cleanup();
        Mockito.verify(adapter).releaseSession(ctx);
        queued.run();
        Mockito.verifyNoInteractions(processor);
        Mockito.verify(adapter, Mockito.never()).resumeAcceptQuery();
        Mockito.verify(adapter).releaseSession(ctx);
    }

    @Test
    public void testRearmFailureClosesAndRestoresUnrelatedContext() throws Exception {
        MysqlProtocolAdapter adapter = mysqlAdapter();
        ConnectContext ctx = new ConnectContext(adapter);
        ConnectContext previous = new ConnectContext();
        ConnectProcessor processor = Mockito.mock(ConnectProcessor.class);
        Mockito.doThrow(new IllegalStateException("read channel failed")).when(adapter).resumeAcceptQuery();
        previous.setThreadLocalInfo();
        enqueue(ctx, processor).run();
        Assertions.assertSame(previous, ConnectContext.get());
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Mockito.verify(processor).processOnce();
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
        previous.cleanup();
    }

    @Test
    public void testBlockingDriverReleasesSessionOnCommandError() throws Exception {
        MysqlProtocolAdapter adapter = mysqlAdapter();
        ConnectContext ctx = new ConnectContext(adapter);
        MysqlConnectProcessor processor = Mockito.spy(new MysqlConnectProcessor(ctx));
        Mockito.doThrow(new AssertionError("command failed")).when(processor).processOnce();
        processor.loop();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertNull(ConnectContext.get());
        Mockito.verify(processor).processOnce();
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
    }

    private MysqlProtocolAdapter mysqlAdapter() {
        MysqlProtocolAdapter adapter = Mockito.mock(MysqlProtocolAdapter.class);
        Mockito.when(adapter.type()).thenReturn(ConnectContext.ConnectType.MYSQL);
        return adapter;
    }

    private Runnable enqueue(ConnectContext ctx, ConnectProcessor processor) {
        ConduitStreamSourceChannel channel = Mockito.mock(ConduitStreamSourceChannel.class);
        XnioWorker worker = Mockito.mock(XnioWorker.class);
        AtomicReference<Runnable> queued = new AtomicReference<>();
        Mockito.when(channel.getWorker()).thenReturn(worker);
        Mockito.doAnswer(call -> {
            queued.set(call.getArgument(0));
            return null;
        }).when(worker).execute(Mockito.any(Runnable.class));
        try (MockedStatic<XnioIoThread> ioThread = Mockito.mockStatic(XnioIoThread.class)) {
            new ReadListener(ctx, processor).handleEvent(channel);
        }
        return queued.get();
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for the handoff");
    }
}
