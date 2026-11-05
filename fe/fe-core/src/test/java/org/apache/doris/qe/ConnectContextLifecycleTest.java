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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.qe.ConnectContext.CloseMode;
import org.apache.doris.qe.protocol.ProtocolAdapter;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Timeout(30)
public class ConnectContextLifecycleTest {
    private boolean savedRunningUnitTest;
    private Env env;
    private ProtocolAdapter adapter;
    private ConnectContext ctx;
    private ConnectPoolMgr pool;

    @BeforeEach
    public void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        env = ConnectPoolTestSupport.envAllowing(10);
        adapter = Mockito.mock(ProtocolAdapter.class);
        Mockito.when(adapter.type()).thenReturn(ConnectContext.ConnectType.MYSQL);
        ctx = Mockito.spy(new ConnectContext(adapter));
        ctx.setEnv(env);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        pool = new ConnectPoolMgr(10);
    }

    @AfterEach
    public void tearDown() {
        ctx.cleanup();
        ConnectContext.remove();
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @Test
    public void testActivationAndReentrantCloseRunCleanupOnce() {
        Assertions.assertFalse(ctx.tryBeginCommand());
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertFalse(ctx.isClosingOrClosed());
        Assertions.assertThrows(IllegalStateException.class, () -> ctx.activateClientSession(pool));
        Mockito.doAnswer(call -> {
            ctx.cleanup(); // MySQL's synchronous channel-close callback.
            return null;
        }).when(adapter).closeConnection(ctx);

        ctx.cleanup();
        ctx.kill(true);
        ctx.cleanup();

        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertEquals(0, pool.getConnectionNum());
        Assertions.assertEquals(0, pool.getUserConnectionMap().get(UserIdentity.ROOT.getQualifiedUser()).get());
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(ctx).closeTxn();
        Mockito.verify(ctx).deleteTempTable();
        Assertions.assertThrows(IllegalStateException.class, () -> ctx.activateClientSession(pool));
        Assertions.assertFalse(ctx.tryBeginCommand());
        Assertions.assertThrows(IllegalStateException.class, () -> ctx.setExecutor(Mockito.mock(StmtExecutor.class)));
    }

    @Test
    public void testInitializationFailureReleasesResourcesWithoutRemovingAnotherContext() {
        ConnectContext registered = ConnectPoolTestSupport.mysqlConnection(env, UserIdentity.ROOT);
        Assertions.assertEquals(-1, registered.activateClientSession(pool));
        // Both have the default id 0. The unregistered context owns no pool entry to revoke.
        Assertions.assertTrue(ctx.beginInitialization());
        ctx.cleanup();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Mockito.verify(ctx, Mockito.never()).closeTxn();
        Assertions.assertThrows(IllegalStateException.class, () -> ctx.activateClientSession(pool));
        ctx.endOperation();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertSame(registered, pool.getContext(0));
        Assertions.assertEquals(1, pool.getConnectionNum());
        Mockito.verify(adapter).releaseSession(ctx);
        registered.cleanup();
    }

    @Test
    public void testQuotaRefusalCanUseTheSameCleanupPath() {
        pool = new ConnectPoolMgr(0);
        Assertions.assertEquals(0, ctx.activateClientSession(pool));
        Assertions.assertFalse(ctx.tryBeginCommand());
        ctx.cleanup();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertEquals(0, pool.getConnectionNum());
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(ctx).closeTxn();
    }

    @Test
    public void testCloseRevokesAdmissionBeforeDrainingRunningCommand() {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(running);
        ctx.setThreadLocalInfo();
        ctx.cleanup();
        Assertions.assertSame(ctx, ConnectContext.get(), "the running command still owns its binding");

        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertNull(pool.getContext(ctx.getConnectionId()));
        Assertions.assertFalse(ctx.tryBeginCommand());
        Mockito.verify(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        Mockito.verify(ctx, Mockito.never()).closeTxn();

        // Publication can race with KILL before the executor exists.
        StmtExecutor late = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(late);
        Mockito.verify(late).cancel(Mockito.any(Status.class), Mockito.eq(false));
        ctx.endOperation();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(ctx).closeTxn();
        Assertions.assertNull(ctx.getExecutor());
    }

    @Test
    public void testDeferredCloseRevokesAdmissionButPreservesTheResponse() {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        ctx.setThreadLocalInfo();
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(running);
        ctx.requestClose(CloseMode.AFTER_RESPONSE);
        ctx.requestClose(CloseMode.AFTER_RESPONSE);

        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertSame(ctx, ConnectContext.get());
        Assertions.assertEquals(0, pool.getConnectionNum());
        Assertions.assertFalse(ctx.tryBeginCommand());
        Assertions.assertTrue(ctx.isClosingAfterResponse());
        Mockito.verify(ctx, Mockito.never()).closeTxn();
        Mockito.verifyNoInteractions(running);
        Mockito.verify(adapter, Mockito.never()).closeConnection(ctx);
        Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        // The admitted multi-statement command can still publish work needed for its response.
        StmtExecutor late = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(late);
        Mockito.verifyNoInteractions(late);

        ctx.endOperation();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verifyNoInteractions(running, late);
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    public void testDeferredCloseWaitsForBothInitializationAndTheCommand() {
        Assertions.assertTrue(ctx.beginInitialization());
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        ctx.requestClose(CloseMode.AFTER_RESPONSE);
        ctx.endOperation();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Mockito.verify(adapter, Mockito.never()).closeConnection(ctx);
        ctx.endOperation();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
    }

    @Test
    public void testImmediateCloseUpgradesADeferredCloseWithoutDowngrading() {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(running);
        ctx.requestClose(CloseMode.AFTER_RESPONSE);
        ctx.kill(true);
        ctx.requestClose(CloseMode.AFTER_RESPONSE);
        ctx.cleanup();
        Assertions.assertFalse(ctx.isClosingAfterResponse());
        Mockito.verify(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        Assertions.assertTrue(ctx.isClosingOrClosed());
        ctx.endOperation();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
    }

    @Test
    public void testDeferredCloseWithNoResponseClosesImmediately() {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        ctx.requestClose(CloseMode.AFTER_RESPONSE);
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertEquals(0, pool.getConnectionNum());
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
    }

    @Test
    public void testImmediateUpgradeWhileRegistrationIsBeingRevoked() throws Exception {
        pool = Mockito.spy(pool);
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        CountDownLatch unregistering = new CountDownLatch(1);
        CountDownLatch allowUnregister = new CountDownLatch(1);
        AtomicBoolean unregisterUnblocked = new AtomicBoolean();
        Mockito.doAnswer(call -> {
            unregistering.countDown();
            unregisterUnblocked.set(allowUnregister.await(10, TimeUnit.SECONDS));
            return call.callRealMethod();
        }).when(pool).unregisterConnection(ctx);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        CountDownLatch upgrading = new CountDownLatch(1);
        try {
            Future<?> deferredClose = workers.submit(() -> ctx.requestClose(CloseMode.AFTER_RESPONSE));
            Assertions.assertTrue(unregistering.await(10, TimeUnit.SECONDS));
            Future<?> immediateClose = workers.submit(() -> {
                upgrading.countDown();
                ctx.cleanup();
            });
            Assertions.assertTrue(upgrading.await(10, TimeUnit.SECONDS));
            Assertions.assertFalse(immediateClose.isDone());
            Mockito.verify(adapter, Mockito.never()).closeConnection(ctx);
            allowUnregister.countDown();
            deferredClose.get(10, TimeUnit.SECONDS);
            immediateClose.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(unregisterUnblocked.get());
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            ctx.endOperation();
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Assertions.assertEquals(0, pool.getConnectionNum());
            Mockito.verify(pool).unregisterConnection(ctx);
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter).releaseSession(ctx);
        } finally {
            allowUnregister.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testForcedCloseRacingWithResponseCompletionReleasesOnce() throws Exception {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        ctx.requestClose(CloseMode.AFTER_RESPONSE);
        CountDownLatch closingTransport = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        AtomicBoolean closeUnblocked = new AtomicBoolean();
        Mockito.doAnswer(call -> {
            closingTransport.countDown();
            closeUnblocked.set(allowClose.await(10, TimeUnit.SECONDS));
            return null;
        }).when(adapter).closeConnection(ctx);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> response = worker.submit(ctx::endOperation);
            Assertions.assertTrue(closingTransport.await(10, TimeUnit.SECONDS));
            ctx.cleanup();
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            allowClose.countDown();
            response.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(closeUnblocked.get());
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter).releaseSession(ctx);
        } finally {
            allowClose.countDown();
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testConcurrentAndReentrantCloseDuringResourceReleaseRunsOnce() throws Exception {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        CountDownLatch releasing = new CountDownLatch(1);
        CountDownLatch allowRelease = new CountDownLatch(1);
        AtomicBoolean releaseUnblocked = new AtomicBoolean();
        IllegalStateException failure = new IllegalStateException("protocol release failed");
        Mockito.doAnswer(call -> {
            ctx.cleanup(); // A resource callback can also re-enter the coordinator.
            releasing.countDown();
            releaseUnblocked.set(allowRelease.await(10, TimeUnit.SECONDS));
            throw failure;
        }).when(adapter).releaseSession(ctx);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> close = worker.submit(() -> ctx.requestClose(CloseMode.AFTER_RESPONSE));
            Assertions.assertTrue(releasing.await(10, TimeUnit.SECONDS));
            ctx.cleanup();
            ctx.requestClose(CloseMode.AFTER_RESPONSE);
            Assertions.assertFalse(ctx.tryBeginCommand());
            Mockito.verify(ctx, Mockito.never()).closeTxn();
            allowRelease.countDown();
            close.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(releaseUnblocked.get());
            Assertions.assertTrue(ctx.isClosingOrClosed());
            ctx.cleanup(); // Failure is terminal for teardown; do not retry released resources.
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter).releaseSession(ctx);
            Mockito.verify(ctx).closeTxn();
            Mockito.verify(ctx).deleteTempTable();
        } finally {
            allowRelease.countDown();
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testLastOperationWaitsForTheCloserDuringCancellation() throws Exception {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(running);
        CountDownLatch cancelling = new CountDownLatch(1);
        CountDownLatch allowCancel = new CountDownLatch(1);
        AtomicBoolean cancelUnblocked = new AtomicBoolean();
        AtomicReference<Thread> closer = new AtomicReference<>();
        AtomicReference<Thread> releaser = new AtomicReference<>();
        Mockito.doAnswer(call -> {
            cancelling.countDown();
            cancelUnblocked.set(allowCancel.await(10, TimeUnit.SECONDS));
            return null;
        }).when(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        Mockito.doAnswer(call -> {
            releaser.set(Thread.currentThread());
            return null;
        }).when(adapter).releaseSession(ctx);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> close = worker.submit(() -> {
                closer.set(Thread.currentThread());
                ctx.cleanup();
            });
            Assertions.assertTrue(cancelling.await(10, TimeUnit.SECONDS));
            ctx.endOperation();
            ctx.cleanup(); // Neither this request nor the last command can steal the closer's reference.
            Mockito.verify(ctx, Mockito.never()).closeTxn();
            Mockito.verify(adapter, Mockito.never()).closeConnection(ctx);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            allowCancel.countDown();
            close.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(cancelUnblocked.get());
            Assertions.assertSame(closer.get(), releaser.get());
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter).releaseSession(ctx);
        } finally {
            allowCancel.countDown();
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCloserReturnsItsReferenceBeforeTheLastOperation() throws Exception {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        AtomicReference<Thread> releaser = new AtomicReference<>();
        Mockito.doAnswer(call -> {
            releaser.set(Thread.currentThread());
            return null;
        }).when(adapter).releaseSession(ctx);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            worker.submit(ctx::cleanup).get(10, TimeUnit.SECONDS);
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            Mockito.verify(ctx, Mockito.never()).closeTxn();
            ctx.endOperation();
            Assertions.assertSame(Thread.currentThread(), releaser.get());
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Mockito.verify(adapter).releaseSession(ctx);
        } finally {
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @RepeatedTest(10)
    public void testDeferredCloseRacesWithTheLastOperation() throws Exception {
        pool = Mockito.spy(pool);
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        CyclicBarrier start = new CyclicBarrier(3);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> close = workers.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                ctx.requestClose(CloseMode.AFTER_RESPONSE);
                return null;
            });
            Future<?> exit = workers.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                ctx.endOperation();
                return null;
            });
            start.await(10, TimeUnit.SECONDS);
            close.get(10, TimeUnit.SECONDS);
            exit.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Mockito.verify(pool).unregisterConnection(ctx);
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter).releaseSession(ctx);
        } finally {
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testFailuresReturnTheSessionReferenceAndStillReleaseEveryResource() {
        pool = Mockito.spy(pool);
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        IllegalStateException unregister = new IllegalStateException("unregister failed");
        IllegalStateException cancel = new IllegalStateException("cancel failed");
        IllegalStateException transport = new IllegalStateException("transport failed");
        IllegalStateException release = new IllegalStateException("release failed");
        Mockito.doAnswer(call -> {
            call.callRealMethod();
            throw unregister;
        }).when(pool).unregisterConnection(ctx);
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(running);
        Mockito.doThrow(cancel).when(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        Mockito.doThrow(transport).when(adapter).closeConnection(ctx);
        Mockito.doThrow(release).when(adapter).releaseSession(ctx);
        ctx.cleanup();
        Assertions.assertEquals(0, pool.getConnectionNum());
        Mockito.verify(ctx, Mockito.never()).closeTxn();
        Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        ctx.endOperation();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertThrows(IllegalStateException.class, () -> ctx.setExecutor(running));
        ctx.cleanup();
        Mockito.verify(pool).unregisterConnection(ctx);
        Mockito.verify(running).cancel(Mockito.any(Status.class), Mockito.eq(false));
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(ctx).closeTxn();
        Mockito.verify(ctx).deleteTempTable();
    }

    @Test
    public void testDeferredUnregistrationFailureStillHandsOffToTheLastOperation() {
        pool = Mockito.spy(pool);
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertTrue(ctx.tryBeginCommand());
        IllegalStateException failure = new IllegalStateException("unregister failed");
        Mockito.doAnswer(call -> {
            call.callRealMethod();
            throw failure;
        }).when(pool).unregisterConnection(ctx);
        ctx.requestClose(CloseMode.AFTER_RESPONSE);
        Assertions.assertTrue(ctx.isClosingAfterResponse());
        Mockito.verify(ctx, Mockito.never()).closeTxn();
        Mockito.verify(adapter, Mockito.never()).closeConnection(ctx);
        ctx.endOperation();
        Assertions.assertFalse(ctx.isClosingAfterResponse());
        Mockito.verify(pool).unregisterConnection(ctx);
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(ctx).closeTxn();
    }

    @Test
    public void testFinalCleanupDetachesTheFinishedContext() {
        ctx.setThreadLocalInfo();
        ctx.cleanup();
        Assertions.assertNull(ConnectContext.get());
        ctx.cleanup();
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
    }

    @Test
    public void testUnmatchedOperationExitCannotReleaseTheSessionReference() {
        Assertions.assertThrows(IllegalStateException.class, ctx::endOperation);
        ctx.activate();
        Assertions.assertThrows(IllegalStateException.class, ctx::endOperation);
        Assertions.assertTrue(ctx.tryBeginCommand());
        ctx.endOperation();
        Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        ctx.cleanup();
        Mockito.verify(adapter).releaseSession(ctx);
        Assertions.assertThrows(IllegalStateException.class, ctx::endOperation);
    }

    @Test
    public void testKillQueryKeepsTheSessionActive() {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(running);
        ctx.kill(false);
        Mockito.verify(running).cancel(Mockito.any(Status.class));
        Assertions.assertFalse(ctx.isClosingOrClosed());
        Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
        Mockito.verify(adapter, Mockito.never()).closeConnection(ctx);
    }

    @Test
    public void testCleanupPreservesTheCallersThreadLocalContext() {
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        AtomicReference<ConnectContext> rollbackContext = new AtomicReference<>();
        Mockito.doAnswer(call -> {
            rollbackContext.set(ConnectContext.get());
            return null;
        }).when(ctx).closeTxn();
        ctx.cleanup();
        Assertions.assertSame(ctx, rollbackContext.get(), "rollback must use the closing session");
        Assertions.assertSame(caller, ConnectContext.get());
        caller.cleanup();
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    public void testCleanupFailureDoesNotSkipOtherResourcesOrRetry() {
        IllegalStateException failure = new IllegalStateException("transport close failed");
        Mockito.doThrow(failure).when(adapter).closeConnection(ctx);
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        ctx.cleanup();
        Assertions.assertEquals(0, pool.getConnectionNum());
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(ctx).closeTxn();
        Mockito.verify(ctx).deleteTempTable();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        ctx.cleanup();
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter).releaseSession(ctx);
    }

    @Test
    public void testRegistrationAndCloseCannotLeaveAnActivePoolEntry() throws Exception {
        CountDownLatch reserving = new CountDownLatch(1);
        CountDownLatch allowRegistration = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        Mockito.when(env.getAuth().getMaxConn(Mockito.anyString())).thenAnswer(call -> {
            reserving.countDown();
            Assertions.assertTrue(allowRegistration.await(10, TimeUnit.SECONDS));
            return 10L;
        });
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<Integer> registration = workers.submit(() -> ctx.activateClientSession(pool));
            Assertions.assertTrue(reserving.await(10, TimeUnit.SECONDS));
            Future<?> close = workers.submit(() -> {
                closing.countDown();
                ctx.cleanup();
            });
            Assertions.assertTrue(closing.await(10, TimeUnit.SECONDS));
            allowRegistration.countDown();
            Assertions.assertEquals(-1, registration.get(10, TimeUnit.SECONDS));
            close.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Assertions.assertEquals(0, pool.getConnectionNum());
            Assertions.assertNull(pool.getContext(ctx.getConnectionId()));
            Mockito.verify(adapter).releaseSession(ctx);
        } finally {
            allowRegistration.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testConcurrentCloseOnlyReleasesOnce() throws Exception {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        CountDownLatch transportClosing = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        AtomicBoolean closeUnblocked = new AtomicBoolean();
        Mockito.doAnswer(call -> {
            transportClosing.countDown();
            closeUnblocked.set(allowClose.await(10, TimeUnit.SECONDS));
            return null;
        }).when(adapter).closeConnection(ctx);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> first = worker.submit(ctx::cleanup);
            Assertions.assertTrue(transportClosing.await(10, TimeUnit.SECONDS));
            ctx.cleanup();
            Assertions.assertEquals(0, pool.getConnectionNum());
            Assertions.assertTrue(ctx.isClosingOrClosed());
            allowClose.countDown();
            first.get(10, TimeUnit.SECONDS);
            Assertions.assertTrue(closeUnblocked.get());
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter).releaseSession(ctx);
            Assertions.assertTrue(ctx.isClosingOrClosed());
        } finally {
            allowClose.countDown();
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCommandAdmissionAndIdleTimeoutUseTheSameLifecycle() {
        ctx.cleanup();
        ctx = Mockito.spy(ConnectPoolTestSupport.flightSession(env, UserIdentity.ROOT, "metadata-token"));
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        ctx.getSessionVariable().waitTimeoutS = 1;
        Assertions.assertTrue(ctx.tryBeginCommand());
        long overdue = ctx.getStartTime() + 1001;
        ctx.checkTimeout(overdue); // Flight metadata can run while command is COM_SLEEP.
        Assertions.assertFalse(ctx.isClosingOrClosed());
        ctx.endOperation();
        ctx.checkTimeout(overdue);
        Mockito.verify(ctx).releaseProtocolSession();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertFalse(ctx.tryBeginCommand());
        Assertions.assertEquals(0, pool.getConnectionNum());
    }

    @Test
    public void testMysqlIncompletePacketCanStillTimeOutWhileReading() {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        ctx.getSessionVariable().waitTimeoutS = 1;
        Assertions.assertTrue(ctx.tryBeginCommand());
        ctx.checkTimeout(ctx.getStartTime() + 1001);
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Assertions.assertEquals(0, pool.getConnectionNum());
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        ctx.endOperation();
        Assertions.assertTrue(ctx.isClosingOrClosed());
        Mockito.verify(adapter).releaseSession(ctx);
    }

    @Test
    public void testQueryTimeoutCancelsTheQueryAndRetainsTheSession() {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        ctx.setCommand(MysqlCommand.COM_QUERY);
        StmtExecutor running = Mockito.mock(StmtExecutor.class);
        ctx.setExecutor(running);
        ctx.checkTimeout(ctx.getStartTime() + ctx.getExecTimeoutS() * 1000L + 1);
        ArgumentCaptor<Status> reason = ArgumentCaptor.forClass(Status.class);
        Mockito.verify(running).cancel(reason.capture(), Mockito.eq(false));
        Assertions.assertEquals(TStatusCode.TIMEOUT, reason.getValue().getErrorCode());
        Assertions.assertFalse(ctx.isClosingOrClosed());
        Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
    }
}
