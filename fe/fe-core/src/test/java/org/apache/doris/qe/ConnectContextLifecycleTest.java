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
import org.apache.doris.qe.ConnectContext.LifecycleState;
import org.apache.doris.qe.protocol.ProtocolAdapter;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
        Assertions.assertEquals(LifecycleState.INITIALIZING, ctx.getLifecycleState());
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        Assertions.assertEquals(LifecycleState.ACTIVE, ctx.getLifecycleState());
        Assertions.assertThrows(IllegalStateException.class, () -> ctx.activateClientSession(pool));
        Mockito.doAnswer(call -> {
            ctx.cleanup(); // MySQL's synchronous channel-close callback.
            return null;
        }).when(adapter).closeConnection(ctx);

        ctx.cleanup();
        ctx.kill(true);
        ctx.cleanup();

        Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
        ctx.getCleanupCompletion().toCompletableFuture().join();
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
        Assertions.assertEquals(LifecycleState.CLOSING, ctx.getLifecycleState());
        Assertions.assertFalse(ctx.getCleanupCompletion().toCompletableFuture().isDone());
        Assertions.assertThrows(IllegalStateException.class, () -> ctx.activateClientSession(pool));
        ctx.endOperation();
        Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
        Assertions.assertSame(registered, pool.getContext(0));
        Assertions.assertEquals(1, pool.getConnectionNum());
        Mockito.verify(adapter).releaseSession(ctx);
        registered.cleanup();
    }

    @Test
    public void testQuotaRefusalCanUseTheSameCleanupPath() {
        pool = new ConnectPoolMgr(0);
        Assertions.assertEquals(0, ctx.activateClientSession(pool));
        Assertions.assertEquals(LifecycleState.INITIALIZING, ctx.getLifecycleState());
        ctx.cleanup();
        Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
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

        Assertions.assertEquals(LifecycleState.CLOSING, ctx.getLifecycleState());
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
        Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(ctx).closeTxn();
        Assertions.assertNull(ctx.getExecutor());
    }

    @Test
    public void testCleanupPreservesTheCallersThreadLocalContext() {
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        Mockito.doAnswer(call -> {
            Assertions.assertSame(ctx, ConnectContext.get(), "rollback must use the closing session");
            return null;
        }).when(ctx).closeTxn();
        ctx.cleanup();
        Assertions.assertSame(caller, ConnectContext.get());
        caller.cleanup();
        Assertions.assertNull(ConnectContext.get());
    }

    @Test
    public void testCleanupFailureDoesNotSkipOtherResourcesOrReportClosed() {
        IllegalStateException failure = new IllegalStateException("transport close failed");
        Mockito.doThrow(failure).when(adapter).closeConnection(ctx);
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        ctx.cleanup();
        Assertions.assertEquals(0, pool.getConnectionNum());
        Mockito.verify(adapter).releaseSession(ctx);
        Mockito.verify(ctx).closeTxn();
        Mockito.verify(ctx).deleteTempTable();
        Assertions.assertEquals(LifecycleState.CLOSING, ctx.getLifecycleState());
        Assertions.assertTrue(ctx.getCleanupCompletion().toCompletableFuture().isCompletedExceptionally());
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
            Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
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
        Mockito.doAnswer(call -> {
            transportClosing.countDown();
            Assertions.assertTrue(allowClose.await(10, TimeUnit.SECONDS));
            return null;
        }).when(adapter).closeConnection(ctx);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> first = worker.submit(ctx::cleanup);
            Assertions.assertTrue(transportClosing.await(10, TimeUnit.SECONDS));
            ctx.cleanup();
            Assertions.assertEquals(0, pool.getConnectionNum());
            Assertions.assertEquals(LifecycleState.CLOSING, ctx.getLifecycleState());
            allowClose.countDown();
            first.get(10, TimeUnit.SECONDS);
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter).releaseSession(ctx);
            Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
        } finally {
            allowClose.countDown();
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testCommandAdmissionAndIdleTimeoutUseTheSameLifecycle() {
        ctx.cleanup();
        ctx = ConnectPoolTestSupport.flightSession(env, UserIdentity.ROOT, "metadata-token");
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        ctx.getSessionVariable().waitTimeoutS = 1;
        Assertions.assertTrue(ctx.tryBeginCommand());
        long overdue = ctx.getStartTime() + 1001;
        ctx.checkTimeout(overdue); // Flight metadata can run while command is COM_SLEEP.
        Assertions.assertEquals(LifecycleState.ACTIVE, ctx.getLifecycleState());
        ctx.endOperation();
        ctx.checkTimeout(overdue);
        Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
        Assertions.assertFalse(ctx.tryBeginCommand());
        Assertions.assertEquals(0, pool.getConnectionNum());
    }

    @Test
    public void testMysqlIncompletePacketCanStillTimeOutWhileReading() {
        Assertions.assertEquals(-1, ctx.activateClientSession(pool));
        ctx.getSessionVariable().waitTimeoutS = 1;
        Assertions.assertTrue(ctx.tryBeginCommand());
        ctx.checkTimeout(ctx.getStartTime() + 1001);
        Assertions.assertEquals(LifecycleState.CLOSING, ctx.getLifecycleState());
        Assertions.assertEquals(0, pool.getConnectionNum());
        Mockito.verify(adapter).closeConnection(ctx);
        Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
        ctx.endOperation();
        Assertions.assertEquals(LifecycleState.CLOSED, ctx.getLifecycleState());
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
        Assertions.assertEquals(LifecycleState.ACTIVE, ctx.getLifecycleState());
        Assertions.assertSame(ctx, pool.getContext(ctx.getConnectionId()));
    }
}
