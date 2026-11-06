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
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.protocol.ProtocolAdapter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Timeout(30)
public class ConnectContextChangeUserTest {
    private static final UserIdentity ALICE = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
    private static final UserIdentity BOB = UserIdentity.createAnalyzedUserIdentWithIp("bob", "%");
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
        ctx.setCurrentUserIdentity(ALICE);
        pool = new ConnectPoolMgr(10);
        Assertions.assertEquals(-1, ctx.registerClientSession(pool));
    }

    @AfterEach
    public void tearDown() {
        ctx.cleanup();
        ConnectContext.remove();
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    private static void stop(ExecutorService workers) throws InterruptedException {
        workers.shutdownNow();
        Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
    }

    private int count(UserIdentity identity) {
        return pool.getUserConnectionMap().get(identity.getQualifiedUser()).get();
    }

    @Test
    public void registeredIdentityCannotBypassQuotaTransfer() {
        Assertions.assertThrows(IllegalStateException.class, () -> ctx.setCurrentUserIdentity(BOB));
        Assertions.assertSame(ALICE, ctx.getCurrentUserIdentity());
        Assertions.assertEquals(1, count(ALICE));
        Assertions.assertFalse(pool.getUserConnectionMap().containsKey(BOB.getQualifiedUser()));
        // Refreshing the matching host entry does not change the per-user quota owner.
        UserIdentity aliceAtHost = UserIdentity.createAnalyzedUserIdentWithIp("alice", "127.0.0.1");
        ctx.setCurrentUserIdentity(aliceAtHost);
        Assertions.assertSame(aliceAtHost, ctx.getCurrentUserIdentity());
        ctx.cleanup();
        Assertions.assertEquals(0, count(ALICE));
        Assertions.assertEquals(0, pool.getConnectionNum());
    }

    @Test
    public void closeWhileQuotaLookupIsBlockedPreventsTransfer() throws Exception {
        CountDownLatch lookup = new CountDownLatch(1);
        CountDownLatch finishLookup = new CountDownLatch(1);
        Auth auth = env.getAuth();
        Mockito.doAnswer(call -> {
            lookup.countDown();
            await(finishLookup);
            return 10L;
        }).when(auth).getMaxConn(BOB.getQualifiedUser());
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> change = workers.submit(() -> ctx.runCommand(() -> {
                Assertions.assertThrows(ConnectContext.SessionClosedException.class,
                        () -> ctx.changeUser(BOB, ""));
                Assertions.assertSame(ctx, ConnectContext.get());
            }));
            await(lookup);
            workers.submit(ctx::cleanup).get(5, TimeUnit.SECONDS);
            Mockito.verify(adapter).closeConnection(ctx);
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            Assertions.assertEquals(0, pool.getConnectionNum());
            Assertions.assertEquals(0, count(ALICE));
            finishLookup.countDown();
            change.get(10, TimeUnit.SECONDS);
            Assertions.assertSame(ALICE, ctx.getCurrentUserIdentity());
            Assertions.assertFalse(pool.getUserConnectionMap().containsKey(BOB.getQualifiedUser()));
            Mockito.verify(adapter).releaseSession(ctx);
        } finally {
            finishLookup.countDown();
            stop(workers);
        }
    }

    @Test
    public void closeAfterTransferReleasesNewUserBeforeCommandReturns() throws Exception {
        CountDownLatch transferred = new CountDownLatch(1);
        CountDownLatch finishCommand = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> change = workers.submit(() -> ctx.runCommand(() -> {
                Assertions.assertTrue(ctx.changeUser(BOB, "").isEmpty());
                Assertions.assertSame(ctx, ConnectContext.get());
                transferred.countDown();
                await(finishCommand);
            }));
            await(transferred);
            Assertions.assertSame(BOB, ctx.getCurrentUserIdentity());
            Assertions.assertEquals(0, count(ALICE));
            Assertions.assertEquals(1, count(BOB));
            workers.submit(ctx::cleanup).get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(0, pool.getConnectionNum());
            Assertions.assertEquals(0, count(ALICE));
            Assertions.assertEquals(0, count(BOB));
            Mockito.verify(adapter, Mockito.never()).releaseSession(ctx);
            finishCommand.countDown();
            change.get(10, TimeUnit.SECONDS);
            Mockito.verify(adapter).releaseSession(ctx);
        } finally {
            finishCommand.countDown();
            stop(workers);
        }
    }

    @Test
    public void quotaLookupFailurePreservesIdentityAndThreadContext() {
        Auth auth = env.getAuth();
        Mockito.when(auth.getMaxConn(BOB.getQualifiedUser())).thenThrow(new IllegalStateException("lookup failed"));
        ctx.runCommand(() -> {
            IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                    () -> ctx.changeUser(BOB, ""));
            Assertions.assertEquals("lookup failed", error.getMessage());
            Assertions.assertSame(ctx, ConnectContext.get());
        });
        Assertions.assertNull(ConnectContext.get());
        Assertions.assertSame(ALICE, ctx.getCurrentUserIdentity());
        Assertions.assertEquals(1, count(ALICE));
        Assertions.assertFalse(pool.getUserConnectionMap().containsKey(BOB.getQualifiedUser()));
        Assertions.assertEquals(1, pool.getConnectionNum());
    }
}
