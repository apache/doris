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

package org.apache.doris.httpv2.websql;

import org.apache.doris.analysis.UserIdentity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WebSqlSessionManagerTest {
    @Test
    void lifecycleOwnershipQuotaResetAndShutdownReleaseConnections() throws Exception {
        Connection first = Mockito.mock(Connection.class);
        Connection second = Mockito.mock(Connection.class);
        Connection third = Mockito.mock(Connection.class);
        AtomicInteger opens = new AtomicInteger();
        WebSqlConnectionFactory factory = (user, password) -> {
            int index = opens.getAndIncrement();
            return index == 0 ? first : index == 1 ? second : third;
        };
        WebSqlSessionManager manager = manager(factory, Mockito.mock(WebSqlStatementExecutor.class),
                limits(2, 1, 1000, 20));

        WebSqlSession session = manager.createSession("alice", "secret");
        Assertions.assertTrue(session.getId().contains("."));
        Assertions.assertSame(session, manager.getSession(session.getId(), "alice"));
        Assertions.assertEquals(1, manager.size());
        assertError(WebSqlError.SESSION_LIMIT_EXCEEDED, () -> manager.createSession("alice", "secret"));
        assertError(WebSqlError.ACCESS_DENIED, () -> manager.getSession(session.getId(), "bob"));
        assertError(WebSqlError.ACCESS_DENIED, () -> manager.reset(session.getId(), "bob", "secret"));

        manager.reset(session.getId(), "alice", "secret");
        Mockito.verify(first).close();
        Assertions.assertTrue(manager.closeSession(session.getId(), "alice"));
        Assertions.assertFalse(manager.closeSession(session.getId(), "alice"));
        assertError(WebSqlError.SESSION_NOT_FOUND,
                () -> manager.execute(session.getId(), "alice", "SELECT 1"));
        Mockito.verify(second).close();

        WebSqlSession remaining = manager.createSession("bob", "secret");
        manager.destroy();
        Mockito.verify(remaining.getConnection()).close();
        Assertions.assertEquals(0, manager.size());
    }

    @Test
    void expiryProducesStableExpiredErrorAndFreesQuota() throws Exception {
        AtomicLong clock = new AtomicLong(100);
        Connection first = Mockito.mock(Connection.class);
        Connection second = Mockito.mock(Connection.class);
        AtomicInteger opens = new AtomicInteger();
        WebSqlConnectionFactory factory = (user, password) -> opens.getAndIncrement() == 0 ? first : second;
        WebSqlSessionManager manager = new WebSqlSessionManager(factory,
                Mockito.mock(WebSqlStatementExecutor.class), limits(1, 1, 10, 20), clock::get, false);

        WebSqlSession expired = manager.createSession("alice", "");
        clock.set(111);
        Assertions.assertEquals(1, manager.cleanupExpired());
        Mockito.verify(first).close();
        assertError(WebSqlError.SESSION_EXPIRED,
                () -> manager.execute(expired.getId(), "alice", "SELECT 1"));
        Assertions.assertNotNull(manager.createSession("alice", ""));
        manager.destroy();
    }

    @Test
    void sameSessionIsSerializedAndDifferentSessionsCanRunConcurrently() throws Exception {
        WebSqlConnectionFactory factory = (user, password) -> Mockito.mock(Connection.class);
        WebSqlStatementExecutor executor = Mockito.mock(WebSqlStatementExecutor.class);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        CountDownLatch bothSessionsActive = new CountDownLatch(2);
        Mockito.when(executor.execute(Mockito.any(), Mockito.anyString(), Mockito.any())).thenAnswer(invocation -> {
            int current = active.incrementAndGet();
            maximum.accumulateAndGet(current, Math::max);
            bothSessionsActive.countDown();
            bothSessionsActive.await(2, TimeUnit.SECONDS);
            Thread.sleep(40);
            active.decrementAndGet();
            return emptyResult();
        });
        WebSqlSessionManager manager = manager(factory, executor, limits(4, 4, 1000, 500));
        WebSqlSession first = manager.createSession("alice", "");
        WebSqlSession second = manager.createSession("alice", "");

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<?> firstRun = pool.submit(() -> manager.execute(first.getId(), "alice", "SELECT 1"));
        Future<?> secondRun = pool.submit(() -> manager.execute(second.getId(), "alice", "SELECT 2"));
        firstRun.get(2, TimeUnit.SECONDS);
        secondRun.get(2, TimeUnit.SECONDS);
        Assertions.assertEquals(2, maximum.get());

        active.set(0);
        maximum.set(0);
        CountDownLatch entered = new CountDownLatch(1);
        Mockito.reset(executor);
        Mockito.when(executor.execute(Mockito.any(), Mockito.anyString(), Mockito.any())).thenAnswer(invocation -> {
            int current = active.incrementAndGet();
            maximum.accumulateAndGet(current, Math::max);
            entered.countDown();
            Thread.sleep(80);
            active.decrementAndGet();
            return emptyResult();
        });
        Future<?> serialOne = pool.submit(() -> manager.execute(first.getId(), "alice", "SELECT 3"));
        Assertions.assertTrue(entered.await(1, TimeUnit.SECONDS));
        Future<?> serialTwo = pool.submit(() -> manager.execute(first.getId(), "alice", "SELECT 4"));
        serialOne.get(2, TimeUnit.SECONDS);
        serialTwo.get(2, TimeUnit.SECONDS);
        Assertions.assertEquals(1, maximum.get());
        pool.shutdownNow();
        manager.destroy();
    }

    @Test
    void connectionAndExecutionFailuresHaveStableErrors() throws Exception {
        WebSqlConnectionFactory broken = (user, password) -> {
            throw new SQLException("secret connection detail");
        };
        WebSqlSessionManager brokenManager = manager(broken, Mockito.mock(WebSqlStatementExecutor.class),
                limits(1, 1, 100, 20));
        assertError(WebSqlError.CONNECTION_ERROR, () -> brokenManager.createSession("alice", "secret"));

        Connection connection = Mockito.mock(Connection.class);
        WebSqlStatementExecutor executor = Mockito.mock(WebSqlStatementExecutor.class);
        Mockito.when(executor.execute(Mockito.any(), Mockito.anyString(), Mockito.any()))
                .thenThrow(new WebSqlException(WebSqlError.QUERY_ERROR));
        WebSqlSessionManager manager = manager((user, password) -> connection, executor,
                limits(1, 1, 100, 20));
        WebSqlSession session = manager.createSession("alice", "");
        assertError(WebSqlError.QUERY_ERROR, () -> manager.execute(session.getId(), "alice", "bad sql"));
        manager.destroy();
    }

    @Test
    void slowConnectionOpenDoesNotBlockSessionClose() throws Exception {
        Connection first = Mockito.mock(Connection.class);
        Connection second = Mockito.mock(Connection.class);
        AtomicInteger opens = new AtomicInteger();
        CountDownLatch opening = new CountDownLatch(1);
        CountDownLatch releaseOpen = new CountDownLatch(1);
        WebSqlConnectionFactory factory = (user, password) -> {
            if (opens.getAndIncrement() == 0) {
                return first;
            }
            opening.countDown();
            try {
                releaseOpen.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new SQLException("connection open interrupted", exception);
            }
            return second;
        };
        WebSqlSessionManager manager = manager(factory, Mockito.mock(WebSqlStatementExecutor.class),
                limits(3, 3, 1000, 20));
        WebSqlSession existing = manager.createSession("alice", "");
        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<WebSqlSession> openingSession = pool.submit(() -> manager.createSession("bob", ""));
        Assertions.assertTrue(opening.await(1, TimeUnit.SECONDS));

        Future<Boolean> closing = pool.submit(() -> manager.closeSession(existing.getId(), "alice"));
        Assertions.assertTrue(closing.get(1, TimeUnit.SECONDS));
        Mockito.verify(first).close();

        releaseOpen.countDown();
        Assertions.assertNotNull(openingSession.get(1, TimeUnit.SECONDS));
        pool.shutdownNow();
        manager.destroy();
    }

    @Test
    void failedConnectionOpenReleasesReservedQuota() {
        Connection connection = Mockito.mock(Connection.class);
        AtomicInteger opens = new AtomicInteger();
        WebSqlConnectionFactory factory = (user, password) -> {
            if (opens.getAndIncrement() == 0) {
                throw new SQLException("first open fails");
            }
            return connection;
        };
        WebSqlSessionManager manager = manager(factory, Mockito.mock(WebSqlStatementExecutor.class),
                limits(1, 1, 1000, 20));

        assertError(WebSqlError.CONNECTION_ERROR, () -> manager.createSession("alice", ""));
        Assertions.assertNotNull(manager.createSession("alice", ""));
        manager.destroy();
    }

    @Test
    void resetCannotInstallAConnectionAfterConcurrentClose() throws Exception {
        Connection original = Mockito.mock(Connection.class);
        Connection replacement = Mockito.mock(Connection.class);
        AtomicInteger opens = new AtomicInteger();
        CountDownLatch replacementOpening = new CountDownLatch(1);
        CountDownLatch releaseReplacement = new CountDownLatch(1);
        WebSqlConnectionFactory factory = (user, password) -> {
            if (opens.getAndIncrement() == 0) {
                return original;
            }
            replacementOpening.countDown();
            try {
                releaseReplacement.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new SQLException("reset interrupted", exception);
            }
            return replacement;
        };
        WebSqlSessionManager manager = manager(factory, Mockito.mock(WebSqlStatementExecutor.class),
                limits(2, 2, 1000, 20));
        WebSqlSession session = manager.createSession("alice", "");
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<WebSqlSession> resetting = pool.submit(() -> manager.reset(session.getId(), "alice", ""));
        Assertions.assertTrue(replacementOpening.await(1, TimeUnit.SECONDS));

        Assertions.assertTrue(manager.closeSession(session.getId(), "alice"));
        Mockito.verify(original).close();
        releaseReplacement.countDown();
        ExecutionException exception = Assertions.assertThrows(
                ExecutionException.class, () -> resetting.get(1, TimeUnit.SECONDS));
        Assertions.assertTrue(exception.getCause() instanceof WebSqlException);
        Assertions.assertEquals(WebSqlError.CONNECTION_ERROR,
                ((WebSqlException) exception.getCause()).getError());
        Mockito.verify(replacement).close();
        Assertions.assertEquals(0, manager.size());
        pool.shutdownNow();
        manager.destroy();
    }

    @Test
    void lockTimeoutReturnsBusyWithoutRunningASecondStatement() throws Exception {
        Connection connection = Mockito.mock(Connection.class);
        WebSqlStatementExecutor executor = Mockito.mock(WebSqlStatementExecutor.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.when(executor.execute(Mockito.any(), Mockito.anyString(), Mockito.any())).thenAnswer(invocation -> {
            entered.countDown();
            release.await(2, TimeUnit.SECONDS);
            return emptyResult();
        });
        WebSqlSessionManager manager = manager((user, password) -> connection, executor,
                limits(1, 1, 1000, 10));
        WebSqlSession session = manager.createSession("alice", "");
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> running = pool.submit(() -> manager.execute(session.getId(), "alice", "SELECT 1"));
        Assertions.assertTrue(entered.await(1, TimeUnit.SECONDS));

        assertError(WebSqlError.SESSION_BUSY,
                () -> manager.execute(session.getId(), "alice", "SELECT 2"));

        release.countDown();
        running.get(1, TimeUnit.SECONDS);
        Mockito.verify(executor, Mockito.times(1)).execute(Mockito.any(), Mockito.anyString(), Mockito.any());
        pool.shutdownNow();
        manager.destroy();
    }

    /**
     * Signing out of one browser must close only that browser's sessions. Shared accounts such as
     * root are the norm, so scoping this to the Doris account would cancel other operators' work.
     */
    @Test
    void closesOnlyTheSessionsOfOneBrowserSession() throws Exception {
        WebSqlConnectionFactory factory = (user, password) -> Mockito.mock(Connection.class);
        WebSqlSessionManager manager = manager(factory, Mockito.mock(WebSqlStatementExecutor.class),
                limits(4, 4, 60000, 500));
        UserIdentity alice = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
        WebSqlSession firstBrowser = manager.createSession(alice, "", "browser-1");
        WebSqlSession secondBrowser = manager.createSession(alice, "", "browser-2");

        Assertions.assertEquals(1, manager.closeSessionsForHttpSession("browser-1"));

        assertError(WebSqlError.SESSION_NOT_FOUND, () -> manager.getSession(firstBrowser.getId(), "alice"));
        Assertions.assertNotNull(manager.getSession(secondBrowser.getId(), "alice"));
        // Sessions opened with HTTP Basic belong to no browser session and are never swept by logout.
        Assertions.assertEquals(0, manager.closeSessionsForHttpSession(null));
        manager.destroy();
    }

    private WebSqlSessionManager manager(WebSqlConnectionFactory factory, WebSqlStatementExecutor executor,
            WebSqlLimits limits) {
        return new WebSqlSessionManager(factory, executor, limits, System::currentTimeMillis, false);
    }

    private WebSqlLimits limits(int maxSessions, int perUser, long idleMillis, int waitMillis) {
        return new WebSqlLimits(true, idleMillis, maxSessions, perUser, 100, waitMillis, 2, 60);
    }

    private WebSqlExecutionResult emptyResult() {
        return new WebSqlExecutionResult(Collections.emptyList(), Collections.emptyList(), 0,
                1, null, Collections.emptyList(), null, null, false);
    }

    private void assertError(WebSqlError expected, Runnable action) {
        WebSqlException exception = Assertions.assertThrows(WebSqlException.class, action::run);
        Assertions.assertEquals(expected, exception.getError());
    }
}
