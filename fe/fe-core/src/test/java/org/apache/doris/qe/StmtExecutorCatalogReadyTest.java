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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.CatalogNotReadyException;
import org.apache.doris.common.Config;
import org.apache.doris.common.QueryTimeoutException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StmtExecutorCatalogReadyTest {
    private final TUniqueId queryId = new TUniqueId(1, 2);
    private int previousRetryCount;
    private ConnectContext context;
    private Env env;
    private QueryState state;
    private LogicalPlanAdapter stmt;
    private StmtExecutor executor;

    @BeforeEach
    public void setUp() throws Exception {
        previousRetryCount = Config.max_query_retry_time;
        Config.max_query_retry_time = 0;
        context = Mockito.mock(ConnectContext.class);
        env = Mockito.mock(Env.class);
        state = new QueryState();
        stmt = Mockito.mock(LogicalPlanAdapter.class);
        Mockito.when(stmt.getLogicalPlan()).thenReturn(Mockito.mock(LogicalPlan.class));
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(context.getState()).thenReturn(state);
        Mockito.when(context.getExecTimeoutS()).thenReturn(2);
        Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
        executor = Mockito.spy(new StmtExecutor(context, "select 1"));
        setField("context", context);
        setField("parsedStmt", stmt);
    }

    @AfterEach
    public void tearDown() {
        Config.max_query_retry_time = previousRetryCount;
        Thread.interrupted();
    }

    private void setField(String name, Object value) throws Exception {
        Field field = StmtExecutor.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(executor, value);
    }

    @Test
    public void testReadOnlyQueryWaitsAndReentersExecution() throws Exception {
        Mockito.when(env.isReady()).thenReturn(false, true);
        Mockito.doAnswer(invocation -> {
            setField("masterOpExecutor", Mockito.mock(MasterOpExecutor.class));
            setField("isForwardedToMaster", true);
            state.setError("catalog not ready");
            throw new CatalogNotReadyException("catalog not ready");
        }).doAnswer(invocation -> {
            Assertions.assertEquals(QueryState.MysqlStateType.OK, state.getStateType());
            Assertions.assertFalse(executor.hasForwardedToMaster());
            Assertions.assertFalse(executor.isForwardToMaster());
            return null;
        }).when(executor).execute(queryId);

        Mockito.when(env.isMaster()).thenReturn(true);
        try (MockedStatic<Env> catalog = Mockito.mockStatic(Env.class)) {
            catalog.when(Env::getCurrentEnv).thenReturn(env);
            executor.queryRetry(queryId);
        }

        Mockito.verify(executor, Mockito.times(2)).execute(queryId);
    }

    @Test
    public void testElectionRacesDoNotConsumeReplanAttempts() throws Exception {
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.doThrow(new CatalogNotReadyException("first election"))
                .doThrow(new CatalogNotReadyException("second election"))
                .doNothing().when(executor).execute(queryId);
        executor.queryRetry(queryId);
        Mockito.verify(executor, Mockito.times(3)).execute(queryId);
    }

    @Test
    public void testWriteIsNotRetried() throws Exception {
        Mockito.when(stmt.getLogicalPlan()).thenReturn(Mockito.mock(Command.class));
        CatalogNotReadyException error = new CatalogNotReadyException("catalog not ready");
        Mockito.doThrow(error).when(executor).execute(queryId);
        Assertions.assertSame(error, Assertions.assertThrows(CatalogNotReadyException.class,
                () -> executor.queryRetry(queryId)));
        Mockito.verify(executor).execute(queryId);
        Mockito.verify(env, Mockito.never()).isReady();
    }

    @Test
    public void testOutfileIsNotRetried() throws Exception {
        Mockito.when(stmt.hasOutFileClause()).thenReturn(true);
        Mockito.doThrow(new CatalogNotReadyException("catalog not ready")).when(executor).execute(queryId);
        Assertions.assertThrows(CatalogNotReadyException.class, () -> executor.queryRetry(queryId));
        Mockito.verify(executor).execute(queryId);
    }

    @Test
    public void testOrdinarySqlErrorIsNotRetriedByMessage() throws Exception {
        UserException error = new UserException("Node catalog is not ready");
        Mockito.doThrow(error).when(executor).execute(queryId);
        Assertions.assertSame(error, Assertions.assertThrows(UserException.class,
                () -> executor.queryRetry(queryId)));
        Mockito.verify(executor).execute(queryId);
    }

    @Test
    public void testReadinessWaitHonorsQueryDeadline() throws Exception {
        Mockito.when(context.getExecTimeoutS()).thenReturn(0);
        Mockito.doThrow(new CatalogNotReadyException("catalog not ready")).when(executor).execute(queryId);
        Assertions.assertThrows(QueryTimeoutException.class, () -> executor.queryRetry(queryId));
        Mockito.verify(executor).execute(queryId);
    }

    @Test
    public void testRepeatedReadinessFailuresDoNotResetQueryDeadline() throws Exception {
        Mockito.when(context.getExecTimeoutS()).thenReturn(1);
        Mockito.when(env.isReady()).thenReturn(true);
        Mockito.doAnswer(invocation -> {
            TimeUnit.MILLISECONDS.sleep(100);
            throw new CatalogNotReadyException("election raced with query routing");
        }).when(executor).execute(queryId);
        Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(3),
                () -> Assertions.assertThrows(QueryTimeoutException.class, () -> executor.queryRetry(queryId)));
        Mockito.verify(executor, Mockito.atLeast(2)).execute(queryId);
    }

    @Test
    public void testCancelStopsQueryWhileWaiting() throws Exception {
        assertWaitCanBeStopped(false);
    }

    @Test
    public void testInterruptStopsWaitAndPreservesInterruptFlag() throws Exception {
        assertWaitCanBeStopped(true);
    }

    private void assertWaitCanBeStopped(boolean interrupt) throws Exception {
        CountDownLatch waiting = new CountDownLatch(1);
        Mockito.when(env.isReady()).thenAnswer(invocation -> {
            waiting.countDown();
            return false;
        });
        Mockito.doThrow(new CatalogNotReadyException("catalog not ready")).when(executor).execute(queryId);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> interrupted = new AtomicReference<>(false);
        Thread query = new Thread(() -> {
            try {
                executor.queryRetry(queryId);
            } catch (Throwable error) {
                failure.set(error);
                interrupted.set(Thread.currentThread().isInterrupted());
            }
        });
        query.start();
        try {
            Assertions.assertTrue(waiting.await(1, TimeUnit.SECONDS));
            if (interrupt) {
                query.interrupt();
            } else {
                executor.cancel(new Status(TStatusCode.CANCELLED, "cancel test"), false);
            }
            query.join(1000);
            Assertions.assertFalse(query.isAlive());
            Class<? extends Exception> expectedType = interrupt ? InterruptedException.class : UserException.class;
            Assertions.assertInstanceOf(expectedType, failure.get());
            Assertions.assertEquals(interrupt, interrupted.get());
            Mockito.verify(executor).execute(queryId);
        } finally {
            query.interrupt();
            query.join(1000);
        }
    }
}
