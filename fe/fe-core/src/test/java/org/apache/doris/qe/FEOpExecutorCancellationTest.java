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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FEOpExecutorCancellationTest {
    @Test
    public void delayedCancellationKeepsOriginalIdentityAndStatementResponse() throws Exception {
        ConnectContext context = context();
        RecordingExecutor executor = new RecordingExecutor(context, true);
        // Identity can be assigned after executor construction. Capture the actual dispatched request.
        TUniqueId queryId = new TUniqueId(11, 12);
        Mockito.when(context.queryId()).thenReturn(queryId);
        executor.queryResult.setQueryId(new TUniqueId(11, 12));
        executor.queryResult.setAuditStatisticsBackendIds(List.of(10001L, 10002L));
        executor.queryResult.setStatusCode(0);
        executor.cancelResult.setStatusCode(1105);
        CountDownLatch cancellerCapturedExecutor = new CountDownLatch(1);
        CountDownLatch allowCancellation = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> statement = workers.submit(() -> {
                executor.execute();
                return null;
            });
            await(executor.statementEntered);
            Future<?> cancellation = workers.submit(() -> {
                cancellerCapturedExecutor.countDown();
                await(allowCancellation);
                executor.cancel();
                return null;
            });
            await(cancellerCapturedExecutor);
            executor.statementMayReturn.countDown();
            statement.get(10, TimeUnit.SECONDS);

            // The old cancellation resumes after the connection has moved to the next statement.
            queryId.setHi(999);
            Mockito.when(context.queryId()).thenReturn(new TUniqueId(21, 22));
            Mockito.when(context.getDatabase()).thenReturn("next_db");
            Mockito.when(context.getQualifiedUser()).thenReturn("next_user");
            allowCancellation.countDown();
            cancellation.get(10, TimeUnit.SECONDS);
            // Another cancellation must keep the same target and leave the SQL response intact.
            executor.cancel();

            Assertions.assertEquals(3, executor.requests.size());
            executor.requests.remove();
            for (TMasterOpRequest cancelled : executor.requests) {
                Assertions.assertTrue(cancelled.isCancelQeury());
                Assertions.assertEquals(new TUniqueId(11, 12), cancelled.getQueryId());
                Assertions.assertEquals("original_db", cancelled.getDb());
                Assertions.assertEquals("original_user", cancelled.getUser());
                Assertions.assertEquals("127.0.0.1", cancelled.getClientNodeHost());
                Assertions.assertEquals(9010, cancelled.getClientNodePort());
                Assertions.assertEquals(7, cancelled.getStmtId());
            }
            Assertions.assertEquals(new TUniqueId(11, 12), executor.getQueryId());
            Assertions.assertEquals(0, executor.getStatusCode());
            Assertions.assertEquals(Set.of(10001L, 10002L), executor.getAuditStatisticsBackendIds());
        } finally {
            allowCancellation.countDown();
            executor.statementMayReturn.countDown();
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void cancellationBeforeTargetPublicationPreventsDispatch() throws Exception {
        ConnectContext context = context();
        Mockito.when(context.queryId()).thenReturn(new TUniqueId(11, 12));
        CountDownLatch buildingRequest = new CountDownLatch(1);
        CountDownLatch allowPublication = new CountDownLatch(1);
        RecordingExecutor executor = new RecordingExecutor(context, false) {
            @Override
            protected TMasterOpRequest buildStmtForwardParams() {
                TMasterOpRequest request = super.buildStmtForwardParams();
                buildingRequest.countDown();
                await(allowPublication);
                return request;
            }
        };
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> statement = worker.submit(() -> {
                executor.execute();
                return null;
            });
            await(buildingRequest);
            executor.cancel();
            allowPublication.countDown();
            ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                    () -> statement.get(10, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(UserException.class, failure.getCause());
            Assertions.assertTrue(executor.requests.isEmpty());
        } finally {
            allowPublication.countDown();
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void requestWithoutQueryIdCannotCancelLaterQuery() throws Exception {
        ConnectContext context = context();
        RecordingExecutor executor = new RecordingExecutor(context, false);
        executor.execute();

        Mockito.when(context.queryId()).thenReturn(new TUniqueId(21, 22));
        executor.cancel();

        Assertions.assertEquals(1, executor.requests.size());
        Assertions.assertFalse(executor.requests.remove().isCancelQeury());
    }

    @Test
    public void masterCancellationWaitsForItsOwnJournalWithoutReplacingStatementResult() throws Exception {
        ConnectContext context = context();
        Mockito.when(context.queryId()).thenReturn(new TUniqueId(11, 12));
        TMasterOpResult queryResult = new TMasterOpResult().setMaxJournalId(17).setStatusCode(0);
        TMasterOpResult cancelResult = new TMasterOpResult().setMaxJournalId(23).setStatusCode(1105);
        MasterOpExecutor executor = new MasterOpExecutor(new OriginStatement("select 1", 0), context,
                RedirectStatus.FORWARD_WITH_SYNC, true) {
            @Override
            protected TMasterOpRequest buildStmtForwardParams() {
                return request(context);
            }

            @Override
            protected TMasterOpResult forward(TMasterOpRequest params) {
                return params.isCancelQeury() ? cancelResult : queryResult;
            }
        };

        executor.execute();
        executor.cancel();

        JournalObservable journal = context.getEnv().getJournalObservable();
        Mockito.verify(journal).waitOn(Mockito.eq(17L), Mockito.anyInt());
        Mockito.verify(journal).waitOn(Mockito.eq(23L), Mockito.anyInt());
        Assertions.assertEquals(0, executor.getStatusCode());
    }

    @Test
    public void masterCancellationBeforeDispatchDoesNotWaitForMissingResponse() throws Exception {
        ConnectContext context = context();
        MasterOpExecutor executor = new MasterOpExecutor(new OriginStatement("select 1", 0), context,
                RedirectStatus.FORWARD_WITH_SYNC, true) {
            @Override
            protected TMasterOpRequest buildStmtForwardParams() {
                return request(context);
            }

            @Override
            protected TMasterOpResult forward(TMasterOpRequest params) {
                throw new AssertionError("A cancelled statement must not be forwarded");
            }
        };

        executor.cancel();
        Assertions.assertThrows(UserException.class, executor::execute);

        Mockito.verifyNoInteractions(context.getEnv().getJournalObservable());
    }

    private static ConnectContext context() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(context.getExecTimeoutS()).thenReturn(1);
        Mockito.when(context.getDatabase()).thenReturn("original_db");
        Mockito.when(context.getQualifiedUser()).thenReturn("original_user");
        Mockito.when(env.getMasterHost()).thenReturn("127.0.0.1");
        Mockito.when(env.getMasterRpcPort()).thenReturn(9010);
        Mockito.when(env.getJournalObservable()).thenReturn(Mockito.mock(JournalObservable.class));
        return context;
    }

    private static TMasterOpRequest request(ConnectContext context) {
        TMasterOpRequest request = new TMasterOpRequest();
        request.setQueryId(context.queryId());
        request.setDb(context.getDatabase());
        request.setUser(context.getQualifiedUser());
        request.setStmtId(7);
        request.setClientNodeHost("127.0.0.1");
        request.setClientNodePort(9010);
        request.setSql("select 1");
        return request;
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS), "Timed out waiting for test handoff");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static class RecordingExecutor extends FEOpExecutor {
        private final ConcurrentLinkedQueue<TMasterOpRequest> requests = new ConcurrentLinkedQueue<>();
        private final CountDownLatch statementEntered = new CountDownLatch(1);
        private final CountDownLatch statementMayReturn;
        private final TMasterOpResult queryResult = new TMasterOpResult();
        private final TMasterOpResult cancelResult = new TMasterOpResult();

        private RecordingExecutor(ConnectContext context, boolean blockStatement) {
            super(new TNetworkAddress("127.0.0.1", 9010), new OriginStatement("select 1", 0), context, true);
            statementMayReturn = new CountDownLatch(blockStatement ? 1 : 0);
        }

        @Override
        protected TMasterOpRequest buildStmtForwardParams() {
            return request(ctx);
        }

        @Override
        protected TMasterOpResult forward(TMasterOpRequest params) {
            requests.add(params.deepCopy());
            if (params.isCancelQeury()) {
                return cancelResult;
            }
            statementEntered.countDown();
            await(statementMayReturn);
            return queryResult;
        }
    }
}
