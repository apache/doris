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

package org.apache.doris.common;

import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QueryLogContextTest {
    private boolean savedEnabled;
    private boolean savedRunningUnitTest;
    private String savedQueryId;
    private ConnectContext savedConnectContext;

    @BeforeEach
    public void setUp() {
        savedEnabled = Config.sys_log_enable_query_id;
        savedRunningUnitTest = FeConstants.runningUnitTest;
        savedQueryId = ThreadContext.get(QueryLogContext.QUERY_ID);
        savedConnectContext = ConnectContext.get();
        Config.sys_log_enable_query_id = true;
        FeConstants.runningUnitTest = true;
        ConnectContext.remove();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        if (savedConnectContext != null) {
            savedConnectContext.setThreadLocalInfo();
        }
        if (savedQueryId == null) {
            ThreadContext.remove(QueryLogContext.QUERY_ID);
        } else {
            ThreadContext.put(QueryLogContext.QUERY_ID, savedQueryId);
        }
        Config.sys_log_enable_query_id = savedEnabled;
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @Test
    public void testNestedScopesAndMissingIdsRestoreTheCaller() {
        Assertions.assertNull(ThreadContext.get(QueryLogContext.QUERY_ID));
        try (QueryLogContext outer = QueryLogContext.open(new TUniqueId(0x1234, 0xabcd))) {
            Assertions.assertEquals("1234-abcd", ThreadContext.get(QueryLogContext.QUERY_ID));
            try (QueryLogContext inner = QueryLogContext.open(new TUniqueId(-1, Long.MIN_VALUE))) {
                Assertions.assertEquals("ffffffffffffffff-8000000000000000",
                        ThreadContext.get(QueryLogContext.QUERY_ID));
            }
            Assertions.assertEquals("1234-abcd", ThreadContext.get(QueryLogContext.QUERY_ID));
            try (QueryLogContext empty = QueryLogContext.open(null)) {
                Assertions.assertNull(ThreadContext.get(QueryLogContext.QUERY_ID));
            }
            try (QueryLogContext zero = QueryLogContext.open(new TUniqueId(0, 0))) {
                Assertions.assertNull(ThreadContext.get(QueryLogContext.QUERY_ID));
            }
            Assertions.assertEquals("1234-abcd", ThreadContext.get(QueryLogContext.QUERY_ID));
        }
        Assertions.assertNull(ThreadContext.get(QueryLogContext.QUERY_ID));
    }

    @Test
    public void testScopeSnapshotsMutableIdAndRestoresAfterException() {
        ThreadContext.put(QueryLogContext.QUERY_ID, "caller");
        TUniqueId queryId = new TUniqueId(1, 2);
        Assertions.assertThrows(IllegalStateException.class, () -> {
            try (QueryLogContext ignored = QueryLogContext.open(queryId)) {
                queryId.setHi(3);
                queryId.setLo(4);
                Assertions.assertEquals("1-2", ThreadContext.get(QueryLogContext.QUERY_ID));
                throw new IllegalStateException("test unwind");
            }
        });
        Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
    }

    @Test
    public void testWrappedCallbackCapturesIdAtRegistration() {
        TUniqueId queryId = new TUniqueId(1, 2);
        Runnable wrapped = QueryLogContext.wrap(() -> {
            Assertions.assertEquals("1-2", ThreadContext.get(QueryLogContext.QUERY_ID));
            throw new IllegalStateException("callback failure");
        }, queryId);
        queryId.setLo(3);
        ThreadContext.put(QueryLogContext.QUERY_ID, "worker");
        Assertions.assertThrows(IllegalStateException.class, wrapped::run);
        Assertions.assertEquals("worker", ThreadContext.get(QueryLogContext.QUERY_ID));

        Runnable withoutQuery = QueryLogContext.wrap(
                () -> Assertions.assertNull(ThreadContext.get(QueryLogContext.QUERY_ID)), null);
        withoutQuery.run();
        Assertions.assertEquals("worker", ThreadContext.get(QueryLogContext.QUERY_ID));
    }

    @Test
    public void testExecutorCapturesIdAndRestoresReusedWorker() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            worker.submit(() -> ThreadContext.put(QueryLogContext.QUERY_ID, "worker"))
                    .get(10, TimeUnit.SECONDS);
            TUniqueId queryId = new TUniqueId(1, 2);
            Executor scopedExecutor = QueryLogContext.executor(worker, queryId);
            queryId.setLo(3);
            ThreadContext.put(QueryLogContext.QUERY_ID, "caller");

            CompletableFuture<String> observed = new CompletableFuture<>();
            scopedExecutor.execute(() -> observed.complete(ThreadContext.get(QueryLogContext.QUERY_ID)));
            Assertions.assertEquals("1-2", observed.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals("worker", worker.submit(() -> ThreadContext.get(QueryLogContext.QUERY_ID))
                    .get(10, TimeUnit.SECONDS));
            Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
        } finally {
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testDisabledFeatureLeavesContextsAndCallbacksUntouched() {
        Config.sys_log_enable_query_id = false;
        ThreadContext.put(QueryLogContext.QUERY_ID, "caller");
        Runnable callback = () -> Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
        Executor executor = Runnable::run;
        TUniqueId queryId = new TUniqueId(1, 2);
        try (QueryLogContext ignored = QueryLogContext.open(queryId)) {
            QueryLogContext.setQueryId(queryId);
            QueryLogContext.clear();
            Assertions.assertSame(callback, QueryLogContext.wrap(callback, queryId));
            Assertions.assertSame(executor, QueryLogContext.executor(executor, queryId));
            callback.run();
        }
        Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
    }

    @Test
    public void testOnlyTheCurrentConnectionUpdatesTheLogIdentity() {
        ConnectContext current = new ConnectContext();
        current.setQueryId(new TUniqueId(1, 2));
        Assertions.assertNull(ThreadContext.get(QueryLogContext.QUERY_ID));
        current.setThreadLocalInfo();
        Assertions.assertEquals("1-2", ThreadContext.get(QueryLogContext.QUERY_ID));

        ConnectContext other = new ConnectContext();
        other.setQueryId(new TUniqueId(3, 4));
        other.resetQueryId();
        Assertions.assertEquals("1-2", ThreadContext.get(QueryLogContext.QUERY_ID));
        current.setQueryId(new TUniqueId(5, 6));
        Assertions.assertEquals("5-6", ThreadContext.get(QueryLogContext.QUERY_ID));
        current.resetQueryId();
        Assertions.assertNull(ThreadContext.get(QueryLogContext.QUERY_ID));
        current.setQueryId(new TUniqueId(7, 8));
        ConnectContext.remove();
        Assertions.assertNull(ThreadContext.get(QueryLogContext.QUERY_ID));
    }

    @Test
    public void testAutoCloseConnectionRestoresIdentityWithoutAnOuterConnection() {
        ThreadContext.put(QueryLogContext.QUERY_ID, "callback");
        ConnectContext inner = new ConnectContext();
        inner.setQueryId(new TUniqueId(1, 2));
        Assertions.assertThrows(IllegalStateException.class, () -> {
            try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(inner)) {
                Assertions.assertSame(inner, ConnectContext.get());
                Assertions.assertEquals("1-2", ThreadContext.get(QueryLogContext.QUERY_ID));
                throw new IllegalStateException("test unwind");
            }
        });
        Assertions.assertNull(ConnectContext.get());
        Assertions.assertEquals("callback", ThreadContext.get(QueryLogContext.QUERY_ID));
    }
}
