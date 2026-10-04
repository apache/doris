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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.proto.Types.PUniqueId;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    public void testMessageSuffixPreservesMissingAndDifferentQueryIds() {
        TUniqueId queryId = new TUniqueId(1, 2);
        Assertions.assertEquals(" [1-2]", QueryLogContext.queryIdSuffix(queryId));
        Assertions.assertEquals("", QueryLogContext.queryIdSuffix((TUniqueId) null));
        Assertions.assertEquals("", QueryLogContext.queryIdSuffix(new TUniqueId(0, 0)));
        Assertions.assertEquals(" [0-2]", QueryLogContext.queryIdSuffix(new TUniqueId(0, 2)));
        Assertions.assertEquals(" [1-0]", QueryLogContext.queryIdSuffix(new TUniqueId(1, 0)));
        try (QueryLogContext ignored = QueryLogContext.open(queryId)) {
            Assertions.assertEquals("", QueryLogContext.queryIdSuffix(queryId));
            Assertions.assertEquals(" [1-3]", QueryLogContext.queryIdSuffix(new TUniqueId(1, 3)));
            Assertions.assertEquals(" [3-2]", QueryLogContext.queryIdSuffix(new TUniqueId(3, 2)));
            // The prefix is a snapshot; an ID changed by a retry must remain explicit.
            queryId.setLo(4);
            Assertions.assertEquals(" [1-4]", QueryLogContext.queryIdSuffix(queryId));
        }
        Assertions.assertEquals(" [1-4]", QueryLogContext.queryIdSuffix(queryId));
        Assertions.assertEquals(" [ffffffffffffffff-8000000000000000]",
                QueryLogContext.queryIdSuffix(new TUniqueId(-1, Long.MIN_VALUE)));
    }

    @Test
    public void testProtobufMessageSuffixUsesTheSameQueryIdentity() {
        PUniqueId queryId = PUniqueId.newBuilder().setHi(1).setLo(2).build();
        Assertions.assertEquals(" [1-2]", QueryLogContext.queryIdSuffix(queryId));
        Assertions.assertEquals("", QueryLogContext.queryIdSuffix((PUniqueId) null));
        Assertions.assertEquals("", QueryLogContext.queryIdSuffix(PUniqueId.getDefaultInstance()));
        Assertions.assertEquals(" [0-2]", QueryLogContext.queryIdSuffix(queryId.toBuilder().setHi(0).build()));
        Assertions.assertEquals(" [1-0]", QueryLogContext.queryIdSuffix(queryId.toBuilder().setLo(0).build()));
        try (QueryLogContext ignored = QueryLogContext.open(new TUniqueId(1, 2))) {
            Assertions.assertEquals("", QueryLogContext.queryIdSuffix(queryId));
            Assertions.assertEquals(" [1-3]",
                    QueryLogContext.queryIdSuffix(queryId.toBuilder().setLo(3).build()));
            Config.sys_log_enable_query_id = false;
            Assertions.assertEquals(" [1-2]", QueryLogContext.queryIdSuffix(queryId));
            Config.sys_log_enable_query_id = true;
        }
    }

    @Test
    public void testTimeoutCheckBindsTheTargetQueryAndRestoresTheWorker() {
        AtomicReference<String> observed = new AtomicReference<>();
        ConnectContext target = new ConnectContext() {
            @Override
            public int getExecTimeoutS() {
                observed.set(ThreadContext.get(QueryLogContext.QUERY_ID));
                return 1;
            }
        };
        target.setCommand(MysqlCommand.COM_QUERY);
        target.setStartTime();
        ThreadContext.put(QueryLogContext.QUERY_ID, "worker");
        target.setQueryId(new TUniqueId(1, 2));
        target.checkTimeout(target.getStartTime());
        Assertions.assertEquals("1-2", observed.get());
        Assertions.assertEquals("worker", ThreadContext.get(QueryLogContext.QUERY_ID));
        target.setQueryId(new TUniqueId(3, 4));
        target.checkTimeout(target.getStartTime());
        Assertions.assertEquals("3-4", observed.get());
        Assertions.assertEquals("worker", ThreadContext.get(QueryLogContext.QUERY_ID));
        for (MysqlCommand command : new MysqlCommand[] {MysqlCommand.COM_FIELD_LIST, MysqlCommand.COM_PING}) {
            target.setCommand(command);
            target.checkTimeout(target.getStartTime());
            Assertions.assertNull(observed.get(), "Metadata commands do not own the retained query ID");
            Assertions.assertEquals("worker", ThreadContext.get(QueryLogContext.QUERY_ID));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testExpiredQueryLogsAndCancelsTheTargetWithoutChangingCheckerIdentity(boolean enabled) {
        Config.sys_log_enable_query_id = enabled;
        ThreadContext.put(QueryLogContext.QUERY_ID, "checker");
        String expected = enabled ? "1-2" : "checker";
        for (MysqlCommand command : new MysqlCommand[] {MysqlCommand.COM_QUERY,
                MysqlCommand.COM_STMT_PREPARE, MysqlCommand.COM_STMT_EXECUTE}) {
            ConnectContext target = new ConnectContext() {
                @Override
                public int getExecTimeoutS() {
                    return 1;
                }
            };
            target.setQueryId(new TUniqueId(1, 2));
            target.setCommand(command);
            target.setStartTime();
            StmtExecutor executor = Mockito.mock(StmtExecutor.class);
            target.setExecutor(executor);
            List<String> cancellations = new ArrayList<>();
            List<Status> reasons = new ArrayList<>();
            Mockito.doAnswer(invocation -> {
                cancellations.add(ThreadContext.get(QueryLogContext.QUERY_ID));
                reasons.add(invocation.getArgument(0));
                return null;
            }).when(executor).cancel(Mockito.any(Status.class));
            try (TestLogAppender appender = TestLogAppender.attach(ConnectContext.class, Level.WARN)) {
                target.checkTimeout(target.getStartTime() + 1001);
                Assertions.assertEquals(Collections.singletonList(expected), cancellations);
                Assertions.assertEquals(TStatusCode.TIMEOUT, reasons.get(0).getErrorCode());
                Assertions.assertTrue(appender.contains(Level.WARN,
                        enabled ? "kill query timeout," : "kill query timeout [1-2],"));
                List<LogEvent> events = Deencapsulation.getField(appender, "events");
                Assertions.assertEquals(2, events.size());
                events.forEach(event -> Assertions.assertEquals(expected,
                        event.getContextData().getValue(QueryLogContext.QUERY_ID)));
            }
            Assertions.assertEquals("checker", ThreadContext.get(QueryLogContext.QUERY_ID));
            Assertions.assertNull(ConnectContext.get());
        }
    }

    @Test
    public void testStatementLogIdentifierKeepsTheStatementAndFallbackQuery() {
        ConnectContext context = new ConnectContext();
        context.setStmtId(7);
        context.setQueryId(new TUniqueId(1, 2));
        Assertions.assertEquals("stmt[7] [1-2]", context.getQueryLogIdentifier());
        try (QueryLogContext ignored = QueryLogContext.open(context.queryId())) {
            Assertions.assertEquals("stmt[7]", context.getQueryLogIdentifier());
            // The identifier used outside runtime logging must remain self-contained.
            Assertions.assertEquals("stmt[7, 1-2]", context.getQueryIdentifier());
        }
    }

    @Test
    public void testRetryLogRetainsTheRelationshipBetweenAttempts() throws Exception {
        int savedRetries = Config.max_query_retry_time;
        ConnectContext queryContext = new ConnectContext();
        queryContext.setThreadLocalInfo();
        List<TUniqueId> attempts = new ArrayList<>();
        StmtExecutor executor = new StmtExecutor(queryContext, "select 1") {
            @Override
            public void execute(TUniqueId queryId) throws Exception {
                queryContext.setQueryId(queryId);
                attempts.add(queryId);
                Assertions.assertEquals(DebugUtil.printId(queryId), ThreadContext.get(QueryLogContext.QUERY_ID));
                if (attempts.size() == 1) {
                    throw new UserException(SystemInfoService.NO_SCAN_NODE_BACKEND_AVAILABLE_MSG);
                }
            }
        };
        try (TestLogAppender appender = TestLogAppender.attach(StmtExecutor.class, Level.WARN)) {
            Config.max_query_retry_time = 1;
            executor.queryRetry(new TUniqueId(1, 2));
            Assertions.assertEquals(2, attempts.size());
            Assertions.assertNotEquals(attempts.get(0), attempts.get(1));
            Assertions.assertTrue(appender.contains(Level.WARN,
                    "first queryId=1-2 last queryId=1-2 new queryId=" + DebugUtil.printId(attempts.get(1))));
            Assertions.assertEquals(DebugUtil.printId(attempts.get(1)), ThreadContext.get(QueryLogContext.QUERY_ID));
        } finally {
            Config.max_query_retry_time = savedRetries;
        }
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
            Assertions.assertEquals(" [1-2]", worker.submit(
                    () -> QueryLogContext.queryIdSuffix(new TUniqueId(1, 2))).get(10, TimeUnit.SECONDS));
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
        Assertions.assertEquals(" [1-2]", QueryLogContext.queryIdSuffix(queryId));
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
