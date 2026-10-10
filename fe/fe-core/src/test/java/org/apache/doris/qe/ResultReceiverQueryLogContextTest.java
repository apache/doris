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

import org.apache.doris.common.Config;
import org.apache.doris.common.QueryLogContext;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.proto.InternalService.PFetchDataResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class ResultReceiverQueryLogContextTest {
    private static final TUniqueId QUERY_ID = new TUniqueId(68694, 501);
    private boolean savedEnabled;
    private String savedQueryId;

    @BeforeEach
    void setUp() {
        savedEnabled = Config.sys_log_enable_query_id;
        savedQueryId = ThreadContext.get(QueryLogContext.QUERY_ID);
        Config.sys_log_enable_query_id = true;
        QueryLogContext.clear();
    }

    @AfterEach
    void tearDown() {
        Config.sys_log_enable_query_id = savedEnabled;
        if (savedQueryId == null) {
            ThreadContext.remove(QueryLogContext.QUERY_ID);
        } else {
            ThreadContext.put(QueryLogContext.QUERY_ID, savedQueryId);
        }
    }

    @Test
    void cancelAndDuplicateCancelKeepFirstReasonWithoutRepeatingPrefixId() {
        Future<PFetchDataResult> future = future();
        Mockito.when(future.cancel(true)).thenReturn(true);
        ResultReceiver receiver = receiver(future);
        try (QueryLogContext ignored = QueryLogContext.open(QUERY_ID);
                TestLogAppender appender = TestLogAppender.attach(ResultReceiver.class)) {
            receiver.cancel(new Status(TStatusCode.CANCELLED, "first cancellation"));
            receiver.cancel(new Status(TStatusCode.CANCELLED, "second cancellation"));
            List<LogEvent> events = events(appender);
            Assertions.assertEquals(2, events.size());
            for (LogEvent event : events) {
                Assertions.assertEquals(DebugUtil.printId(QUERY_ID),
                        event.getContextData().getValue(QueryLogContext.QUERY_ID));
                Assertions.assertFalse(event.getMessage().getFormattedMessage().contains(DebugUtil.printId(QUERY_ID)));
            }
            Status runStatus = Deencapsulation.getField(receiver, "runStatus");
            Assertions.assertEquals("first cancellation", runStatus.getErrorMsg());
            Mockito.verify(future).cancel(true);
        }
    }

    @Test
    void completedFutureCancellationKeepsTargetIdWhenPrefixIsDisabledOrDifferent() {
        for (boolean enabled : new boolean[] {false, true}) {
            Config.sys_log_enable_query_id = enabled;
            ThreadContext.put(QueryLogContext.QUERY_ID, "another-query");
            Future<PFetchDataResult> future = future();
            Mockito.when(future.cancel(true)).thenReturn(false);
            try (TestLogAppender appender = TestLogAppender.attach(ResultReceiver.class)) {
                receiver(future).cancel(new Status(TStatusCode.CANCELLED, "cancel completed future"));
                Assertions.assertTrue(appender.contains(Level.WARN, DebugUtil.printId(QUERY_ID)));
                Assertions.assertTrue(appender.contains(Level.WARN, "future is finished"));
                Assertions.assertEquals("another-query", ThreadContext.get(QueryLogContext.QUERY_ID));
            }
        }
    }

    @Test
    void interruptedFetchRetainsQueryInWarningAndPropagatesStatus() throws Exception {
        Future<PFetchDataResult> future = future();
        Mockito.when(future.get(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS)))
                .thenThrow(new InterruptedException("injected interruption"));
        ResultReceiver receiver = receiver(future);
        Status result = new Status();
        try (TestLogAppender appender = TestLogAppender.attach(ResultReceiver.class)) {
            Assertions.assertNull(receiver.getNext(result));
            Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, result.getErrorCode());
            Assertions.assertTrue(appender.contains(Level.WARN, DebugUtil.printId(QUERY_ID)));
            Assertions.assertTrue(appender.contains(Level.WARN, "interrupted"));
            Assertions.assertNull(Deencapsulation.getField(receiver, "currentThread"));
            Assertions.assertNull(ThreadContext.get(QueryLogContext.QUERY_ID));
        }
    }

    @Test
    void cancellationDuringFetchKeepsOriginalReasonAndCorrectEventIdentity() throws Exception {
        Future<PFetchDataResult> future = future();
        Mockito.when(future.cancel(true)).thenReturn(true);
        ResultReceiver receiver = receiver(future);
        Mockito.when(future.get(Mockito.anyLong(), Mockito.eq(TimeUnit.MILLISECONDS))).thenAnswer(invocation -> {
            receiver.cancel(new Status(TStatusCode.CANCELLED, "cancel while waiting"));
            throw new CancellationException("injected cancellation");
        });
        try (QueryLogContext ignored = QueryLogContext.open(QUERY_ID);
                TestLogAppender appender = TestLogAppender.attach(ResultReceiver.class)) {
            Status result = new Status();
            Assertions.assertNull(receiver.getNext(result));
            Assertions.assertEquals(TStatusCode.CANCELLED, result.getErrorCode());
            Assertions.assertEquals("cancel while waiting", result.getErrorMsg());
            LogEvent warning = events(appender).stream().filter(e -> e.getLevel() == Level.WARN)
                    .findFirst().orElseThrow();
            Assertions.assertEquals(DebugUtil.printId(QUERY_ID),
                    warning.getContextData().getValue(QueryLogContext.QUERY_ID));
            Assertions.assertFalse(warning.getMessage().getFormattedMessage().contains(DebugUtil.printId(QUERY_ID)));
            Assertions.assertNull(Deencapsulation.getField(receiver, "currentThread"));
        }
    }

    private static ResultReceiver receiver(Future<PFetchDataResult> future) {
        ResultReceiver receiver = new ResultReceiver(QUERY_ID, new TUniqueId(68694, 502), 7L,
                new TNetworkAddress("127.0.0.1", 8060), System.currentTimeMillis() + 60000, 1024, false);
        Deencapsulation.setField(receiver, "fetchDataAsyncFuture", future);
        return receiver;
    }

    @SuppressWarnings("unchecked")
    private static Future<PFetchDataResult> future() {
        return Mockito.mock(Future.class);
    }

    private static List<LogEvent> events(TestLogAppender appender) {
        return Deencapsulation.getField(appender, "events");
    }
}
