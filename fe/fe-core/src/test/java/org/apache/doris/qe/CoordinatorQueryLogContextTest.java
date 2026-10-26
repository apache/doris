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

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.QueryLogContext;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.proto.InternalService.PCancelPlanFragmentResult;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.resource.workloadgroup.QueryQueue;
import org.apache.doris.resource.workloadgroup.QueueToken;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CoordinatorQueryLogContextTest {
    private boolean savedEnabled;
    private String savedQueryId;

    @BeforeEach
    public void setUp() {
        savedEnabled = Config.sys_log_enable_query_id;
        savedQueryId = ThreadContext.get(QueryLogContext.QUERY_ID);
        Config.sys_log_enable_query_id = true;
        ThreadContext.put(QueryLogContext.QUERY_ID, "caller");
    }

    @AfterEach
    public void tearDown() {
        Config.sys_log_enable_query_id = savedEnabled;
        if (savedQueryId == null) {
            ThreadContext.remove(QueryLogContext.QUERY_ID);
        } else {
            ThreadContext.put(QueryLogContext.QUERY_ID, savedQueryId);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCloseAttributesQueueAndScanFailuresToTheTarget(boolean enabled) {
        Config.sys_log_enable_query_id = enabled;
        String expected = enabled ? "1-2" : "caller";
        List<String> observed = new ArrayList<>();
        ScanNode scan = Mockito.mock(ScanNode.class);
        QueryQueue queue = Mockito.mock(QueryQueue.class);
        QueueToken token = Mockito.mock(QueueToken.class);
        Coordinator coordinator = coordinator(ImmutableList.of(scan));
        Deencapsulation.setField(coordinator, "queryQueue", queue);
        Deencapsulation.setField(coordinator, "queueToken", token);
        Mockito.doAnswer(invocation -> {
            observed.add(ThreadContext.get(QueryLogContext.QUERY_ID));
            throw new IllegalStateException("queue cleanup failed");
        }).when(queue).releaseAndNotify(token);
        Mockito.doAnswer(invocation -> {
            observed.add(ThreadContext.get(QueryLogContext.QUERY_ID));
            throw new IllegalStateException("scan cleanup failed");
        }).when(scan).stop();

        try (TestLogAppender appender = TestLogAppender.attach(Coordinator.class, Level.ERROR)) {
            coordinator.close();
            // The implementation catches cleanup failures, so assert outside its callbacks.
            Assertions.assertEquals(ImmutableList.of(expected, expected), observed);
            Assertions.assertEquals(2, events(appender).size());
            events(appender).forEach(event -> Assertions.assertEquals(expected,
                    event.getContextData().getValue(QueryLogContext.QUERY_ID)));
        }
        Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCancelKeepsItsIdentityThroughRepeatedCancellationAndScanFailure(boolean enabled) {
        Config.sys_log_enable_query_id = enabled;
        String expected = enabled ? "1-2" : "caller";
        List<String> observed = new ArrayList<>();
        ScanNode failing = Mockito.mock(ScanNode.class);
        ScanNode remaining = Mockito.mock(ScanNode.class);
        Coordinator coordinator = coordinator(ImmutableList.of(failing, remaining));
        Mockito.doAnswer(invocation -> {
            observed.add(ThreadContext.get(QueryLogContext.QUERY_ID));
            throw new IllegalStateException("scan cleanup failed");
        }).when(failing).stop();
        Mockito.doAnswer(invocation -> {
            observed.add(ThreadContext.get(QueryLogContext.QUERY_ID));
            return null;
        }).when(remaining).stop();
        Status reason = new Status(TStatusCode.TIMEOUT, "target timed out");

        try (TestLogAppender appender = TestLogAppender.attach(Coordinator.class, Level.DEBUG)) {
            coordinator.cancel(reason);
            coordinator.cancel(new Status(TStatusCode.CANCELLED, "later cancellation"));
            Assertions.assertEquals(ImmutableList.of(expected, expected, expected, expected), observed);
            Assertions.assertEquals(TStatusCode.TIMEOUT, coordinator.getExecStatus().getErrorCode());
            Assertions.assertEquals("target timed out", coordinator.getExecStatus().getErrorMsg());
            Assertions.assertTrue(appender.contains(Level.DEBUG, "received cancel again"));
            Assertions.assertEquals(2, events(appender).stream().filter(e -> e.getLevel() == Level.ERROR).count());
            events(appender).forEach(event -> Assertions.assertEquals(expected,
                    event.getContextData().getValue(QueryLogContext.QUERY_ID)));
            Assertions.assertTrue(appender.contains(Level.WARN,
                    enabled ? "Cancel execution of query," : "Cancel execution of query [1-2],"));
        }
        Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
        Assertions.assertThrows(RuntimeException.class, () -> coordinator.cancel(Status.OK));
        Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCancelCallbackCapturesRegistrationIdAndRestoresWorker(boolean failedFuture) throws Exception {
        ExecutorService original = Coordinator.backendRpcCallbackExecutor;
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Coordinator.backendRpcCallbackExecutor = worker;
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        SettableFuture<PCancelPlanFragmentResult> future = SettableFuture.create();
        TUniqueId queryId = new TUniqueId(1, 2);
        Coordinator coordinator = coordinator(Collections.emptyList());
        Backend backend = new Backend(7L, "127.0.0.1", 9050);
        TNetworkAddress address = new TNetworkAddress("127.0.0.1", 8060);
        Coordinator.PipelineExecContexts remote = new Coordinator.PipelineExecContexts(
                queryId, backend, address, false, 1);
        Map<Long, Coordinator.PipelineExecContexts> remotes =
                Deencapsulation.getField(coordinator, "beToPipelineExecCtxs");
        remotes.put(backend.getId(), remote);
        Status reason = new Status(TStatusCode.CANCELLED, "cancel target");
        try (MockedStatic<BackendServiceProxy> mocked = Mockito.mockStatic(BackendServiceProxy.class);
                TestLogAppender appender = TestLogAppender.attach(Coordinator.class, Level.WARN)) {
            mocked.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Mockito.when(proxy.cancelPipelineXPlanFragmentAsync(address, queryId, reason)).thenReturn(future);
            worker.submit(() -> ThreadContext.put(QueryLogContext.QUERY_ID, "worker")).get(10, TimeUnit.SECONDS);
            coordinator.cancel(reason);
            Mockito.verify(proxy).cancelPipelineXPlanFragmentAsync(address, queryId, reason);
            queryId.setLo(3);
            coordinator.setQueryId(new TUniqueId(9, 9));
            if (failedFuture) {
                future.setException(new IllegalStateException("cancel RPC failed"));
            } else {
                future.set(PCancelPlanFragmentResult.newBuilder().setStatus(PStatus.newBuilder()
                        .setStatusCode(TStatusCode.INTERNAL_ERROR.getValue()).addErrorMsgs("cancel rejected")).build());
            }
            // This barrier runs after the callback on the same single worker.
            Assertions.assertEquals("worker", worker.submit(() -> ThreadContext.get(QueryLogContext.QUERY_ID))
                    .get(10, TimeUnit.SECONDS));
            List<LogEvent> callbacks = events(appender).stream()
                    .filter(event -> event.getMessage().getFormattedMessage().contains("Failed to cancel query"))
                    .collect(Collectors.toList());
            Assertions.assertEquals(1, callbacks.size());
            Assertions.assertEquals("1-2", callbacks.get(0).getContextData().getValue(QueryLogContext.QUERY_ID));
            Assertions.assertFalse(remote.cancelInProcess);
            Assertions.assertFalse(remote.hasCancelled);
            Assertions.assertEquals("caller", ThreadContext.get(QueryLogContext.QUERY_ID));
        } finally {
            Coordinator.backendRpcCallbackExecutor = original;
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static Coordinator coordinator(List<ScanNode> scans) {
        return new Coordinator(0L, new TUniqueId(1, 2), new DescriptorTable(), Collections.emptyList(),
                scans, "UTC", false, false);
    }

    private static List<LogEvent> events(TestLogAppender appender) {
        return Deencapsulation.getField(appender, "events");
    }
}
