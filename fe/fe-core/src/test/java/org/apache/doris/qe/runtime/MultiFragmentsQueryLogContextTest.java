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

package org.apache.doris.qe.runtime;

import org.apache.doris.common.Config;
import org.apache.doris.common.QueryLogContext;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.mysql.authenticate.TestLogAppender;
import org.apache.doris.proto.InternalService.PCancelPlanFragmentResult;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class MultiFragmentsQueryLogContextTest {
    @Test
    void rejectedCancellationRunsCallbackWithCapturedQuery() throws Exception {
        verifyCallback(PCancelPlanFragmentResult.newBuilder().setStatus(PStatus.newBuilder()
                .setStatusCode(TStatusCode.INTERNAL_ERROR.getValue()).addErrorMsgs("backend rejection")).build(), null);
    }

    @Test
    void missingCancellationStatusRunsCallbackWithCapturedQuery() throws Exception {
        verifyCallback(PCancelPlanFragmentResult.getDefaultInstance(), null);
    }

    @Test
    void failedCancellationRunsCallbackWithCapturedQuery() throws Exception {
        verifyCallback(null, new IllegalStateException("transport failed"));
    }

    @Test
    void successfulCancellationKeepsDuplicateRequestIdAndRestoresWorker() throws Exception {
        verifyCallback(PCancelPlanFragmentResult.newBuilder().setStatus(PStatus.newBuilder()
                .setStatusCode(TStatusCode.OK.getValue())).build(), null);
    }

    private void verifyCallback(PCancelPlanFragmentResult response, Throwable failure) throws Exception {
        boolean savedEnabled = Config.sys_log_enable_query_id;
        String savedQueryId = ThreadContext.get(QueryLogContext.QUERY_ID);
        ExecutorService savedExecutor = Coordinator.backendRpcCallbackExecutor;
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Config.sys_log_enable_query_id = true;
        Coordinator.backendRpcCallbackExecutor = worker;
        TUniqueId queryId = new TUniqueId(68694, 601);
        CoordinatorContext context = Mockito.mock(CoordinatorContext.class);
        Deencapsulation.setField(context, "queryId", queryId);
        Backend backend = Mockito.mock(Backend.class);
        TNetworkAddress address = new TNetworkAddress("127.0.0.1", 8060);
        Mockito.when(backend.getBrpcAddress()).thenReturn(address);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        SettableFuture<PCancelPlanFragmentResult> pending = SettableFuture.create();
        Mockito.when(proxy.cancelPipelineXPlanFragmentAsync(Mockito.eq(address), Mockito.eq(queryId), Mockito.any()))
                .thenReturn(pending);
        MultiFragmentsPipelineTask task = new MultiFragmentsPipelineTask(context, backend, proxy,
                ByteString.EMPTY, Collections.emptyMap());
        Status reason = new Status(TStatusCode.CANCELLED, "requested cancellation");
        try (MockedStatic<BackendServiceProxy> proxies = Mockito.mockStatic(BackendServiceProxy.class);
                TestLogAppender appender = TestLogAppender.attach(PipelineExecutionTask.class)) {
            proxies.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            worker.submit(() -> ThreadContext.put(QueryLogContext.QUERY_ID, "callback-worker"))
                    .get(10, TimeUnit.SECONDS);
            try (QueryLogContext ignored = QueryLogContext.open(queryId)) {
                task.cancelExecute(reason);
            }
            ThreadContext.put(QueryLogContext.QUERY_ID, "completion-caller");
            if (failure == null) {
                pending.set(response);
            } else {
                pending.setException(failure);
            }
            // The barrier runs after the callback on the same worker, without sleeps or timing assumptions.
            Assertions.assertEquals("callback-worker", worker.submit(() ->
                    ThreadContext.get(QueryLogContext.QUERY_ID)).get(10, TimeUnit.SECONDS));
            Assertions.assertEquals("completion-caller", ThreadContext.get(QueryLogContext.QUERY_ID));
            AtomicBoolean inProcess = Deencapsulation.getField(task, "cancelInProcess");
            AtomicBoolean cancelled = Deencapsulation.getField(task, "hasCancelled");
            Assertions.assertFalse(inProcess.get());
            boolean success = failure == null && response.hasStatus()
                    && response.getStatus().getStatusCode() == TStatusCode.OK.getValue();
            Assertions.assertEquals(success, cancelled.get());
            if (success) {
                task.cancelExecute(reason);
                Assertions.assertTrue(appender.contains(Level.INFO, "already been cancelled"));
                Assertions.assertTrue(appender.contains(Level.INFO, DebugUtil.printId(queryId)));
            } else {
                List<LogEvent> events = Deencapsulation.getField(appender, "events");
                LogEvent warning = events.stream().filter(e -> e.getLevel() == Level.WARN).findFirst().orElseThrow();
                Assertions.assertEquals(DebugUtil.printId(queryId),
                        warning.getContextData().getValue(QueryLogContext.QUERY_ID));
                Assertions.assertFalse(warning.getMessage().getFormattedMessage().contains(DebugUtil.printId(queryId)));
                if (failure != null) {
                    Assertions.assertSame(failure, warning.getThrown());
                    Assertions.assertTrue(warning.getMessage().getFormattedMessage().contains("requested cancellation"));
                } else {
                    Assertions.assertTrue(warning.getMessage().getFormattedMessage().contains(
                            response.hasStatus() ? "backend rejection" : "without status"));
                }
            }
            Mockito.verify(proxy).cancelPipelineXPlanFragmentAsync(address, queryId, reason);
        } finally {
            Coordinator.backendRpcCallbackExecutor = savedExecutor;
            worker.shutdownNow();
            Config.sys_log_enable_query_id = savedEnabled;
            if (savedQueryId == null) {
                ThreadContext.remove(QueryLogContext.QUERY_ID);
            } else {
                ThreadContext.put(QueryLogContext.QUERY_ID, savedQueryId);
            }
        }
    }
}
