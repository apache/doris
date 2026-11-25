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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.proto.InternalService.PExecPlanFragmentResult;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.NativeFileEncryptionSupport;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BackendHbResponse;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.tls.PrivateRpcTls;
import org.apache.doris.tls.server.FeServerStarterFactory;
import org.apache.doris.tls.server.TlsProtocolSet.Protocol;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

class PipelineExecutionTaskTest {
    @ParameterizedTest
    @CsvSource({"unchanged,true", "port,false", "removed,false", "replaced,false", "capability,false"})
    void encryptedFragmentsRequireTheRegisteredBoundDestination(String change, boolean allowed) throws Exception {
        SystemInfoService service = new SystemInfoService();
        Backend backend = new Backend(1, "192.0.2.1", 9050);
        long now = System.currentTimeMillis();
        BackendHbResponse heartbeat = new BackendHbResponse(1, 9060, 8040, 8060, now, now,
                "fixture-version", Tag.VALUE_MIX, 0, 0, false, 8070);
        heartbeat.setNativeFileEncryptionSupported(true);
        backend.handleHbResponse(heartbeat, false);
        backend.setBrpcPort(8060);
        service.addBackend(backend);
        TNetworkAddress boundAddress = backend.getBrpcAddress();
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        ByteString fragments = ByteString.copyFromUtf8("fixture-fragment");
        Mockito.when(proxy.execPlanFragmentsAsync(boundAddress, fragments, false))
                .thenReturn(CompletableFuture.completedFuture(PExecPlanFragmentResult.getDefaultInstance()));
        MultiFragmentsPipelineTask task = new MultiFragmentsPipelineTask(Mockito.mock(CoordinatorContext.class),
                backend, proxy, fragments, Map.of(), true);
        switch (change) {
            case "port":
                backend.setBrpcPort(8061);
                break;
            case "removed":
                service.replayDropBackend(backend);
                break;
            case "replaced":
                Backend replacement = new Backend(backend.getId(), backend.getHost(), backend.getHeartbeatPort());
                replacement.setBrpcPort(boundAddress.port);
                service.addBackend(replacement);
                break;
            case "capability":
                heartbeat.setNativeFileEncryptionSupported(false);
                backend.handleHbResponse(heartbeat, false);
                break;
            default:
                break;
        }
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<PrivateRpcTls> tls = Mockito.mockStatic(PrivateRpcTls.class);
                MockedStatic<FeServerStarterFactory> listener = Mockito.mockStatic(FeServerStarterFactory.class)) {
            env.when(Env::getCurrentSystemInfo).thenReturn(service);
            tls.when(() -> PrivateRpcTls.supportsPrivateFileKeys(Protocol.BRPC)).thenReturn(true);
            tls.when(() -> PrivateRpcTls.supportsPrivateFileKeys(Protocol.THRIFT)).thenReturn(true);
            listener.when(FeServerStarterFactory::supportsPrivateFileKeys).thenReturn(true);
            Assertions.assertEquals(!"capability".equals(change), NativeFileEncryptionSupport.supported(backend));
            PExecPlanFragmentResult result = task.sendPhaseOneRpc(false).get();
            if (allowed) {
                Assertions.assertEquals(TStatusCode.OK.getValue(), result.getStatus().getStatusCode());
                Mockito.verify(proxy).execPlanFragmentsAsync(boundAddress, fragments, false);
            } else {
                Assertions.assertEquals(TStatusCode.NOT_IMPLEMENTED_ERROR.getValue(), result.getStatus().getStatusCode());
                Assertions.assertTrue(result.getStatus().getErrorMsgs(0).contains("READ_UNAVAILABLE"));
                Mockito.verifyNoInteractions(proxy);
            }
        }
    }

    @Test
    void encryptionCapabilityIsCheckedAgainBeforeSendingSerializedFragments() throws Exception {
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        CoordinatorContext context = Mockito.mock(CoordinatorContext.class);
        MultiFragmentsPipelineTask task = new MultiFragmentsPipelineTask(context, backend, proxy,
                ByteString.copyFromUtf8("fixture-fragment"), Map.of(), true);
        try (MockedStatic<NativeFileEncryptionSupport> proof = Mockito.mockStatic(NativeFileEncryptionSupport.class)) {
            proof.when(() -> NativeFileEncryptionSupport.require(backend.getId(), backend.getBrpcAddress()))
                    .thenThrow(new IllegalStateException("File encryption [READ_UNAVAILABLE]"));
            PExecPlanFragmentResult result = task.sendPhaseOneRpc(false).get();
            Assertions.assertTrue(result.getStatus().getErrorMsgs(0).contains("READ_UNAVAILABLE"));
            Assertions.assertEquals(TStatusCode.NOT_IMPLEMENTED_ERROR.getValue(), result.getStatus().getStatusCode());
            Mockito.verifyNoInteractions(proxy);
            Assertions.assertNull(Deencapsulation.getField(task, "serializeFragments"));
        }
    }

    @Test
    void expiredDeadlineCancelsAlreadySubmittedFragments() throws Exception {
        CoordinatorContext coordinatorContext = Mockito.mock(CoordinatorContext.class);
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setExecutionTimeout(1);
        queryOptions.setQueryTimeout(1);
        Deencapsulation.setField(coordinatorContext, "queryOptions", queryOptions);
        Deencapsulation.setField(coordinatorContext, "queryId", new TUniqueId(1, 2));
        Deencapsulation.setField(coordinatorContext, "timeoutDeadline", (Supplier<Long>) () -> 0L);
        Mockito.when(coordinatorContext.withLock(ArgumentMatchers.<Callable<Object>>any()))
                .thenAnswer(invocation -> invocation.<Callable<Object>>getArgument(0).call());
        Mockito.when(coordinatorContext.readCloneStatus()).thenReturn(Status.OK);
        Mockito.when(coordinatorContext.twoPhaseExecution()).thenReturn(false);

        MultiFragmentsPipelineTask fragmentsTask = Mockito.mock(MultiFragmentsPipelineTask.class);
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(10001L);
        Mockito.when(fragmentsTask.getBackend()).thenReturn(backend);
        Mockito.when(fragmentsTask.getChildrenTasks()).thenReturn(Collections.emptyMap());
        Mockito.when(fragmentsTask.sendPhaseOneRpc(false))
                .thenReturn(CompletableFuture.completedFuture(PExecPlanFragmentResult.getDefaultInstance()));
        PipelineExecutionTask executionTask = new PipelineExecutionTask(
                coordinatorContext,
                Mockito.mock(BackendServiceProxy.class),
                Collections.singletonMap(Mockito.mock(BackendWorker.class), fragmentsTask));

        UserException exception = Assertions.assertThrows(UserException.class, executionTask::execute);

        Assertions.assertTrue(exception.getMessage().contains("timeout before waiting send fragments rpc"));
        Mockito.verify(fragmentsTask).sendPhaseOneRpc(false);
        Mockito.verify(coordinatorContext).updateStatusIfOk(ArgumentMatchers.argThat(
                status -> hasDeadlineTimeoutMessage(status)));
        Mockito.verify(coordinatorContext).cancelSchedule(ArgumentMatchers.argThat(
                status -> hasDeadlineTimeoutMessage(status)));
    }

    @Test
    void auditParticipantsIncludeOnlyAttemptedDispatches() throws Exception {
        CoordinatorContext coordinatorContext = Mockito.mock(CoordinatorContext.class);
        Deencapsulation.setField(coordinatorContext, "timeoutDeadline", (Supplier<Long>) () -> Long.MAX_VALUE);
        Mockito.when(coordinatorContext.withLock(ArgumentMatchers.<Callable<Object>>any()))
                .thenAnswer(invocation -> invocation.<Callable<Object>>getArgument(0).call());
        Mockito.when(coordinatorContext.readCloneStatus()).thenReturn(Status.OK);
        Mockito.when(coordinatorContext.twoPhaseExecution()).thenReturn(false);

        MultiFragmentsPipelineTask first = mockFragmentTask(10001L);
        MultiFragmentsPipelineTask uncertain = mockFragmentTask(10002L);
        MultiFragmentsPipelineTask neverAttempted = mockFragmentTask(10003L);
        Mockito.when(first.sendPhaseOneRpc(false))
                .thenReturn(CompletableFuture.completedFuture(PExecPlanFragmentResult.getDefaultInstance()));
        Mockito.when(uncertain.sendPhaseOneRpc(false)).thenThrow(new IllegalStateException("dispatch failed"));
        Map<BackendWorker, MultiFragmentsPipelineTask> tasks = new LinkedHashMap<>();
        tasks.put(Mockito.mock(BackendWorker.class), first);
        tasks.put(Mockito.mock(BackendWorker.class), uncertain);
        tasks.put(Mockito.mock(BackendWorker.class), neverAttempted);
        PipelineExecutionTask executionTask = new PipelineExecutionTask(
                coordinatorContext, Mockito.mock(BackendServiceProxy.class), tasks);

        Assertions.assertThrows(IllegalStateException.class, executionTask::execute);

        Assertions.assertEquals(Set.of(10001L, 10002L),
                executionTask.getDispatchedBackendIds());
        Mockito.verify(neverAttempted, Mockito.never()).sendPhaseOneRpc(false);
    }

    @Test
    void timeoutStatusPreventsFragmentDispatch() throws Exception {
        CoordinatorContext coordinatorContext = Mockito.mock(CoordinatorContext.class);
        Deencapsulation.setField(coordinatorContext, "timeoutDeadline", (Supplier<Long>) () -> Long.MAX_VALUE);
        Mockito.when(coordinatorContext.withLock(ArgumentMatchers.<Callable<Object>>any()))
                .thenAnswer(invocation -> invocation.<Callable<Object>>getArgument(0).call());
        Mockito.when(coordinatorContext.readCloneStatus())
                .thenReturn(new Status(TStatusCode.TIMEOUT, "timeout before fragment dispatch"));

        MultiFragmentsPipelineTask fragmentsTask = mockFragmentTask(10001L);
        PipelineExecutionTask executionTask = new PipelineExecutionTask(
                coordinatorContext,
                Mockito.mock(BackendServiceProxy.class),
                Collections.singletonMap(Mockito.mock(BackendWorker.class), fragmentsTask));

        UserException exception = Assertions.assertThrows(UserException.class, executionTask::execute);

        Assertions.assertTrue(exception.getMessage().contains("timeout before fragment dispatch"));
        Mockito.verify(fragmentsTask, Mockito.never()).sendPhaseOneRpc(ArgumentMatchers.anyBoolean());
    }

    @Test
    void loadProcessorCancelToleratesTaskPublishedBeforeLatch() {
        LoadProcessor processor = Mockito.mock(LoadProcessor.class, Mockito.CALLS_REAL_METHODS);
        PipelineExecutionTask task = Mockito.mock(PipelineExecutionTask.class);
        Mockito.when(task.getChildrenTasks()).thenReturn(Collections.emptyMap());
        Deencapsulation.setField(processor, "executionTask", java.util.Optional.of(task));
        // AfterSetPipelineExecutionTask has not created the latch yet (Objenesis skipped the constructor).
        Deencapsulation.setField(processor, "latch", java.util.Optional.empty());
        // latch deliberately stays Optional.empty(): the window between executionTask publication and
        // afterSetPipelineExecutionTask creating the latch must not throw out of the coordinator cleanup.
        Assertions.assertDoesNotThrow(() ->
                processor.cancel(new Status(TStatusCode.TIMEOUT, "cancel during publication")));
    }

    private static MultiFragmentsPipelineTask mockFragmentTask(long backendId) {
        MultiFragmentsPipelineTask task = Mockito.mock(MultiFragmentsPipelineTask.class);
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getId()).thenReturn(backendId);
        Mockito.when(task.getBackend()).thenReturn(backend);
        Mockito.when(task.getChildrenTasks()).thenReturn(Collections.emptyMap());
        return task;
    }

    private static boolean hasDeadlineTimeoutMessage(Status status) {
        return status.getErrorMsg().contains("timeout before waiting send fragments rpc");
    }
}
