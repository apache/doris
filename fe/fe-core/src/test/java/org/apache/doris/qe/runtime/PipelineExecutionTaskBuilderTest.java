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
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.profile.ExecutionProfile;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.proto.InternalService;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.JobProcessor;
import org.apache.doris.rpc.BackendServiceClient;
import org.apache.doris.rpc.BackendServiceClientChannelProvider;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.BackendServiceProxy.BackendRpcTarget;
import org.apache.doris.rpc.OssBackendServiceClientChannelProvider;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDetailedReportParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TQueryProfile;
import org.apache.doris.thrift.TRuntimeProfileNode;
import org.apache.doris.thrift.TRuntimeProfileTree;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class PipelineExecutionTaskBuilderTest {
    private static CoordinatorContext contextWithTargets() {
        CoordinatorContext context = Mockito.mock(CoordinatorContext.class);
        Deencapsulation.setField(context, "rpcTargets", new java.util.concurrent.ConcurrentHashMap<>());
        Mockito.when(context.getBackendRpcTarget(Mockito.any())).thenCallRealMethod();
        return context;
    }

    @Test
    void oneWorkerBindingServesEveryExecutionAndFetchPhase() throws Exception {
        CoordinatorContext context = contextWithTargets();
        Deencapsulation.setField(context, "queryId", new TUniqueId(1, 2));
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        backend.setBrpcPort(8060);
        BackendWorker worker = new BackendWorker(InternalCatalog.INTERNAL_CATALOG_ID, backend);
        BackendServiceProxy selected = Mockito.mock(BackendServiceProxy.class);
        SettableFuture<InternalService.PCancelPlanFragmentResult> pending = SettableFuture.create();
        Mockito.when(selected.cancelPipelineXPlanFragmentAsync(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(pending);
        try (MockedStatic<BackendServiceProxy> factory =
                Mockito.mockStatic(BackendServiceProxy.class, Mockito.CALLS_REAL_METHODS)) {
            factory.when(BackendServiceProxy::getInstance).thenReturn(selected);
            BackendRpcTarget target = context.getBackendRpcTarget(worker);
            Assertions.assertSame(target, context.getBackendRpcTarget(
                    new BackendWorker(InternalCatalog.INTERNAL_CATALOG_ID, backend)));
            MultiFragmentsPipelineTask task = new MultiFragmentsPipelineTask(context, backend, target,
                    ByteString.EMPTY, Collections.emptyMap());
            org.apache.doris.qe.ResultReceiver receiver = new org.apache.doris.qe.ResultReceiver(
                    context.queryId, new TUniqueId(3, 4), backend.getId(),
                    context.getBackendRpcTarget(worker), Long.MAX_VALUE, 1 << 20, false);
            task.sendPhaseOneRpc(false);
            task.sendPhaseTwoRpc();
            Status reason = new Status(TStatusCode.CANCELLED, "fixture cancellation");
            task.cancelExecute(reason);
            receiver.createFuture(null);
            receiver.createFuture(null);
            task.removeProxy();
            TNetworkAddress address = backend.getBrpcAddress();
            Mockito.verify(selected).execPlanFragmentsAsync(address, ByteString.EMPTY, false);
            Mockito.verify(selected).execPlanFragmentStartAsync(Mockito.eq(address), Mockito.any());
            Mockito.verify(selected).cancelPipelineXPlanFragmentAsync(address, context.queryId, reason);
            Mockito.verify(selected, Mockito.times(2)).fetchDataAsyncWithCallback(
                    Mockito.eq(address), Mockito.any(), Mockito.isNull());
            Mockito.verify(selected).removeProxy(address);
            factory.verify(BackendServiceProxy::getInstance, Mockito.times(1));
        }
    }

    @Test
    void mixedPrivateAndRemoteWorkersFailBeforeCreatingAnyTask() {
        boolean previousTls = Config.enable_tls;
        String previousExcluded = Config.tls_excluded_protocols;
        try {
            Config.enable_tls = true;
            Config.tls_excluded_protocols = "http,mysql,arrowflight,bdbje";
            CoordinatorContext context = contextWithTargets();
            Deencapsulation.setField(context, "timeoutDeadline", (java.util.function.Supplier<Long>) () -> Long.MAX_VALUE);
            Mockito.when(context.getJobProcessor()).thenReturn(Mockito.mock(JobProcessor.class));
            Backend backend = new Backend(1, "127.0.0.1", 9050);
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> params = new LinkedHashMap<>();
            params.put(new BackendWorker(InternalCatalog.INTERNAL_CATALOG_ID, backend),
                    new TPipelineFragmentParamsList().setParamsList(Collections.emptyList()));
            params.put(new BackendWorker(9, backend),
                    new TPipelineFragmentParamsList().setParamsList(Collections.emptyList()));
            try (MockedStatic<BackendServiceProxy> factory = Mockito.mockStatic(BackendServiceProxy.class)) {
                factory.when(() -> BackendServiceProxy.bindTarget(Mockito.any(TNetworkAddress.class), Mockito.anyLong()))
                        .thenCallRealMethod();
                factory.when(BackendServiceProxy::getInstance).thenReturn(Mockito.mock(BackendServiceProxy.class));
                factory.when(() -> BackendServiceProxy.getInstance(false))
                        .thenReturn(Mockito.mock(BackendServiceProxy.class));
                IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                        () -> PipelineExecutionTaskBuilder.build(context, params));
                Assertions.assertTrue(failure.getMessage().contains("use_arrow_flight=true"));
                factory.verifyNoInteractions();
                Mockito.verify(context, Mockito.never()).getJobProcessor();
            }
        } finally {
            Config.enable_tls = previousTls;
            Config.tls_excluded_protocols = previousExcluded;
        }
    }

    @Test
    void remoteWorkerPolicyCoversExchangesAndReverseThriftCallbacks() {
        boolean previousTls = Config.enable_tls;
        String previousExcluded = Config.tls_excluded_protocols;
        try {
            OssBackendServiceClientChannelProvider provider = new OssBackendServiceClientChannelProvider();
            Config.enable_tls = true;
            for (String excluded : new String[] {"", "brpc", "thrift"}) {
                Config.tls_excluded_protocols = excluded;
                Assertions.assertFalse(provider.supportsRemoteWorkers());
            }
            Config.tls_excluded_protocols = "brpc,thrift";
            Assertions.assertTrue(provider.supportsRemoteWorkers());
            Config.enable_tls = false;
            Config.tls_excluded_protocols = "";
            Assertions.assertTrue(provider.supportsRemoteWorkers());
            Config.enable_tls = true;
            BackendServiceClientChannelProvider legacy = (address, ip, executor) -> {
                throw new UnsupportedOperationException("fixture does not create channels");
            };
            Assertions.assertTrue(legacy.supportsRemoteWorkers());
            Assertions.assertTrue(new OssBackendServiceClientChannelProvider() { }.supportsRemoteWorkers(),
                    "legacy subclasses keep responsibility for their complete endpoint policy");
        } finally {
            Config.enable_tls = previousTls;
            Config.tls_excluded_protocols = previousExcluded;
        }
    }

    @Test
    void isolatedRemoteWorkersAlsoRequireACompatibleCoordinatorReportPolicy() {
        try (MockedStatic<BackendServiceClient> policy = Mockito.mockStatic(BackendServiceClient.class);
                MockedStatic<BackendServiceProxy> factory = Mockito.mockStatic(BackendServiceProxy.class)) {
            policy.when(BackendServiceClient::supportsRemoteWorkers).thenReturn(false);
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> remote = Map.of(
                    new BackendWorker(9, new Backend(1, "127.0.0.1", 9050)),
                    new TPipelineFragmentParamsList().setParamsList(Collections.emptyList()));
            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> PipelineExecutionTaskBuilder.build(contextWithTargets(), remote));
            Assertions.assertTrue(failure.getMessage().contains("use_arrow_flight=true"));
            factory.verifyNoInteractions();
        }
    }

    @Test
    void localWorkersAndProviderSupportedRemoteGraphsRemainUsable() {
        try (MockedStatic<BackendServiceClient> policy = Mockito.mockStatic(BackendServiceClient.class);
                MockedStatic<BackendServiceProxy> factory = Mockito.mockStatic(BackendServiceProxy.class)) {
            factory.when(() -> BackendServiceProxy.bindTarget(Mockito.any(TNetworkAddress.class), Mockito.anyLong()))
                    .thenCallRealMethod();
            factory.when(BackendServiceProxy::getInstance).thenReturn(Mockito.mock(BackendServiceProxy.class));
            factory.when(() -> BackendServiceProxy.getInstance(false))
                    .thenReturn(Mockito.mock(BackendServiceProxy.class));
            CoordinatorContext context = contextWithTargets();
            Deencapsulation.setField(context, "timeoutDeadline", (java.util.function.Supplier<Long>) () -> Long.MAX_VALUE);
            Mockito.when(context.getJobProcessor()).thenReturn(Mockito.mock(JobProcessor.class));
            Backend backend = new Backend(1, "127.0.0.1", 9050);
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> params = new LinkedHashMap<>();
            params.put(new BackendWorker(InternalCatalog.INTERNAL_CATALOG_ID, backend),
                    new TPipelineFragmentParamsList().setParamsList(Collections.emptyList()));
            policy.when(BackendServiceClient::supportsRemoteWorkers).thenReturn(false);
            Assertions.assertNotNull(PipelineExecutionTaskBuilder.build(context, params));
            params.put(new BackendWorker(9, backend),
                    new TPipelineFragmentParamsList().setParamsList(Collections.emptyList()));
            policy.when(BackendServiceClient::supportsRemoteWorkers).thenReturn(true);
            Assertions.assertNotNull(PipelineExecutionTaskBuilder.build(context, params));
        }
    }

    @Test
    void cancellationAndFailedChannelRemovalUseTheTaskProxy() throws Exception {
        CoordinatorContext context = contextWithTargets();
        Deencapsulation.setField(context, "queryId", new TUniqueId(1, 2));
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        BackendServiceProxy selected = Mockito.mock(BackendServiceProxy.class);
        SettableFuture<InternalService.PCancelPlanFragmentResult> pending = SettableFuture.create();
        Mockito.when(selected.cancelPipelineXPlanFragmentAsync(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(pending);
        MultiFragmentsPipelineTask task = new MultiFragmentsPipelineTask(context, backend, selected,
                ByteString.EMPTY, Collections.emptyMap());
        Status reason = new Status(TStatusCode.CANCELLED, "fixture cancellation");
        try (MockedStatic<BackendServiceProxy> factory = Mockito.mockStatic(BackendServiceProxy.class)) {
            task.cancelExecute(reason);
            factory.verifyNoInteractions();
        }
        Mockito.verify(selected).cancelPipelineXPlanFragmentAsync(backend.getBrpcAddress(), context.queryId, reason);
        task.removeProxy();
        Mockito.verify(selected).removeProxy(backend.getBrpcAddress());
    }

    @Test
    void localAndRemoteWorkersWithMatchingIdsAndAddressesUseSeparateProxies() {
        CoordinatorContext context = contextWithTargets();
        Backend backend = new Backend(1, "127.0.0.1", 9050);
        BackendWorker local = new BackendWorker(InternalCatalog.INTERNAL_CATALOG_ID, backend);
        BackendWorker remote = new BackendWorker(9, backend);
        Map<DistributedPlanWorker, TPipelineFragmentParamsList> params = new LinkedHashMap<>();
        params.put(local, new TPipelineFragmentParamsList().setParamsList(Collections.emptyList()));
        params.put(remote, new TPipelineFragmentParamsList().setParamsList(Collections.emptyList()));
        Map<BackendWorker, MultiFragmentsPipelineTask> tasks = Deencapsulation.invoke(
                new PipelineExecutionTaskBuilder(context), "buildMultiFragmentTasks", context, params);
        BackendRpcTarget localTarget = Deencapsulation.getField(tasks.get(local), "target");
        BackendRpcTarget remoteTarget = Deencapsulation.getField(tasks.get(remote), "target");
        Assertions.assertSame(context.getBackendRpcTarget(local), localTarget);
        Assertions.assertSame(context.getBackendRpcTarget(remote), remoteTarget);
        BackendServiceProxy localProxy = Deencapsulation.getField(localTarget, "proxy");
        BackendServiceProxy remoteProxy = Deencapsulation.getField(remoteTarget, "proxy");
        Assertions.assertNotSame(localProxy, remoteProxy);
        Assertions.assertTrue((Boolean) Deencapsulation.getField(localProxy, "internalRpc"));
        Assertions.assertFalse((Boolean) Deencapsulation.getField(remoteProxy, "internalRpc"));
    }

    @Test
    void buildSingleFragmentPipelineTaskRegistersBackendWithExecutionProfile() {
        TUniqueId queryId = new TUniqueId(1, 2);
        ExecutionProfile executionProfile = new ExecutionProfile(queryId, Lists.newArrayList(0));
        Backend backend = new Backend(1L, "127.0.0.1", 9050);

        Map<Integer, SingleFragmentPipelineTask> tasks =
                PipelineExecutionTaskBuilder.buildSingleFragmentPipelineTask(
                        executionProfile, backend, createFragmentParamsList(0));

        Assertions.assertEquals(1, tasks.size());
        Assertions.assertTrue(tasks.containsKey(0));

        executionProfile.updateProfile(createQueryProfile(queryId, 0), backend.getHeartbeatAddress(), true);
        Assertions.assertTrue(executionProfile.isCompleted());
    }

    private static TPipelineFragmentParamsList createFragmentParamsList(int fragmentId) {
        TPipelineInstanceParams instanceParams = new TPipelineInstanceParams();
        instanceParams.setFragmentInstanceId(new TUniqueId(3, 4));

        TPipelineFragmentParams fragmentParams = new TPipelineFragmentParams();
        fragmentParams.setFragmentId(fragmentId);
        fragmentParams.setLocalParams(Lists.newArrayList(instanceParams));

        TPipelineFragmentParamsList paramsList = new TPipelineFragmentParamsList();
        paramsList.setParamsList(Lists.newArrayList(fragmentParams));
        return paramsList;
    }

    private static TQueryProfile createQueryProfile(TUniqueId queryId, int fragmentId) {
        TQueryProfile queryProfile = new TQueryProfile();
        queryProfile.setQueryId(queryId);
        queryProfile.putToFragmentIdToProfile(fragmentId, Lists.newArrayList(createReportParam()));
        return queryProfile;
    }

    private static TDetailedReportParams createReportParam() {
        TRuntimeProfileNode node = new TRuntimeProfileNode();
        node.setName("PipelineProfile");
        node.setNumChildren(0);
        node.setCounters(Lists.newArrayList());
        node.setMetadata(0);
        node.setIndent(false);
        node.setInfoStrings(Maps.newHashMap());
        node.setInfoStringsDisplayOrder(Lists.newArrayList());
        node.setChildCountersMap(Maps.newHashMap());
        node.setTimestamp(0);

        TRuntimeProfileTree tree = new TRuntimeProfileTree();
        tree.setNodes(Lists.newArrayList(node));

        TDetailedReportParams params = new TDetailedReportParams();
        params.setProfile(tree);
        params.setIsFragmentLevel(false);
        return params;
    }
}
