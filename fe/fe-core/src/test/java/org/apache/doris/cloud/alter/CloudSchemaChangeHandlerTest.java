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

package org.apache.doris.cloud.alter;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.Config;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CloudSchemaChangeHandlerTest {
    private int originalCloudTxnTabletBatchSize;
    private String originalCloudUniqueId;
    private String originalMetaServiceEndpoint;

    @Before
    public void setUp() {
        originalCloudTxnTabletBatchSize = Config.cloud_txn_tablet_batch_size;
        originalCloudUniqueId = Config.cloud_unique_id;
        originalMetaServiceEndpoint = Config.meta_service_endpoint;
        Config.cloud_txn_tablet_batch_size = 2;
        Config.cloud_unique_id = "cloud-test";
        Config.meta_service_endpoint = "127.0.0.1:20121";
    }

    @After
    public void tearDown() {
        Config.cloud_txn_tablet_batch_size = originalCloudTxnTabletBatchSize;
        Config.cloud_unique_id = originalCloudUniqueId;
        Config.meta_service_endpoint = originalMetaServiceEndpoint;
    }

    @Test
    public void testUpdateTablePropertiesNotifiesAllAliveBackendsInBatches() throws Exception {
        CloudSchemaChangeHandler handler = new CloudSchemaChangeHandler();
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        org.apache.doris.catalog.Tablet tablet1 = Mockito.mock(org.apache.doris.catalog.Tablet.class);
        org.apache.doris.catalog.Tablet tablet2 = Mockito.mock(org.apache.doris.catalog.Tablet.class);
        org.apache.doris.catalog.Tablet tablet3 = Mockito.mock(org.apache.doris.catalog.Tablet.class);

        Mockito.when(db.getTableOrMetaException("tbl", org.apache.doris.catalog.Table.TableType.OLAP))
                .thenReturn(table);
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getCompactionPolicy()).thenReturn("size_based");
        Mockito.when(table.getKeysType()).thenReturn(org.apache.doris.catalog.KeysType.DUP_KEYS);
        Mockito.when(table.getPartitions()).thenReturn(Arrays.asList(partition));
        Mockito.when(table.getPartition("p1")).thenReturn(partition);
        Mockito.when(partition.getName()).thenReturn("p1");
        Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(Arrays.asList(index));
        Mockito.when(index.getTablets()).thenReturn(Arrays.asList(tablet1, tablet2, tablet3));
        Mockito.when(tablet1.getId()).thenReturn(101L);
        Mockito.when(tablet2.getId()).thenReturn(102L);
        Mockito.when(tablet3.getId()).thenReturn(103L);

        Backend aliveBackend1 = new Backend(1L, "be-1", 9050);
        aliveBackend1.setAlive(true);
        aliveBackend1.setBrpcPort(8060);
        Backend aliveBackend2 = new Backend(2L, "be-2", 9050);
        aliveBackend2.setAlive(true);
        aliveBackend2.setBrpcPort(8061);
        Backend deadBackend = new Backend(3L, "be-dead", 9050);
        deadBackend.setAlive(false);
        deadBackend.setBrpcPort(8062);

        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of(
                aliveBackend1.getId(), aliveBackend1,
                aliveBackend2.getId(), aliveBackend2,
                deadBackend.getId(), deadBackend));

        Env env = Mockito.mock(Env.class);

        MetaServiceProxy metaServiceProxy = Mockito.mock(MetaServiceProxy.class);
        Mockito.when(metaServiceProxy.updateTablet(Mockito.any())).thenReturn(okUpdateTabletResponse());

        BackendServiceProxy backendServiceProxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(backendServiceProxy.syncTabletMeta(Mockito.any(), Mockito.any()))
                .thenReturn(Futures.immediateFuture(okSyncTabletMetaResponse()));

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<MetaServiceProxy> metaProxyMock = Mockito.mockStatic(MetaServiceProxy.class);
                MockedStatic<BackendServiceProxy> backendProxyMock = Mockito.mockStatic(BackendServiceProxy.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            metaProxyMock.when(MetaServiceProxy::getInstance).thenReturn(metaServiceProxy);
            backendProxyMock.when(BackendServiceProxy::getInstance).thenReturn(backendServiceProxy);

            Map<String, String> properties = new HashMap<>();
            properties.put("compaction_policy", "time_series");
            handler.updateTableProperties(db, "tbl", properties);
        }

        ArgumentCaptor<Cloud.UpdateTabletRequest> updateCaptor =
                ArgumentCaptor.forClass(Cloud.UpdateTabletRequest.class);
        Mockito.verify(metaServiceProxy, Mockito.times(2)).updateTablet(updateCaptor.capture());
        List<Cloud.UpdateTabletRequest> updateRequests = updateCaptor.getAllValues();
        Assert.assertEquals(Arrays.asList(101L, 102L),
                updateRequests.get(0).getTabletMetaInfosList().stream()
                        .map(Cloud.TabletMetaInfoPB::getTabletId).collect(Collectors.toList()));
        Assert.assertEquals(Arrays.asList(103L),
                updateRequests.get(1).getTabletMetaInfosList().stream()
                        .map(Cloud.TabletMetaInfoPB::getTabletId).collect(Collectors.toList()));

        ArgumentCaptor<TNetworkAddress> addressCaptor = ArgumentCaptor.forClass(TNetworkAddress.class);
        ArgumentCaptor<InternalService.PSyncTabletMetaRequest> syncCaptor =
                ArgumentCaptor.forClass(InternalService.PSyncTabletMetaRequest.class);
        Mockito.verify(backendServiceProxy, Mockito.times(4))
                .syncTabletMeta(addressCaptor.capture(), syncCaptor.capture());

        List<TNetworkAddress> addresses = addressCaptor.getAllValues();
        Assert.assertFalse(addresses.stream().anyMatch(addr -> "be-dead".equals(addr.getHostname())));
        Assert.assertEquals(2L, addresses.stream().filter(addr -> "be-1".equals(addr.getHostname())).count());
        Assert.assertEquals(2L, addresses.stream().filter(addr -> "be-2".equals(addr.getHostname())).count());

        List<InternalService.PSyncTabletMetaRequest> syncRequests = syncCaptor.getAllValues();
        Assert.assertEquals(Arrays.asList(101L, 102L), syncRequests.get(0).getTabletIdsList());
        Assert.assertEquals(Arrays.asList(101L, 102L), syncRequests.get(1).getTabletIdsList());
        Assert.assertEquals(Arrays.asList(103L), syncRequests.get(2).getTabletIdsList());
        Assert.assertEquals(Arrays.asList(103L), syncRequests.get(3).getTabletIdsList());
    }

    @Test
    public void testNotifyBackendsToSyncTabletMetaSwallowsDispatchFailure() throws Exception {
        CloudSchemaChangeHandler handler = new CloudSchemaChangeHandler();
        Backend aliveBackend1 = new Backend(1L, "be-1", 9050);
        aliveBackend1.setAlive(true);
        aliveBackend1.setBrpcPort(8060);
        Backend aliveBackend2 = new Backend(2L, "be-2", 9050);
        aliveBackend2.setAlive(true);
        aliveBackend2.setBrpcPort(8061);

        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of(
                aliveBackend1.getId(), aliveBackend1,
                aliveBackend2.getId(), aliveBackend2));

        BackendServiceProxy backendServiceProxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(backendServiceProxy.syncTabletMeta(Mockito.argThat(
                        addr -> addr != null && "be-1".equals(addr.getHostname())), Mockito.any()))
                .thenReturn(Futures.immediateFuture(okSyncTabletMetaResponse()));
        Mockito.when(backendServiceProxy.syncTabletMeta(Mockito.argThat(
                        addr -> addr != null && "be-2".equals(addr.getHostname())), Mockito.any()))
                .thenThrow(new org.apache.doris.rpc.RpcException("be-2", "dispatch failed"));

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> backendProxyMock = Mockito.mockStatic(BackendServiceProxy.class)) {
            envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            backendProxyMock.when(BackendServiceProxy::getInstance).thenReturn(backendServiceProxy);

            handler.notifyBackendsToSyncTabletMeta(Arrays.asList(101L, 102L));
        }

        Mockito.verify(backendServiceProxy, Mockito.times(2))
                .syncTabletMeta(Mockito.any(), Mockito.any());
    }

    @Test
    public void testNotifyBackendsToSyncTabletMetaSwallowsNonOkResponse() throws Exception {
        CloudSchemaChangeHandler handler = new CloudSchemaChangeHandler();
        Backend aliveBackend1 = new Backend(1L, "be-1", 9050);
        aliveBackend1.setAlive(true);
        aliveBackend1.setBrpcPort(8060);
        Backend aliveBackend2 = new Backend(2L, "be-2", 9050);
        aliveBackend2.setAlive(true);
        aliveBackend2.setBrpcPort(8061);

        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster()).thenReturn(ImmutableMap.of(
                aliveBackend1.getId(), aliveBackend1,
                aliveBackend2.getId(), aliveBackend2));

        BackendServiceProxy backendServiceProxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(backendServiceProxy.syncTabletMeta(Mockito.any(), Mockito.any()))
                .thenReturn(Futures.immediateFuture(InternalService.PSyncTabletMetaResponse.newBuilder()
                        .setStatus(org.apache.doris.proto.Types.PStatus.newBuilder()
                                .setStatusCode(TStatusCode.CANCELLED.getValue()))
                        .setFailedTablets(2)
                        .build()));

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> backendProxyMock = Mockito.mockStatic(BackendServiceProxy.class)) {
            envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            backendProxyMock.when(BackendServiceProxy::getInstance).thenReturn(backendServiceProxy);

            handler.notifyBackendsToSyncTabletMeta(Arrays.asList(101L, 102L));
        }

        Mockito.verify(backendServiceProxy, Mockito.times(2))
                .syncTabletMeta(Mockito.any(), Mockito.any());
    }

    @Test
    public void testNotifyBackendsToSyncTabletMetaReturnsWhenBackendDiscoveryFails() throws Exception {
        CloudSchemaChangeHandler handler = new CloudSchemaChangeHandler();
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Mockito.when(systemInfoService.getAllBackendsByAllCluster())
                .thenThrow(new org.apache.doris.common.AnalysisException("backend discovery failed"));

        BackendServiceProxy backendServiceProxy = Mockito.mock(BackendServiceProxy.class);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> backendProxyMock = Mockito.mockStatic(BackendServiceProxy.class)) {
            envMock.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
            backendProxyMock.when(BackendServiceProxy::getInstance).thenReturn(backendServiceProxy);

            handler.notifyBackendsToSyncTabletMeta(Arrays.asList(101L, 102L));
        }

        Mockito.verifyNoInteractions(backendServiceProxy);
    }

    private Cloud.UpdateTabletResponse okUpdateTabletResponse() {
        return Cloud.UpdateTabletResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK))
                .build();
    }

    private InternalService.PSyncTabletMetaResponse okSyncTabletMetaResponse() {
        return InternalService.PSyncTabletMetaResponse.newBuilder()
                .setStatus(org.apache.doris.proto.Types.PStatus.newBuilder()
                        .setStatusCode(TStatusCode.OK.getValue()))
                .setSyncedTablets(1)
                .build();
    }
}
