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

package org.apache.doris.binlog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudReplica;
import org.apache.doris.cloud.catalog.CloudTablet;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.qe.ComputeGroupException.FailedTypeEnum;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RowBinlogTtlDiscoveryTest {
    @ParameterizedTest
    @EnumSource(value = FailedTypeEnum.class, names = {"CURRENT_COMPUTE_GROUP_NO_BE", "COMPUTE_GROUPS_NO_ALIVE_BE"})
    public void unavailableComputeGroupDoesNotBlockHealthyGroups(FailedTypeEnum failedType) throws Exception {
        verifyHealthyGroups(failedType, false);
    }

    @Test
    public void synchronousDispatchFailureDoesNotBlockLaterBackends() throws Exception {
        verifyHealthyGroups(FailedTypeEnum.CURRENT_COMPUTE_GROUP_NO_BE, true);
    }

    private void verifyHealthyGroups(FailedTypeEnum failedType, boolean dispatchFails) throws Exception {
        boolean previous = Config.enable_feature_binlog;
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex binlogIndex = Mockito.mock(MaterializedIndex.class);
        CloudReplica replica = Mockito.mock(CloudReplica.class);
        CloudSystemInfoService info = Mockito.mock(CloudSystemInfoService.class);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Backend firstBackend = Mockito.mock(Backend.class);
        Backend lastBackend = Mockito.mock(Backend.class);
        TNetworkAddress firstAddress = new TNetworkAddress("first-backend", 8060);
        TNetworkAddress lastAddress = new TNetworkAddress("last-backend", 8060);
        List<Tablet> tablets = new ArrayList<>();
        for (long id = 1; id <= 2; id++) {
            Tablet tablet = Mockito.mock(Tablet.class);
            Mockito.when(tablet.getId()).thenReturn(id);
            Mockito.when(tablet.getReplicas()).thenReturn(List.of(replica));
            tablets.add(tablet);
        }
        // Expose one catalog sweep so repeated calls can accommodate the discovery time budget
        // without rediscovering tablets and hiding a lost batch.
        Mockito.when(catalog.getDbs()).thenReturn(List.of(database), List.of());
        Mockito.when(database.getTables()).thenReturn(List.of(table));
        Mockito.when(table.hasRowBinlogTtl()).thenReturn(true);
        Mockito.when(table.getPartitions()).thenReturn(List.of(partition));
        Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE, true))
                .thenReturn(List.of(binlogIndex));
        Mockito.when(binlogIndex.isRowBinlog()).thenReturn(true);
        Mockito.when(binlogIndex.getTablets()).thenReturn(tablets);
        Mockito.when(info.getCloudClusterIds()).thenReturn(List.of("first-healthy", "unavailable", "last-healthy"));
        Mockito.when(replica.getBackendIdWithClusterId("first-healthy")).thenReturn(7L);
        Mockito.when(replica.getBackendIdWithClusterId("unavailable"))
                .thenThrow(new ComputeGroupException("compute group unavailable", failedType));
        Mockito.when(replica.getBackendIdWithClusterId("last-healthy")).thenReturn(8L);
        Mockito.when(info.getBackend(7L)).thenReturn(firstBackend);
        Mockito.when(info.getBackend(8L)).thenReturn(lastBackend);
        Mockito.when(firstBackend.isAlive()).thenReturn(true);
        Mockito.when(lastBackend.isAlive()).thenReturn(true);
        Mockito.when(firstBackend.getBrpcAddress()).thenReturn(firstAddress);
        Mockito.when(lastBackend.getBrpcAddress()).thenReturn(lastAddress);
        Map<TNetworkAddress, List<Long>> delivered = new HashMap<>();
        Map<TNetworkAddress, List<Long>> expected = dispatchFails ? Map.of(lastAddress, List.of(1L, 2L)) : Map.of(
                firstAddress, List.of(1L, 2L), lastAddress, List.of(1L, 2L));
        Mockito.when(proxy.syncTabletMeta(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            TNetworkAddress address = invocation.getArgument(0);
            if (dispatchFails && address.equals(firstAddress)) {
                throw new RpcException("first-backend", "injected synchronous dispatch failure");
            }
            InternalService.PSyncTabletMetaRequest request = invocation.getArgument(1);
            Assertions.assertTrue(request.getDiscoverRowBinlogTtl());
            delivered.computeIfAbsent(address, ignored -> new ArrayList<>()).addAll(request.getTabletIdsList());
            return Futures.immediateFuture(InternalService.PSyncTabletMetaResponse.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0)).build());
        });
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
            Config.enable_feature_binlog = true;
            config.when(Config::isCloudMode).thenReturn(true);
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentSystemInfo).thenReturn(info);
            service.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            RowBinlogTtlDiscovery discovery = new RowBinlogTtlDiscovery();
            for (int round = 0; round < 100 && !delivered.equals(expected); round++) {
                discovery.discover();
            }
            Assertions.assertEquals(expected, delivered);
            Mockito.verify(replica, Mockito.times(2)).getBackendIdWithClusterId("unavailable");
        } finally {
            Config.enable_feature_binlog = previous;
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 32})
    public void batchesRespectGlobalAndBackendLimitsAcrossComputeGroups(int backendsPerGroup) throws Exception {
        boolean previous = Config.enable_feature_binlog;
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex binlogIndex = Mockito.mock(MaterializedIndex.class);
        CloudSystemInfoService info = Mockito.mock(CloudSystemInfoService.class);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        List<String> groups = List.of("first-group", "last-group");
        Map<Long, Backend> backends = new HashMap<>();
        Map<TNetworkAddress, Integer> addressGroups = new HashMap<>();
        Map<TNetworkAddress, List<Long>> expected = new HashMap<>();
        List<CloudReplica> replicas = new ArrayList<>();
        for (int backendIndex = 0; backendIndex < backendsPerGroup; backendIndex++) {
            CloudReplica replica = Mockito.mock(CloudReplica.class);
            for (int group = 0; group < groups.size(); group++) {
                long backendId = 1L + group * backendsPerGroup + backendIndex;
                Backend backend = Mockito.mock(Backend.class);
                TNetworkAddress address = new TNetworkAddress(groups.get(group) + "-" + backendIndex, 8060);
                Mockito.when(backend.isAlive()).thenReturn(true);
                Mockito.when(backend.getBrpcAddress()).thenReturn(address);
                Mockito.when(replica.getBackendIdWithClusterId(groups.get(group))).thenReturn(backendId);
                backends.put(backendId, backend);
                addressGroups.put(address, group);
                expected.put(address, new ArrayList<>());
            }
            replicas.add(replica);
        }
        List<Tablet> tablets = new ArrayList<>();
        for (long id = 1; id <= 2050; id++) {
            int backendIndex = (int) ((id - 1) % backendsPerGroup);
            CloudTablet tablet = new CloudTablet(id);
            tablet.addReplica(replicas.get(backendIndex), true);
            tablets.add(tablet);
            for (int group = 0; group < groups.size(); group++) {
                long backendId = 1L + group * backendsPerGroup + backendIndex;
                expected.get(backends.get(backendId).getBrpcAddress()).add(id);
            }
        }
        // A single catalog sweep prevents later sweeps from hiding tablets lost at a batch boundary.
        Mockito.when(catalog.getDbs()).thenReturn(List.of(database), List.of());
        Mockito.when(database.getTables()).thenReturn(List.of(table));
        Mockito.when(table.hasRowBinlogTtl()).thenReturn(true);
        Mockito.when(table.getPartitions()).thenReturn(List.of(partition));
        Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE, true))
                .thenReturn(List.of(binlogIndex));
        Mockito.when(binlogIndex.isRowBinlog()).thenReturn(true);
        Mockito.when(binlogIndex.getTablets()).thenReturn(tablets);
        Mockito.when(info.getCloudClusterIds()).thenReturn(groups);
        Mockito.when(info.getBackend(Mockito.anyLong()))
                .thenAnswer(invocation -> backends.get(invocation.<Long>getArgument(0)));
        Map<TNetworkAddress, List<Long>> delivered = new HashMap<>();
        Map<TNetworkAddress, List<Long>> batch = new HashMap<>();
        Mockito.when(proxy.syncTabletMeta(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            TNetworkAddress address = invocation.getArgument(0);
            InternalService.PSyncTabletMetaRequest request = invocation.getArgument(1);
            Assertions.assertTrue(request.getDiscoverRowBinlogTtl());
            delivered.computeIfAbsent(address, ignored -> new ArrayList<>()).addAll(request.getTabletIdsList());
            batch.computeIfAbsent(address, ignored -> new ArrayList<>()).addAll(request.getTabletIdsList());
            return Futures.immediateFuture(InternalService.PSyncTabletMetaResponse.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0)).build());
        });
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
            Config.enable_feature_binlog = true;
            config.when(Config::isCloudMode).thenReturn(true);
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentSystemInfo).thenReturn(info);
            service.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            RowBinlogTtlDiscovery discovery = new RowBinlogTtlDiscovery();
            // The time budget may end any batch early, so check limits and coverage across calls.
            for (int round = 0; round < 10000 && !delivered.equals(expected); round++) {
                batch.clear();
                discovery.discover();
                List<Set<Long>> groupTablets = List.of(new HashSet<>(), new HashSet<>());
                for (Map.Entry<TNetworkAddress, List<Long>> target : batch.entrySet()) {
                    Assertions.assertTrue(target.getValue().size() <= 64);
                    groupTablets.get(addressGroups.get(target.getKey())).addAll(target.getValue());
                }
                Assertions.assertTrue(groupTablets.get(0).size() <= 1024);
                // The tablet that fills a BE's batch must still reach the remaining compute group.
                Assertions.assertEquals(groupTablets.get(0), groupTablets.get(1));
            }
            Assertions.assertEquals(expected, delivered);
        } finally {
            Config.enable_feature_binlog = previous;
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {130, 100000})
    public void boundedSweepsVisitUncachedTabletsAndRetryFailedRequests(int tabletCount) throws Exception {
        boolean previous = Config.enable_feature_binlog;
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable enabled = Mockito.mock(OlapTable.class);
        OlapTable disabled = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex dataIndex = Mockito.mock(MaterializedIndex.class);
        MaterializedIndex binlogIndex = Mockito.mock(MaterializedIndex.class);
        CloudReplica replica = Mockito.mock(CloudReplica.class);
        CloudSystemInfoService info = Mockito.mock(CloudSystemInfoService.class);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Backend backend = Mockito.mock(Backend.class);
        TNetworkAddress address = new TNetworkAddress("backend", 8060);
        List<Tablet> tablets = new ArrayList<>();
        Set<Long> expected = new HashSet<>();
        for (long id = 1; id <= tabletCount; id++) {
            CloudTablet tablet = new CloudTablet(id);
            tablet.addReplica(replica, true);
            tablets.add(tablet);
            expected.add(id);
        }
        Mockito.when(catalog.getDbs()).thenReturn(List.of(database));
        Mockito.when(database.getTables()).thenReturn(List.of(disabled, enabled));
        Mockito.when(enabled.hasRowBinlogTtl()).thenReturn(true);
        Mockito.when(enabled.getPartitions()).thenReturn(List.of(partition));
        Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE, true))
                .thenReturn(List.of(dataIndex, binlogIndex));
        Mockito.when(binlogIndex.isRowBinlog()).thenReturn(true);
        Mockito.when(binlogIndex.getTablets()).thenReturn(tablets);
        Mockito.when(info.getCloudClusterIds()).thenReturn(List.of("compute-group"));
        Mockito.when(replica.getBackendIdWithClusterId("compute-group")).thenReturn(7L);
        Mockito.when(info.getBackend(7L)).thenReturn(backend);
        Mockito.when(backend.isAlive()).thenReturn(true);
        Mockito.when(backend.getBrpcAddress()).thenReturn(address);
        List<InternalService.PSyncTabletMetaRequest> requests = new ArrayList<>();
        Set<Long> delivered = new HashSet<>();
        Mockito.when(proxy.syncTabletMeta(Mockito.eq(address), Mockito.any())).thenAnswer(invocation -> {
            InternalService.PSyncTabletMetaRequest request = invocation.getArgument(1);
            requests.add(request);
            if (requests.size() == 1) {
                return Futures.immediateFailedFuture(new IllegalStateException("backend restarting"));
            }
            delivered.addAll(request.getTabletIdsList());
            return Futures.immediateFuture(InternalService.PSyncTabletMetaResponse.newBuilder()
                    .setStatus(Types.PStatus.newBuilder().setStatusCode(0)).build());
        });
        try (MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
            Config.enable_feature_binlog = true;
            config.when(Config::isCloudMode).thenReturn(true);
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentSystemInfo).thenReturn(info);
            service.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            RowBinlogTtlDiscovery discovery = new RowBinlogTtlDiscovery();
            long scheduledDelayMs = 0;
            int rounds = 0;
            while (rounds++ < 10000 && delivered.size() < tabletCount) {
                discovery.discover();
                scheduledDelayMs += discovery.getInterval();
            }
            // Includes a failed RPC and a second sweep, without sleeping in the unit test.
            Assertions.assertTrue(scheduledDelayMs < 400000, "scheduled delay ms=" + scheduledDelayMs);
            System.out.println("ROW discovery tablets=" + tabletCount + ", batches=" + rounds
                    + ", scheduled delay ms=" + scheduledDelayMs);
            Assertions.assertEquals(expected, delivered);
            for (InternalService.PSyncTabletMetaRequest request : requests) {
                Assertions.assertTrue(request.getDiscoverRowBinlogTtl());
                Assertions.assertTrue(request.getTabletIdsCount() <= 64);
            }
            Mockito.verify(disabled, Mockito.never()).getPartitions();
            Mockito.verify(dataIndex, Mockito.never()).getTablets();
            int count = requests.size();
            Config.enable_feature_binlog = false;
            discovery.discover();
            Assertions.assertEquals(count, requests.size());
        } finally {
            Config.enable_feature_binlog = previous;
        }
    }
}
