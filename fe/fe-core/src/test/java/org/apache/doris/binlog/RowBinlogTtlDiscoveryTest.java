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
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RowBinlogTtlDiscoveryTest {
    @Test
    public void boundedSweepsVisitUncachedTabletsAndRetryFailedRequests() throws Exception {
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
        for (long id = 1; id <= 130; id++) {
            Tablet tablet = Mockito.mock(Tablet.class);
            Mockito.when(tablet.getId()).thenReturn(id);
            Mockito.when(tablet.getReplicas()).thenReturn(List.of(replica));
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
            for (int round = 0; round < 100 && !delivered.equals(expected); round++) {
                discovery.discover();
            }
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
