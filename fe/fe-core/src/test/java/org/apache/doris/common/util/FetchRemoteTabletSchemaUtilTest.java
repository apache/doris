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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.RemoteIndexSchemaProcDir;
import org.apache.doris.common.util.FetchRemoteTabletSchemaUtil.PartitionTablets;
import org.apache.doris.proto.InternalService.PFetchRemoteSchemaRequest;
import org.apache.doris.proto.InternalService.PFetchRemoteSchemaResponse;
import org.apache.doris.proto.OlapFile.ColumnPB;
import org.apache.doris.proto.OlapFile.TabletSchemaPB;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.AbstractList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class FetchRemoteTabletSchemaUtilTest {
    private static final PFetchRemoteSchemaResponse FAILED = PFetchRemoteSchemaResponse.newBuilder()
            .setStatus(PStatus.newBuilder().setStatusCode(TStatusCode.INTERNAL_ERROR.getValue())
                    .addErrorMsgs("injected metadata read failure"))
            .build();
    private static final PStatus OK = PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()).build();

    @Test
    void failsWhenAllCoordinatorsFail() throws Exception {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any()))
                .thenReturn(Futures.immediateFuture(FAILED));
        Assertions.assertThrows(AnalysisException.class, () -> fetch(proxy, true, true));
        Mockito.verify(proxy, Mockito.times(2)).fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any());
    }

    @Test
    void failsWhenTabletHasNoLiveBackend() {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Assertions.assertThrows(AnalysisException.class, () -> fetch(proxy, false, false));
    }

    @Test
    void failsWhenSuccessfulRpcHasNoSchema() throws Exception {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any())).thenReturn(
                Futures.immediateFuture(PFetchRemoteSchemaResponse.newBuilder().setStatus(OK).build()));
        Assertions.assertThrows(AnalysisException.class, () -> fetch(proxy, true, false));
    }

    @Test
    void retriesAndReturnsOnlyTheCompleteResponse() throws Exception {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        PFetchRemoteSchemaResponse recovered = PFetchRemoteSchemaResponse.newBuilder()
                .setStatus(OK)
                .setMergedSchema(TabletSchemaPB.newBuilder().addColumn(
                        ColumnPB.newBuilder().setUniqueId(-1).setName("v.key").setType("INT")))
                .build();
        Mockito.when(proxy.fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any())).thenReturn(
                Futures.immediateFuture(FAILED), Futures.immediateFuture(recovered));
        Assertions.assertEquals(List.of("v.key"), fetch(proxy, true, true).stream().map(Column::getName).toList());
        Mockito.verify(proxy, Mockito.times(2)).fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any());
    }

    @Test
    void emptyTabletListIsValid() throws Exception {
        Assertions.assertEquals(List.of(), new FetchRemoteTabletSchemaUtil(List.of()).fetch());
    }

    @Test
    void rejectsOversizedPartitionWithoutCopyingItsTablets() {
        List<Tablet> oversizedTablets = new AbstractList<>() {
            @Override
            public Tablet get(int index) {
                throw new AssertionError("Oversized tablet lists must only be counted");
            }

            @Override
            public int size() {
                return 2048;
            }
        };
        List<PartitionTablets> partitions = List.of(
                new PartitionTablets(Mockito.mock(Partition.class), List.of(tabletWithReplicas(101))),
                new PartitionTablets(Mockito.mock(Partition.class), oversizedTablets));
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
        try (MockedStatic<ConnectContext> contextStatic = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<Config> configStatic = Mockito.mockStatic(Config.class)) {
            contextStatic.when(ConnectContext::get).thenReturn(context);
            configStatic.when(Config::isCloudMode).thenReturn(false);
            AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                    () -> FetchRemoteTabletSchemaUtil.collectTablets(partitions));
            Assertions.assertTrue(e.getMessage().contains("needs to read 2049 tablets"), e.getMessage());
        }
    }

    @Test
    void collectsCloudTabletsUsingOneVersionLookup() throws Exception {
        List<CloudPartition> cloudPartitions = List.of(Mockito.mock(CloudPartition.class),
                Mockito.mock(CloudPartition.class), Mockito.mock(CloudPartition.class));
        Tablet first = tabletWithReplicas(101);
        Tablet last = tabletWithReplicas(303);
        List<PartitionTablets> partitions = List.of(new PartitionTablets(cloudPartitions.get(0), List.of(first)),
                new PartitionTablets(cloudPartitions.get(1), List.of(tabletWithReplicas(202))),
                new PartitionTablets(cloudPartitions.get(2), List.of(last)));
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
        long initVersion = Partition.PARTITION_INIT_VERSION;
        try (MockedStatic<ConnectContext> contextStatic = Mockito.mockStatic(ConnectContext.class);
                MockedStatic<Config> configStatic = Mockito.mockStatic(Config.class);
                MockedStatic<CloudPartition> cloudStatic = Mockito.mockStatic(CloudPartition.class)) {
            contextStatic.when(ConnectContext::get).thenReturn(context);
            configStatic.when(Config::isCloudMode).thenReturn(true);
            cloudStatic.when(() -> CloudPartition.getSnapshotVisibleVersion(cloudPartitions))
                    .thenReturn(List.of(initVersion + 1, initVersion, initVersion + 2));
            Assertions.assertEquals(List.of(first, last), FetchRemoteTabletSchemaUtil.collectTablets(partitions));
            cloudStatic.verify(() -> CloudPartition.getSnapshotVisibleVersion(cloudPartitions));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void keepsSchemaAndTabletsTogetherAcrossConcurrentIndexReplacement(boolean namedPartition) throws Exception {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.doAnswer(invocation -> {
            lock.readLock().lock();
            return null;
        }).when(table).readLock();
        Mockito.doAnswer(invocation -> {
            lock.readLock().unlock();
            return null;
        }).when(table).readUnlock();
        long indexId = 1;
        MaterializedIndexMeta indexMeta = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.when(table.getIndexMetaByIndexId(indexId)).thenReturn(indexMeta);
        AtomicReference<List<Column>> schema = new AtomicReference<>(List.of(new Column("before", Type.INT)));
        Mockito.when(table.getSchemaByIndexId(indexId)).thenAnswer(invocation -> {
            Assertions.assertTrue(lock.getReadHoldCount() > 0);
            return schema.get();
        });

        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(table.getPartitions()).thenReturn(List.of(partition));
        Mockito.when(table.getPartition("p1")).thenReturn(partition);
        Tablet originalTablet = tabletWithReplicas(101);
        Tablet replacementTablet = tabletWithReplicas(202);
        MaterializedIndex original = Mockito.mock(MaterializedIndex.class);
        Mockito.when(original.getTablets()).thenReturn(List.of(originalTablet));
        MaterializedIndex replacement = Mockito.mock(MaterializedIndex.class);
        Mockito.when(replacement.getTablets()).thenReturn(List.of(replacementTablet));
        AtomicReference<MaterializedIndex> baseIndex = new AtomicReference<>(original);
        Mockito.when(partition.getBaseIndex()).thenAnswer(invocation -> {
            Assertions.assertTrue(lock.getReadHoldCount() > 0);
            return baseIndex.get();
        });
        Mockito.when(partition.getVisibleVersion()).thenAnswer(invocation -> {
            Assertions.assertEquals(0, lock.getReadHoldCount());
            CompletableFuture.runAsync(() -> {
                lock.writeLock().lock();
                try {
                    schema.set(List.of(new Column("after", Type.INT)));
                    baseIndex.set(replacement);
                } finally {
                    lock.writeLock().unlock();
                }
            }).get(5, TimeUnit.SECONDS);
            return Partition.PARTITION_INIT_VERSION + 1L;
        });

        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            Assertions.assertEquals(0, lock.getReadHoldCount());
            PFetchRemoteSchemaRequest request = invocation.getArgument(1);
            Assertions.assertEquals(List.of(101L), request.getTabletLocation(0).getTabletIdList());
            return Futures.immediateFuture(PFetchRemoteSchemaResponse.newBuilder()
                    .setStatus(OK).setMergedSchema(TabletSchemaPB.getDefaultInstance()).build());
        });

        try (MockedStatic<Config> configStatic = Mockito.mockStatic(Config.class)) {
            configStatic.when(Config::isCloudMode).thenReturn(true);
            ProcResult result = withBackends(proxy, true, false, () -> {
                RemoteIndexSchemaProcDir dir = new RemoteIndexSchemaProcDir(table, indexId);
                ProcNodeInterface node = namedPartition ? dir.lookup("p1") : dir;
                return node.fetchResult();
            });
            Assertions.assertEquals(List.of("before"), result.getRows().stream().map(row -> row.get(0)).toList());
        }
    }

    private List<Column> fetch(BackendServiceProxy proxy, boolean firstAlive, boolean secondAlive) throws Exception {
        Tablet tablet = tabletWithReplicas(101);
        return withBackends(proxy, firstAlive, secondAlive,
                () -> new FetchRemoteTabletSchemaUtil(List.of(tablet)).fetch());
    }

    private Tablet tabletWithReplicas(long tabletId) {
        Tablet tablet = Mockito.mock(Tablet.class);
        Mockito.when(tablet.getId()).thenReturn(tabletId);
        Replica first = Mockito.mock(Replica.class);
        Replica second = Mockito.mock(Replica.class);
        Mockito.when(first.isAlive()).thenReturn(true);
        Mockito.when(second.isAlive()).thenReturn(true);
        Mockito.when(first.getBackendIdWithoutException()).thenReturn(1L);
        Mockito.when(second.getBackendIdWithoutException()).thenReturn(2L);
        Mockito.when(tablet.getReplicas()).thenReturn(List.of(first, second));
        return tablet;
    }

    private <T> T withBackends(BackendServiceProxy proxy, boolean firstAlive, boolean secondAlive,
            Callable<T> fetch) throws Exception {
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Backend firstBackend = new Backend(1L, "backend1", 9050);
        firstBackend.setAlive(firstAlive);
        Backend secondBackend = new Backend(2L, "backend2", 9050);
        secondBackend.setAlive(secondAlive);
        Mockito.when(systemInfo.getBackend(1L)).thenReturn(firstBackend);
        Mockito.when(systemInfo.getBackend(2L)).thenReturn(secondBackend);

        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> proxyStatic = Mockito.mockStatic(BackendServiceProxy.class);
                MockedStatic<ConnectContext> contextStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            proxyStatic.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            contextStatic.when(ConnectContext::get).thenReturn(context);
            return fetch.call();
        }
    }
}
