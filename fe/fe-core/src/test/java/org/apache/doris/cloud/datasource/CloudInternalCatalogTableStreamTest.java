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

package org.apache.doris.cloud.datasource;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.TableStreamBaseTableInfo;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.rpc.VersionHelper;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class CloudInternalCatalogTableStreamTest {

    @Test
    public void testCreateBatchesOffsetsAndCommitsIndexLast() throws Exception {
        int previousBatchSize = Config.cloud_table_stream_create_partition_batch_size;
        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        Config.cloud_table_stream_create_partition_batch_size = 2;
        Config.cloud_unique_id = "cloud_table_stream_ut";
        Config.meta_service_endpoint = "127.0.0.1:20121";
        try {
            List<Cloud.TableStreamOffsetPB> offsets = LongStream.rangeClosed(1, 5)
                    .mapToObj(partitionId -> Cloud.TableStreamOffsetPB.newBuilder()
                            .setPartitionId(partitionId)
                            .setState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                            .setOffsetTso(100 + partitionId)
                            .build())
                    .collect(Collectors.toList());
            TestCloudInternalCatalog catalog = new TestCloudInternalCatalog(offsets);
            Database streamDb = Mockito.mock(Database.class);
            Mockito.when(streamDb.getId()).thenReturn(30L);
            OlapTable baseTable = Mockito.mock(OlapTable.class);
            Mockito.when(baseTable.getId()).thenReturn(20L);
            Mockito.when(baseTable.getPartitionIds()).thenReturn(
                    offsets.stream().map(Cloud.TableStreamOffsetPB::getPartitionId).collect(Collectors.toList()));
            OlapTableStream stream = mockStream(10, 20, 40);

            MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
            Cloud.IndexResponse indexResponse = Cloud.IndexResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK))
                    .build();
            Cloud.PartitionResponse partitionResponse = Cloud.PartitionResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK))
                    .build();
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                Mockito.when(proxy.prepareIndex(Mockito.any())).thenReturn(indexResponse);
                Mockito.when(proxy.commitPartition(Mockito.any())).thenReturn(partitionResponse);
                Mockito.when(proxy.commitIndex(Mockito.any())).thenReturn(indexResponse);

                List<Long> basePartitionIds = offsets.stream()
                        .map(Cloud.TableStreamOffsetPB::getPartitionId)
                        .collect(Collectors.toList());
                catalog.runBeforeCreate(streamDb, stream, baseTable, basePartitionIds);
                Mockito.verify(proxy, Mockito.never()).commitIndex(Mockito.any());
                catalog.runAfterCreate(streamDb, stream, baseTable);

                InOrder order = Mockito.inOrder(proxy);
                order.verify(proxy).prepareIndex(Mockito.any());
                order.verify(proxy, Mockito.times(3)).commitPartition(Mockito.any());
                order.verify(proxy).commitIndex(Mockito.any());

                ArgumentCaptor<Cloud.IndexRequest> prepareCaptor = ArgumentCaptor.forClass(Cloud.IndexRequest.class);
                Mockito.verify(proxy).prepareIndex(prepareCaptor.capture());
                Assertions.assertEquals(Cloud.IndexObjectTypePB.TABLE_STREAM,
                        prepareCaptor.getValue().getObjectType());
                Assertions.assertEquals(List.of(40L), prepareCaptor.getValue().getIndexIdsList());
                Assertions.assertEquals(30, prepareCaptor.getValue().getStreamDbId());

                ArgumentCaptor<Cloud.PartitionRequest> partitionCaptor =
                        ArgumentCaptor.forClass(Cloud.PartitionRequest.class);
                Mockito.verify(proxy, Mockito.times(3)).commitPartition(partitionCaptor.capture());
                List<Integer> batchSizes = partitionCaptor.getAllValues().stream()
                        .map(Cloud.PartitionRequest::getTableStreamOffsetsCount)
                        .collect(Collectors.toList());
                Assertions.assertEquals(List.of(2, 2, 1), batchSizes);
                partitionCaptor.getAllValues().forEach(request -> {
                    Assertions.assertEquals(Cloud.IndexObjectTypePB.TABLE_STREAM, request.getObjectType());
                    Assertions.assertEquals(30, request.getStreamDbId());
                    Assertions.assertEquals(request.getPartitionIdsList(), request.getTableStreamOffsetsList()
                            .stream().map(Cloud.TableStreamOffsetPB::getPartitionId).collect(Collectors.toList()));
                });
            }
        } finally {
            Config.cloud_table_stream_create_partition_batch_size = previousBatchSize;
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
        }
    }

    @Test
    public void testTsoEnabledTableInitializesPartitionVersions() throws Exception {
        String previousCloudUniqueId = Config.cloud_unique_id;
        String previousMetaServiceEndpoint = Config.meta_service_endpoint;
        boolean previousCheckRecycleKey = Config.check_create_table_recycle_key_remained;
        Config.cloud_unique_id = "cloud_table_stream_ut";
        Config.meta_service_endpoint = "127.0.0.1:20121";
        Config.check_create_table_recycle_key_remained = false;
        try {
            CloudInternalCatalog catalog = new CloudInternalCatalog();
            OlapTable table = Mockito.mock(OlapTable.class);
            Mockito.when(table.enableTso()).thenReturn(true);
            MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
            Cloud.IndexResponse indexResponse = Cloud.IndexResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK))
                    .build();
            Cloud.PartitionResponse partitionResponse = Cloud.PartitionResponse.newBuilder()
                    .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK))
                    .build();
            try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
                mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
                Mockito.when(proxy.commitIndex(Mockito.any())).thenReturn(indexResponse);
                Mockito.when(proxy.commitPartition(Mockito.any())).thenReturn(partitionResponse);

                catalog.afterCreatePartitions(10, 20, List.of(30L), List.of(40L), true, true, table);
                ArgumentCaptor<Cloud.IndexRequest> indexCaptor = ArgumentCaptor.forClass(Cloud.IndexRequest.class);
                Mockito.verify(proxy).commitIndex(indexCaptor.capture());
                Assertions.assertTrue(indexCaptor.getValue().getEnableTso());

                catalog.afterCreatePartitions(10, 20, List.of(31L), List.of(40L), false, false, table);
                ArgumentCaptor<Cloud.PartitionRequest> partitionCaptor =
                        ArgumentCaptor.forClass(Cloud.PartitionRequest.class);
                Mockito.verify(proxy).commitPartition(partitionCaptor.capture());
                Assertions.assertTrue(partitionCaptor.getValue().getEnableTso());
            }
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
            Config.meta_service_endpoint = previousMetaServiceEndpoint;
            Config.check_create_table_recycle_key_remained = previousCheckRecycleKey;
        }
    }

    @Test
    public void testCaptureInitialOffsetsUsesVersionCommitTsoSnapshot() throws Exception {
        CaptureCloudInternalCatalog catalog = new CaptureCloudInternalCatalog();
        OlapTable baseTable = Mockito.mock(OlapTable.class);
        Mockito.when(baseTable.getId()).thenReturn(20L);
        OlapTableStream stream = mockStream(10, 20, 40, true);
        Cloud.GetVersionResponse response = versionResponse(List.of(1L, 2L), List.of(-1L, 101L));

        try (MockedStatic<VersionHelper> mockedVersionHelper = Mockito.mockStatic(VersionHelper.class)) {
            mockedVersionHelper.when(() -> VersionHelper.getVersionFromMeta(Mockito.any()))
                    .thenAnswer(invocation -> {
                        Cloud.GetVersionRequest request = invocation.getArgument(0);
                        Assertions.assertTrue(request.getBatchMode());
                        Assertions.assertTrue(request.getWaitForPendingTxn());
                        Assertions.assertEquals(List.of(10L, 10L), request.getDbIdsList());
                        Assertions.assertEquals(List.of(20L, 20L), request.getTableIdsList());
                        Assertions.assertEquals(List.of(1L, 2L), request.getPartitionIdsList());
                        return response;
                    });

            List<Cloud.TableStreamOffsetPB> offsets = catalog.capture(stream, baseTable, List.of(1L, 2L));
            Assertions.assertEquals(2, offsets.size());
            Assertions.assertEquals(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED,
                    offsets.get(0).getState());
            Assertions.assertEquals(-1, offsets.get(0).getOffsetTso());
            Assertions.assertEquals(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING,
                    offsets.get(1).getState());
            Assertions.assertEquals(101, offsets.get(1).getOffsetTso());
        }
    }

    @Test
    public void testCaptureInitialOffsetsRejectsInvalidVersionResponse() throws Exception {
        CaptureCloudInternalCatalog catalog = new CaptureCloudInternalCatalog();
        OlapTable baseTable = Mockito.mock(OlapTable.class);
        Mockito.when(baseTable.getId()).thenReturn(20L);
        OlapTableStream stream = mockStream(10, 20, 40, false);
        Cloud.GetVersionResponse missingInitialVersion = versionResponse(List.of(-1L), List.of(-1L));
        Cloud.GetVersionResponse sizeMismatch = versionResponse(List.of(1L), List.of());

        try (MockedStatic<VersionHelper> mockedVersionHelper = Mockito.mockStatic(VersionHelper.class)) {
            mockedVersionHelper.when(() -> VersionHelper.getVersionFromMeta(Mockito.any()))
                    .thenReturn(missingInitialVersion, sizeMismatch);

            DdlException invalidVersion = Assertions.assertThrows(DdlException.class,
                    () -> catalog.capture(stream, baseTable, List.of(1L)));
            Assertions.assertTrue(invalidVersion.getMessage().contains("Invalid version or commit TSO"));

            DdlException invalidSize = Assertions.assertThrows(DdlException.class,
                    () -> catalog.capture(stream, baseTable, List.of(1L)));
            Assertions.assertTrue(invalidSize.getMessage().contains("response size"));
        }
    }

    @Test
    public void testValidateBaseTableSnapshot() throws Exception {
        TestCloudInternalCatalog catalog = new TestCloudInternalCatalog(List.of());
        OlapTable baseTable = Mockito.mock(OlapTable.class);
        Mockito.when(baseTable.getBaseSchemaVersion()).thenReturn(7);
        Mockito.when(baseTable.getPartitionIds()).thenReturn(List.of(1L, 2L, 3L));
        OlapTableStream stream = mockStream(10, 20, 40);
        Mockito.when(stream.getBaseTableInfo().getTableNullable()).thenReturn(baseTable);

        catalog.runValidateBaseSnapshot(stream, baseTable, List.of(1L, 2L), 7);
        Mockito.verify(baseTable).checkAsTableStreamBaseTable(stream.getStreamScanType());

        Mockito.when(baseTable.getPartitionIds()).thenReturn(List.of(1L, 3L));
        DdlException partitionException = Assertions.assertThrows(DdlException.class,
                () -> catalog.runValidateBaseSnapshot(stream, baseTable, List.of(1L, 2L), 7));
        Assertions.assertTrue(partitionException.getMessage().contains("partition changed"));

        Mockito.when(baseTable.getPartitionIds()).thenReturn(List.of(1L, 2L, 3L));
        Mockito.when(baseTable.getBaseSchemaVersion()).thenReturn(8);
        DdlException schemaException = Assertions.assertThrows(DdlException.class,
                () -> catalog.runValidateBaseSnapshot(stream, baseTable, List.of(1L, 2L), 7));
        Assertions.assertTrue(schemaException.getMessage().contains("schema changed"));

        Mockito.when(stream.getBaseTableInfo().getTableNullable()).thenReturn(Mockito.mock(OlapTable.class));
        DdlException tableException = Assertions.assertThrows(DdlException.class,
                () -> catalog.runValidateBaseSnapshot(stream, baseTable, List.of(1L, 2L), 8));
        Assertions.assertTrue(tableException.getMessage().contains("Base table changed"));
    }

    private static OlapTableStream mockStream(long baseDbId, long baseTableId, long streamId) {
        return mockStream(baseDbId, baseTableId, streamId, false);
    }

    private static OlapTableStream mockStream(long baseDbId, long baseTableId, long streamId,
            boolean showInitialRows) {
        TableStreamBaseTableInfo baseTableInfo = Mockito.mock(TableStreamBaseTableInfo.class);
        Mockito.when(baseTableInfo.getDbId()).thenReturn(baseDbId);
        Mockito.when(baseTableInfo.getTableId()).thenReturn(baseTableId);
        OlapTableStream stream = Mockito.mock(OlapTableStream.class);
        Mockito.when(stream.getId()).thenReturn(streamId);
        Mockito.when(stream.getBaseTableInfo()).thenReturn(baseTableInfo);
        Mockito.when(stream.isShowInitialRows()).thenReturn(showInitialRows);
        return stream;
    }

    private static Cloud.GetVersionResponse versionResponse(List<Long> versions, List<Long> commitTsos) {
        return Cloud.GetVersionResponse.newBuilder()
                .setStatus(Cloud.MetaServiceResponseStatus.newBuilder().setCode(Cloud.MetaServiceCode.OK))
                .addAllVersions(versions)
                .addAllCommitTsos(commitTsos)
                .build();
    }

    private static class CaptureCloudInternalCatalog extends CloudInternalCatalog {
        private List<Cloud.TableStreamOffsetPB> capture(OlapTableStream stream, OlapTable baseTable,
                List<Long> basePartitionIds) throws DdlException {
            return captureTableStreamInitialOffsets(stream, baseTable, basePartitionIds);
        }
    }

    private static class TestCloudInternalCatalog extends CloudInternalCatalog {
        private final List<Cloud.TableStreamOffsetPB> offsets;

        private TestCloudInternalCatalog(List<Cloud.TableStreamOffsetPB> offsets) {
            this.offsets = new ArrayList<>(offsets);
        }

        @Override
        protected List<Cloud.TableStreamOffsetPB> captureTableStreamInitialOffsets(
                OlapTableStream stream, OlapTable baseTable, List<Long> basePartitionIds) {
            return offsets;
        }

        private void runBeforeCreate(Database streamDb, OlapTableStream stream, OlapTable baseTable,
                List<Long> basePartitionIds)
                throws DdlException {
            beforeCreateTableStream(streamDb, stream, baseTable, basePartitionIds);
        }

        private void runAfterCreate(Database streamDb, OlapTableStream stream, OlapTable baseTable)
                throws DdlException {
            afterCreateTableStream(streamDb, stream, baseTable);
        }

        private void runValidateBaseSnapshot(OlapTableStream stream, OlapTable baseTable,
                List<Long> basePartitionIds, int baseSchemaVersion) throws DdlException {
            validateTableStreamBaseSnapshot(stream, baseTable, basePartitionIds, baseSchemaVersion);
        }
    }
}
