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

                catalog.runBeforeCreate(streamDb, stream, baseTable);

                InOrder order = Mockito.inOrder(proxy);
                order.verify(proxy).requireTableStreamControlPlaneCapability();
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
    public void testCreateRejectsMetaServiceWithoutCapabilityBeforePrepare() throws Exception {
        List<Cloud.TableStreamOffsetPB> offsets = List.of(Cloud.TableStreamOffsetPB.newBuilder()
                .setPartitionId(1)
                .setState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                .setOffsetTso(101)
                .build());
        TestCloudInternalCatalog catalog = new TestCloudInternalCatalog(offsets);
        Database streamDb = Mockito.mock(Database.class);
        Mockito.when(streamDb.getId()).thenReturn(30L);
        OlapTable baseTable = Mockito.mock(OlapTable.class);
        Mockito.when(baseTable.getId()).thenReturn(20L);
        Mockito.when(baseTable.getPartitionIds()).thenReturn(List.of(1L));
        OlapTableStream stream = mockStream(10, 20, 40);
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);
        Mockito.doThrow(new org.apache.doris.rpc.RpcException("", "old MetaService"))
                .when(proxy).requireTableStreamControlPlaneCapability();

        try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            DdlException exception = Assertions.assertThrows(DdlException.class,
                    () -> catalog.runBeforeCreate(streamDb, stream, baseTable));
            Assertions.assertTrue(exception.getMessage().contains("old MetaService"));
            Assertions.assertEquals(0, catalog.getCaptureCallCount());
            Mockito.verify(proxy, Mockito.never()).prepareIndex(Mockito.any());
        }
    }

    @Test
    public void testCreateRequiresAuthoritativeInitialTso() throws Exception {
        CloudInternalCatalog catalog = new CloudInternalCatalog();
        Database streamDb = Mockito.mock(Database.class);
        OlapTable baseTable = Mockito.mock(OlapTable.class);
        OlapTableStream stream = mockStream(10, 20, 40);
        MetaServiceProxy proxy = Mockito.mock(MetaServiceProxy.class);

        try (MockedStatic<MetaServiceProxy> mockedProxy = Mockito.mockStatic(MetaServiceProxy.class)) {
            mockedProxy.when(MetaServiceProxy::getInstance).thenReturn(proxy);
            DdlException exception = Assertions.assertThrows(DdlException.class,
                    () -> catalog.beforeCreateTableStream(streamDb, stream, baseTable));
            Assertions.assertTrue(exception.getMessage().contains("visible TSO API"));
            Mockito.verify(proxy).requireTableStreamControlPlaneCapability();
        }
    }

    private static OlapTableStream mockStream(long baseDbId, long baseTableId, long streamId) {
        TableStreamBaseTableInfo baseTableInfo = Mockito.mock(TableStreamBaseTableInfo.class);
        Mockito.when(baseTableInfo.getDbId()).thenReturn(baseDbId);
        Mockito.when(baseTableInfo.getTableId()).thenReturn(baseTableId);
        OlapTableStream stream = Mockito.mock(OlapTableStream.class);
        Mockito.when(stream.getId()).thenReturn(streamId);
        Mockito.when(stream.getBaseTableInfo()).thenReturn(baseTableInfo);
        return stream;
    }

    private static class TestCloudInternalCatalog extends CloudInternalCatalog {
        private final List<Cloud.TableStreamOffsetPB> offsets;
        private int captureCallCount;

        private TestCloudInternalCatalog(List<Cloud.TableStreamOffsetPB> offsets) {
            this.offsets = new ArrayList<>(offsets);
        }

        @Override
        protected List<Cloud.TableStreamOffsetPB> captureTableStreamInitialOffsets(
                OlapTableStream stream, OlapTable baseTable) {
            captureCallCount++;
            return offsets;
        }

        private int getCaptureCallCount() {
            return captureCallCount;
        }

        private void runBeforeCreate(Database streamDb, OlapTableStream stream, OlapTable baseTable)
                throws DdlException {
            beforeCreateTableStream(streamDb, stream, baseTable);
        }
    }
}
