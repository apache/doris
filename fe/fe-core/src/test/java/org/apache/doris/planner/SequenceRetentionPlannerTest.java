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

package org.apache.doris.planner;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.cloud.rpc.VersionHelper;
import org.apache.doris.cloud.transaction.CloudGlobalTransactionMgr;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TOlapTablePartitionParam;
import org.apache.doris.thrift.TOlapTableSink;
import org.apache.doris.transaction.GlobalTransactionMgrIface;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class SequenceRetentionPlannerTest {
    private OlapTable table(PartitionInfo info) {
        OlapTable table = new OlapTable(11, "retention", List.of(), KeysType.UNIQUE_KEYS,
                info, new RandomDistributionInfo(1));
        table.setTableProperty(new TableProperty(Map.of(PropertyAnalyzer.PROPERTIES_SEQUENCE_RETENTION_GAP, "100")));
        return table;
    }

    private TOlapTablePartition partition() {
        return new TOlapTablePartition().setId(22);
    }

    @Test
    public void testLegacySinkDoesNotReadInternalMetadata() throws Exception {
        TOlapTableSink sink = new TOlapTableSink().setPartition(
                new TOlapTablePartitionParam().setPartitions(List.of(partition())));
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            SequenceRetentionPlanner.prepare(sink);
            env.verifyNoInteractions();
        }
    }

    @Test
    public void testCaptureOutsideTableLockAndKeepFirstSnapshot() throws Exception {
        String original = Config.cloud_unique_id;
        Config.cloud_unique_id = "";
        try {
            OlapTable table = table(new SinglePartitionInfo());
            Partition current = new Partition(22, "p", new MaterializedIndex(33, MaterializedIndex.IndexState.NORMAL),
                    new RandomDistributionInfo(1));
            current.updateVisibleVersion(620);
            table.addPartition(current);
            TOlapTablePartition partition = partition();
            MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(table, "rwLock");
            GlobalTransactionMgrIface manager = Mockito.mock(GlobalTransactionMgrIface.class);
            Mockito.when(manager.captureSequenceRetentionVersions(Mockito.eq(1L), Mockito.eq(9L),
                    Mockito.eq(11L), Mockito.eq(100L), Mockito.anyMap())).thenAnswer(invocation -> {
                        Assertions.assertEquals(0, lock.getReadHoldCount());
                        Assertions.assertFalse(lock.isWriteLockedByCurrentThread());
                        Assertions.assertEquals(Map.of(22L, current.getVisibleVersion()), invocation.getArgument(4));
                        return Map.of(22L, 620L);
                    });
            try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
                env.when(Env::getCurrentGlobalTransactionMgr).thenReturn(manager);
                SequenceRetentionPlanner.preparePartitions(1, 9, table, List.of(partition));
                Assertions.assertEquals(620, partition.getSequenceRetentionPlanVersion());
                current.updateVisibleVersion(650);
                SequenceRetentionPlanner.preparePartitions(1, 9, table, List.of(partition));
                Assertions.assertEquals(620, partition.getSequenceRetentionPlanVersion());
            }
        } finally {
            Config.cloud_unique_id = original;
        }
    }

    @Test
    public void testCloudPlanUsesBatchVersionReadAndSavedSnapshot() throws Exception {
        String original = Config.cloud_unique_id;
        String originalEndpoint = Config.meta_service_endpoint;
        Config.cloud_unique_id = "test_cloud_unique_id";
        Config.meta_service_endpoint = "127.0.0.1:20121";
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<VersionHelper> versions = Mockito.mockStatic(VersionHelper.class);
                MockedStatic<MetaServiceProxy> services = Mockito.mockStatic(MetaServiceProxy.class)) {
            OlapTable table = table(new SinglePartitionInfo());
            for (long id : List.of(22L, 23L)) {
                table.addPartition(new CloudPartition(id, "p" + id, new MaterializedIndex(),
                        new RandomDistributionInfo(1), 1, table.getId()));
            }
            InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
            env.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            env.when(Env::getCurrentGlobalTransactionMgr).thenReturn(new CloudGlobalTransactionMgr());
            MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(table, "rwLock");
            Cloud.MetaServiceResponseStatus ok = Cloud.MetaServiceResponseStatus.newBuilder()
                    .setCode(Cloud.MetaServiceCode.OK).build();
            versions.when(() -> VersionHelper.getVersionFromMeta(Mockito.any(), Mockito.anyInt()))
                    .thenAnswer(invocation -> {
                        Cloud.GetVersionRequest request = invocation.getArgument(0);
                        Assertions.assertEquals(0, lock.getReadHoldCount());
                        Assertions.assertTrue(request.getBatchMode());
                        Assertions.assertTrue(request.getWaitForPendingTxn());
                        Assertions.assertEquals(List.of(22L, 23L), request.getPartitionIdsList());
                        return Cloud.GetVersionResponse.newBuilder().setStatus(ok).addVersions(650)
                                .addVersions(-1).build();
                    });
            MetaServiceProxy service = Mockito.mock(MetaServiceProxy.class);
            services.when(MetaServiceProxy::getInstance).thenReturn(service);
            Mockito.when(service.recordSequenceRetentionPlan(Mockito.any())).thenAnswer(invocation -> {
                Cloud.RecordSequenceRetentionPlanRequest request = invocation.getArgument(0);
                Assertions.assertEquals(9, request.getTxnId());
                Assertions.assertEquals(Map.of(22L, 650L, 23L, Partition.PARTITION_INIT_VERSION),
                        request.getPlan().getPartitionVersionsMap());
                return Cloud.RecordSequenceRetentionPlanResponse.newBuilder().setStatus(ok)
                        .setPlan(request.getPlan().toBuilder().putPartitionVersions(22, 620)).build();
            });
            TOlapTablePartition first = partition();
            TOlapTablePartition second = new TOlapTablePartition().setId(23);
            SequenceRetentionPlanner.preparePartitions(1, 9, table, List.of(first, second));
            Assertions.assertEquals(620, first.getSequenceRetentionPlanVersion());
            Assertions.assertEquals(Partition.PARTITION_INIT_VERSION, second.getSequenceRetentionPlanVersion());
            Mockito.verify(service).recordSequenceRetentionPlan(Mockito.any());
            Mockito.verifyNoMoreInteractions(service);
        } finally {
            Config.cloud_unique_id = original;
            Config.meta_service_endpoint = originalEndpoint;
        }
    }

    @Test
    public void testAutomaticDummyPartitionDoesNotCapture() throws Exception {
        PartitionInfo info = Mockito.mock(PartitionInfo.class);
        Mockito.when(info.enableAutomaticPartition()).thenReturn(true);
        OlapTable table = table(info);
        TOlapTablePartition dummy = new TOlapTablePartition().setId(0);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            SequenceRetentionPlanner.preparePartitions(1, 9, table, List.of(dummy));
            env.verifyNoInteractions();
        }
        Assertions.assertFalse(dummy.isSetSequenceRetentionPlanVersion());
    }
}
