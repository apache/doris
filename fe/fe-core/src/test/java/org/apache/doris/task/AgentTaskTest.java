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

package org.apache.doris.task;

import org.apache.doris.alter.AlterJobV2;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.binlog.BinlogTestUtils;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.thrift.TAgentTaskRequest;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TEncryptionAlgorithm;
import org.apache.doris.thrift.TRemoteTabletSnapshot;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletRole;
import org.apache.doris.thrift.TTabletType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class AgentTaskTest {

    private AgentBatchTask agentBatchTask;

    private long backendId1 = 1000L;
    private long backendId2 = 1001L;

    private long dbId = 10000L;
    private long tableId = 20000L;
    private long partitionId = 20000L;
    private long indexId1 = 30000L;
    private long indexId2 = 30001L;

    private long tabletId1 = 40000L;
    private long tabletId2 = 40001L;

    private long replicaId1 = 50000L;
    private long replicaId2 = 50001L;

    private short shortKeyNum = (short) 2;
    private int schemaHash1 = 60000;
    private int schemaHash2 = 60001;
    private long version = 1L;

    private TStorageType storageType = TStorageType.COLUMN;
    private long rowStorePageSize = 16384L;
    private long storagePageSize = 65536L;

    private long storageDictPageSize = 262144L;

    private List<Column> columns;
    private MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<Long, Long>(3);

    private Range<PartitionKey> range1;
    private Range<PartitionKey> range2;
    private Map<Object, Object> objectPool;

    private AgentTask createReplicaTask;
    private AgentTask dropTask;
    private AgentTask cloneTask;
    private AgentTask cancelDeleteTask;
    private AgentTask storageMediaMigrationTask;

    @BeforeEach
    public void setUp() throws AnalysisException {
        MetricRepo.init();
        agentBatchTask = new AgentBatchTask();

        columns = new LinkedList<Column>();
        columns.add(new Column("k1", ScalarType.createType(PrimitiveType.INT), false, null, "1", ""));
        columns.add(new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.SUM, "1", ""));

        PartitionKey pk1 = PartitionKey.createInfinityPartitionKey(Arrays.asList(columns.get(0)), false);
        PartitionKey pk2 = PartitionKey.createPartitionKey(
                Arrays.asList(new PartitionValue("10")), Arrays.asList(columns.get(0)));
        range1 = Range.closedOpen(pk1, pk2);

        PartitionKey pk3 = PartitionKey.createInfinityPartitionKey(Arrays.asList(columns.get(0)), true);
        range2 = Range.closedOpen(pk2, pk3);

        // create tasks
        objectPool = new HashMap<Object, Object>();
        // create
        createReplicaTask = new CreateReplicaTask(backendId1, dbId, tableId, partitionId,
                indexId1, tabletId1, replicaId1, shortKeyNum, schemaHash1, version, KeysType.AGG_KEYS, storageType,
                TStorageMedium.SSD, columns, null, 0, latch, null, false, TTabletType.TABLET_TYPE_DISK, null,
                TCompressionType.LZ4F, false, "", false, false, "", 0, 0, 0, 0, 0, false, null, null, objectPool, rowStorePageSize, false,
                storagePageSize, TEncryptionAlgorithm.PLAINTEXT, storageDictPageSize, new HashMap<>(), 5, -1,
                Optional.empty());

        // drop
        dropTask = new DropReplicaTask(backendId1, tabletId1, replicaId1, schemaHash1, false);

        // clone
        cloneTask =
                new CloneTask(new TBackend("host2", 8290, 8390), backendId1, dbId, tableId, partitionId,
                        indexId1, tabletId1, replicaId1, schemaHash1,
                        Arrays.asList(new TBackend("host1", 8290, 8390)), TStorageMedium.HDD, -1, 3600);

        // storageMediaMigrationTask
        storageMediaMigrationTask =
                new StorageMediaMigrationTask(backendId1, tabletId1, schemaHash1, TStorageMedium.HDD);
        ((StorageMediaMigrationTask) storageMediaMigrationTask).setDataDir("/home/a");
    }

    @Test
    public void addTaskTest() {
        // add null
        agentBatchTask.addTask(null);
        Assertions.assertEquals(0, agentBatchTask.getTaskNum());

        // normal
        agentBatchTask.addTask(createReplicaTask);
        Assertions.assertEquals(1, agentBatchTask.getTaskNum());

        List<AgentTask> allTasks = agentBatchTask.getAllTasks();
        Assertions.assertEquals(1, allTasks.size());

        for (AgentTask agentTask : allTasks) {
            if (agentTask instanceof CreateReplicaTask) {
                Assertions.assertEquals(createReplicaTask, agentTask);
            } else {
                Assertions.fail();
            }
        }
    }

    @Test
    public void rowTtlTaskClassificationTest() {
        Column ttlColumn = new Column(Column.TTL_COL, ScalarType.createDatetimeV2Type(6),
                false, AggregateType.NONE, true, "row ttl", false);
        AlterReplicaTask alterReplicaTask = new AlterReplicaTask(
                backendId1, dbId, tableId, partitionId, indexId1, indexId2, tabletId1,
                tabletId2, replicaId1, schemaHash1, schemaHash2, version, 1,
                AlterJobV2.JobType.SCHEMA_CHANGE, null, null, List.of(ttlColumn),
                new HashMap<>(), null, 0, "", null, null);
        CloneTask rowTtlCloneTask = new CloneTask(
                new TBackend("host2", 8290, 8390), backendId1, dbId, tableId, partitionId,
                indexId1, tabletId1, replicaId1, schemaHash1,
                List.of(new TBackend("host1", 8290, 8390)), TStorageMedium.HDD, -1, 3600, true);
        DownloadTask rowTtlDownloadTask = new DownloadTask(
                null, backendId1, 1, 2, dbId, List.<TRemoteTabletSnapshot>of(), true);
        DirMoveTask rowTtlDirMoveTask = new DirMoveTask(
                null, backendId1, 1, 2, dbId, tableId, partitionId, indexId1,
                tabletId1, "/snapshot", schemaHash1, true, true);

        Assertions.assertTrue(alterReplicaTask.isRowTtlTask());
        Assertions.assertTrue(rowTtlCloneTask.isRowTtlTask());
        Assertions.assertTrue(rowTtlDownloadTask.isRowTtlTask());
        Assertions.assertTrue(rowTtlDirMoveTask.isRowTtlTask());
        Assertions.assertTrue(AgentBatchTask.isRowTtlTask(rowTtlCloneTask));
        Assertions.assertFalse(cloneTask.isRowTtlTask());
        Assertions.assertFalse(dropTask.isRowTtlTask());
    }

    @Test
    public void toThriftTest() throws Exception {
        Class<? extends AgentBatchTask> agentBatchTaskClass = agentBatchTask.getClass();
        Class[] typeParams = new Class[] { AgentTask.class };
        Method toAgentTaskRequest = agentBatchTaskClass.getDeclaredMethod("toAgentTaskRequest", typeParams);
        toAgentTaskRequest.setAccessible(true);

        // create
        TAgentTaskRequest request = (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, createReplicaTask);
        Assertions.assertEquals(TTaskType.CREATE, request.getTaskType());
        Assertions.assertEquals(createReplicaTask.getSignature(), request.getSignature());
        Assertions.assertNotNull(request.getCreateTabletReq());

        List<Column> rowTtlColumns = new LinkedList<>();
        rowTtlColumns.add(new Column("k1", ScalarType.createType(PrimitiveType.INT), true,
                null, false, null, ""));
        rowTtlColumns.add(new Column("event_time", ScalarType.createDatetimeV2Type(6),
                false, AggregateType.NONE, true, null, ""));
        rowTtlColumns.add(new Column(Column.TTL_COL, ScalarType.createDatetimeV2Type(6),
                false, AggregateType.NONE, true, "row ttl", false));
        AgentTask createWithRowTtl = new CreateReplicaTask(
                backendId1, dbId, tableId, partitionId, indexId1, tabletId1, replicaId1,
                shortKeyNum, schemaHash1, version, KeysType.DUP_KEYS, storageType,
                TStorageMedium.SSD, rowTtlColumns, null, 0, latch, null, false,
                TTabletType.TABLET_TYPE_DISK, null, TCompressionType.LZ4F, false, "", false,
                false, "", 0, 0, 0, 0, 0, false, null, null, new HashMap<>(), rowStorePageSize,
                false, storagePageSize, TEncryptionAlgorithm.PLAINTEXT, storageDictPageSize,
                new HashMap<>(), 5, 86_400_000_000L, Optional.of(28_800));
        TAgentTaskRequest requestWithRowTtl =
                (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, createWithRowTtl);
        Assertions.assertEquals(2,
                requestWithRowTtl.getCreateTabletReq().getTabletSchema().getTtlColIdx());
        Assertions.assertEquals(86_400_000_000L,
                requestWithRowTtl.getCreateTabletReq().getTabletSchema().getRowTtlDurationUs());
        Assertions.assertEquals(28_800,
                requestWithRowTtl.getCreateTabletReq().getTabletSchema().getRowTtlTimeZoneOffsetSeconds());
        Assertions.assertFalse(requestWithRowTtl.getCreateTabletReq().getTabletSchema()
                .getColumns().get(2).isVisible());

        // create with row binlog tablet
        BinlogConfig binlogConfig = BinlogTestUtils.newTestRowBinlogConfig(true, false);
        CreateReplicaTask createWithRowBinlog = new CreateReplicaTask(backendId1, dbId, tableId, partitionId,
                indexId1, tabletId1, replicaId1, shortKeyNum, schemaHash1, version, KeysType.AGG_KEYS, storageType,
                TStorageMedium.SSD, columns, null, 0, latch, null, false, TTabletType.TABLET_TYPE_DISK, null,
                TCompressionType.LZ4F, false, "", false, false, "", 0, 0, 0, 0, 0, false,
                binlogConfig, null, objectPool, rowStorePageSize, false, storagePageSize,
                TEncryptionAlgorithm.PLAINTEXT, storageDictPageSize, new HashMap<>(), 5, 86_400_000_000L,
                Optional.empty());
        createWithRowBinlog.setTabletRole(TTabletRole.TABLET_ROLE_ROW_BINLOG);
        TAgentTaskRequest requestWithRowBinlog =
                (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, createWithRowBinlog);
        Assertions.assertNotNull(requestWithRowBinlog.getCreateTabletReq());
        Assertions.assertEquals(TTabletRole.TABLET_ROLE_ROW_BINLOG,
                requestWithRowBinlog.getCreateTabletReq().getTabletRole());
        Assertions.assertEquals(-1,
                requestWithRowBinlog.getCreateTabletReq().getTabletSchema().getRowTtlDurationUs());

        List<Index> bfIndexes = Arrays.asList(new Index(1L, "bf_k1", Arrays.asList("k1"),
                IndexType.BLOOMFILTER, Map.of("bloom_filter_fpp", "0.02"), ""));
        AgentTask createWithBfIndex = new CreateReplicaTask(backendId1, dbId, tableId, partitionId,
                indexId1, tabletId1, replicaId1, shortKeyNum, schemaHash1, version, KeysType.AGG_KEYS, storageType,
                TStorageMedium.SSD, columns, null, 0, latch, bfIndexes, false,
                TTabletType.TABLET_TYPE_DISK, null, TCompressionType.LZ4F, false, "", false, false, "", 0, 0, 0,
                0, 0, false, null, null, new HashMap<>(), rowStorePageSize, false, storagePageSize,
                TEncryptionAlgorithm.PLAINTEXT, storageDictPageSize, new HashMap<>(), 5, -1, Optional.empty());
        TAgentTaskRequest requestWithBfIndex =
                (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, createWithBfIndex);
        Assertions.assertNotNull(requestWithBfIndex.getCreateTabletReq());
        Assertions.assertTrue(requestWithBfIndex.getCreateTabletReq().getTabletSchema()
                .getColumns().get(0).isIsBloomFilterColumn());
        Assertions.assertFalse(requestWithBfIndex.getCreateTabletReq().getTabletSchema()
                .getColumns().get(1).isSetIsBloomFilterColumn());
        // bfColumns is null, so table-level FPP is not set for BfIndex-only tables.
        // Each BfIndex carries its own FPP in its properties.
        Assertions.assertFalse(requestWithBfIndex.getCreateTabletReq().getTabletSchema().isSetBloomFilterFpp());
        Assertions.assertTrue(requestWithBfIndex.getCreateTabletReq().getTabletSchema()
                .getIndexes().get(0).getProperties().containsKey("bloom_filter_fpp"));
        Assertions.assertEquals("0.02", requestWithBfIndex.getCreateTabletReq().getTabletSchema()
                .getIndexes().get(0).getProperties().get("bloom_filter_fpp"));

        Set<String> bfColumns = new HashSet<>();
        bfColumns.add("k1");
        AgentTask createWithBfColumns = new CreateReplicaTask(backendId1, dbId, tableId, partitionId,
                indexId1, tabletId1, replicaId1, shortKeyNum, schemaHash1, version, KeysType.AGG_KEYS, storageType,
                TStorageMedium.SSD, columns, bfColumns, 0.02, latch, null, false,
                TTabletType.TABLET_TYPE_DISK, null, TCompressionType.LZ4F, false, "", false, false, "", 0, 0, 0,
                0, 0, false, null, null, new HashMap<>(), rowStorePageSize, false, storagePageSize,
                TEncryptionAlgorithm.PLAINTEXT, storageDictPageSize, new HashMap<>(), 5, -1, Optional.empty());
        TAgentTaskRequest requestWithBfColumns =
                (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, createWithBfColumns);
        Assertions.assertNotNull(requestWithBfColumns.getCreateTabletReq());
        Assertions.assertTrue(requestWithBfColumns.getCreateTabletReq().getTabletSchema()
                .getColumns().get(0).isIsBloomFilterColumn());
        Assertions.assertEquals(0.02,
                requestWithBfColumns.getCreateTabletReq().getTabletSchema().getBloomFilterFpp(), 0);

        List<Column> shadowColumns = Arrays.asList(
                new Column(Column.SHADOW_NAME_PREFIX + "k1", ScalarType.createType(PrimitiveType.INT),
                        false, null, "1", ""),
                new Column("v1", ScalarType.createType(PrimitiveType.INT), false, AggregateType.SUM, "1", ""));
        AgentTask createWithShadowBfIndex = new CreateReplicaTask(backendId1, dbId, tableId, partitionId,
                indexId1, tabletId1, replicaId1, shortKeyNum, schemaHash1, version, KeysType.AGG_KEYS, storageType,
                TStorageMedium.SSD, shadowColumns, null, 0, latch, bfIndexes, false,
                TTabletType.TABLET_TYPE_DISK, null, TCompressionType.LZ4F, false, "", false, false, "", 0, 0, 0,
                0, 0, false, null, null, new HashMap<>(), rowStorePageSize, false, storagePageSize,
                TEncryptionAlgorithm.PLAINTEXT, storageDictPageSize, new HashMap<>(), 5, -1, Optional.empty());
        TAgentTaskRequest requestWithShadowBfIndex =
                (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, createWithShadowBfIndex);
        Assertions.assertEquals("k1", requestWithShadowBfIndex.getCreateTabletReq().getTabletSchema()
                .getColumns().get(0).getColumnName());
        Assertions.assertTrue(requestWithShadowBfIndex.getCreateTabletReq().getTabletSchema()
                .getColumns().get(0).isIsBloomFilterColumn());

        AgentTask createWithFoldedBfIndex = new CreateReplicaTask(backendId1, dbId, tableId, partitionId,
                indexId1, tabletId1, replicaId1, shortKeyNum, schemaHash1, version, KeysType.AGG_KEYS, storageType,
                TStorageMedium.SSD, shadowColumns, null, 0, latch,
                Arrays.asList(new Index(2L, "bf_shadow_k1", Arrays.asList("k1"),
                        IndexType.BLOOMFILTER, Map.of("bloom_filter_fpp", "0.03"), "")), false,
                TTabletType.TABLET_TYPE_DISK, null, TCompressionType.LZ4F, false, "", false, false, "", 0, 0, 0,
                0, 0, false, null, null, new HashMap<>(), rowStorePageSize, false, storagePageSize,
                TEncryptionAlgorithm.PLAINTEXT, storageDictPageSize, new HashMap<>(), 5, -1, Optional.empty());
        TAgentTaskRequest requestWithFoldedBfIndex =
                (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, createWithFoldedBfIndex);
        Assertions.assertEquals("k1", requestWithFoldedBfIndex.getCreateTabletReq().getTabletSchema()
                .getColumns().get(0).getColumnName());
        Assertions.assertTrue(requestWithFoldedBfIndex.getCreateTabletReq().getTabletSchema()
                .getColumns().get(0).isIsBloomFilterColumn());
        // bfColumns is null, so table-level FPP is not set. BfIndexes carry their own FPP.
        Assertions.assertFalse(requestWithFoldedBfIndex.getCreateTabletReq().getTabletSchema().isSetBloomFilterFpp());
        Assertions.assertTrue(requestWithFoldedBfIndex.getCreateTabletReq().getTabletSchema().isSetIndexes());
        Assertions.assertEquals("0.03", requestWithFoldedBfIndex.getCreateTabletReq().getTabletSchema()
                .getIndexes().get(0).getProperties().get("bloom_filter_fpp"));

        Set<String> emptyBfColumns = new HashSet<>();
        // Empty bfColumns do not define a table-level bloom filter, so FPP is not set.
        // BfIndexes carry their own per-index FPP via index properties.
        AgentTask createWithEmptyBfColumnsAndBfIndex = new CreateReplicaTask(backendId1, dbId, tableId,
                partitionId, indexId1, tabletId1, replicaId1, shortKeyNum, schemaHash1, version, KeysType.AGG_KEYS,
                storageType, TStorageMedium.SSD, columns, emptyBfColumns, 0, latch,
                bfIndexes, false, TTabletType.TABLET_TYPE_DISK, null, TCompressionType.LZ4F, false,
                "", false, false, "", 0, 0, 0, 0, 0, false, null, null, new HashMap<>(), rowStorePageSize, false,
                storagePageSize, TEncryptionAlgorithm.PLAINTEXT, storageDictPageSize, new HashMap<>(), 5, -1,
                Optional.empty());
        TAgentTaskRequest requestWithEmptyBfColumnsAndBfIndex =
                (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask,
                        createWithEmptyBfColumnsAndBfIndex);
        Assertions.assertTrue(requestWithEmptyBfColumnsAndBfIndex.getCreateTabletReq().getTabletSchema()
                .getColumns().get(0).isIsBloomFilterColumn());
        Assertions.assertFalse(requestWithEmptyBfColumnsAndBfIndex.getCreateTabletReq().getTabletSchema()
                .isSetBloomFilterFpp());

        // drop
        TAgentTaskRequest request2 = (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, dropTask);
        Assertions.assertEquals(TTaskType.DROP, request2.getTaskType());
        Assertions.assertEquals(dropTask.getSignature(), request2.getSignature());
        Assertions.assertNotNull(request2.getDropTabletReq());

        // clone
        TAgentTaskRequest request4 = (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, cloneTask);
        Assertions.assertEquals(TTaskType.CLONE, request4.getTaskType());
        Assertions.assertEquals(cloneTask.getSignature(), request4.getSignature());
        Assertions.assertNotNull(request4.getCloneReq());

        // storageMediaMigrationTask
        TAgentTaskRequest request7 =
                (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, storageMediaMigrationTask);
        Assertions.assertEquals(TTaskType.STORAGE_MEDIUM_MIGRATE, request7.getTaskType());
        Assertions.assertEquals(storageMediaMigrationTask.getSignature(), request7.getSignature());
        Assertions.assertNotNull(request7.getStorageMediumMigrateReq());
        Assertions.assertTrue(request7.getStorageMediumMigrateReq().isSetDataDir());
        Assertions.assertEquals(request7.getStorageMediumMigrateReq().getDataDir(), "/home/a");
    }

    @Test
    public void agentTaskQueueTest() {
        AgentTaskQueue.clearAllTasks();
        Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());

        // add
        AgentTaskQueue.addTask(createReplicaTask);
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());
        Assertions.assertFalse(AgentTaskQueue.addTask(createReplicaTask));

        // get
        AgentTask task = AgentTaskQueue.getTask(backendId1, TTaskType.CREATE, createReplicaTask.getSignature());
        Assertions.assertEquals(createReplicaTask, task);

        Map<TTaskType, Set<Long>> runningTasks = new HashMap<TTaskType, Set<Long>>();
        List<AgentTask> diffTasks = AgentTaskQueue.getDiffTasks(backendId1, runningTasks);
        Assertions.assertEquals(1, diffTasks.size());

        Set<Long> set = new HashSet<Long>();
        set.add(createReplicaTask.getSignature());
        runningTasks.put(TTaskType.CREATE, set);
        diffTasks = AgentTaskQueue.getDiffTasks(backendId1, runningTasks);
        Assertions.assertEquals(0, diffTasks.size());

        // remove
        AgentTaskQueue.removeTask(backendId1, TTaskType.CREATE, createReplicaTask.getSignature());
        Assertions.assertEquals(0, AgentTaskQueue.getTaskNum());
    }

    @Test
    public void failedAgentTaskTest() {
        AgentTaskQueue.clearAllTasks();

        AgentTaskQueue.addTask(dropTask);
        Assertions.assertEquals(0, dropTask.getFailedTimes());
        dropTask.failed();
        Assertions.assertEquals(1, dropTask.getFailedTimes());

        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum());
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum(backendId1, TTaskType.DROP, false));
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum(-1, TTaskType.DROP, false));
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum(backendId1, TTaskType.DROP, true));

        dropTask.failed();
        DropReplicaTask dropTask2 = new DropReplicaTask(backendId2, tabletId1, replicaId1, schemaHash1, false);
        AgentTaskQueue.addTask(dropTask2);
        dropTask2.failed();
        Assertions.assertEquals(1, AgentTaskQueue.getTaskNum(backendId1, TTaskType.DROP, true));
        Assertions.assertEquals(2, AgentTaskQueue.getTaskNum(-1, TTaskType.DROP, true));
    }
}
