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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.thrift.TAgentTaskRequest;
import org.apache.doris.thrift.TBackend;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Range;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    private List<Column> columns;
    private MarkedCountDownLatch<Long, Long> latch = new MarkedCountDownLatch<Long, Long>(3);

    private Range<PartitionKey> range1;
    private Range<PartitionKey> range2;

    private AgentTask createReplicaTask;
    private AgentTask dropTask;
    private AgentTask cloneTask;
    private AgentTask cancelDeleteTask;
    private AgentTask storageMediaMigrationTask;

    @Before
    public void setUp() throws AnalysisException {
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
        Map<Object, Object> objectPool = new HashMap<Object, Object>();
        // create
        createReplicaTask = new CreateReplicaTask(backendId1, dbId, tableId, partitionId,
                indexId1, tabletId1, replicaId1, shortKeyNum, schemaHash1, version, KeysType.AGG_KEYS, storageType,
                TStorageMedium.SSD, columns, null, 0, latch, null, false, TTabletType.TABLET_TYPE_DISK, null,
                TCompressionType.LZ4F, false, "", false, false, false, "", 0, 0, 0, 0, 0, false, null, null, objectPool, rowStorePageSize);

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
        Assert.assertEquals(0, agentBatchTask.getTaskNum());

        // normal
        agentBatchTask.addTask(createReplicaTask);
        Assert.assertEquals(1, agentBatchTask.getTaskNum());

        List<AgentTask> allTasks = agentBatchTask.getAllTasks();
        Assert.assertEquals(1, allTasks.size());

        for (AgentTask agentTask : allTasks) {
            if (agentTask instanceof CreateReplicaTask) {
                Assert.assertEquals(createReplicaTask, agentTask);
            } else {
                Assert.fail();
            }
        }
    }

    @Test
    public void toThriftTest() throws Exception {
        Class<? extends AgentBatchTask> agentBatchTaskClass = agentBatchTask.getClass();
        Class[] typeParams = new Class[] { AgentTask.class };
        Method toAgentTaskRequest = agentBatchTaskClass.getDeclaredMethod("toAgentTaskRequest", typeParams);
        toAgentTaskRequest.setAccessible(true);

        // create
        TAgentTaskRequest request = (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, createReplicaTask);
        Assert.assertEquals(TTaskType.CREATE, request.getTaskType());
        Assert.assertEquals(createReplicaTask.getSignature(), request.getSignature());
        Assert.assertNotNull(request.getCreateTabletReq());

        // drop
        TAgentTaskRequest request2 = (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, dropTask);
        Assert.assertEquals(TTaskType.DROP, request2.getTaskType());
        Assert.assertEquals(dropTask.getSignature(), request2.getSignature());
        Assert.assertNotNull(request2.getDropTabletReq());

        // clone
        TAgentTaskRequest request4 = (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, cloneTask);
        Assert.assertEquals(TTaskType.CLONE, request4.getTaskType());
        Assert.assertEquals(cloneTask.getSignature(), request4.getSignature());
        Assert.assertNotNull(request4.getCloneReq());

        // storageMediaMigrationTask
        TAgentTaskRequest request7 =
                (TAgentTaskRequest) toAgentTaskRequest.invoke(agentBatchTask, storageMediaMigrationTask);
        Assert.assertEquals(TTaskType.STORAGE_MEDIUM_MIGRATE, request7.getTaskType());
        Assert.assertEquals(storageMediaMigrationTask.getSignature(), request7.getSignature());
        Assert.assertNotNull(request7.getStorageMediumMigrateReq());
        Assert.assertTrue(request7.getStorageMediumMigrateReq().isSetDataDir());
        Assert.assertEquals(request7.getStorageMediumMigrateReq().getDataDir(), "/home/a");
    }

    @Test
    public void agentTaskQueueTest() {
        AgentTaskQueue.clearAllTasks();
        Assert.assertEquals(0, AgentTaskQueue.getTaskNum());

        // add
        AgentTaskQueue.addTask(createReplicaTask);
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        Assert.assertFalse(AgentTaskQueue.addTask(createReplicaTask));

        // get
        AgentTask task = AgentTaskQueue.getTask(backendId1, TTaskType.CREATE, createReplicaTask.getSignature());
        Assert.assertEquals(createReplicaTask, task);

        Map<TTaskType, Set<Long>> runningTasks = new HashMap<TTaskType, Set<Long>>();
        List<AgentTask> diffTasks = AgentTaskQueue.getDiffTasks(backendId1, runningTasks);
        Assert.assertEquals(1, diffTasks.size());

        Set<Long> set = new HashSet<Long>();
        set.add(createReplicaTask.getSignature());
        runningTasks.put(TTaskType.CREATE, set);
        diffTasks = AgentTaskQueue.getDiffTasks(backendId1, runningTasks);
        Assert.assertEquals(0, diffTasks.size());

        // remove
        AgentTaskQueue.removeTask(backendId1, TTaskType.CREATE, createReplicaTask.getSignature());
        Assert.assertEquals(0, AgentTaskQueue.getTaskNum());
    }

    @Test
    public void failedAgentTaskTest() {
        AgentTaskQueue.clearAllTasks();

        AgentTaskQueue.addTask(dropTask);
        Assert.assertEquals(0, dropTask.getFailedTimes());
        dropTask.failed();
        Assert.assertEquals(1, dropTask.getFailedTimes());

        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum(backendId1, TTaskType.DROP, false));
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum(-1, TTaskType.DROP, false));
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum(backendId1, TTaskType.DROP, true));

        dropTask.failed();
        DropReplicaTask dropTask2 = new DropReplicaTask(backendId2, tabletId1, replicaId1, schemaHash1, false);
        AgentTaskQueue.addTask(dropTask2);
        dropTask2.failed();
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum(backendId1, TTaskType.DROP, true));
        Assert.assertEquals(2, AgentTaskQueue.getTaskNum(-1, TTaskType.DROP, true));
    }
}
