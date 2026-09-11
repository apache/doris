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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.LocalTabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.master.ReportHandler;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TPublishVersionRequest;
import org.apache.doris.thrift.TRowBinlogWriteColumnMappings;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.PartitionCommitInfo;
import org.apache.doris.transaction.PublishVersionDaemon;
import org.apache.doris.transaction.TableCommitInfo;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.ImmutableMap;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Regression tests for the invariant: PublishVersionTask.getSuccTablets() must never return null,
 * so that DatabaseTransactionMgr.checkReplicaContinuousVersionSucc cannot NPE when a task is
 * force-finished without a real BE response (AgentTaskCleanupDaemon path) or finished from a
 * non-OK BE callback (MasterImpl.finishPublishVersion path).
 */
public class PublishVersionTaskTest {

    @Test
    public void testReportRetainsSnapshotAfterSubtransactionAliasRemoved() throws Exception {
        TransactionState state = GsonUtils.GSON.fromJson("{\"dbId\":1,\"txnId\":100,\"rowBinlogMappings\":{"
                + "\"101\":{\"10\":{\"historical\":false,\"columns\":[{\"source\":1,\"current\":31}]}}}}",
                TransactionState.class);
        Deencapsulation.setField(state, "subTxnIds", Arrays.asList(100L, 101L));
        TableCommitInfo table = new TableCommitInfo(123L);
        PartitionCommitInfo partition = new PartitionCommitInfo();
        Deencapsulation.setField(partition, "partitionId", 2L);
        partition.setVersion(3L);
        table.addPartitionCommitInfo(partition);
        state.getSubTxnIdToTableCommitInfo().put(101L, table);
        Map<Long, Pair<TransactionState, Set<TPartitionVersionInfo>>> classified = new HashMap<>();
        TabletMeta tablet = new TabletMeta(1L, 123L, 2L, 10L, 1, TStorageMedium.HDD, false);
        LocalTabletInvertedIndex index = new LocalTabletInvertedIndex();
        Deencapsulation.invoke(index, "publishPartition", state, 101L, tablet, 2L, classified);
        Deencapsulation.invoke(index, "publishPartition", state, 101L, tablet, 2L, classified);
        Assertions.assertEquals(1, classified.size());
        Assertions.assertEquals(1, classified.get(101L).second.size());
        long backendId = 10002L;
        // Becoming VISIBLE removes the subtransaction alias, not the parent state's snapshot.
        GlobalTransactionMgrIface manager = Mockito.mock(GlobalTransactionMgrIface.class);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
            env.when(Env::getCurrentGlobalTransactionMgr).thenReturn(manager);
            Deencapsulation.invoke(ReportHandler.class, "handleRepublishVersionInfo", classified, backendId);
            PublishVersionTask task = (PublishVersionTask) AgentTaskQueue.getTask(
                    backendId, TTaskType.PUBLISH_VERSION, 101L);
            Assertions.assertNotNull(task);
            assertWireMapping(task, 101L, 31, false);
        } finally {
            AgentTaskQueue.removeTask(backendId, TTaskType.PUBLISH_VERSION, 101L);
        }
    }

    @Test
    public void testNormalAndReportPublishUseReplayedSubtransactionSnapshots() throws Exception {
        TransactionState state = GsonUtils.GSON.fromJson("{\"dbId\":1,\"txnId\":100,\"rowBinlogMappings\":{"
                + "\"100\":{\"10\":{\"historical\":true,\"columns\":[{\"source\":1,\"current\":11}]}},"
                + "\"101\":{\"10\":{\"historical\":false,\"columns\":[{\"source\":1,\"current\":31}]}}}}",
                TransactionState.class);
        long backendId = 10001L;
        TPartitionVersionInfo version = new TPartitionVersionInfo(2L, 3L, 0L);
        try {
            AgentBatchTask batch = new AgentBatchTask();
            Deencapsulation.invoke(new PublishVersionDaemon(), "addPublishVersionTask",
                    Collections.singleton(backendId), 100L, state, Collections.singletonList(version),
                    Collections.emptyMap(), 0L, batch);
            Assertions.assertEquals(1, batch.getAllTasks().size());
            assertWireMapping((PublishVersionTask) batch.getAllTasks().get(0), 100L, 11, true);

            try (MockedStatic<AgentTaskExecutor> executor = Mockito.mockStatic(AgentTaskExecutor.class)) {
                Deencapsulation.invoke(ReportHandler.class, "handleRepublishVersionInfo",
                        ImmutableMap.of(101L, Pair.of(state, Collections.singleton(version))), backendId);
                PublishVersionTask republish = (PublishVersionTask) AgentTaskQueue.getTask(
                        backendId, TTaskType.PUBLISH_VERSION, 101L);
                Assertions.assertNotNull(republish);
                assertWireMapping(republish, 101L, 31, false);
            }
        } finally {
            AgentTaskQueue.removeTask(backendId, TTaskType.PUBLISH_VERSION, 100L);
            AgentTaskQueue.removeTask(backendId, TTaskType.PUBLISH_VERSION, 101L);
        }
    }

    private static void assertWireMapping(PublishVersionTask task, long txnId, int current, boolean historical)
            throws Exception {
        TPublishVersionRequest request = new TPublishVersionRequest();
        new TDeserializer().deserialize(request, new TSerializer().serialize(task.toThrift()));
        Assertions.assertEquals(txnId, request.getTransactionId());
        Assertions.assertEquals(1, request.getRowBinlogColumnMappingsSize());
        TRowBinlogWriteColumnMappings mapping = request.getRowBinlogColumnMappings().get(10L);
        Assertions.assertTrue(mapping.isSetNeedHistoricalValue());
        Assertions.assertEquals(historical, mapping.isNeedHistoricalValue());
        Assertions.assertEquals(1, mapping.getEntriesSize());
        Assertions.assertEquals(1, mapping.getEntries().get(0).getSourceColumnUniqueId());
        Assertions.assertEquals(current, mapping.getEntries().get(0).getCurrentColumnUniqueId());
        Assertions.assertFalse(mapping.getEntries().get(0).isSetBeforeColumnUniqueId());
    }

    private PublishVersionTask newTask() {
        return new PublishVersionTask(
                /* backendId   */ 10001L,
                /* transactionId*/ 99L,
                /* dbId        */ 1L,
                /* partitionVersionInfos */ null,
                /* createTime  */ System.currentTimeMillis());
    }

    /** Default constructor must yield a non-null succTablets. */
    @Test
    public void testDefaultSuccTabletsIsNotNull() {
        PublishVersionTask task = newTask();
        Assertions.assertNotNull(task.getSuccTablets(), "succTablets must be non-null right after construction");
        Assertions.assertTrue(task.getSuccTablets().isEmpty(), "succTablets must start empty");
        // Should not NPE.
        Assertions.assertFalse(task.getSuccTablets().containsKey(1L));
    }

    /** setSuccTablets(null) must coerce to an empty map, not store null. */
    @Test
    public void testSetSuccTabletsNullCoercesToEmptyMap() {
        PublishVersionTask task = newTask();
        task.setSuccTablets(null);
        Assertions.assertNotNull(task.getSuccTablets());
        Assertions.assertTrue(task.getSuccTablets().isEmpty());
        Assertions.assertFalse(task.getSuccTablets().containsKey(123L));
    }

    /** A populated map must be returned as-is by the getter. */
    @Test
    public void testSetSuccTabletsKeepsValues() {
        PublishVersionTask task = newTask();
        Map<Long, Long> populated = ImmutableMap.of(1L, 100L, 2L, 200L);
        task.setSuccTablets(populated);
        Assertions.assertEquals(populated, task.getSuccTablets());
        Assertions.assertTrue(task.getSuccTablets().containsKey(1L));
    }

    /**
     * Simulate AgentTaskCleanupDaemon.removeInactiveBeAgentTasks: the daemon flips isFinished to
     * true on every queued PublishVersionTask without ever calling setSuccTablets. Pre-fix this
     * left succTablets at the constructor's null and any caller of getSuccTablets() NPE'd.
     * After the fix, succTablets is a non-null empty map and downstream checks see
     * "no tablet succeeded" instead of crashing.
     */
    @Test
    public void testForceFinishWithoutSetSuccTabletsDoesNotNpe() {
        PublishVersionTask task = newTask();
        task.setFinished(true);
        // No setSuccTablets call — this is the AgentTaskCleanupDaemon code path.
        Map<Long, Long> succ = task.getSuccTablets();
        Assertions.assertNotNull(succ, "getSuccTablets() must not return null even when force-finished");
        Assertions.assertTrue(task.isFinished());
        Assertions.assertFalse(succ.containsKey(42L));
    }

    /**
     * Simulate MasterImpl.finishPublishVersion on a non-OK BE response that does not set the
     * succTablets field on the Thrift request. Pre-fix this stored null on the task; after the
     * fix it stores an empty map.
     */
    @Test
    public void testFinishPublishVersionPathWithNullSuccTablets() {
        PublishVersionTask task = newTask();
        task.setSuccTablets(null);     // emulates request.isSetSuccTablets() == false
        task.setFinished(true);        // matches MasterImpl ordering
        Map<Long, Long> succ = task.getSuccTablets();
        Assertions.assertNotNull(succ);
        Assertions.assertEquals(Collections.emptyMap(), succ);
        // The exact line that crashed pre-fix at DatabaseTransactionMgr.java:1478.
        Assertions.assertFalse(succ.containsKey(7L));
    }
}
