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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.RecoverInfo;
import org.apache.doris.persist.TableStreamCleanupInfo;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

public class TableStreamManagerCleanupTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;
        createDatabase("test_stream_cleanup");
        connectContext.setDatabase("test_stream_cleanup");
    }

    @Test
    public void testCleanupRemovedPartitionOffsets() throws Exception {
        StreamContext context = createStreamContext("cleanup_normal");
        long keptPartitionId = context.baseTable.getPartition("p1").getId();
        long removedPartitionId = context.baseTable.getPartition("p2").getId();
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);

        alterTableSync("alter table test_stream_cleanup." + context.baseTable.getName() + " drop partition p2");
        Env.getCurrentEnv().getTableStreamManager().cleanupStalePartitionOffsets();

        assertPartitionState(context.stream, keptPartitionId, removedPartitionId, true);
    }

    @Test
    public void testCleanupRetainsOffsetForPartitionAddedAfterSnapshot() throws Exception {
        StreamContext context = createStreamContext("cleanup_partition_race");
        String debugPointName =
                "TableStreamManager.cleanupStalePartitionOffsets.blockAfterPartitionSnapshot";
        DebugPoint debugPoint = new DebugPoint();
        debugPoint.executeLimit = Integer.MAX_VALUE;
        debugPoint.params.put("value", String.valueOf(context.stream.getId()));
        boolean debugPointsEnabled = Config.enable_debug_points;
        Config.enable_debug_points = true;
        DebugPointUtil.addDebugPoint(debugPointName, debugPoint);

        MonitoredReentrantReadWriteLock baseTableLock = Deencapsulation.getField(context.baseTable, "rwLock");
        AtomicReference<Thread> addPartitionThread = new AtomicReference<>();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> cleanup = executor.submit(
                () -> Env.getCurrentEnv().getTableStreamManager().cleanupStalePartitionOffsets());
        Future<?> addPartition = null;
        try {
            try {
                await(() -> debugPoint.executeNum.get() > 0);
                addPartition = executor.submit(() -> {
                    connectContext.setThreadLocalInfo();
                    addPartitionThread.set(Thread.currentThread());
                    alterTableSync("alter table test_stream_cleanup." + context.baseTable.getName()
                            + " add partition p3 values less than (\"300\")");
                    long partitionId = context.baseTable.getPartition("p3").getId();
                    updatePartitionOffset(context.stream, partitionId, 33L, 333L);
                    return null;
                });
                Future<?> addPartitionResult = addPartition;
                await(() -> addPartitionResult.isDone()
                        || addPartitionThread.get() != null
                        && baseTableLock.hasQueuedThread(addPartitionThread.get()));
            } finally {
                DebugPointUtil.removeDebugPoint(debugPointName);
                Config.enable_debug_points = debugPointsEnabled;
            }

            cleanup.get(10, TimeUnit.SECONDS);
            addPartition.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        long partitionId = context.baseTable.getPartition("p3").getId();
        Assertions.assertTrue(context.stream.hasConsumedData(partitionId));
    }

    @Test
    public void testCleanupJournalOrderMatchesLeaderState() throws Exception {
        StreamContext context = createStreamContext("cleanup_journal_race");
        long keptPartitionId = context.baseTable.getPartition("p1").getId();
        long removedPartitionId = context.baseTable.getPartition("p2").getId();
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);
        alterTableSync("alter table test_stream_cleanup." + context.baseTable.getName() + " drop partition p2");

        List<Object> journalOrder = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch allowUpdate = new CountDownLatch(1);
        CountDownLatch updateDone = new CountDownLatch(1);
        OlapTableStreamUpdate streamUpdate = new OlapTableStreamUpdate(
                Collections.emptyMap(), Collections.singletonMap(removedPartitionId, 44L));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> update = executor.submit(() -> {
            Assertions.assertTrue(allowUpdate.await(10, TimeUnit.SECONDS));
            context.stream.writeLock();
            try {
                journalOrder.add(streamUpdate);
                context.stream.unprotectedUpdateStreamUpdate(streamUpdate, 444L);
            } finally {
                context.stream.writeUnlock();
                updateDone.countDown();
            }
            return null;
        });

        EditLog editLog = Env.getCurrentEnv().getEditLog();
        EditLog spyEditLog = Mockito.spy(editLog);
        Mockito.doAnswer(invocation -> {
            TableStreamCleanupInfo cleanupInfo = invocation.getArgument(0);
            allowUpdate.countDown();
            if (!context.stream.isWriteLockHeldByCurrentThread()) {
                Assertions.assertTrue(updateDone.await(10, TimeUnit.SECONDS));
            }
            journalOrder.add(cleanupInfo);
            return Mockito.mock(EditLog.EditLogItem.class);
        }).when(spyEditLog).logTableStreamCleanup(Mockito.any(TableStreamCleanupInfo.class));
        Env.getCurrentEnv().setEditLog(spyEditLog);
        try {
            Env.getCurrentEnv().getTableStreamManager().cleanupStalePartitionOffsets();
            update.get(10, TimeUnit.SECONDS);
        } finally {
            Env.getCurrentEnv().setEditLog(editLog);
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        Map<Long, Long> leaderOffsets = new HashMap<>(
                Deencapsulation.getField(context.stream, "partitionOffset"));
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);
        for (Object journal : journalOrder) {
            if (journal instanceof OlapTableStreamUpdate) {
                updatePartitionOffset(context.stream, removedPartitionId, 44L, 444L);
            } else {
                Env.getCurrentEnv().getTableStreamManager()
                        .replayTableStreamCleanup((TableStreamCleanupInfo) journal);
            }
        }

        Map<Long, Long> replayedOffsets = Deencapsulation.getField(context.stream, "partitionOffset");
        Assertions.assertEquals(leaderOffsets, replayedOffsets);
    }

    @Test
    public void testDropStreamDuringCleanupReplaysDeterministically() throws Exception {
        StreamContext context = createStreamContext("cleanup_drop_race");
        long keptPartitionId = context.baseTable.getPartition("p1").getId();
        long removedPartitionId = context.baseTable.getPartition("p2").getId();
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);
        alterTableSync("alter table test_stream_cleanup." + context.baseTable.getName() + " drop partition p2");

        String debugPointName =
                "TableStreamManager.cleanupStalePartitionOffsets.blockAfterPartitionSnapshot";
        DebugPoint debugPoint = new DebugPoint();
        debugPoint.executeLimit = Integer.MAX_VALUE;
        debugPoint.params.put("value", String.valueOf(context.stream.getId()));
        boolean debugPointsEnabled = Config.enable_debug_points;
        Config.enable_debug_points = true;
        DebugPointUtil.addDebugPoint(debugPointName, debugPoint);

        List<Object> journalOrder = Collections.synchronizedList(new ArrayList<>());
        EditLog editLog = Env.getCurrentEnv().getEditLog();
        EditLog spyEditLog = Mockito.spy(editLog);
        Mockito.doAnswer(invocation -> {
            journalOrder.add(invocation.getArgument(0));
            return null;
        }).when(spyEditLog).logDropTable(Mockito.any(DropInfo.class));
        Mockito.doAnswer(invocation -> {
            journalOrder.add(invocation.getArgument(0));
            return Mockito.mock(EditLog.EditLogItem.class);
        }).when(spyEditLog).logTableStreamCleanup(Mockito.any(TableStreamCleanupInfo.class));
        Mockito.doAnswer(invocation -> {
            journalOrder.add(invocation.getArgument(0));
            return null;
        }).when(spyEditLog).logRecoverTable(Mockito.any(RecoverInfo.class));
        Env.getCurrentEnv().setEditLog(spyEditLog);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> cleanup = executor.submit(
                () -> Env.getCurrentEnv().getTableStreamManager().cleanupStalePartitionOffsets());
        try {
            try {
                await(() -> debugPoint.executeNum.get() > 0);
                Env.getCurrentInternalCatalog().dropTable(
                        "test_stream_cleanup", context.stream.getName(), false, false, true,
                        false, false, false);
            } finally {
                DebugPointUtil.removeDebugPoint(debugPointName);
                Config.enable_debug_points = debugPointsEnabled;
            }
            cleanup.get(10, TimeUnit.SECONDS);
            Env.getCurrentEnv().recoverTable(
                    "test_stream_cleanup", context.stream.getName(), "", -1L);
        } finally {
            executor.shutdownNow();
            try {
                Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            } finally {
                Env.getCurrentEnv().setEditLog(editLog);
            }
        }

        Map<Long, Long> leaderOffsets = new HashMap<>(
                Deencapsulation.getField(context.stream, "partitionOffset"));
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream_cleanup");
        for (Object journal : journalOrder) {
            if (journal instanceof DropInfo) {
                DropInfo dropInfo = (DropInfo) journal;
                Env.getCurrentEnv().replayDropTable(
                        db, dropInfo.getTableId(), dropInfo.isForceDrop(), dropInfo.getRecycleTime());
            } else if (journal instanceof TableStreamCleanupInfo) {
                Env.getCurrentEnv().getTableStreamManager()
                        .replayTableStreamCleanup((TableStreamCleanupInfo) journal);
            } else {
                Env.getCurrentEnv().replayRecoverTable((RecoverInfo) journal);
            }
        }

        Map<Long, Long> replayedOffsets = Deencapsulation.getField(context.stream, "partitionOffset");
        Assertions.assertEquals(leaderOffsets, replayedOffsets);
        Assertions.assertTrue(replayedOffsets.containsKey(removedPartitionId));
    }

    @Test
    public void testCleanupSkipsDisabledStream() throws Exception {
        StreamContext context = createStreamContext("cleanup_disabled");
        long keptPartitionId = context.baseTable.getPartition("p1").getId();
        long removedPartitionId = context.baseTable.getPartition("p2").getId();
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);

        alterTableSync("alter table test_stream_cleanup." + context.baseTable.getName() + " drop partition p2");
        context.stream.writeLock();
        try {
            context.stream.setDisabled(true);
        } finally {
            context.stream.writeUnlock();
        }
        Env.getCurrentEnv().getTableStreamManager().cleanupStalePartitionOffsets();

        assertPartitionState(context.stream, keptPartitionId, removedPartitionId, false);
    }

    @Test
    public void testCleanupSkipsStaleStream() throws Exception {
        StreamContext context = createStreamContext("cleanup_stale");
        long keptPartitionId = context.baseTable.getPartition("p1").getId();
        long removedPartitionId = context.baseTable.getPartition("p2").getId();
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);

        alterTableSync("alter table test_stream_cleanup." + context.baseTable.getName() + " drop partition p2");
        context.stream.writeLock();
        try {
            context.stream.setStale(true);
            context.stream.setStaleReason("ut");
        } finally {
            context.stream.writeUnlock();
        }
        Env.getCurrentEnv().getTableStreamManager().cleanupStalePartitionOffsets();

        assertPartitionState(context.stream, keptPartitionId, removedPartitionId, false);
    }

    @Test
    public void testReplayPrunePartitionOffsetsDirectly() throws Exception {
        StreamContext context = createStreamContext("replay_prune");
        long keptPartitionId = context.baseTable.getPartition("p1").getId();
        long removedPartitionId = context.baseTable.getPartition("p2").getId();
        setPartitionState(context.stream, keptPartitionId, removedPartitionId);

        context.stream.writeLock();
        try {
            context.stream.setDisabled(true);
        } finally {
            context.stream.writeUnlock();
        }
        Env.getCurrentEnv().getTableStreamManager().replayTableStreamCleanup(
                new TableStreamCleanupInfo(Collections.singletonList(
                        new TableStreamCleanupInfo.PartitionOffsetPruneEntry(
                                context.stream.getDatabase().getId(), context.stream.getId(),
                                Collections.singleton(removedPartitionId)))));

        assertPartitionState(context.stream, keptPartitionId, removedPartitionId, true);
    }

    @Test
    public void testReplayRemoveStaleDbAndStream() throws Exception {
        StreamContext context = createStreamContext("replay_remove");
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream_cleanup");
        long dbId = db.getId();
        long streamId = context.stream.getId();

        Assertions.assertTrue(
                Env.getCurrentEnv().getTableStreamManager().getTableStreamIds(db).contains(streamId));

        Env.getCurrentEnv().getTableStreamManager().replayTableStreamCleanup(
                new TableStreamCleanupInfo(Collections.emptyList(), Collections.emptyList(),
                        Collections.singletonList(Pair.of(dbId, streamId))));

        Assertions.assertFalse(
                Env.getCurrentEnv().getTableStreamManager().getTableStreamIds(db).contains(streamId));
    }

    @Test
    public void testGetCloudTableStreamsForBaseTable() throws Exception {
        StreamContext context = createStreamContext("cloud_identity");
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream_cleanup");

        List<Cloud.TableStreamIdentityPB> identities = Env.getCurrentEnv().getTableStreamManager()
                .getCloudTableStreamsForBaseTable(db.getId(), context.baseTable.getId());

        Assertions.assertEquals(1, identities.size());
        Cloud.TableStreamIdentityPB identity = identities.get(0);
        Assertions.assertEquals(db.getId(), identity.getBaseDbId());
        Assertions.assertEquals(context.baseTable.getId(), identity.getBaseTableId());
        Assertions.assertEquals(db.getId(), identity.getStreamDbId());
        Assertions.assertEquals(context.stream.getId(), identity.getStreamId());
    }

    private StreamContext createStreamContext(String suffix) throws Exception {
        String tableName = "tbl_" + suffix;
        String streamName = "s_" + suffix;
        createTable("create table test_stream_cleanup." + tableName + " (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "unique key(k1)\n"
                + "partition by range(k1)\n"
                + "(partition p1 values less than (\"100\"),\n"
                + " partition p2 values less than (\"200\"))\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\"=\"1\",\"binlog.enable\"=\"true\","
                + "\"binlog.format\"=\"ROW\","
                + "\"binlog.need_historical_value\"=\"true\")");
        createTable("create stream test_stream_cleanup." + streamName + " on table test_stream_cleanup." + tableName
                + " properties('type' = 'append_only', 'show_initial_rows' = 'true')");

        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException("test_stream_cleanup");
        return new StreamContext((OlapTable) db.getTableOrMetaException(tableName),
                (OlapTableStream) db.getTableOrMetaException(streamName));
    }

    private void setPartitionState(OlapTableStream stream, long keptPartitionId, long removedPartitionId) {
        Map<Long, Long> partitionOffset = new HashMap<>();
        partitionOffset.put(keptPartitionId, 11L);
        partitionOffset.put(removedPartitionId, 22L);
        Map<Long, Long> partitionConsumptionTime = new HashMap<>();
        partitionConsumptionTime.put(keptPartitionId, 111L);
        partitionConsumptionTime.put(removedPartitionId, 222L);
        Map<Long, Long> historicalPartitionTSO = new HashMap<>();
        historicalPartitionTSO.put(keptPartitionId, 1001L);
        historicalPartitionTSO.put(removedPartitionId, 2002L);
        Deencapsulation.setField(stream, "partitionOffset", partitionOffset);
        Deencapsulation.setField(stream, "partitionConsumptionTime", partitionConsumptionTime);
        Deencapsulation.setField(stream, "historicalPartitionTSO", historicalPartitionTSO);
    }

    private void assertPartitionState(OlapTableStream stream, long keptPartitionId, long removedPartitionId,
            boolean removedExpected) {
        Map<Long, Long> partitionOffset = Deencapsulation.getField(stream, "partitionOffset");
        Map<Long, Long> partitionConsumptionTime = Deencapsulation.getField(stream, "partitionConsumptionTime");
        Map<Long, Long> historicalPartitionTSO = Deencapsulation.getField(stream, "historicalPartitionTSO");

        Assertions.assertTrue(partitionOffset.containsKey(keptPartitionId));
        Assertions.assertTrue(partitionConsumptionTime.containsKey(keptPartitionId));
        Assertions.assertTrue(historicalPartitionTSO.containsKey(keptPartitionId));
        Assertions.assertEquals(!removedExpected, partitionOffset.containsKey(removedPartitionId));
        Assertions.assertEquals(!removedExpected, partitionConsumptionTime.containsKey(removedPartitionId));
        Assertions.assertEquals(!removedExpected, historicalPartitionTSO.containsKey(removedPartitionId));
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        Assertions.assertTrue(condition.getAsBoolean());
    }

    private static void updatePartitionOffset(
            OlapTableStream stream, long partitionId, long offset, long commitTimeMs) {
        stream.writeLock();
        try {
            stream.unprotectedUpdateStreamUpdate(new OlapTableStreamUpdate(
                    Collections.emptyMap(), Collections.singletonMap(partitionId, offset)), commitTimeMs);
        } finally {
            stream.writeUnlock();
        }
    }

    private static class StreamContext {
        private final OlapTable baseTable;
        private final OlapTableStream stream;

        private StreamContext(OlapTable baseTable, OlapTableStream stream) {
            this.baseTable = baseTable;
            this.stream = stream;
        }
    }
}
